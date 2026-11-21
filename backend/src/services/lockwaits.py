"""结果落库锁等待追踪。

worker 提交结果写入时启动 LockWatchdog 看门狗：用独立连接观察本次写入的
backend pid 是否等待锁；等待期间沿 pg_blocking_pids 从本次写入追到最后一个
持锁事务，并把链环节存入 lock_wait_links。下一采集轮次用同一等待单复核证据
（pid + backend_xid + xact_start）：对得上标 confirmed，并发解锁或连接复用
导致证据对不上的标 unconfirmed。等待解除后关闭等待单，confirmed 链随剖面留存。
持锁方对外只暴露 backend_type，不下发语句、采样值或服务器侧证据。
"""
import logging,threading
from sqlalchemy import text
from src.config.database import Session
from src.models.lockwait import LockWait,LockWaitLink
GRACE=0.4      # 写入开始后首次观测延迟（秒）
INTERVAL=0.8   # 等待期间采集/复核间隔（秒）
MAX_DEPTH=8    # 持锁链最大深度，防循环
def _db_now(s):return s.execute(text('SELECT clock_timestamp()::timestamp')).scalar()
def _is_blocked(s,pid):return s.execute(text("SELECT 1 FROM pg_stat_activity WHERE pid=:p AND wait_event_type='Lock'"),{'p':pid}).fetchone() is not None
def collect_chain(s,pid):
 """从被阻塞的 pid 沿 pg_blocking_pids 追到链尾；同一层多个持锁方取最小 pid 保证确定性。"""
 chain=[];seen=set();current=pid
 for _ in range(MAX_DEPTH):
  row=s.execute(text("SELECT backend_type,backend_xid::text,extract(epoch from xact_start)::text,wait_event FROM pg_stat_activity WHERE pid=:p"),{'p':current}).fetchone()
  chain.append({'pid':current,'backend_type':(row[0] if row and row[0] else '未知来源'),'xid':row[1] if row else None,'xact_start':row[2] if row else None,'wait_event':row[3] if row else None})
  seen.add(current)
  nxt=[b for b in s.execute(text('SELECT unnest(pg_blocking_pids(:p))'),{'p':current}).scalars().all() if b not in seen]
  if not nxt:break
  current=min(nxt)
 return chain
def _evidence(node):return {'xid':node['xid'],'xact_start':node['xact_start'],'wait_event':node['wait_event']}
def _match(link,node):
 """核对链环节证据：position 0 是 worker 自己的写入会话，pid 一致即可；
 持锁方要求 pid、事务id、事务开始时间全部一致，且至少有一项身份证据非空，
 否则视为并发解锁或连接复用，证据对不上。"""
 if link.pid!=node['pid']:return False
 if link.position==0:return True
 if not (node.get('xid') or node.get('xact_start')):return False
 ev=link.evidence or {}
 return ev.get('xid')==node.get('xid') and ev.get('xact_start')==node.get('xact_start')
def _reconcile(s,wait,fresh,now):
 """用新一轮链快照复核链环节。confirmed 为终态（核实过即随剖面留存）；
 pending 环节证据对得上标 confirmed，对不上（并发解锁/连接复用/等待结束）标 unconfirmed。"""
 links=s.query(LockWaitLink).filter(LockWaitLink.wait_id==wait.id).order_by(LockWaitLink.position,LockWaitLink.id).all()
 by_pos={}
 for l in links:by_pos.setdefault(l.position,[]).append(l)
 for pos,node in enumerate(fresh):
  cur=next((l for l in reversed(by_pos.get(pos,[])) if l.verdict=='pending'),None)
  if cur and _match(cur,node):
   cur.verdict='confirmed';cur.last_seen_at=now;cur.evidence=_evidence(node)
  else:
   if cur:cur.verdict='unconfirmed';cur.last_seen_at=now
   if not any(l.verdict=='confirmed' and _match(l,node) for l in by_pos.get(pos,[])):
    s.add(LockWaitLink(wait_id=wait.id,position=pos,pid=node['pid'],backend_type=node['backend_type'],verdict='pending',evidence=_evidence(node),first_seen_at=now,last_seen_at=now))
 for pos,ls in by_pos.items():
  if pos>=len(fresh):
   for l in ls:
    if l.verdict=='pending':l.verdict='unconfirmed';l.last_seen_at=now
class LockWatchdog(threading.Thread):
 """结果写入期间的锁等待看门狗；只观测，不触碰写入会话，异常绝不上抛。"""
 def __init__(self,trace_id,writer_pid,began,phase='result-write'):
  super().__init__(daemon=True)
  self.trace_id=trace_id;self.writer_pid=writer_pid;self.began=began;self.phase=phase
  self._done=threading.Event();self._wait_id=None
 def run(self):
  try:
   if self._done.wait(GRACE):return
   while not self._done.is_set():
    try:self.tick()
    except Exception:logging.exception('lock watchdog tick')
    self._done.wait(INTERVAL)
  except Exception:logging.exception('lock watchdog')
 def finish(self):
  """写入结束（成功或失败）后调用：停线程，并把仍未关闭的等待单做最后关闭。"""
  self._done.set();self.join(timeout=10)
  try:
   with Session() as s:
    wait=self._find_wait(s)
    if wait is not None:self._finalize(s,wait,_db_now(s))
  except Exception:logging.exception('lock watchdog finish')
 def _find_wait(self,s):
  """同一段持续等待只认一张等待单：优先按记忆 id，再按 (trace,pid,phase,waiting) 去重。"""
  if self._wait_id is not None:
   w=s.get(LockWait,self._wait_id)
   if w is not None and w.status=='waiting':return w
   self._wait_id=None
  w=s.query(LockWait).filter(LockWait.trace_id==self.trace_id,LockWait.blocked_pid==self.writer_pid,LockWait.phase==self.phase,LockWait.status=='waiting').order_by(LockWait.id.desc()).first()
  self._wait_id=w.id if w else None
  return w
 def _open(self,s,now):
  w=LockWait(trace_id=self.trace_id,phase=self.phase,blocked_pid=self.writer_pid,status='waiting',started_at=self.began or now,collect_attempts=0)
  s.add(w);s.flush();self._wait_id=w.id
  return w
 def tick(self):
  with Session() as s:
   now=_db_now(s)
   wait=self._find_wait(s)
   if not _is_blocked(s,self.writer_pid):
    if wait is not None:self._finalize(s,wait,now)
    return
   try:
    fresh=collect_chain(s,self.writer_pid)
   except Exception as exc:
    # 采集失败：记录错误并提交，下一轮自动重试；同一段等待不重复建单
    logging.exception('lock chain collect failed')
    s.rollback()
    wait=self._find_wait(s) or self._open(s,now)
    wait.collect_attempts+=1;wait.collect_error=str(exc)[:500]
    s.commit();return
   if wait is None:wait=self._open(s,now)
   wait.collect_attempts+=1
   _reconcile(s,wait,fresh,now)
   s.commit()
 def _finalize(self,s,wait,now):
  """等待解除：用最后一轮快照复核后关闭等待单；采不到快照则把未核实的环节标未确认。"""
  try:
   fresh=collect_chain(s,self.writer_pid)
   _reconcile(s,wait,fresh,now)
  except Exception:
   logging.exception('final collect failed')
   s.rollback();wait=s.get(LockWait,wait.id)
   for l in s.query(LockWaitLink).filter(LockWaitLink.wait_id==wait.id,LockWaitLink.verdict=='pending'):
    l.verdict='unconfirmed';l.last_seen_at=now
  wait.status='resolved';wait.resolved_at=now
  wait.duration_ms=max(0,int((now-wait.started_at).total_seconds()*1000))
  s.commit();self._wait_id=None
def _iso(v):return v.isoformat() if v else None
def _public_link(l):
 """对外视图：持锁方（position>=1）只给来源类型与核对结论，不给 pid、事务证据、语句或采样值。"""
 d={'position':l.position,'backend_type':l.backend_type,'verdict':l.verdict,'first_seen_at':_iso(l.first_seen_at),'last_seen_at':_iso(l.last_seen_at)}
 if l.position==0:
  d['pid']=l.pid;d['wait_event']=(l.evidence or {}).get('wait_event')
 return d
def public_waits(s,trace_ids):
 """按 trace_id 归组的锁等待公开视图，供剖面页随轨迹下发。"""
 if not trace_ids:return {}
 waits=s.query(LockWait).filter(LockWait.trace_id.in_(trace_ids)).order_by(LockWait.id).all()
 if not waits:return {}
 links=s.query(LockWaitLink).filter(LockWaitLink.wait_id.in_([w.id for w in waits])).order_by(LockWaitLink.wait_id,LockWaitLink.position,LockWaitLink.id).all()
 by_wait={}
 for l in links:by_wait.setdefault(l.wait_id,[]).append(l)
 out={}
 for w in waits:
  out.setdefault(w.trace_id,[]).append({'id':w.id,'phase':w.phase,'status':w.status,'started_at':_iso(w.started_at),'resolved_at':_iso(w.resolved_at),'duration_ms':w.duration_ms,'collect_attempts':w.collect_attempts,'collect_error':w.collect_error,'links':[_public_link(l) for l in by_wait.get(w.id,[])]})
 return out
