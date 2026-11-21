"""锁等待追踪集成测试：真实 PostgreSQL（embedded），覆盖需求各条。"""
import os,sys,time,threading,json
os.environ.setdefault('DATABASE_URL','postgresql+psycopg://postgres@/operatortrace?host=/tmp/pgtest/data')
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ.setdefault('REDIS_URL','redis://localhost:1/0')
import psycopg
from src.config.database import Session
from src.models.trace import Trace
from src.models.lockwait import LockWait,LockWaitLink
from src.services import lockwaits
from src.services.lockwaits import public_waits
from src.worker import write_result
PG='postgresql://postgres@/operatortrace?host=/tmp/pgtest/data'
RESULT={'operators':[{'op':'scan','input_rows':0,'output_rows':3,'duration_ms':1.0,'detail':{},'checksum':'x','preview':[]}],'output':[],'output_rows':0,'checksum':'abc','row_visits':3,'duration_ms':1.0}
def new_trace(name):
 with Session() as s:
  t=Trace(name=name,plan_id=1,dataset_id=1,status='running',snapshot={'dataset':{},'plan':{}})
  s.add(t);s.commit();s.refresh(t);return t.id
def waits_of(tid):
 with Session() as s:return s.query(LockWait).filter_by(trace_id=tid).all()
def links_of(wid):
 with Session() as s:return s.query(LockWaitLink).filter_by(wait_id=wid).order_by(LockWaitLink.position,LockWaitLink.id).all()
def until(fn,timeout=8,step=0.1):
 t0=time.time()
 while time.time()-t0<timeout:
  v=fn()
  if v:return v
  time.sleep(step)
 raise AssertionError('until timeout')
class Holder:
 """持锁方：SELECT FOR UPDATE 行锁。"""
 def __init__(self,tid):
  self.c=psycopg.connect(PG);self.tid=tid
  self.pid=self.c.execute('SELECT pg_backend_pid()').fetchone()[0]
 def lock(self):self.c.execute('SELECT id FROM traces WHERE id=%s FOR UPDATE',(self.tid,))
 def release(self):self.c.commit()
 def close(self):self.c.close()
class ShareHolder:
 """SHARE 表锁持锁方：可多方同时持有，用于构造链改道。"""
 def __init__(self):
  self.c=psycopg.connect(PG)
  self.pid=self.c.execute('SELECT pg_backend_pid()').fetchone()[0]
 def lock(self):self.c.execute('LOCK TABLE traces IN SHARE MODE')
 def release(self):self.c.commit()
 def close(self):self.c.close()
def scenario_a():
 """确认链：等待期间证据复核一致，解除后 confirmed 链随剖面留存。"""
 tid=new_trace('locktest-a');h=Holder(tid);h.lock()
 th=threading.Thread(target=write_result,args=(tid,RESULT));t0=time.time();th.start()
 w=until(lambda:(lambda ws:ws[0] if ws else None)([w for w in waits_of(tid) if w.status=='waiting']))
 until(lambda:len(links_of(w.id))==2)
 time.sleep(1.8)  # 多轮复核 → confirmed
 h.release();th.join(timeout=15);assert not th.is_alive()
 with Session() as s:
  t=s.get(Trace,tid);assert t.status=='completed' and t.result['checksum']=='abc','结果照常落库'
 ws=waits_of(tid);assert len(ws)==1,'同一段持续等待只记一单'
 w=ws[0];assert w.status=='resolved' and w.duration_ms and w.duration_ms>=1500,(w.status,w.duration_ms)
 ls=links_of(w.id)
 assert [l.position for l in ls]==[0,1] and all(l.verdict=='confirmed' for l in ls),[(l.position,l.verdict) for l in ls]
 assert ls[1].backend_type=='client backend'
 with Session() as s:
  pub=public_waits(s,[tid])[tid][0]
  assert set(pub['links'][1])=={'position','backend_type','verdict','first_seen_at','last_seen_at'},'持锁方只显示来源类型'
  assert pub['links'][0]['pid']==w.blocked_pid
 h.close();print('A ok 确认链留存 + 持锁方脱敏')
def scenario_b():
 """并发解锁：SHARE 锁一方先释放，链改道，未核实的旧环节标未确认，新持锁方确认。"""
 tid=new_trace('locktest-b');h1,h2=ShareHolder(),ShareHolder()
 first,second=(h1,h2) if h1.pid<h2.pid else (h2,h1)  # collect_chain 取最小 pid
 first.lock();second.lock()
 th=threading.Thread(target=write_result,args=(tid,RESULT));th.start()
 w=until(lambda:(lambda ws:ws[0] if ws else None)([w for w in waits_of(tid) if w.status=='waiting']))
 until(lambda:len(links_of(w.id))==2)  # 第一轮：W→first（pending，尚未复核）
 first.release()                      # 并发解锁：writer 仍被 second 阻塞，链改道
 time.sleep(1.8)
 second.release();th.join(timeout=15);assert not th.is_alive()
 ws=waits_of(tid);assert len(ws)==1 and ws[0].status=='resolved'
 ls=links_of(ws[0].id);pos1=[l for l in ls if l.position==1]
 assert len(pos1)==2,[(l.pid,l.verdict) for l in pos1]
 assert pos1[0].pid==first.pid and pos1[0].verdict=='unconfirmed','证据对不上的链标作未确认'
 assert pos1[1].pid==second.pid and pos1[1].verdict=='confirmed'
 h1.close();h2.close();print('B ok 并发解锁改道 → 未确认/已确认并存')
def scenario_c():
 """采集失败可重试，且同一段持续等待不重复建单。"""
 tid=new_trace('locktest-c');h=Holder(tid);h.lock()
 orig=lockwaits.collect_chain;calls={'n':0}
 def flaky(s,pid):
  calls['n']+=1
  if calls['n']<=2:raise RuntimeError('采集探针故障')
  return orig(s,pid)
 lockwaits.collect_chain=flaky
 try:
  th=threading.Thread(target=write_result,args=(tid,RESULT));th.start()
  w=until(lambda:(lambda ws:ws[0] if ws and ws[0].collect_error else None)(waits_of(tid)))
  time.sleep(0.9)  # 第二次失败仍落在同一等待单
  mid=waits_of(tid);assert len(mid)==1 and mid[0].collect_attempts>=2,'失败重试不重复建单'
  until(lambda:len(links_of(w.id))==2)  # 探针恢复后链采到
  time.sleep(1.2)
  h.release();th.join(timeout=15);assert not th.is_alive()
 finally:
  lockwaits.collect_chain=orig
 ws=waits_of(tid);assert len(ws)==1,'整段等待只有一单'
 w=ws[0];assert w.status=='resolved' and '采集探针故障' in (w.collect_error or '')
 assert w.collect_attempts>=3
 assert any(l.verdict=='confirmed' for l in links_of(w.id))
 h.close();print('C ok 采集失败重试 + 等待单去重')
def scenario_d():
 """无锁阻塞：普通结果照常落库，不产生等待单。"""
 tid=new_trace('locktest-d');t0=time.time();write_result(tid,RESULT);dt=time.time()-t0
 assert dt<2,dt
 with Session() as s:
  t=s.get(Trace,tid);assert t.status=='completed' and t.result['checksum']=='abc'
 assert waits_of(tid)==[]
 print('D ok 普通结果照常落库')
def scenario_u():
 """证据核对规则单元测试。"""
 L=lambda **kw:LockWaitLink(**kw)
 base=L(position=1,pid=42,evidence={'xid':'100','xact_start':'1.7'})
 assert lockwaits._match(base,{'pid':42,'xid':'100','xact_start':'1.7'})
 assert not lockwaits._match(base,{'pid':42,'xid':'200','xact_start':'1.7'}), '连接复用：同事务开始时间不同xid'
 assert not lockwaits._match(base,{'pid':42,'xid':'100','xact_start':'9.9'}), '连接复用：同pid新事务'
 assert not lockwaits._match(base,{'pid':43,'xid':'100','xact_start':'1.7'})
 assert not lockwaits._match(base,{'pid':42,'xid':None,'xact_start':None}), '无身份证据不能确认'
 w=L(position=0,pid=7,evidence={'xid':None,'xact_start':None})
 assert lockwaits._match(w,{'pid':7,'xid':'1','xact_start':'2'}), 'position 0 是worker自己，pid一致即可'
 print('U ok 证据核对规则')
def scenario_e():
 """API：剖面数据带锁等待留证，持锁方不下发证据/语句/采样值。"""
 from fastapi.testclient import TestClient
 from src.main import app
 c=TestClient(app)
 r=c.post('/auth/login',json={'email':'viewer@operator.local','password':'Trace2026!'})
 assert r.status_code==200,r.text
 auth={'Authorization':'Bearer '+r.json()['token']}
 lst=c.get('/traces',headers=auth).json()
 item=[x for x in lst if x['name']=='locktest-a'][0]
 assert item['lock_waits'] and item['lock_waits'][0]['status']=='resolved'
 det=c.get(f"/traces/{item['id']}",headers=auth).json()
 w=det['lock_waits'][0]
 assert w['duration_ms'] is not None and w['links']
 for l in w['links']:
  assert 'evidence' not in l and 'xid' not in json.dumps(l) and 'query' not in l
  if l['position']>0:assert set(l)=={'position','backend_type','verdict','first_seen_at','last_seen_at'},l
 exp=c.get(f"/traces/{item['id']}/export",headers=auth)
 assert exp.status_code==200 and 'lock_waits' in exp.text,'确认链随导出留存'
 print('E ok API 留证与脱敏')
if __name__=='__main__':
 scenario_u();scenario_a();scenario_b();scenario_c();scenario_d();scenario_e()
 print('ALL LOCKWAIT TESTS PASSED')
