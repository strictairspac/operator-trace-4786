import time,logging,redis
from pathlib import Path
from sqlalchemy import text
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.dataset import Dataset
from src.models.plan import Plan
from src.models.trace import Trace
from src.services.operators import execute
from src.services.audit import log
from src.services.lockwaits import LockWatchdog
q=redis.Redis.from_url(REDIS_URL)
def write_result(run_id,result):
 """结果落库：提交期间由看门狗观测锁等待；无阻塞时行为与原先一致。"""
 with Session() as s:
  r=s.get(Trace,run_id)
  pid,began=s.execute(text('SELECT pg_backend_pid(),clock_timestamp()::timestamp')).one()
  r.result=result;r.status='completed'
  dog=LockWatchdog(run_id,pid,began)
  dog.start()
  try:s.commit()
  finally:dog.finish()
def run_job(run_id):
 with Session() as s:
  row=s.get(Trace,run_id)
  if not row or row.status!='queued':return
  row.status='running';snapshot=row.snapshot;s.commit()
 try:
  result=execute(snapshot['dataset'],snapshot['plan']['steps'])
  write_result(run_id,result)
  log('worker','trace.completed',str(run_id))
 except Exception as exc:
  logging.exception('trace failed')
  with Session() as s:r=s.get(Trace,run_id);r.status='failed';r.error=str(exc)[:1000];s.commit()
def main():
 while True:
  Path('/tmp/worker-heartbeat').touch()
  try:
   item=q.blpop('operator-traces',timeout=2)
   if not item:continue
   run_job(int(item[1]))
  except Exception:logging.exception('worker loop');time.sleep(2)
if __name__=='__main__':main()
