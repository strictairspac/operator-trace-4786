import redis
from fastapi import HTTPException
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.dataset import Dataset
from src.models.plan import Plan
from src.models.trace import Trace
from src.services.lockwaits import public_waits
from src.utils.serialization import record
queue=redis.Redis.from_url(REDIS_URL)
def _attach(s,recs):
 waits=public_waits(s,[r['id'] for r in recs])
 for r in recs:r['lock_waits']=waits.get(r['id'],[])
 return recs
def list_items():
 with Session() as s:return _attach(s,[record(r) for r in s.query(Trace).order_by(Trace.id.desc()).limit(100).all()])
def detail(item_id):
 with Session() as s:
  row=s.get(Trace,item_id)
  if not row:raise HTTPException(404,'未找到执行轨迹')
  return _attach(s,[record(row)])[0]
def create(data):
 with Session() as s:
  p=s.get(Plan,data.plan_id)
  if not p:raise HTTPException(404,'未找到算子计划')
  d=s.get(Dataset,p.dataset_id)
  row=Trace(name=data.name,plan_id=p.id,dataset_id=d.id,status='queued',snapshot={'dataset':record(d),'plan':record(p)})
  s.add(row);s.commit();s.refresh(row);result=record(row)
 try:queue.rpush('operator-traces',result['id'])
 except redis.RedisError:
  with Session() as s:r=s.get(Trace,result['id']);r.status='failed';r.error='队列不可用，请创建新任务';s.commit()
  raise HTTPException(503,'队列不可用，请稍后重试')
 return result
