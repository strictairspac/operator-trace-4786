from sqlalchemy import Column,Integer,String,JSON,DateTime,func,ForeignKey
from src.config.database import Base
class LockWait(Base):
 __tablename__='lock_waits'
 id=Column(Integer,primary_key=True)
 # 故意不加 traces 外键：FK 插入检查要对父行加 KEY SHARE 锁，
 # 会与结果写入等待的行锁冲突，把看门狗自己也拖进同一段锁等待。
 trace_id=Column(Integer,nullable=False)
 phase=Column(String(40),nullable=False,default='result-write')
 blocked_pid=Column(Integer,nullable=False)
 status=Column(String(20),nullable=False,default='waiting')
 started_at=Column(DateTime,nullable=False)
 resolved_at=Column(DateTime,nullable=True)
 duration_ms=Column(Integer,nullable=True)
 collect_attempts=Column(Integer,nullable=False,default=0)
 collect_error=Column(String(500),nullable=True)
 created_at=Column(DateTime,server_default=func.now())
class LockWaitLink(Base):
 __tablename__='lock_wait_links'
 id=Column(Integer,primary_key=True)
 wait_id=Column(Integer,ForeignKey('lock_waits.id'),nullable=False)
 position=Column(Integer,nullable=False)
 pid=Column(Integer,nullable=False)
 backend_type=Column(String(60),nullable=False,default='')
 verdict=Column(String(20),nullable=False,default='pending')
 evidence=Column(JSON,nullable=False,default=dict)
 first_seen_at=Column(DateTime,nullable=False)
 last_seen_at=Column(DateTime,nullable=False)
