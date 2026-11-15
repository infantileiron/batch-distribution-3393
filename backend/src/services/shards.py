import hashlib,json,uuid,csv,io
from fastapi import HTTPException
from sqlalchemy import select
from src.models import DistributionStudy,DistributionRun,ShardJob
from src.services.analysis import finalize_result
from src.utils.serialization import serialize
from src.utils.time import now

SHARD_FORMAT='qichuan-offline-shard/1'
RUNNER_NAME='qichuan_offline_runner.py'
# Checksum covers exactly these transport-stable keys. Optional transport fields
# (note/runner/raw batches/checksum itself) are excluded so both sides hash identical bytes.
SHARD_CHECKSUM_KEYS=('format','package_id','run_id','study_id','study_name','snapshot_digest','B','seed','scaling','bandwidth','sigma','sample_sizes','sample_ids','interval','permutations','energy_statistics','mmd_statistics')

def canonical(payload):
 return json.dumps(payload,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()

def shard_canonical(payload):
 return canonical({key:payload[key] for key in SHARD_CHECKSUM_KEYS})

def sha256_hex(payload):
 return hashlib.sha256(shard_canonical(payload)).hexdigest()

def snapshot_digest(snapshot):
 return hashlib.sha256(json.dumps(snapshot,sort_keys=True,ensure_ascii=False).encode()).hexdigest()

def get_study(db,study_id):
 study=db.get(DistributionStudy,study_id)
 if not study:raise HTTPException(404,'分布研究不存在')
 return study

def build_intervals(B,shard_count):
 if isinstance(shard_count,bool) or not isinstance(shard_count,int) or not 1<=shard_count<=B:
  raise HTTPException(422,'分片数须为1至B之间的整数')
 base,extra=divmod(B,shard_count)
 intervals=[];start=0
 for i in range(shard_count):
  count=base+(1 if i<extra else 0)
  intervals.append({'index':i,'start':start,'end':start+count,'count':count});start+=count
 return intervals

def create(db,payload,user):
 study=get_study(db,payload.study_id);policy=study.snapshot['policy'];B=policy['permutations']
 intervals=build_intervals(B,payload.shard_count)
 run=DistributionRun(id=str(uuid.uuid4()),study_id=study.id,status='sharded',result={},error='',created_at=now())
 job=ShardJob(id=str(uuid.uuid4()),study_id=study.id,run_id=run.id,status='collecting',shard_count=len(intervals),shard_size=intervals[0]['count'],intervals=intervals,received=[],shards={},result_checksum='',error='',created_at=now(),updated_at=now())
 db.add(run);db.add(job);db.commit()
 return serialize(job)

def package_payload(study,job):
 data=study.snapshot['dataset'];policy=study.snapshot['policy']
 return {'format':SHARD_FORMAT,'package_id':job.id,'run_id':job.run_id,'study_id':study.id,'study_name':study.name,'snapshot_digest':snapshot_digest(study.snapshot),'B':policy['permutations'],'seed':policy['seed'],'scaling':policy['scaling'],'bandwidth':policy['bandwidth'],'sigma':policy['sigma'],'sample_sizes':[len(data['batch_a']),len(data['batch_b'])],'sample_ids':[r['sample_id'] for r in data['batch_a']]+[r['sample_id'] for r in data['batch_b']],'batch_a':data['batch_a'],'batch_b':data['batch_b'],'intervals':job.intervals,'runner':RUNNER_NAME,'note':'离线实验室仅按编号区间复放固定seed排列流；缩放与核宽由全体样本一次估计并随包锁定，禁止重估。'}

def package(db,id):
 job=db.get(ShardJob,id)
 if not job:raise HTTPException(404,'离线计算包不存在')
 return package_payload(get_study(db,job.study_id),job)

def require_job(db,id):
 job=db.get(ShardJob,id)
 if not job:raise HTTPException(404,'离线计算包不存在')
 return job

def import_shard(db,id,payload):
 job=require_job(db,id)
 if job.status=='completed':raise HTTPException(409,'计算包已收齐发布，拒绝再导入')
 study=get_study(db,job.study_id);policy=study.snapshot['policy'];B=policy['permutations']
 # --- structural envelope validation (extra keys forbidden by pydantic) ---
 if payload.get('format')!=SHARD_FORMAT:raise HTTPException(422,'分片格式标识不匹配')
 if payload.get('package_id')!=job.id or payload.get('study_id')!=study.id or payload.get('run_id')!=job.run_id:
  raise HTTPException(422,'分片不属于此计算包（错研究/错运行，拒绝）')
 if payload.get('B')!=B or payload.get('seed')!=policy['seed'] or payload.get('scaling')!=policy['scaling'] or payload.get('bandwidth')!=policy['bandwidth'] or payload.get('sigma')!=policy['sigma']:
  raise HTTPException(422,'分片口径与冻结研究不一致，拒绝')
 if payload.get('snapshot_digest')!=snapshot_digest(study.snapshot):raise HTTPException(422,'快照校验和不一致（错研究数据），拒绝')
 interval=payload.get('interval') or {};index=interval.get('index');start=interval.get('start');end=interval.get('end')
 expected=next((iv for iv in job.intervals if iv['index']==index),None)
 if expected is None:raise HTTPException(422,'分片编号超出计算包编号区间，拒绝')
 if not isinstance(start,int) or not isinstance(end,int) or start!=expected['start'] or end!=expected['end'] or end<=start:
  raise HTTPException(422,'分片区间与分发清单重叠或不一致，拒绝')
 count=end-start
 orders=payload.get('permutations');energy_values=payload.get('energy_statistics');mmd_values=payload.get('mmd_statistics')
 if not isinstance(orders,list) or not isinstance(energy_values,list) or not isinstance(mmd_values,list):raise HTTPException(422,'缺少置换索引或统计序列')
 if not (len(orders)==len(energy_values)==len(mmd_values)==count):raise HTTPException(422,'分片长度与编号区间不一致，拒绝')
 n,m=len(study.snapshot['dataset']['batch_a']),len(study.snapshot['dataset']['batch_b']);N=n+m
 for order in orders:
  if not isinstance(order,list) or len(order)!=N or sorted(order)!=list(range(N)):raise HTTPException(422,'置换索引不是0..N-1的完整重排，拒绝')
 for values in (energy_values,mmd_values):
  if any(not isinstance(v,(int,float)) or isinstance(v,bool) for v in values):raise HTTPException(422,'统计值须为有限数值')
 # --- payload checksum: same-number different-value is rejected, retries are idempotent ---
 digest=sha256_hex(payload)
 if payload.get('checksum')!=digest:raise HTTPException(422,'分片校验和不一致（同编号异值或传输损坏），拒绝')
 received=list(job.received);stored=dict(job.shards)
 if index in received:
  if stored[str(index)]['checksum']==digest:
   return {'status':'duplicate','message':'分片已收讫，重复导入不重复计数','job':serialize(job)}
  message=f'分片{index}同编号异值，拒绝导入'
  job.error=message;job.updated_at=now();db.commit()
  raise HTTPException(409,message)
 # --- overlap guard: interval must exactly match the manifest ---
 stored[str(index)]={'checksum':digest,'start':start,'end':end,'count':count,'permutations':orders,'energy_statistics':energy_values,'mmd_statistics':mmd_values}
 received.append(index);received.sort()
 job.received=received;job.shards=stored;job.error='';job.updated_at=now()
 missing=[iv['index'] for iv in job.intervals if iv['index'] not in received]
 if not missing:
  try:_publish(db,job,study)
  except HTTPException:raise
  except ValueError as e:
   job.error=str(e);job.updated_at=now();db.commit()
   raise HTTPException(409,str(e))
 db.commit()
 return {'status':job.status,'missing':missing,'job':serialize(job)}

def _publish(db,job,study):
 '''All numbered intervals collected: merge once, verify against same-seed monolithic run, then publish.'''
 ordered=[job.shards[str(iv['index'])] for iv in job.intervals]
 # contiguous-coverage guard (interval equality already validated; double-check no holes/overlap)
 cursor=0
 for piece,iv in zip(ordered,job.intervals):
  if piece['start']!=cursor or piece['end']!=iv['end']:raise HTTPException(409,'分片区间存在缺口或重叠，拒绝发布')
  cursor=piece['end']
 if cursor!=study.snapshot['policy']['permutations']:raise HTTPException(409,'分片未覆盖完整置换预算，拒绝发布')
 result=finalize_result(study.snapshot,ordered,job.id)
 run=db.get(DistributionRun,job.run_id)
 if not run:raise HTTPException(404,'关联运行不存在')
 run.result=result;run.status='completed';run.error=''
 job.status='completed';job.result_checksum=result['checksum'];job.updated_at=now()

def coverage(db,id):
 job=require_job(db,id)
 received=set(job.received)
 return {'package_id':job.id,'run_id':job.run_id,'status':job.status,'shard_count':job.shard_count,'received_count':len(received),'received':sorted(received),'missing':[iv['index'] for iv in job.intervals if iv['index'] not in received],'intervals':job.intervals,'result_checksum':job.result_checksum,'error':job.error,'updated_at':job.updated_at}

def listing(db):
 return [serialize(x) for x in db.scalars(select(ShardJob).order_by(ShardJob.created_at.desc())).all()]

def detail(db,id):return serialize(require_job(db,id))

def detail_csv(db,id):
 job=require_job(db,id);received=set(job.received);buf=io.StringIO()
 writer=csv.DictWriter(buf,['package_id','run_id','status','shard_index','start','end','count','received','shard_checksum','result_checksum'])
 writer.writeheader()
 for iv in job.intervals:
  piece=job.shards.get(str(iv['index']))
  writer.writerow({'package_id':job.id,'run_id':job.run_id,'status':job.status,'shard_index':iv['index'],'start':iv['start'],'end':iv['end'],'count':iv['count'],'received':iv['index'] in received,'shard_checksum':piece['checksum'] if piece else '','result_checksum':job.result_checksum})
 return chr(0xfeff)+buf.getvalue()
