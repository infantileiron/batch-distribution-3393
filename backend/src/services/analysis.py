import hashlib,json,statistics
from src.services.permutation import permutation_test,prepare_context,assemble

def _summaries(a,b):
 summaries=[]
 for label,rows in (('A',a),('B',b)):
  summaries.append({'batch':label,'n':len(rows),'mean':[statistics.fmean(x[j] for x in rows) for j in range(3)],'population_sd':[statistics.pstdev(x[j] for x in rows) for j in range(3)]})
 return summaries

ASSUMPTIONS=['Energy V使用欧氏距离，组内均值包含对角；MMD U使用RBF核，组内排对角并按n(n-1)归一，原始U值可为负，不截0。','缩放仅在合并样本上估计总体均值/SD；自动sigma为合并样本全部非对角欧氏距离中位数，包含零距离，仅估计一次。固定sigma也在置换前锁定。','合并样本SD=0维显式以scale=1保留，中心化后为0；pooled median=0则自动核宽失败，须改明确固定正sigma，不静默加epsilon或跳过零值。','两个统计共享同一seed排列，每次保持两批样本数；p=(1+置换统计>=观测数的次数)/(B+1)。原始统计、全部排列索引和两条置换分布保留。','独立、可交换样本假设；成对、时序或同缸相关读数可能不适用。两个检验的p未作多重校正，分别复核，不合成为自动迁移结论。','p不是迁移概率，有限随机排列有分辨率与Monte Carlo变动；模拟L*a*b*分布变化不是颜色质量或工艺因果判定。']

def _rows(data):
 return [[r[k] for k in ('l','a','b')] for r in data['batch_a']],[[r[k] for k in ('l','a','b')] for r in data['batch_b']]

def _envelope(result,snapshot,extra):
 data=snapshot['dataset']
 return {**result,'raw_a':data['batch_a'],'raw_b':data['batch_b'],'summaries':_summaries(*_rows(data)),'policy':snapshot['policy'],'checksum':hashlib.sha256(json.dumps(snapshot,sort_keys=True,ensure_ascii=False).encode()).hexdigest(),'assumptions':ASSUMPTIONS,**extra}

def analyze(snapshot):
 data=snapshot['dataset'];policy=snapshot['policy'];a,b=_rows(data)
 result=permutation_test(a,b,policy['permutations'],policy['seed'],policy['scaling'],policy['bandwidth'],policy['sigma'])
 return _envelope(result,snapshot,{'assembly':'monolithic'})

def finalize_result(snapshot,ordered_shards,shard_job_id=''):
 '''Merge offline shard outputs. Context is rebuilt from the frozen snapshot so scaling
 and kernel width stay fixed over the full sample; merged orders/statistics must equal
 a same-seed monolithic run (verified by callers/tests).'''
 data=snapshot['dataset'];policy=snapshot['policy'];a,b=_rows(data)
 context=prepare_context(a,b,policy['scaling'],policy['bandwidth'],policy['sigma'])
 result=assemble(policy['permutations'],policy['seed'],context,ordered_shards)
 envelope=_envelope(result,snapshot,{'assembly':'offline_shards','shard_job_id':shard_job_id,'shard_count':len(ordered_shards)})
 # same-seed monolithic reference: orders, statistics, extremes and p-values must coincide
 reference=analyze(snapshot)
 for key in ('permutations',):
  if envelope[key]!=reference[key]:raise ValueError('合并排列与同种子整批运行不一致，拒绝发布')
 for method in ('energy','mmd'):
  for field in ('statistic','extreme_count','denominator','pvalue','permutation_statistics'):
   if envelope[method][field]!=reference[method][field]:raise ValueError(f'合并{method}的{field}与同种子整批运行不一致，拒绝发布')
 if envelope['transform']!=reference['transform']:raise ValueError('合并变换参数与同种子整批运行不一致，拒绝发布')
 return envelope
