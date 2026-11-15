#!/usr/bin/env python3
"""绮川印染 · 离线置换分片计算工具 (qichuan-offline-runner)

用法:
  python qichuan_offline_runner.py <计算包.json> [--index N] [--outdir 目录]

- 仅依赖 Python3.11 标准库与 NumPy(Float64)。
- 缩放与核宽按计算包内全体样本一次估计并锁定，置换期间不重估任何标签依赖参数。
- 固定 seed 的排列流通过确定性复放定位到分片编号区间，乱序交回也能与整批运行逐位一致。
- 每个分片输出独立 JSON，携带与计算包一致的快照校验和及本分片内容校验和。
"""
import argparse,hashlib,json,math,os,random,sys
import numpy as np

FORMAT='qichuan-offline-shard/1'
# Must stay identical to src.services.shards.SHARD_CHECKSUM_KEYS (offline runner has no project imports).
CHECKSUM_KEYS=('format','package_id','run_id','study_id','study_name','snapshot_digest','B','seed','scaling','bandwidth','sigma','sample_sizes','sample_ids','interval','permutations','energy_statistics','mmd_statistics')

def samples(rows):
 arr=np.asarray([[r['l'],r['a'],r['b']] for r in rows],dtype=np.float64)
 if arr.ndim!=2 or arr.shape[1]<1 or len(arr)<2:raise ValueError('每组至少2个等维样本')
 if not np.isfinite(arr).all():raise ValueError('样本须为有限数值')
 return arr

def pairwise(points):
 d=points[:,None,:]-points[None,:,:]
 sq=np.sum(d*d,axis=2,dtype=np.float64)
 if not np.isfinite(sq).all():raise ValueError('距离矩阵溢出')
 return np.sqrt(sq)

def energy_score(distance,ia,ib):
 n,m=len(ia),len(ib)
 aa=math.fsum(distance[np.ix_(ia,ia)].flat)/(n*n)
 bb=math.fsum(distance[np.ix_(ib,ib)].flat)/(m*m)
 ab=math.fsum(distance[np.ix_(ia,ib)].flat)/(n*m)
 return 2*ab-(aa+bb)

def kernel_matrix(distance,sigma):
 if not math.isfinite(sigma) or sigma<=0:raise ValueError('sigma必须为正有限值')
 ratio=distance/sigma
 with np.errstate(over='ignore'):return np.exp(-.5*ratio*ratio)

def mmd_score(kernel,ia,ib):
 n,m=len(ia),len(ib)
 aa=(math.fsum(kernel[np.ix_(ia,ia)].flat)-n)/(n*(n-1))
 bb=(math.fsum(kernel[np.ix_(ib,ib)].flat)-m)/(m*(m-1))
 ab=math.fsum(kernel[np.ix_(ia,ib)].flat)/(n*m)
 return (aa+bb)-2*ab

def prepare(pkg):
 a,b=samples(pkg['batch_a']),samples(pkg['batch_b'])
 pooled=np.vstack([a,b]);n,m=len(a),len(b)
 if pkg['scaling']=='pooled_std':
  center=np.mean(pooled,axis=0);sd=np.std(pooled,axis=0,ddof=0)
  if not np.isfinite(center).all() or not np.isfinite(sd).all():raise ValueError('pooled scaling overflow')
  scales=np.where(sd==0,1.,sd);pooled=(pooled-center)/scales
 elif pkg['scaling']!='none':raise ValueError('unknown scaling')
 distance=pairwise(pooled)
 if pkg['bandwidth']=='pooled_median':
  median=float(np.median(distance[np.triu_indices(n+m,1)]))
  if median<=0 or not math.isfinite(median):raise ValueError('pooled median为0，自动核宽不可估；须改用固定正sigma')
  sigma=median
 elif pkg['bandwidth']=='fixed':
  sigma=pkg['sigma']
  if sigma is None or not math.isfinite(sigma) or sigma<=0:raise ValueError('fixed sigma must be positive')
 else:raise ValueError('unknown bandwidth policy')
 if pkg['sigma'] is not None and pkg['bandwidth']=='fixed' and float(sigma)!=float(pkg['sigma']):raise ValueError('固定sigma与计算包不一致')
 kernel=kernel_matrix(distance,sigma)
 return n,m,n+m,distance,kernel

def rng_at(seed,start,N):
 rng=random.Random(seed)
 for _ in range(start):
  order=list(range(N));rng.shuffle(order)
 return rng

def canonical(payload):
 return json.dumps({k:payload[k] for k in CHECKSUM_KEYS},sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()

def compute(pkg,interval):
 n,m,N,distance,kernel=prepare(pkg);start,end=interval['start'],interval['end'];count=end-start
 if start<0 or end>pkg['B'] or end<=start:raise ValueError('区间越界')
 rng=rng_at(pkg['seed'],start,N);orders=[];energy_null=[];mmd_null=[]
 for _ in range(count):
  order=list(range(N));rng.shuffle(order);pa=sorted(order[:n]);pb=sorted(order[n:]);orders.append(order)
  energy_null.append(energy_score(distance,pa,pb));mmd_null.append(mmd_score(kernel,pa,pb))
 return orders,energy_null,mmd_null

def build_shard(pkg,interval):
 orders,energy_null,mmd_null=compute(pkg,interval)
 shard={k:pkg[k] for k in ('format','package_id','run_id','study_id','study_name','snapshot_digest','B','seed','scaling','bandwidth','sigma','sample_sizes','sample_ids')}
 shard.update({'interval':interval,'permutations':orders,'energy_statistics':energy_null,'mmd_statistics':mmd_null,'runner':os.path.basename(__file__),'note':'离线分片：固定seed复放，缩放/核宽随计算包按全体样本锁定'})
 shard['checksum']=hashlib.sha256(canonical(shard)).hexdigest()
 return shard

def main():
 ap=argparse.ArgumentParser(description='离线置换分片计算')
 ap.add_argument('package');ap.add_argument('--index',type=int,default=None,help='只计算指定编号；缺省计算全部编号')
 ap.add_argument('--outdir',default='.');args=ap.parse_args()
 with open(args.package,encoding='utf-8') as f:pkg=json.load(f)
 if pkg.get('format')!=FORMAT:raise SystemExit('计算包格式不兼容: '+str(pkg.get('format')))
 indexes=[iv['index'] for iv in pkg['intervals']] if args.index is None else [args.index]
 os.makedirs(args.outdir,exist_ok=True)
 for index in indexes:
  interval=next((iv for iv in pkg['intervals'] if iv['index']==index),None)
  if interval is None:raise SystemExit(f'编号{index}不在计算包区间清单内')
  shard=build_shard(pkg,interval)
  out=os.path.join(args.outdir,f"qichuan-shard-{pkg['package_id'][:8]}-{index:03d}.json")
  with open(out,'w',encoding='utf-8') as f:json.dump(shard,f,ensure_ascii=False,indent=1)
  print(f"wrote {out} interval=[{interval['start']},{interval['end']}) checksum={shard['checksum'][:16]}")

if __name__=='__main__':main()
