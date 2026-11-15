import math,random,numpy as np
from src.services.distances import samples,pairwise
from src.services.energy import score as energy_score
from src.services.mmd import score as mmd_score,kernel_matrix

def prepare_context(a,b,scaling='pooled_std',bandwidth='pooled_median',sigma=None):
 '''Label-independent preparation shared by monolithic and offline-shard runs.
 Scaling and kernel width are estimated once over the full pooled sample and frozen.'''
 a,b=samples(a,b)
 pooled=np.vstack([a,b]);n,m=len(a),len(b);N=n+m
 center=np.zeros(pooled.shape[1]);scales=np.ones(pooled.shape[1]);constant=[]
 if scaling=='pooled_std':
  center=np.mean(pooled,axis=0);sd=np.std(pooled,axis=0,ddof=0)
  if not np.isfinite(center).all() or not np.isfinite(sd).all():raise ValueError('pooled scaling overflow')
  constant=[int(i) for i in np.flatnonzero(sd==0)];scales=np.where(sd==0,1.,sd);pooled=(pooled-center)/scales
 elif scaling!='none':raise ValueError('unknown scaling')
 distance=pairwise(pooled);median=float(np.median(distance[np.triu_indices(N,1)]))
 if bandwidth=='pooled_median':
  if median<=0 or not math.isfinite(median):raise ValueError('pooled median为0，自动核宽不可估；请选择明确正值的固定sigma，不跳过零距离')
  sigma=median
 elif bandwidth=='fixed':
  if sigma is None or not math.isfinite(sigma) or sigma<=0:raise ValueError('fixed sigma must be positive')
 else:raise ValueError('unknown bandwidth policy')
 kernel=kernel_matrix(distance,sigma)
 transform={'scaling':scaling,'center':np.asarray(center).tolist(),'scales':np.asarray(scales).tolist(),'constant_dimensions':constant,'constant_dimension_policy':'pooled SD=0 dimensions centered to 0 with scale=1; explicitly retained','bandwidth':bandwidth,'sigma':float(sigma),'pooled_median_distance':median,'label_independent':True}
 return {'n':n,'m':m,'N':N,'distance':distance,'kernel':kernel,'transform':transform,'transformed':pooled}

def rng_at(seed,start,N):
 '''Deterministic RNG positioned after `start` shuffles of 0..N-1.
 Replaying the same fixed-seed stream makes offline shards bit-identical to a monolithic run.'''
 if isinstance(seed,bool) or not isinstance(seed,int) or seed<0:raise ValueError('invalid seed')
 rng=random.Random(seed)
 for _ in range(start):
  order=list(range(N));rng.shuffle(order)
 return rng

def validate_range(permutations,start,count):
 if isinstance(permutations,bool) or not isinstance(permutations,int) or not 1<=permutations<=1999:raise ValueError('invalid permutation count')
 if isinstance(start,bool) or not isinstance(start,int) or start<0:raise ValueError('invalid shard start')
 if isinstance(count,bool) or not isinstance(count,int) or count<1 or start+count>permutations:raise ValueError('invalid shard range')

def compute_shard(context,permutations,seed,start,count):
 '''Compute one numbered interval [start, start+count) of the shared permutation stream.'''
 validate_range(permutations,start,count)
 n,m,N=context['n'],context['m'],context['N'];distance,kernel=context['distance'],context['kernel']
 rng=rng_at(seed,start,N);orders=[];energy_null=[];mmd_null=[]
 for _ in range(count):
  order=list(range(N));rng.shuffle(order);pa=sorted(order[:n]);pb=sorted(order[n:]);orders.append(order)
  energy_null.append(energy_score(distance,pa,pb));mmd_null.append(mmd_score(kernel,pa,pb))
 return {'start':start,'count':count,'end':start+count,'permutations':orders,'energy_statistics':energy_null,'mmd_statistics':mmd_null}

def observed_statistics(context):
 n=context['n'];m=context['m'];ia=list(range(n));ib=list(range(n,n+m))
 return energy_score(context['distance'],ia,ib),mmd_score(context['kernel'],ia,ib)

def assemble(permutations,seed,context,shards):
 '''Merge shard results in index order; callers validate identity/coverage first.'''
 shards=sorted(shards,key=lambda s:s['start'])
 orders=[];energy_null=[];mmd_null=[]
 for shard in shards:
  orders.extend(shard['permutations']);energy_null.extend(shard['energy_statistics']);mmd_null.extend(shard['mmd_statistics'])
 observed_energy,observed_mmd=observed_statistics(context)
 def result(observed,values):
  extreme=sum(value>=observed for value in values)
  return {'statistic':observed,'extreme_count':extreme,'denominator':permutations+1,'pvalue':(1+extreme)/(permutations+1),'permutation_statistics':values}
 return {'energy':result(observed_energy,energy_null),'mmd':result(observed_mmd,mmd_null),'B':permutations,'seed':seed,'permutations':orders,'sample_sizes':[context['n'],context['m']],'transform':context['transform'],'transformed_a':context['transformed'][:context['n']].tolist(),'transformed_b':context['transformed'][context['n']:].tolist(),'comparison':'>= observed; strict Float64 evaluation; +1 numerator and denominator','pvalues_adjusted':False}

def permutation_test(a,b,permutations=199,seed=2407,scaling='pooled_std',bandwidth='pooled_median',sigma=None):
 context=prepare_context(a,b,scaling,bandwidth,sigma)
 shard=compute_shard(context,permutations,seed,0,permutations)
 return assemble(permutations,seed,context,[shard])
