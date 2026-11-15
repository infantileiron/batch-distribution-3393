from pydantic import BaseModel,ConfigDict
class ShardJobInput(BaseModel):
 model_config=ConfigDict(extra='forbid')
 study_id:str
 shard_count:int
class ShardInterval(BaseModel):
 model_config=ConfigDict(extra='forbid')
 index:int
 start:int
 end:int
class ShardImport(BaseModel):
 model_config=ConfigDict(extra='forbid')
 format:str
 package_id:str
 run_id:str
 study_id:str
 study_name:str
 snapshot_digest:str
 B:int
 seed:int
 scaling:str
 bandwidth:str
 sigma:float|None
 sample_sizes:list[int]
 sample_ids:list[str]
 interval:ShardInterval
 permutations:list[list[int]]
 energy_statistics:list[float]
 mmd_statistics:list[float]
 checksum:str
 note:str|None=None
 runner:str|None=None
 batch_a:list[dict]|None=None
 batch_b:list[dict]|None=None
