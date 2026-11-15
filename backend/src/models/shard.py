from sqlalchemy import String,Integer,JSON,ForeignKey,Text
from sqlalchemy.orm import Mapped,mapped_column
from src.config.database import Base
class ShardJob(Base):
 __tablename__='shard_jobs'
 id:Mapped[str]=mapped_column(String,primary_key=True)
 study_id:Mapped[str]=mapped_column(ForeignKey('distribution_studies.id'))
 run_id:Mapped[str]=mapped_column(ForeignKey('distribution_runs.id'))
 status:Mapped[str]=mapped_column(String)
 shard_count:Mapped[int]=mapped_column(Integer)
 shard_size:Mapped[int]=mapped_column(Integer)
 intervals:Mapped[list]=mapped_column(JSON)
 received:Mapped[list]=mapped_column(JSON,default=list)
 shards:Mapped[dict]=mapped_column(JSON,default=dict)
 result_checksum:Mapped[str]=mapped_column(String,default='')
 error:Mapped[str]=mapped_column(Text,default='')
 created_at:Mapped[str]=mapped_column(String)
 updated_at:Mapped[str]=mapped_column(String)
