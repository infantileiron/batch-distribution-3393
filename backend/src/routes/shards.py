from fastapi import APIRouter,Depends
from fastapi.responses import JSONResponse,Response
from sqlalchemy import select
from src.config.database import db_session
from src.controllers.auth import current_user,writer
from src.models import ShardJob
from src.schemas.shard import ShardJobInput,ShardImport
from src.services import shards as service
router=APIRouter(prefix='/shard-jobs')
@router.get('')
def listing(db=Depends(db_session),user=Depends(current_user)):return service.listing(db)
@router.post('')
def create(payload:ShardJobInput,db=Depends(db_session),user=Depends(writer)):return service.create(db,payload,user)
@router.get('/{id}')
def detail(id:str,db=Depends(db_session),user=Depends(current_user)):return service.detail(db,id)
@router.get('/{id}/package')
def package(id:str,db=Depends(db_session),user=Depends(current_user)):
 payload=service.package(db,id)
 return JSONResponse(payload,headers={'Content-Disposition':f'attachment; filename=qichuan-shard-package-{id[:8]}.json'})
@router.get('/{id}/coverage')
def coverage(id:str,db=Depends(db_session),user=Depends(current_user)):return service.coverage(db,id)
@router.get('/{id}/detail.csv')
def detail_csv(id:str,db=Depends(db_session),user=Depends(current_user)):
 return Response(service.detail_csv(db,id),media_type='text/csv; charset=utf-8',headers={'Content-Disposition':f'attachment; filename=qichuan-shard-detail-{id[:8]}.csv'})
@router.post('/{id}/shards')
def import_shard(id:str,payload:ShardImport,db=Depends(db_session),user=Depends(writer)):return service.import_shard(db,id,payload.model_dump())
