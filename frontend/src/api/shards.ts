import {request,downloadText,uploadJson} from './client';import type {ShardJob,ShardCoverage} from '../types/domain';
export const list=()=>request<ShardJob[]>('/shard-jobs');
export const create=(data:{study_id:string;shard_count:number})=>request<ShardJob>('/shard-jobs',data);
export const coverage=(id:string)=>request<ShardCoverage>('/shard-jobs/'+id+'/coverage');
export const downloadPackage=(id:string)=>downloadText('/shard-jobs/'+id+'/package','qichuan-shard-package-'+id.slice(0,8)+'.json');
export const downloadDetailCsv=(id:string)=>downloadText('/shard-jobs/'+id+'/detail.csv','qichuan-shard-detail-'+id.slice(0,8)+'.csv');
export const importShard=(id:string,file:File)=>uploadJson<{status:string;missing?:number[];message?:string;job:ShardJob}>('/shard-jobs/'+id+'/shards',file);
