import {useState,useRef} from 'react';import type {Workspace} from '../stores/workspace';import type {ShardJob} from '../types/domain';
import * as shardsApi from '../api/shards';import {useAction} from '../hooks/useAction';import {Heading} from '../components/common/Heading';import {Notice} from '../components/common/Notice';import {Empty} from '../components/common/Empty';import {PolicyStrip} from '../components/PolicyStrip';import {SHARD_JOB_STATUSES} from '../constants/enums';import {short} from '../utils/format';

export function Shards({data,write,refresh,open}:{data:Workspace;write:boolean;refresh:()=>Promise<void>;open:(runId:string)=>void}){
 const[study,setStudy]=useState(data.studies[0]?.id||''),[count,setCount]=useState(3),[selected,setSelected]=useState(''),a=useAction();const fileRef=useRef<HTMLInputElement>(null);
 const job=data.shardJobs.find(j=>j.id===selected)||data.shardJobs[0];
 const studyOf=(j:ShardJob)=>data.studies.find(s=>s.id===j.study_id);
 const runOf=(j:ShardJob)=>data.runs.find(r=>r.id===j.run_id);
 async function onImport(file:File){
  if(!job)return;
  await a.act(async()=>{const out=await shardsApi.importShard(job.id,file);await refresh();return out.status==='duplicate'?(out.message||'分片已收讫，重复导入不重复计数'):'分片已核对并入册';});
  if(fileRef.current)fileRef.current.value='';
 }
 return <>
  <Heading index="05 / OFFLINE SHARD LAB" title="置换计算分包给离线实验室，收齐才发布。" detail="按冻结研究导出带编号区间的计算包；缩放与核宽按全体样本固定。离线结果乱序交回可合并，重复导入不重复计数，错研究/重叠区间/同编号异值一律拒绝；缺片只显示缺口，收齐后一次性发布与同种子整批运行逐位一致的完整结果。"/>
  {write&&<section className="form-section">
   <form className="form-grid" onSubmit={e=>{e.preventDefault();void a.act(async()=>{const j=await shardsApi.create({study_id:study,shard_count:count});await refresh();setSelected(j.id)},'离线计算包已建立，可下载分发')}}>
    <label>冻结研究（快照与口径）<select aria-label="选择冻结研究" value={study} onChange={e=>setStudy(e.target.value)}>{data.studies.map(s=><option key={s.id} value={s.id}>{s.name}</option>)}</select></label>
    <label>分片数量<input aria-label="分片数量" type="number" min={1} max={999} value={count} onChange={e=>setCount(Number(e.target.value))} required/></label>
    <button className="primary" disabled={a.busy||!study}>建立离线计算包</button>
   </form>
   <Notice error={a.error} notice={a.notice}/>
  </section>}
  {!data.shardJobs.length&&<Empty>先冻结一份分布研究，再在此建立带编号区间的离线计算包。</Empty>}
  {!!data.shardJobs.length&&<label className="selector">选择计算包<select aria-label="选择计算包" value={job?.id||''} onChange={e=>setSelected(e.target.value)}>{data.shardJobs.map(j=><option key={j.id} value={j.id}>{studyOf(j)?.name} · {SHARD_JOB_STATUSES[j.status]} · {short(j.id)}</option>)}</select></label>}
  {job&&(()=>{const st=studyOf(job);const run=runOf(job);const received=new Set(job.received);return <article className="shard-panel">
   <div className="shard-head">
    <div>
     <p className="eyebrow">PACKAGE {short(job.id)} / RUN {short(job.run_id)} / {SHARD_JOB_STATUSES[job.status]}</p>
     <h2>{st?.name}</h2>
     {st&&<PolicyStrip policy={st.snapshot.policy}/>}
     <p className="muted">B={st?.snapshot.policy.permutations} · seed={st?.snapshot.policy.seed} · 分片{job.shard_count}个 · 已收 {job.received.length}/{job.shard_count}</p>
    </div>
    <div className="actions">
     {write&&<button disabled={a.busy} onClick={()=>void a.act(()=>shardsApi.downloadPackage(job.id),'计算包已下载')}>下载计算包 JSON</button>}
     {write&&job.status==='collecting'&&<button className="primary" disabled={a.busy} onClick={()=>fileRef.current?.click()}>导入离线分片</button>}
     <input ref={fileRef} type="file" accept="application/json,.json" style={{display:'none'}} onChange={e=>{const f=e.target.files?.[0];if(f)void onImport(f)}}/>
     {job.status==='completed'&&run&&<button onClick={()=>open(run.id)}>查看发布的完整结果 →</button>}
     {job.status==='completed'&&<button disabled={a.busy} onClick={()=>void a.act(()=>shardsApi.downloadDetailCsv(job.id),'最终明细CSV已下载')}>导出最终明细 CSV</button>}
    </div>
   </div>
   {job.error&&<p role="alert" className="notice error">最近一次拒绝：{job.error}</p>}
   <div className="coverage-grid" role="table" aria-label="分片覆盖区间核对">
    {job.intervals.map(iv=>{const ok=received.has(iv.index);const piece=job.shards[String(iv.index)];return <div key={iv.index} className={'coverage-cell '+(ok?'received':'missing')} role="row">
     <b>#{iv.index}</b>
     <span>[{iv.start}, {iv.end}) · {iv.count}次</span>
     <small>{ok?'已收讫·校验和 '+(piece?short(piece.checksum):''):'缺口·等待交回'}</small>
    </div>})}
   </div>
   {job.status==='collecting'
    ? <p role="status" className="notice">尚未收齐：缺片编号 {job.intervals.filter(iv=>!received.has(iv.index)).map(iv=>iv.index).join(', ')}。只显示缺口，不发布部分结果。</p>
    : <p className="notice">全部编号区间已收齐并一次发布；结果校验和 {job.result_checksum}，可进入统计与置换证据页复核。</p>}
   {job.status==='completed'&&run&&<FinalDetail job={job}/>}
  </article>})()}
 </>;
}

function FinalDetail({job}:{job:ShardJob}){
 const[open,setOpen]=useState(false);
 const rows=job.intervals.map(iv=>{const piece=job.shards[String(iv.index)];return {iv,piece}});
 return <details open={open} onToggle={e=>setOpen((e.target as HTMLDetailsElement).open)}>
  <summary>最终明细：编号区间、覆盖与逐分片校验和（收齐后导出复核）</summary>
  <div className="table-scroll"><table>
   <thead><tr><th>编号</th><th>区间[起,止)</th><th>置换次数</th><th>状态</th><th>分片校验和SHA256</th></tr></thead>
   <tbody>{rows.map(({iv,piece})=><tr key={iv.index}><td>#{iv.index}</td><td>[{iv.start}, {iv.end})</td><td>{iv.count}</td><td>{piece?'已收讫':'缺口'}</td><td className="mono">{piece?piece.checksum:'—'}</td></tr>)}</tbody>
  </table></div>
  <p className="muted">合并结果沿用运行级CSV导出（统计与置换证据页/迁移复核归档页），绑定运行、口径、复核版本与快照校验和。</p>
 </details>;
}
