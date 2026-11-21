/** @typedef {{id:number,name:string,sensors:object[],readings:object[]}} Dataset */
/** @typedef {{id:number,name:string,dataset_id:number,steps:object[]}} Plan */
/** @typedef {{position:number,backend_type:string,verdict:'pending'|'confirmed'|'unconfirmed',first_seen_at:string,last_seen_at:string,pid?:number,wait_event?:string}} LockWaitLinkView 持锁方只含来源类型，不含语句与采样值 */
/** @typedef {{id:number,phase:string,status:'waiting'|'resolved',started_at:string,resolved_at:string|null,duration_ms:number|null,collect_attempts:number,collect_error:string|null,links:LockWaitLinkView[]}} LockWaitView */
/** @typedef {{id:number,name:string,status:'queued'|'running'|'completed'|'failed',result:object|null,lock_waits?:LockWaitView[]}} Trace */
export {};
