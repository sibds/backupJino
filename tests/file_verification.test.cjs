const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const source=fs.readFileSync('src/jino_backup/web/app.js','utf8');
const context=vm.createContext({count:n=>new Intl.NumberFormat('ru-RU').format(n)});
const start=source.indexOf('function fileVerificationProgress(');
vm.runInContext(start<0?'':source.slice(start,source.indexOf('function serverWorkProgress(',start)),context);
function task(extra={}) {
 return {state:'RUNNING',stage:'FILE_VERIFY',progress:{download_phase_complete:true,inventory_complete:true,
   planned_files:7673,processed_files:1261,verified_files:7902,pending_verifications:6412},...extra};
}
test('current verification uses processed files, not historical checkpoint count',()=>{
 const status=context.fileVerificationProgress(task());
 assert.equal(status.checked,1261);assert.equal(status.total,7673);assert.equal(status.pending,6412);
 assert.match(status.title,/Передача файлов завершена/);
 assert.match(status.caption,/Проверено 1\s261 из 7\s673/);
 assert.match(status.caption,/6\s412 ожидают проверки/);
});
test('waiting verification retains its counters and names the waiting phase',()=>{
 const status=context.fileVerificationProgress(task({state:'QUEUED',stage:'VERIFICATION_WAIT'}));
 assert.equal(status.checked,1261);assert.match(status.note,/Ожидает очереди проверки/);
});
test('unknown inventory and legacy counters do not invent a completed verification',()=>{
 const status=context.fileVerificationProgress(task({progress:{verified_files:7902,pending_verifications:50}}));
 assert.equal(status.checked,null);assert.equal(status.total,null);
 assert.doesNotMatch(status.caption,/7\s902/);assert.match(status.caption,/50 ожидают проверки/);
});
test('success can use result counts but partial and failed results do not claim success',()=>{
 const done=context.fileVerificationProgress(task({state:'FINISHED',result:{status:'FILES_ONLY',files_count:10},progress:{}}));
 assert.equal(done.checked,10);assert.equal(done.total,10);assert.equal(done.successful,true);
 for (const state of ['FAILED','STOPPED']) {
   const value=context.fileVerificationProgress(task({state}));
   assert.equal(value.successful,false);assert.doesNotMatch(value.title,/скачаны и проверены/);
 }
 const partial=context.fileVerificationProgress(task({state:'FINISHED',result:{status:'PARTIAL'}}));
 assert.equal(partial.successful,false);assert.match(partial.title,/частичный/i);
});
test('successful result count takes precedence over inventory before deduplication',()=>{
 const status=context.fileVerificationProgress(task({state:'FINISHED',result:{status:'FILES_ONLY',files_count:2},
   progress:{inventory_complete:true,planned_files:3,processed_files:2,pending_verifications:0}}));
 assert.equal(status.total,2);assert.equal(status.checked,2);assert.equal(status.pending,0);
});
