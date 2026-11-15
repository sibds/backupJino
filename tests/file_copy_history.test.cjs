const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');const vm=require('node:vm');
const source=fs.readFileSync('src/jino_backup/web/app.js','utf8');
const context=vm.createContext({});const start=source.indexOf('function fileCopyHistory(');
vm.runInContext(start<0?'':source.slice(start,source.indexOf('function fileVerificationProgress(',start)),context);
const old={id:'old',kind:'files',revision:1,state:'FAILED',progress:{received_bytes:33772692669,verified_files:14992}};
test('new empty job retains previous saved data without adding versions together',()=>{
 const current={id:'new',kind:'files',revision:1,state:'QUEUED',progress:{}};
 const result=context.fileCopyHistory({latest_tasks:{files:current},tasks:[current,old]});
 assert.equal(result.current.bytes,0);assert.equal(result.previous.bytes,33772692669);
 assert.equal(result.previous.files,14992);assert.equal(result.display.bytes,33772692669);
 assert.equal(result.fromPrevious,true);
});
test('current saved count is independent of current verification count',()=>{
 const current={id:'current',kind:'files',progress:{received_bytes:714229120,verified_files:7902,processed_files:1261}};
 const result=context.fileCopyHistory({latest_tasks:{files:current},tasks:[current]});
 assert.equal(result.display.files,7902);assert.equal(result.previous,null);
});
test('a growing new copy and previous copy remain separate',()=>{
 const current={id:'new',kind:'files',revision:1,progress:{received_bytes:1024,verified_files:3}};
 const result=context.fileCopyHistory({latest_tasks:{files:current},tasks:[current,old]});
 assert.equal(result.display.bytes,1024);assert.equal(result.previous.bytes,33772692669);
 assert.equal(result.fromPrevious,false);
});
test('history from a different binding is not assigned to the current copy',()=>{
 const current={id:'new',kind:'files',revision:2,progress:{}};
 const result=context.fileCopyHistory({latest_tasks:{files:current},tasks:[current,old]});
 assert.equal(result.previous,null);assert.equal(result.display.bytes,0);
});
