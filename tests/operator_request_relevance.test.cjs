const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');const vm=require('node:vm');
const source=fs.readFileSync('src/jino_backup/web/app.js','utf8');const context=vm.createContext({});
const start=source.indexOf('function operatorDecisionRequests(');
vm.runInContext(start<0?'':source.slice(start,source.indexOf('function fileCopyHistory(',start)),context);
const task={id:'current',kind:'databases',operator_request:{id:'request',actions:['retry']}};
test('blocked and deleted servers do not ask for decisions',()=>{
 const result=context.operatorDecisionRequests([{row:1,blocked:true,tasks:[task]},{row:2,deleted:true,tasks:[task]},{row:3,tasks:[task]}]);
 assert.equal(result.length,1);assert.equal(result[0].server.row,3);
});
test('superseded and held jobs do not ask for ineffective decisions',()=>{
 const old={...task,id:'old'};const held={...task,id:'held',first_copy_wait_for:'current'};
 const result=context.operatorDecisionRequests([{row:1,tasks:[old,held,task],latest_tasks:{databases:task}}]);
 assert.equal(result.length,1);assert.equal(result[0].task.id,'current');
});

test('automatic inspection and requests without a decision do not ask the operator',()=>{
 const inspecting={...task,id:'inspection',operator_request:{id:'a',inspection_pending:true,actions:['inspect_exports']}};
 const empty={...task,id:'empty',operator_request:{id:'b',actions:[]}};
 const conflict={...task,id:'conflict',kind:'files',operator_request:{id:'c',code:'FILE_CONFLICT_PENDING',actions:[]}};
 const result=context.operatorDecisionRequests([{row:1,tasks:[inspecting,empty,conflict]}]);
 assert.equal(result.length,1);assert.equal(result[0].task.id,'conflict');
});

test('saved file decisions stop asking while waiting for automatic application',()=>{
 const conflict={...task,kind:'files',conflict_decisions:{one:'keep_both'},operator_request:{code:'FILE_CONFLICT_PENDING',actions:[]}};
 const server={row:7,tasks:[conflict],conflict_events:[{task_id:task.id,conflict:{id:'one'}}]};
 assert.equal(context.operatorDecisionRequests([server]).length,0);
 server.conflict_events.push({task_id:task.id,conflict:{id:'two'}});
 assert.equal(context.operatorDecisionRequests([server]).length,1);
});
