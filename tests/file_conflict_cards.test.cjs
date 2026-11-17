const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');const vm=require('node:vm');
const source=fs.readFileSync('src/jino_backup/web/manage.js','utf8');
function element(tag,cls,text){return {tag,text,children:[],append(...items){this.children.push(...items)}};}
function render(task,conflict){
 const context=vm.createContext({detail:{conflict_events:[{task_id:task.id,conflict}]},node:element,pending:false,bytes:String,conflictDate:String,document:{createTextNode:text=>text},actionButton:(text,callback)=>({text,callback}),command:()=>{}});
 const start=source.indexOf('  function renderFileConflicts(');
 vm.runInContext(source.slice(start,source.indexOf('  function renderLogs(',start)),context);
 const fragment=element('fragment');context.renderFileConflicts(fragment,task);return fragment;
}
test('answered file cards disappear while history remains untouched',()=>{
 const conflict={id:'answered',remote_path:'log',local_path:'log'};
 assert.equal(render({id:'task',conflict_decisions:{answered:'new_version'}},conflict).children.length,0);
 assert.equal(conflict.id,'answered');
});
test('unanswered changing file offers an unchecked session rule',()=>{
 const view=render({id:'task'},{id:'new',rule_id:'rule',reason:'LOCAL_CONTENT_DIFFERS',remote_path:'log',local_path:'log'});
 const serialized=JSON.stringify(view);
 assert.match(serialized,/checkbox/);
 assert.match(serialized,/текущем задании/);
});
