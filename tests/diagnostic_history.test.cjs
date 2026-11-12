const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const source=fs.readFileSync('src/jino_backup/web/app.js','utf8');
const context=vm.createContext({});
vm.runInContext(source.slice(source.indexOf('function displayDiagnostics('),source.indexOf('function visibleServers(')),context);
test('legacy HTTPS failures stay in history and leave the table and its reason filter',()=>{
 const old={source:'Файлы HTTPS',code:'PANEL_RESPONSE_FAILED',message:'Панель хостинга не ответила корректно'};
 const server={diagnostics:[old],latest_tasks:{files:{transport:'ftps',state:'QUEUED'}}};
 assert.equal(context.displayDiagnostics(server,{table:true}).length,0);
 const history=context.displayDiagnostics(server);
 assert.equal(history.length,1);
 assert.equal(history[0].historical,true);
 assert.match(history[0].source,/Прежняя.*HTTPS/);
 assert.equal(history[0].code,old.code);
 assert.equal(old.source,'Файлы HTTPS');
});
test('a current HTTPS failure with the same code stays visible before history',()=>{
 const server={diagnostics:[{source:'Файлы HTTPS',code:'PANEL_RESPONSE_FAILED',message:'old'},
 {source:'Файлы',code:'PANEL_RESPONSE_FAILED',message:'current'}],latest_tasks:{files:{transport:'https_panel',state:'FAILED'}}};
 const table=context.displayDiagnostics(server,{table:true});
 assert.equal(table.length,1);
 assert.equal(table[0].message,'current');
 assert.equal(context.displayDiagnostics(server)[0].message,'current');
});
