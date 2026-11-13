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

for (const state of ['RUNNING','FINISHED','FAILED','PAUSED']) {
 test(`started ${state} database job clears legacy warning from table and card`,()=>{
  const old={source:'Базы данных',code:'UI_CHANGED',message:'old'};
  const server={diagnostics:[old],latest_tasks:{databases:{state,started_at:'2026-11-14T00:00:00Z'}}};
  assert.equal(context.displayDiagnostics(server).length,0);
  assert.equal(context.displayDiagnostics(server,{table:true}).length,0);
  assert.equal(server.diagnostics.length,1,'Original history is retained');
 });
}
test('queueing a new job does not clear its previous failure',()=>{
 const server={diagnostics:[{source:'Базы данных',code:'UI_CHANGED',message:'old'}],
  latest_tasks:{databases:{state:'QUEUED',started_at:null}}};
 assert.equal(context.displayDiagnostics(server).length,1);
});
test('file activity does not clear database failures',()=>{
 const server={diagnostics:[{source:'Базы данных',code:'UI_CHANGED',message:'db failure'}],
  latest_tasks:{files:{state:'RUNNING',started_at:'2026-11-14T00:00:00Z'}}};
 assert.equal(context.displayDiagnostics(server).length,1);
});
test('new database failure remains visible even when it repeats the old code',()=>{
 const server={diagnostics:[{source:'Базы данных',code:'UI_CHANGED',message:'old'},
  {source:'Базы данных',code:'UI_CHANGED',message:'new'}],
  latest_tasks:{databases:{state:'FAILED',error_code:'UI_CHANGED',started_at:'2026-11-14T00:00:00Z'}}};
 assert.equal(context.displayDiagnostics(server).length,1);
 assert.equal(context.displayDiagnostics(server)[0].message,'new');
});
test('successful file job clears old FTP and HTTPS failures from the card too',()=>{
 const server={diagnostics:[{source:'FTP',code:'TLS_HANDSHAKE_FAILED'},
  {source:'Файлы HTTPS',code:'PANEL_RESPONSE_FAILED'}],
  latest_tasks:{files:{state:'FINISHED',started_at:'2026-11-14T00:00:00Z',result:{status:'PARTIAL'}}}};
 assert.equal(context.displayDiagnostics(server).length,0);
});
test('a new TLS failure remains visible while its historical predecessor is cleared',()=>{
 const server={diagnostics:[{source:'FTP',code:'TLS_HANDSHAKE_FAILED',message:'old'},
  {source:'Файлы',code:'TLS_HANDSHAKE_FAILED',message:'new'}],
  latest_tasks:{files:{state:'FAILED',started_at:'2026-11-14T00:00:00Z',error_code:'TLS_HANDSHAKE_FAILED'}}};
 assert.deepEqual(Array.from(context.displayDiagnostics(server),x=>x.message),['new']);
});
test('sidebar error follows the same clearing rule and retains a fresh failure',()=>{
 const server={error_message:'old',diagnostics:[{source:'Панель',code:'UI_CHANGED',message:'old'}],
  latest_tasks:{databases:{state:'RUNNING',started_at:'2026-11-14T00:00:00Z'}}};
 assert.equal(context.currentServerError(server),null);
 server.error_message='new';
 server.diagnostics.push({source:'Базы данных',code:'UI_CHANGED',message:'new'});
 server.latest_tasks.databases.error_code='UI_CHANGED';
 assert.equal(context.currentServerError(server),'new');
});
