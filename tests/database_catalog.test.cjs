const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const source=fs.readFileSync('src/jino_backup/web/app.js','utf8');
const context=vm.createContext({});
const begin=source.indexOf('function databaseCatalogSummary(');
assert.ok(begin>=0,'Database catalog presentation helpers are missing');
vm.runInContext(source.slice(begin,source.indexOf('function displayDiagnostics(')),context);
test('only checked copies contribute to saved size; zero and unknown stay distinct',()=>{
 const value=context.databaseCatalogSummary([{validation:'PASS',size_bytes:0},
  {validation:'PASS',size_bytes:100},{validation:'PASS'},{size_bytes:999}],7);
 assert.equal(value.saved,3); assert.equal(value.total,7); assert.equal(value.bytes,100);
 assert.equal(value.unknownSizes,1);
});
test('newly downloaded old snapshot is not described as a newer source',()=>{
 const value=context.databaseCopyState({validation:'PASS',snapshot_date:'2026-11-10',
  downloaded_at:'2026-11-14T10:00:00Z',available_snapshot_date:'2026-11-13'});
 assert.equal(value.saved,true); assert.equal(value.newer,true);
 assert.equal(context.databaseCopyState({available_snapshot_date:'2026-11-14'}).saved,false);
 assert.equal(context.databaseCopyState({validation:'PASS'}).newer,false);
});
test('intervals have explicit human labels for weekly and 72-hour schedules',()=>{
 assert.equal(context.databaseIntervalLabel(168),'Раз в неделю');
 assert.equal(context.databaseIntervalLabel(72),'Каждые 72 часа');
 assert.equal(context.databaseIntervalLabel(24),'Каждые 24 часа');
 assert.equal(context.databaseIntervalLabel(36),'Каждые 36 ч');
});
test('one-click missing download excludes every verified copy, including old snapshots and zero-sized records',()=>{
 const items=[{db_alias:'saved-old',validation:'PASS',snapshot_date:'2026-11-10',available_snapshot_date:'2026-11-14'},
  {db_alias:'zero',validation:'PASS',size_bytes:0},{db_alias:'missing'},
  {db_alias:'partial',validation:'FAIL'},{db_alias:'missing'}];
 assert.deepEqual(Array.from(context.databaseMissingAliases(items)),['missing','partial']);
 assert.equal(context.databaseMissingAliases(items.slice(0,2)).length,0);
});
