const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('src/jino_backup/web/app.js', 'utf8').split('const $ =')[0];
const context = vm.createContext({});
vm.runInContext(source, context);
function data(bytes, files, id='job-1', state='RUNNING') {
  return {servers:[{row:1, latest_tasks:{files:{id,state,progress:{received_bytes:bytes,verified_files:files}}}}]};
}
test('measures deltas and bytes per second without counting the initial backup', () => {
  const meter = new context.BackupActivity();
  meter.observe(data(1000000, 100), 60000);
  assert.equal(meter.row(1, 60000).speed, null);
  meter.observe(data(1100000, 103), 65000);
  assert.equal(meter.row(1, 65000).speed, 20000);
  assert.equal(meter.minutes[0].bytes, 100000);
  assert.equal(meter.minutes[0].files, 3);
});
test('resets baselines for changed jobs, counter decreases and observation gaps', () => {
  const meter = new context.BackupActivity();
  meter.observe(data(1000, 5), 60000);
  meter.observe(data(2000, 10, 'new'), 65000);
  assert.equal(meter.minutes[0].bytes, 0);
  meter.observe(data(1000, 3, 'new'), 70000);
  assert.equal(meter.minutes[0].files, 0);
  meter.observe(data(999999, 30, 'new'), 200000);
  assert.equal(meter.row(1, 200000).speed, null);
  assert.equal(meter.minutes.length, 1);
});
test('distinguishes zero activity, stale polling and browser reload', () => {
  const meter = new context.BackupActivity();
  meter.observe(data(0, 0), 60000);
  meter.observe(data(0, 0), 65000);
  assert.equal(meter.row(1, 65000).speed, 0);
  assert.equal(meter.row(1, 100000).speed, null);
  const restored = new context.BackupActivity(JSON.parse(JSON.stringify(meter.save())));
  assert.equal(restored.minutes.length, 1);
  assert.equal(restored.row(1, 100000).speed, null);
});
test('keeps observed minutes separate and includes database bytes without SQL as files', () => {
  const meter = new context.BackupActivity();
  const first = data(100, 2);
  first.servers[0].latest_tasks.databases = {id:'db',state:'RUNNING',progress:{received_bytes:1000,verified_count:1}};
  meter.observe(first, 58000);
  const next = structuredClone(first);
  next.servers[0].latest_tasks.files.progress.received_bytes=200;
  next.servers[0].latest_tasks.files.progress.verified_files=3;
  next.servers[0].latest_tasks.databases.progress.received_bytes=2000;
  meter.observe(next, 61000);
  assert.equal(meter.minutes[0].time,60000);
  assert.equal(meter.minutes[0].bytes,1100);
  assert.equal(meter.minutes[0].files,1);
  assert.equal(meter.row(null, 61000).speed,1100/3);
});
test('does not invent activity from queued jobs or bridge a browser reload', () => {
  const meter = new context.BackupActivity();
  meter.observe(data(100, 1, 'queued', 'QUEUED'),60000);
  meter.observe(data(200, 2, 'queued', 'QUEUED'),65000);
  assert.equal(meter.minutes[0].bytes,0);
  const restored = new context.BackupActivity(JSON.parse(JSON.stringify(meter.save())));
  restored.observe(data(99999, 99),70000);
  assert.equal(restored.row(null,70000).speed,null);
  assert.equal(restored.minutes[0].bytes,0);
});
test('rechecking saved files counts work without inventing downloaded bytes', () => {
  const meter = new context.BackupActivity();
  const sample = data(1000000,1000);
  sample.servers[0].latest_tasks.files.progress.processed_files=0;
  meter.observe(sample,60000);
  sample.servers[0].latest_tasks.files.progress.processed_files=5;
  meter.observe(sample,65000);
  assert.equal(meter.minutes[0].files,5);
  assert.equal(meter.minutes[0].bytes,0);
  assert.equal(meter.row(1,65000).speed,0);
});
