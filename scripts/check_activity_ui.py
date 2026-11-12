"""Synthetic-only browser check; no customer input or Jino connections."""
import json
import re
import tempfile
import threading
from pathlib import Path
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright, expect
from jino_backup.dashboard import DashboardData, make_server
from jino_backup.dashboard_management import DashboardManagement
from jino_backup.state import Settings


def main():
    root = Path(tempfile.mkdtemp(prefix='activity-ui-', dir='.test-tmp')).absolute()
    source = root / 'synthetic.json'
    source.write_text(json.dumps([{'id': str(i), 'name': f'Сервер {i:02}',
        'login': f'demo-{i}', 'password': 'synthetic', 'ftp_host': 'ftp.example.test',
        'ftp_login': f'demo-{i}', 'ftp_password': 'synthetic'} for i in range(1, 4)]), encoding='utf-8')
    mapping = {key:key for key in ('id','name','login','password','ftp_host','ftp_login','ftp_password')}
    mapping.update(ftp_protocol='ftps', ftp_port=21)
    Settings.initialize(root / 'state', root / 'archives', root / 'state/master.key')
    data = DashboardData(root / 'state', root / 'copies', source, mapping, 'synthetic-activity')
    management = DashboardManagement(data, runner=lambda *args: {'status':'FILES_ONLY'})
    management._schedule = lambda: None
    data.management = management
    for row in range(1, 4):
        task = management.start(row, 'files', {})
        management._update(task['id'], state='RUNNING' if row < 3 else 'QUEUED',
            stage='FILES_DOWNLOAD', progress={'received_bytes':row*1000000000,
            'verified_files':row*100, 'inventory_complete':False, 'active_transfers':1})
    server = make_server(data, port=0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    captures = Path('.impeccable/review')
    captures.mkdir(parents=True, exist_ok=True)
    blocked, errors = [], []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(channel='chrome', headless=True)
            for width in (1440, 1000, 800, 390):
                page = browser.new_page(viewport={'width':width, 'height':1100})
                page.on('pageerror', lambda e: errors.append(str(e)))
                def guard(route):
                    url = urlsplit(route.request.url)
                    if url.hostname != '127.0.0.1' or url.port != server.server_port or route.request.method != 'GET':
                        blocked.append({'method':route.request.method,'host':url.hostname})
                        route.abort()
                    else:
                        route.continue_()
                page.route('**/*', guard)
                page.goto(f'http://127.0.0.1:{server.server_port}/', wait_until='networkidle')
                page.locator('#auto-refresh').uncheck()
                expect(page.locator('#activity-heading')).to_be_visible()
                page.evaluate('''() => {
                    activity = new BackupActivity();
                    const now=Date.now();
                    for(let i=0; i<=240; i++) {
                        const sample=structuredClone(state.data);
                        for(const s of sample.servers) {
                            const p=s.latest_tasks.files.progress;
                            if(s.row<3) {
                                p.received_bytes += Math.round(i*2500000 + Math.sin(i/8)*5000000)*(s.row===1?2:1);
                                p.verified_files += Math.floor(i/12)*(s.row===1?12:4);
                            }
                        }
                        activity.observe(sample, now-(240-i)*5000);
                    }
                    localStorage.setItem('jino-activity-v1',JSON.stringify(activity.save()));
                    renderActivity(); renderTable();
                }''')
                expect(page.locator('#activity-speed')).to_contain_text('МБ/с')
                assert page.locator('#activity-bytes-chart rect').count() == 30
                page.locator('.activity-history summary').click()
                assert page.locator('#activity-history-rows tr').count() >= 20
                expect(page.locator('#server-rows .activity-row-speed').first).to_contain_text(re.compile('[МК]Б/с'))
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), width
                page.locator('.activity-history summary').click()
                page.evaluate('window.scrollTo(0,0)')
                page.screenshot(path=str(captures / f'activity-{width}.png'), full_page=True)
                page.evaluate('''() => {
                    activity.previous.time -= 20000;
                    renderActivity(); renderTable();
                }''')
                expect(page.locator('#activity-speed')).to_have_text('Нет замера скорости')
                expect(page.locator('#activity-status')).to_contain_text('Нет свежих наблюдений')
                page.reload(wait_until='networkidle')
                assert page.locator('#activity-bytes-chart .activity-byte-bar').count() >= 20
                page.close()
            browser.close()
    finally:
        server.shutdown()
        server.server_close()
        management.close()
    assert not errors, errors
    unexpected = [x for x in blocked if x['method'] != 'GET' or not (x['host'] or '').endswith('.kaspersky-labs.com')]
    assert not unexpected, unexpected
    print(json.dumps({'widths':[1440,1000,800,390], 'page_errors':0,
        'external_or_mutating_requests_sent':0, 'blocked_background_requests':len(blocked),
        'history_reload':True, 'synthetic_only':True}))


if __name__ == '__main__':
    main()
