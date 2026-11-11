"""Operator CLI. All machine-readable output is UTF-8 JSON without credential values."""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from .errors import BackupError
from .bulk import DownloadBatch, plan_servers
from .importers import inspect_jino, parse_filezilla, parse_jino, valid_endpoint
from .scheduler import Scheduler
from .service import BackupService, unpack_archive
from .state import Repository, Settings


def emit(value):
    print(json.dumps(value, ensure_ascii=False, indent=2))


def result_code(job):
    status = job.get("status")
    if status in {"FILES_ONLY", "DB_ONLY", "STALE_SNAPSHOT"}:
        return 2
    return 0 if status == "COMPLETE" else 1


def batch_result_code(jobs):
    codes = [result_code(job) for job in jobs]
    return 1 if 1 in codes else 2 if 2 in codes else 0


class SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's default message echoes invalid arguments, which may be secrets.
        raise BackupError("CLI_ARGUMENT_INVALID")


def parser():
    root = SafeArgumentParser(prog="backupctl", description="Jino Backup Manager — резервные копии сайтов")
    root.add_argument("--state-dir", type=Path,
                      default=Path(os.environ.get("JINO_BACKUP_STATE_DIR", ".state")))
    groups = root.add_subparsers(dest="command", required=True)
    dashboard = groups.add_parser('ui', help='Локальная панель прогресса и проверки доступа')
    dashboard.add_argument('--file', type=Path, default=Path('abw_sibds__server.json'))
    dashboard.add_argument('--mapping', type=Path, default=Path('examples/jino-mapping.json'))
    dashboard.add_argument('--destination', type=Path)
    dashboard.add_argument('--batch-id')
    dashboard.add_argument('--port', type=int, default=8765)
    dashboard.add_argument('--open', action='store_true')
    init = groups.add_parser("init", help="Создать настройки и локальный ключ шифрования")
    init.add_argument("--backup-dir", type=Path, required=True)
    init.add_argument("--key-file", type=Path, required=True)

    settings = groups.add_parser("settings").add_subparsers(dest="action", required=True)
    settings.add_parser("show")
    location = settings.add_parser("set-backup-dir", help="Изменить каталог будущих копий")
    location.add_argument("--path", type=Path, required=True)
    mysql = settings.add_parser("set-mysql", help="Настроить MySQL-клиент и доверенный CA")
    mysql.add_argument("--executable", required=True)
    mysql.add_argument("--ca-file", type=Path, required=True)

    credentials = groups.add_parser("credentials").add_subparsers(dest="action", required=True)
    inspect = credentials.add_parser("inspect-jino", help="Вывести структуру и количества без значений")
    inspect.add_argument("--file", type=Path, required=True)
    jino = credentials.add_parser("import-jino")
    jino.add_argument("--file", type=Path, required=True)
    jino.add_argument("--mapping", type=Path)
    flags = jino.add_mutually_exclusive_group()
    flags.add_argument("--confirm", action="store_true")
    flags.add_argument("--dry-run", action="store_true")

    sites = groups.add_parser("sites").add_subparsers(dest="action", required=True)
    listing = sites.add_parser("list")
    listing.add_argument("--status", choices=["needs-review", "confirmed"])
    filezilla = sites.add_parser("import-filezilla")
    filezilla.add_argument("--file", type=Path, required=True)
    flags = filezilla.add_mutually_exclusive_group()
    flags.add_argument("--confirm", action="store_true")
    flags.add_argument("--dry-run", action="store_true")
    site = groups.add_parser("site").add_subparsers(dest="action", required=True)
    for group in (site, sites):
        add = group.add_parser("add", help="Привязать клиента и корень к файловому профилю")
        add.add_argument("--site-id", required=True)
        add.add_argument("--client-id", required=True)
        add.add_argument("--profile-id", "--ftp-profile-id", dest="profile_id", required=True)
        add.add_argument("--remote-root", required=True)
        add.add_argument("--known-hosts", type=Path)
        add.add_argument("--confirm", action="store_true")
    validate = site.add_parser("validate", help="Проверить локальную привязку без подключения")
    validate.add_argument("--site-id", required=True)
    database = site.add_parser("database-add", help="Явно привязать базу к сайту")
    database.add_argument("--site-id", required=True)
    database.add_argument("--db-id", required=True)
    database.add_argument("--profile-id", required=True)
    database.add_argument("--database-name", required=True)
    for action in ("databases", "confirm-databases"):
        child = site.add_parser(action)
        child.add_argument("--site-id", required=True)
    groups.add_parser("profiles").add_subparsers(dest="action", required=True).add_parser("list")

    backup = groups.add_parser("backup").add_subparsers(dest="action", required=True)
    accounts=backup.add_parser('download-accounts',help='Скачать файлы и доступные снимки БД всех аккаунтов через HTTPS')
    accounts.add_argument('--file',type=Path,required=True)
    accounts.add_argument('--mapping',type=Path,required=True)
    accounts.add_argument('--destination',type=Path,required=True)
    accounts.add_argument('--batch-id',required=True)
    accounts.add_argument('--workers',type=int,choices=[1,2],default=2)
    account_flags=accounts.add_mutually_exclusive_group()
    account_flags.add_argument('--execute',action='store_true')
    account_flags.add_argument('--dry-run',action='store_true')
    accounts.add_argument('--plaintext',action='store_true')
    accounts.add_argument('--inspect-only',action='store_true')
    accounts.add_argument('--timeout-seconds',type=int,default=86400)
    accounts.add_argument('--max-bytes',type=int,default=1024**4)
    accounts.add_argument('--max-files',type=int,default=1000000)
    naming=backup.add_parser('name-server-folders',help='Переименовать локальные папки серверов по логинам')
    naming.add_argument('--file',type=Path,required=True)
    naming.add_argument('--mapping',type=Path,required=True)
    naming.add_argument('--destination',type=Path,required=True)
    naming_flags=naming.add_mutually_exclusive_group()
    naming_flags.add_argument('--execute',action='store_true')
    naming_flags.add_argument('--dry-run',action='store_true')
    panel = backup.add_parser('export-jino-db',help='Проверить панельную выгрузку БД конкретного аккаунта')
    panel.add_argument('--file',type=Path,required=True)
    panel.add_argument('--mapping',type=Path,required=True)
    panel.add_argument('--source-row',type=int,required=True)
    panel.add_argument('--destination',type=Path,required=True)
    panel.add_argument('--idempotency-key',required=True)
    panel.add_argument('--reuse-snapshot',required=True)
    flags = panel.add_mutually_exclusive_group()
    flags.add_argument('--execute',action='store_true')
    flags.add_argument('--dry-run',action='store_true')
    panel.add_argument('--inspect-only',action='store_true')
    panel.add_argument('--preflight-only',action='store_true')
    panel.add_argument('--inspect-contract',action='store_true')
    panel.add_argument('--plaintext',action='store_true')
    panel.add_argument('--browser-channel',choices=['chrome','msedge','chromium'],default='chrome')
    panel.add_argument('--timeout-seconds',type=int,default=300)
    panel.add_argument('--max-bytes',type=int,default=10*1024**3)
    bulk = backup.add_parser('download-servers', help='Скачать корни серверов с сохранением прогресса')
    bulk.add_argument('--file', type=Path, required=True)
    bulk.add_argument('--mapping', type=Path, required=True)
    bulk.add_argument('--destination', type=Path, required=True)
    bulk.add_argument('--batch-id', required=True)
    bulk.add_argument('--remote-root', required=True)
    bulk.add_argument('--known-hosts', type=Path)
    flags = bulk.add_mutually_exclusive_group()
    flags.add_argument('--execute', action='store_true')
    flags.add_argument('--dry-run', action='store_true')
    bulk.add_argument('--plaintext', action='store_true')
    bulk.add_argument('--max-bytes', type=int, default=1024**4)
    bulk.add_argument('--max-files', type=int, default=1000000)
    bulk.add_argument('--timeout-seconds', type=int, default=86400)
    run = backup.add_parser("run")
    run.add_argument("--site-id", required=True)
    run.add_argument("--idempotency-key", required=True)
    run.add_argument("--mode", choices=["auto", "files-only"], default="auto")
    for name in ["status", "verify"]:
        child = backup.add_parser(name)
        child.add_argument("--job-id", required=True)
    listing = backup.add_parser("list")
    listing.add_argument("--site-id")
    restore = backup.add_parser("restore-files", help="Извлечь файлы в НОВЫЙ локальный каталог")
    restore.add_argument("--job-id", required=True)
    restore.add_argument("--destination", type=Path, required=True)
    restore_tests = groups.add_parser("restore").add_subparsers(dest="action", required=True)
    restore_test = restore_tests.add_parser("test", help="Проверить извлечение файлов, без БД и приложения")
    restore_test.add_argument("--job-id", required=True)
    restore_test.add_argument("--destination", type=Path, required=True)
    restore_test.add_argument("--isolated", action="store_true", required=True)
    standalone = groups.add_parser("archive").add_subparsers(dest="action", required=True)
    unpack = standalone.add_parser("unpack", help="Извлечь архив, включая SQL, без реестра заданий")
    unpack.add_argument("--file", type=Path, required=True)
    unpack.add_argument("--key-file", type=Path, required=True)
    unpack.add_argument("--job-id", required=True)
    unpack.add_argument("--destination", type=Path, required=True)

    schedule = groups.add_parser("schedule").add_subparsers(dest="action", required=True)
    schedule.add_parser("list")
    set_schedule = schedule.add_parser("set")
    set_schedule.add_argument("--site-id", required=True)
    interval = set_schedule.add_mutually_exclusive_group()
    interval.add_argument("--every-hours", type=int, default=24)
    interval.add_argument("--interval-seconds", type=int)
    start = set_schedule.add_mutually_exclusive_group(required=True)
    start.add_argument("--start-at", help="Время ISO 8601 с часовым поясом")
    start.add_argument("--next-run", type=int, help="Время UTC в секундах Unix")
    disable = schedule.add_parser("disable")
    disable.add_argument("--site-id", required=True)
    scheduler = groups.add_parser("scheduler").add_subparsers(dest="action", required=True)
    for group in (schedule, scheduler):
        tick = group.add_parser("tick", help="Однократно выполнить задания по расписанию")
        tick.add_argument("--now", type=int)
    loop = scheduler.add_parser("run", help="Выполнять расписание до прерывания")
    loop.add_argument("--poll-seconds", type=int, default=30)
    return root


def context(args):
    settings = Settings.load(args.state_dir)
    repo = Repository(settings.state_dir / "registry.sqlite3", settings.read_key())
    return settings, repo


def execute(args):
    if args.command == 'ui':
        from .dashboard import DashboardData, make_server, read_json
        from .state import private_dir, write_json
        from datetime import timezone

        if not 0 <= args.port <= 65535:
            raise BackupError('PORT_INVALID')
        active = read_json(args.state_dir / 'active-download.json', {})
        destination = args.destination or (
            Path(active['report']).parent if active.get('report') else None
        )
        batch_id = args.batch_id or active.get('batch_id')
        if destination is None or not batch_id:
            raise BackupError('DASHBOARD_BATCH_REQUIRED')
        try:
            mapping = json.loads(args.mapping.read_text(encoding='utf-8-sig'))
        except (ValueError, UnicodeError):
            raise BackupError('MAPPING_INVALID') from None
        data = DashboardData(args.state_dir, destination, args.file, mapping, batch_id)
        try:
            server = make_server(data, port=args.port)
        except OSError:
            raise BackupError('DASHBOARD_PORT_UNAVAILABLE') from None
        try:
            data.enable_management()
        except Exception:
            server.server_close()
            raise
        url = 'http://127.0.0.1:' + str(server.server_port) + '/'
        private_dir(args.state_dir)
        write_json(args.state_dir / 'dashboard-server.json', {
            'pid': os.getpid(),
            'url': url,
            'started_at': datetime.now(timezone.utc).isoformat(),
            'report': str(data.report_path),
        })
        emit({'url': url, 'local_only': True})
        sys.stdout.flush()
        if args.open:
            import webbrowser
            webbrowser.open(url)
        try:
            server.serve_forever(poll_interval=.5)
        finally:
            server.server_close()
        return 0
    if args.command=='backup' and args.action=='download-accounts':
        from .account_batch import AccountBatch,account_plan
        try:
            mapping=json.loads(args.mapping.read_text(encoding='utf-8-sig'))
        except (ValueError,UnicodeError):
            raise BackupError('MAPPING_INVALID') from None
        if not args.execute:
            report,selected=account_plan(args.file,mapping)
            emit({**report,'dry_run':True})
            return 0
        if not args.inspect_only and not args.plaintext:
            raise BackupError('PLAINTEXT_ACKNOWLEDGEMENT_REQUIRED')
        result=AccountBatch(args.state_dir,args.destination,args.file,mapping,args.batch_id,
            workers=args.workers,inspect_only=args.inspect_only,timeout_seconds=args.timeout_seconds,
            max_bytes=args.max_bytes,max_files=args.max_files,
            notify=lambda event:print(json.dumps(event,ensure_ascii=True),flush=True)).run()
        emit({'finished':result['finished'],'report_file':result['batch_id']+'.json',
              'accounts_count':len(result['servers']),
              'failed':sum(x['status']=='FAILED' for x in result['servers']),
              'needs_review':result['needs_review']})
        return 1 if any(x['status']=='FAILED' for x in result['servers']) or result['needs_review'] else 2
    if args.command=='backup' and args.action=='name-server-folders':
        from .importers import jino_rows
        from .server_folders import migrate_folders
        from .state import private_dir,write_json
        import uuid
        try:
            mapping=json.loads(args.mapping.read_text(encoding='utf-8-sig'))
        except (ValueError,UnicodeError):
            raise BackupError('MAPPING_INVALID') from None
        if not isinstance(mapping,dict) or not isinstance(mapping.get('id'),str):
            raise BackupError('MAPPING_INVALID')
        result=migrate_folders(jino_rows(args.file),mapping,args.destination,args.state_dir,execute=args.execute)
        if args.execute:
            private_dir(args.state_dir)
            report=args.state_dir/('server-folders-'+uuid.uuid4().hex+'.json')
            write_json(report,result)
        emit(result)
        return 0
    if args.command=='backup' and args.action=='export-jino-db':
        from .panel import select_panel_source
        try:
            mapping = json.loads(args.mapping.read_text(encoding='utf-8-sig'))
        except (ValueError,UnicodeError):
            raise BackupError('MAPPING_INVALID') from None
        selected = select_panel_source(args.file,mapping,args.source_row)
        if not args.execute:
            emit({'dry_run':True,'source_row':selected.source_row,'account_ref':selected.account_ref,
                  'snapshot_date':args.reuse_snapshot,'snapshot_action':'REUSE_ONLY'})
            return 0
        if not (args.inspect_only or args.preflight_only) and not args.plaintext:
            raise BackupError('PLAINTEXT_ACKNOWLEDGEMENT_REQUIRED')
        from .panel import JinoPanelBrowser
        from .panel_export import PanelExportJob
        settings,repo = context(args)
        repo.import_profiles(selected.candidates)
        def factory():
            return JinoPanelBrowser(repo.credentials(selected.account_ref),channel=args.browser_channel,
                                    timeout_seconds=args.timeout_seconds,
                                    research_dir=settings.state_dir/'panel-research' if args.inspect_contract else None)
        def notify(event):
            print(json.dumps(event,ensure_ascii=True),flush=True)
        result = PanelExportJob(args.state_dir,args.destination,selected.account_ref,args.idempotency_key,
                                args.reuse_snapshot,settings.read_key(),factory,
                                timeout_seconds=args.timeout_seconds,max_bytes=args.max_bytes,notify=notify,
                                server_folder=selected.folder_name).run(args.inspect_only,args.preflight_only)
        emit(result)
        return 0 if result.get('inspection_ok') or result.get('transport_verified') else result_code(result)
    if args.command == 'backup' and args.action == 'download-servers':
        try:
            mapping = json.loads(args.mapping.read_text(encoding='utf-8-sig'))
        except (ValueError, UnicodeError):
            raise BackupError('MAPPING_INVALID') from None
        plan = plan_servers(args.file, mapping)
        if not args.execute:
            emit({**plan.report, 'dry_run':True})
            return 0
        if not args.plaintext:
            raise BackupError('PLAINTEXT_ACKNOWLEDGEMENT_REQUIRED')
        def notify(event):
            print(json.dumps(event, ensure_ascii=True), flush=True)
        result = DownloadBatch(plan, args.destination, args.batch_id, args.remote_root,
                               notify=notify, max_bytes=args.max_bytes, max_files=args.max_files,
                               timeout_seconds=args.timeout_seconds,
                               known_hosts=str(args.known_hosts) if args.known_hosts else None).run()
        emit(result)
        return 1 if any(item['status'] == 'FAILED' for item in result['servers']) or result['needs_review'] else 2
    if args.command == "init":
        settings = Settings.initialize(args.state_dir, args.backup_dir, args.key_file)
        emit({"initialized": True, **settings.public()})
        return 0
    if args.command == "archive":
        key = args.key_file.read_bytes().strip()
        from cryptography.fernet import Fernet
        try:
            Fernet(key)
        except ValueError:
            raise BackupError("KEY_UNAVAILABLE") from None
        emit(unpack_archive(args.file, args.destination, key, args.job_id))
        return 0
    if args.command == "credentials" and args.action == "inspect-jino":
        emit(inspect_jino(args.file))
        return 0
    if ((args.command == "credentials" and args.action == "import-jino")
            or (args.command == "sites" and args.action == "import-filezilla")):
        if args.command == "credentials":
            if args.mapping is None:
                if args.confirm:
                    raise BackupError("MAPPING_REQUIRED")
                emit({**inspect_jino(args.file), "dry_run": True, "mapping_required": True,
                      "imported": 0})
                return 0
            try:
                mapping = json.loads(args.mapping.read_text(encoding="utf-8-sig"))
            except (ValueError, UnicodeError):
                raise BackupError("MAPPING_INVALID") from None
            if not isinstance(mapping, dict):
                raise BackupError("MAPPING_INVALID")
            batch = parse_jino(args.file, mapping)
        else:
            batch = parse_filezilla(args.file)
        report = {**batch.report, "dry_run": not args.confirm, "imported": 0}
        if args.confirm:
            _, repo = context(args)
            report["imported"] = repo.import_profiles(batch.candidates)
        emit(report)
        return 0
    settings = Settings.load(args.state_dir)
    if args.command == "settings":
        if args.action == "set-backup-dir":
            settings.set_backup_dir(args.path)
        elif args.action == "set-mysql":
            if not args.ca_file.is_file() or not args.executable.strip():
                raise BackupError("MYSQL_CONFIGURATION_INVALID")
            settings.mysql_dump = args.executable
            settings.mysql_ca_file = str(args.ca_file.resolve())
            settings.save()
        emit(settings.public())
        return 0
    _, repo = context(args)
    if args.command == "profiles":
        emit(repo.profiles())
    elif args.command == "sites" and args.action == "list":
        rows = repo.sites()
        if args.status:
            rows = [row for row in rows if bool(row["confirmed"]) == (args.status == "confirmed")]
        emit(rows)
    elif args.command in {"site", "sites"}:
        if args.action in {"database-add", "databases", "confirm-databases"}:
            if args.action == "database-add":
                repo.add_database(args.site_id, args.db_id, args.profile_id, args.database_name)
            elif args.action == "confirm-databases":
                repo.confirm_databases(args.site_id)
            emit({"site_id": args.site_id, "databases": repo.databases(args.site_id),
                  "database_scope_confirmed": bool(repo.site(args.site_id)["database_scope_confirmed"])})
            return 0
        if args.action == "add":
            repo.add_site(args.site_id, args.client_id, args.profile_id, args.remote_root,
                          confirmed=args.confirm, known_hosts=args.known_hosts)
        site = repo.site(args.site_id)
        if args.action == "validate":
            if not site["confirmed"]:
                raise BackupError("PROFILE_UNVERIFIED")
            credentials = repo.credentials(site["profile_id"])
            if not valid_endpoint(credentials):
                raise BackupError("FTP_PROFILE_NEEDS_REVIEW")
            if credentials["protocol"] == "sftp" and (
                    not site["known_hosts"] or not Path(site["known_hosts"]).is_file()):
                raise BackupError("KNOWN_HOSTS_REQUIRED")
        emit({**site, "network_checked": False})
    elif args.command == "restore":
        emit(BackupService(settings, repo).restore_files(args.job_id, args.destination))
    elif args.command == "backup":
        service = BackupService(settings, repo)
        if args.action == "run":
            job = service.run(args.site_id, args.idempotency_key, mode=args.mode)
            emit(job)
            return result_code(job)
        if args.action == "status":
            emit(repo.job(args.job_id))
        elif args.action == "list":
            emit(repo.jobs(args.site_id))
        elif args.action == "verify":
            emit(service.verify(args.job_id))
        elif args.action == "restore-files":
            emit(service.restore_files(args.job_id, args.destination))
    elif args.command in {"schedule", "scheduler"}:
        scheduler = Scheduler(repo, BackupService(settings, repo))
        if args.action == "tick":
            results = scheduler.tick(now=args.now)
            emit(results)
            return batch_result_code(results)
        if args.command == "schedule":
            if args.action == "set":
                next_run = args.next_run
                if next_run is None:
                    try:
                        start = datetime.fromisoformat(args.start_at.replace("Z", "+00:00"))
                        if start.utcoffset() is None:
                            raise ValueError
                        next_run = int(start.timestamp())
                    except (ValueError, OverflowError, OSError):
                        raise BackupError("SCHEDULE_TIME_INVALID") from None
                seconds = args.interval_seconds if args.interval_seconds is not None else args.every_hours * 3600
                scheduler.set(args.site_id, seconds, next_run)
            elif args.action == "disable":
                scheduler.disable(args.site_id)
            emit(scheduler.list())
        else:
            if not 1 <= args.poll_seconds <= 3600:
                raise BackupError("POLL_INTERVAL_INVALID")
            while True:
                results = scheduler.tick()
                if results:
                    emit(results)
                    sys.stdout.flush()
                time.sleep(args.poll_seconds)
    return 0


def main(argv=None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    try:
        return execute(parser().parse_args(argv))
    except BackupError as exc:
        emit({"error_code": exc.code, **exc.details})
        return 1
    except KeyboardInterrupt:
        emit({"error_code": "CANCELLED"})
        return 130
    except FileNotFoundError:
        emit({"error_code": "FILE_NOT_FOUND"})
        return 1
    except PermissionError:
        emit({"error_code": "FILESYSTEM_PERMISSION_DENIED"})
        return 1
    except Exception:
        emit({"error_code": "INTERNAL_ERROR"})
        return 1
