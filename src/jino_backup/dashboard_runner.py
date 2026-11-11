"""Operator jobs reuse the existing file and MySQL snapshot workflows."""

import json
import posixpath
import sqlite3
import time
from contextlib import ExitStack

from .account_batch import completed_database_copy
from .bulk import DownloadBatch, Server, ServerPlan
from .downloads import FileLock, local_path
from .errors import BackupError
from .importers import identity
from .panel import JinoPanelBrowser
from .panel_downloads import PanelFileJob, PanelFileSource
from .panel_export import PanelExportJob
from .state import Settings
from .transfer import relative_path, name_compatibility_issue, source_for
from .retry import RecoveringSource


class ControlledWriter:
    def __init__(self, outgoing, control, received):
        self.outgoing, self.control, self.received = outgoing, control, received

    def __getattr__(self, name):
        return getattr(self.outgoing, name)

    def write(self, chunk):
        self.control.checkpoint()
        count = self.outgoing.write(chunk)
        self.received(len(chunk))
        return count


class ControlledSource:
    def __init__(self, source, control, progress, output, *, skip_unsupported_names=False):
        self.source, self.control, self.progress, self.output = source, control, progress, output
        self.inventory = {}
        self.received_bytes = 0
        self.last_notice = 0
        self.skip_unsupported_names = skip_unsupported_names

    def __getattr__(self, name):
        return getattr(self.source, name)

    def __enter__(self):
        self.source.__enter__()
        try:
            checkpoint = self.output / ".jino-progress/progress.sqlite3"
            if checkpoint.exists():
                db = sqlite3.connect(checkpoint.as_uri() + "?mode=ro", uri=True)
                try:
                    self.received_bytes = db.execute(
                        "SELECT COALESCE(SUM(committed),0) FROM files"
                    ).fetchone()[0]
                finally:
                    db.close()
            queue = [("", 0)]
            deadline = time.monotonic() + 86400
            planned_bytes = planned_files = 0
            last_scan_notice = 0
            self.progress(
                stage="SCANNING", received_bytes=self.received_bytes, inventory_complete=False,
                skipped_count=0,
            )
            skipped_count = 0
            while queue:
                self.control.checkpoint()
                parent, depth = queue.pop()
                if depth > 64 or planned_files + len(self.inventory) + len(queue) > 1000000:
                    raise BackupError("TRANSFER_LIMIT_EXCEEDED")
                if time.monotonic() > deadline:
                    raise BackupError("TRANSFER_TIMEOUT")
                entries = list(self.source.listdir(parent))
                self.inventory[parent] = entries
                for entry in entries:
                    issue = name_compatibility_issue(entry.name)
                    path = posixpath.join(parent, entry.name)
                    if issue:
                        if not self.skip_unsupported_names:
                            raise BackupError("REMOTE_PATH_UNSAFE")
                        skipped_count += 1
                        self.progress(stage="FILE_SKIPPED", log=True, error_code="REMOTE_NAME_UNSUPPORTED",
                                      skipped_count=skipped_count, skipped_path=path,
                                      skip_reason=issue, skipped_kind=entry.kind)
                        continue
                    relative_path(path)
                    if entry.kind == "dir":
                        queue.append((path, depth + 1))
                    elif entry.kind == "file":
                        planned_files += 1
                        planned_bytes += entry.size
                if time.monotonic() - last_scan_notice >= 0.5:
                    self.progress(
                        stage="SCANNING",
                        planned_bytes=planned_bytes,
                        planned_files=planned_files,
                        directories_seen=len(self.inventory),
                    )
                    last_scan_notice = time.monotonic()
            self.progress(
                stage="FILES_DOWNLOAD",
                planned_bytes=planned_bytes,
                planned_files=planned_files,
                directories_seen=len(self.inventory),
                inventory_complete=True,
                log=True,
            )
            return self
        except BaseException:
            self.source.__exit__()
            raise

    def __exit__(self, *args):
        self.source.__exit__(*args)

    def listdir(self, relative):
        self.control.checkpoint()
        return self.inventory[relative]

    def stat(self, relative):
        self.control.checkpoint()
        return self.source.stat(relative)

    def download(self, relative, outgoing, offset):
        self.control.checkpoint()
        received = offset
        self.progress(
            stage="FILE_DOWNLOAD",
            log=True,
            current_file=relative,
            current_bytes=offset,
            current_size=self.source.stat(relative).size,
        )

        def notice(size):
            nonlocal received
            received += size
            self.received_bytes += size
            if time.monotonic() - self.last_notice >= 0.5:
                self.progress(received_bytes=self.received_bytes, current_bytes=received)
                self.last_notice = time.monotonic()

        try:
            return self.source.download(
                relative, ControlledWriter(outgoing, self.control, notice), offset
            )
        except BackupError as error:
            self.progress(stage="FILE_ERROR", log=True, error_code=error.code)
            raise
        finally:
            self.progress(received_bytes=self.received_bytes, current_bytes=received, log=True)


class OperatorRunner:
    def __init__(self, data, catalog):
        self.data, self.catalog = data, catalog

    def __call__(self, server, task, control, progress):
        skip_names = task.get("skip_unsupported_names", server.get("skip_unsupported_names", False))
        with ExitStack() as stack:
            stack.enter_context(
                FileLock(
                    self.data.state_dir / "account_batches" / (server["account_ref"] + ".lock")
                )
            )
            credentials = self.catalog.credentials(server["row"])
            control.checkpoint()
            progress(stage="AUTHENTICATE", retry_attempt=0, retry_limit=5, retry_at=None, retry_delay_seconds=0)
            if task["kind"] == "files" and task["transport"] in {"ftps", "sftp"}:
                ftp = dict(credentials["ftp"], protocol=task["transport"])
                plan = ServerPlan(
                    {"rows_total": 1, "servers_ready": 1, "needs_review": 0},
                    [Server(server["server_id"], server["row"], ftp, server["folder"])],
                )
                output = local_path(
                    self.data.destination, server["folder"] + "/" + identity("download", task["id"])
                )

                def factory(values, site):
                    return ControlledSource(RecoveringSource(source_for(values, site), wait=control.wait, notify=progress), control, progress, output,
                                            skip_unsupported_names=skip_names)

                batch = DownloadBatch(
                    plan,
                    self.data.destination,
                    task["id"],
                    server["remote_root"],
                    source_factory=factory,
                    known_hosts=server.get("known_hosts") or None,
                    notify=lambda event: progress(received_bytes=event.get("bytes_saved")),
                    continue_on_error=True,
                    skip_unsupported_names=skip_names,
                    retry_wait=control.wait,
                )
                result = batch.run()["servers"][0]
                self._file_totals(output, progress)
                return result
            panel = stack.enter_context(JinoPanelBrowser(credentials["panel"]))
            panel.control = control.checkpoint
            control.checkpoint()
            panel.inspect_files(prepare_transport=task["kind"] == "files")
            progress(stage="ACCESS_VERIFIED")
            if task["kind"] == "files":
                workflow_key = task.get("workflow_key") or task["id"]
                if not task.get("workflow_key") and self._legacy_binding(server):
                    previous = local_path(
                        self.data.destination,
                        server["folder"]
                        + "/"
                        + identity("download", self.data.batch_id, "https-panel"),
                    )
                    manifest = previous / "manifest.json"
                    if (previous / ".jino-progress/progress.sqlite3").exists() and (
                        not manifest.exists()
                        or json.loads(manifest.read_text(encoding="utf-8")).get("status")
                        != "FILES_ONLY"
                    ):
                        workflow_key = self.data.batch_id
                progress(workflow_key=workflow_key)
                job_id = identity("download", workflow_key, "https-panel")
                output = local_path(self.data.destination, server["folder"] + "/" + job_id)

                def factory():
                    scope = panel.inspect_files()
                    return ControlledSource(
                        RecoveringSource(PanelFileSource(panel, scope, skip_unsupported_names=skip_names), wait=control.wait, notify=progress), control, progress, output,
                        skip_unsupported_names=skip_names,
                    )

                result = PanelFileJob(
                    self.data.destination,
                    server["server_id"],
                    server["folder"],
                    workflow_key,
                    factory,
                    state_dir=self.data.state_dir,
                    skip_unsupported_names=skip_names,
                ).run()
                self._file_totals(output, progress)
                return result
            progress(stage="SELECT_SNAPSHOT")
            snapshot = task.get("snapshot_date") or panel.latest_mysql_snapshot()
            if not snapshot:
                raise BackupError("SNAPSHOT_NOT_FOUND")
            progress(stage="DATABASES", snapshot_date=snapshot)
            key = Settings.load(self.data.state_dir).read_key()
            scope = panel.inspect(snapshot)
            control.checkpoint()
            progress(expected_count=len(scope["databases"]), verified_count=0)
            reused = completed_database_copy(
                self.data.state_dir,
                self.data.destination,
                server["account_ref"],
                server["folder"],
                scope,
                key,
            )
            if reused:
                progress(
                    verified_count=reused["databases_count"],
                    received_bytes=sum(item["size_bytes"] for item in reused["artifacts"]),
                )
                return reused
            workflow_key = task.get("workflow_key") or task["id"]
            if not task.get("workflow_key") and self._legacy_binding(server):
                previous_key = self.data.batch_id + "-" + snapshot
                journal = (
                    self.data.state_dir
                    / "panel_jobs"
                    / (identity("panel-job", server["account_ref"], previous_key) + ".enc")
                )
                if journal.exists():
                    workflow_key = previous_key
            progress(workflow_key=workflow_key)
            verified = 0
            received = 0

            def notify(event):
                nonlocal verified, received
                if event["stage"] == "SQL_VERIFIED":
                    verified += 1
                    received += event["size_bytes"]
                progress(stage=event["stage"], verified_count=verified, received_bytes=received)

            panel.download_progress = lambda amount, total: progress(
                received_bytes=received + amount,
                current_bytes=amount,
                current_size=total,
                current_file="SQL-дамп",
            )

            from .account_batch import BorrowedPanel

            return PanelExportJob(
                self.data.state_dir,
                self.data.destination,
                server["account_ref"],
                workflow_key,
                snapshot,
                key,
                lambda: BorrowedPanel(panel, scope),
                server_folder=server["folder"],
                notify=notify,
                control=control.checkpoint,
            ).run()

    @staticmethod
    def _legacy_binding(server):
        return (
            server.get("source_row") is not None
            and server["binding_revision"] == 1
            and server["folder"] == server["legacy_folder"]
        )

    @staticmethod
    def _file_totals(output, progress):
        checkpoint = local_path(output, ".jino-progress/progress.sqlite3")
        if not checkpoint.exists():
            return
        db = sqlite3.connect(checkpoint.as_uri() + "?mode=ro", uri=True)
        try:
            count, size = db.execute(
                "SELECT COALESCE(SUM(CASE WHEN state='done' THEN 1 ELSE 0 END),0),COALESCE(SUM(committed),0) FROM files"
            ).fetchone()
            progress(verified_files=count, received_bytes=size)
        finally:
            db.close()
