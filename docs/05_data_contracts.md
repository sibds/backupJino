# Модели данных и контракты интерфейсов

Файловые задания управляемой очереди поддерживают этап AUTO_RESUME_WAIT в состоянии SCHEDULED и поля auto_resume_at, auto_resume_count, auto_resume_error, auto_resume_bytes, first_started_at, last_attempt_result. Идентификатор, ключ копии и checkpoint не меняются. scheduled_at указывает ближайший запуск, ручная пауза/остановка его отменяет. Автопродолжение не меняет контракт manifest.json и не применяется к БД. [Поведение и пределы](34_automatic_file_resume.md).

## Дополнение 13 ноября: приватная карточка БД

GET `/api/servers/{row}` дополнен `server.database_inventory[]`: `db_alias`, приватное `name`, необязательные `snapshot_date`, `available_snapshot_date`, `downloaded_at`, `size_bytes`, `validation`, `schedule`. Общий `/api/state` не содержит этот справочник. POST `/api/servers/{row}/jobs` для `kind=databases` принимает известные `db_aliases[]`, `inspect_only` и `new_version` (логические значения); без выбора сохраняется прежняя выгрузка всего снимка. Неизвестный псевдоним отклоняется. POST `/api/servers/{row}/database-schedules` принимает `db_alias`, `every_hours` (целое 1–8760), `enabled` (bool). Расписание хранит `next_at` UTC и ссылку на последнюю постановку. В задаче сохраняется безопасный `schedule_occurrence`. Манифест DB_ONLY выбранной работы описывает выбранный состав, не все БД аккаунта. [Семантика, безопасность и проверки](32_database_card_and_schedules.md).

## Реализованный локальный контракт 0.1

Для явно выбранных обычных файлов добавлена команда `backup download-servers --file SOURCE --mapping MAP --destination PATH --batch-id KEY --remote-root ROOT [--dry-run | --execute --plaintext]`. По умолчанию она выполняет только локальный dry-run. Отчёт содержит общее число записей, число пригодных профилей и обезличенные ошибки; выполнение создаёт отдельные папки серверов и запусков, журнал продолжения и файловый манифест `FILES_ONLY`. Этот режим не заказывает дампы БД. [Команды и поведение при сбоях](../README.md), [живые результаты](10_download_and_panel_pilot.md).

Пример ниже в разделе `manifest.json` описывает целевую модель. Фактический манифест прототипа содержит `schema_version: "1.0"`, идентификаторы задания и сайта, статус полноты, `capture_consistency: "BEST_EFFORT"`, времена захвата, `files` с полями `count`, `bytes`, `items[]`, `directories[]`, а также `databases[]`, `database_errors[]` и `warnings[]`.

Каждый файловый элемент содержит `path`, `size_bytes`, `sha256`; элемент БД — `db_id`, `engine`, `size_bytes`, `sha256`, `source_snapshot_at`, `export_finished_at`, `validation`. Исходное время файлового снимка отсутствует (`source_snapshot_at: null`), поскольку файлы читаются с живого сайта. Имена БД и параметры подключений в отчёт не включаются.

`storage_verification: "RECORDED_IN_JOB_RECEIPT"` указывает на квитанцию в `BackupJob.result`: `artifact` содержит абсолютный `storage_root`, относительный `storage_key`, `size_bytes`, `sha256`; `storage_verified: true` устанавливается после расшифровки и проверки реально записанного файла. Это позволяет изменить папку для будущих заданий без потери адресов предыдущих архивов.

Пользователь выбрал локальный backend. Статус `COMPLETE` требует подтверждённого полного списка привязанных БД и успешной проверки всех артефактов, но не доказывает единую точку времени или успешный запуск приложения. При отсутствии подтверждения или ошибке БД результат — `FILES_ONLY`. Полный пакет извлекается командой `archive unpack` независимо от реестра. Фактические команды перечислены в [README](../README.md).

## Основные сущности

| Сущность | Поля |
| --- | --- |
| `Client` | `client_id`, `name`, `owner_contact`, `authorization_status` |
| `Site` | `site_id`, `client_id`, `domain`, `hosting_type`, `site_root`, `cms`, `panel_account_id`, `ftp_profile_id`, `database_ids[]`, `policy_id` |
| `PanelAccount` | `panel_account_id`, `source_record_id`, `panel_login`, `password_secret_ref`, `mfa_required`, `status` |
| `FtpProfile` | `ftp_profile_id`, `host`, `port`, `protocol`, `username`, `password_secret_ref`, `remote_root`, `status` |
| `DbConnection` | `db_id`, `site_id`, `engine`, `host_ref`, `database_alias`, `credential_secret_ref`, `export_mode` |
| `BackupPolicy` | `policy_id`, `schedule`, `retention`, `excluded_paths`, `preferred_modes`, `alert_rules` |
| `BackupJob` | `job_id`, `site_id`, `requested_by`, `status`, `method`, `started_at`, `finished_at`, `source_snapshot_at`, `error_code`, `warnings[]` |
| `Artifact` | `artifact_id`, `job_id`, `kind`, `storage_key`, `size_bytes`, `sha256`, `verified_at` |
| `RestoreTest` | `restore_test_id`, `job_id`, `tested_at`, `files_ok`, `db_ok`, `application_ok`, `notes` |

`source_record_id` — безопасный идентификатор строки в `abw_sibds__server.json`; его точное имя в MySQL-дампе устанавливается после изучения файла. Не делать `PanelAccount` идентичным `FtpProfile`: права и пароли могут различаться.

## Статусы

| Статус | Значение |
| --- | --- |
| `COMPLETE` | Все обязательные файлы и БД присутствуют, артефакты проверены и находятся во внешнем хранилище. |
| `FILES_ONLY` | Получены файлы, но нет полного набора БД. |
| `DB_ONLY` | Получена БД, но нет полного набора файлов. |
| `STALE_SNAPSHOT` | Копия из исторического снимка старше согласованного порога. |
| `FAILED` | Не удалось получить проверяемую копию согласно политике. |
| `WAITING_FOR_MFA` | Сервис ожидает разрешённое интерактивное подтверждение. |
| `RETRY_WAIT` | Ожидание безопасной повторной попытки. |

Успех `COMPLETE` не означает автоматически успешный тест восстановления приложения: последний имеет отдельную запись `RestoreTest`.

## `manifest.json`

```json
{
  "schema_version": "1.0",
  "job_id": "job-example-001",
  "site_id": "site-example-017",
  "status": "COMPLETE",
  "capture_method": "jino_panel_snapshot",
  "source_snapshot_at": "2026-11-10T09:40:00Z",
  "capture_consistency": "PROVIDER_SNAPSHOT",
  "files": {
    "count": 12042,
    "bytes": 0,
    "artifact_sha256": "<sha256-placeholder>"
  },
  "databases": [
    {
      "engine": "mysql",
      "name_alias": "db1",
      "bytes": 0,
      "artifact_sha256": "<sha256-placeholder>",
      "validation": "PASS"
    }
  ],
  "offsite_verified": true,
  "retention_policy_id": "standard",
  "warnings": []
}
```

Это **схема с заглушками**, не реальный результат. Манифест не содержит логинов, паролей, cookies, токенов и сырых строк подключения.

## CLI MVP

```bash
backupctl sites import-filezilla --file sites.xml --dry-run
backupctl credentials import-jino --file abw_sibds__server.json --dry-run
backupctl credentials import-jino --file abw_sibds__server.json --mapping mapping.json --confirm
backupctl sites list --status needs-review
backupctl site validate --site-id site-017
backupctl backup run --site-id site-017 --mode auto
backupctl backup status --job-id job-example-001
backupctl backup verify --job-id job-example-001
backupctl restore test --job-id job-example-001 --isolated
```

`--dry-run` обязателен по умолчанию при первом импорте и не должен сохранять секретные значения. Флаг `--confirm` разрешает только утверждённый импорт и сохранение в vault; он не разрешает автоматически входить в аккаунты клиентов.

### Фактический локальный CLI и манифест

Команды прототипа описаны в [README](../README.md). Карта импорта сейчас поддерживается в JSON; YAML выше в описании проектной модели является только иллюстрацией. `site validate` выполняет локальные проверки без сетевой авторизации. Команда `restore test --isolated --destination PATH` проверяет извлечение файлов в новый каталог и сохраняет `application_restore_tested: false`; полноценный `RestoreTest` приложения ещё не реализован.

Манифест прототипа сохраняет `schema_version: "1.0"`, времена начала/завершения захвата, `source_snapshot_at: null` для живых файлов и `capture_consistency: "BEST_EFFORT"`. В `files.items[]` находятся пути, размеры и SHA-256 каждого файла; `files.directories[]` сохраняет каталоги. Прямые MySQL-дампы описываются через безопасный `db_id`, размер, SHA-256, время и результат первичной проверки. Имена БД и учётные данные в манифест не попадают.

Шифрованный архив записывается однократно. Поэтому результат последующей проверки хранилища хранится в квитанции задания (`result.storage_verified`, `result.artifact`), а внутри архива используется `storage_verification: "RECORDED_IN_JOB_RECEIPT"`. Поля целевой модели `offsite_verified` и `retention_policy_id` пока не заявляются: backend прототипа — выбранный файловый каталог без Object Lock. SHA-256 всего архива также находится во внешней квитанции, чтобы избежать самоссылки манифеста.

Результат `COMPLETE` возможен для прямого MySQL только после подтверждения оператором полного состава БД сайта. Добавление новой привязки сбрасывает подтверждение. `files-only` исключает экспорт БД. В `backup run --mode auto` панель, SSH и PHP пока не подключены. Панельная выгрузка реализована отдельной командой ниже.

### Дополнительный контракт панельного задания

`backup download-accounts --file PATH --mapping PATH --destination PATH --batch-id KEY [--workers 1|2] [--execute --plaintext] [--inspect-only]` запускает общий проход по однозначным аккаунтам. По умолчанию — локальный dry-run. Отчёт `jino-all-*.json` содержит `finished`, очередь и отдельные результаты `files`/`databases`. `RUNNING` и подтверждённые части не означают готовую копию; полный набор файлов и SQL ещё не даёт `COMPLETE` без подтверждённых привязок и полноты. [Правила массового прохода](13_all_accounts_download.md).

С 11 ноября локальные папки серверов именуются по логину исходной записи, а не по `server_id`. Служебные ID в сущностях, блокировках и манифестах остаются прежними. Команда `backup name-server-folders --file PATH --mapping PATH --destination PATH [--execute | --dry-run]` переносит принадлежащие приложению старые папки без сетевых действий. По умолчанию — только проверка плана. При переносе прежнего завершённого панельного задания разрешено обновить только подтверждённый прежний путь того же аккаунта, назначения и даты после проверки владельца и артефактов. [Точные правила и проверка содержимого](12_login_folder_names.md).

```text
backupctl backup export-jino-db --file PATH --mapping PATH --source-row N
  --destination PATH --idempotency-key KEY --reuse-snapshot YYYY-MM-DD
  [--execute] [--inspect-only | --preflight-only] [--plaintext]
  [--browser-channel chrome|msedge|chromium] [--timeout-seconds N] [--max-bytes N]
```

Без `--execute` выполняется только выбор пригодной записи, без авторизации и сохранения секретов. `INSPECTED` подтверждает чтение конкретного снимка, `TRANSPORT_VERIFIED` — также проверку защищённого чтения файлов; в обоих случаях `test_passed: false`, код `0`, дампы не заказываются. Исполнение без этих флагов требует `--plaintext`. Успех выгрузки всех БД снимка — `DB_ONLY`, `test_passed: true`, код `2`; ошибка — `FAILED`, код `1`. Состав конкретного сайта здесь не подтверждается, поэтому `COMPLETE` недоступен.

`panel-job-*` принадлежит аккаунту `panel-*`; блокировка действует на аккаунт. Ключ связывается с аккаунтом, датой и абсолютным назначением. Изменение параметров того же ключа отклоняется. Приватный журнал зашифрован; публичные отчёт и события содержат только псевдонимы и обезличенную диагностику. Фаза `SUBMITTING` записывается перед запросом; повтор сначала ищет фактический результат по сохранённому исходному списку каталога.

Манифест отдельного панельного задания имеет `schema_version: "1.0"`, `job_id`, `account_ref`, `status: "DB_ONLY"`, `artifacts` с `db_alias`, `relative_path`, `size_bytes`, `sha256`, `validation`. Дополнительные поля: `source_snapshot_date`, `source_snapshot_at: null`, `snapshot_time_precision: "day"`, `capture_consistency: "PROVIDER_SNAPSHOT"`, `encrypted: false`, `files_included: false`, `application_restore_tested: false`. Точное время снимка не выводится из даты или времени скачивания. `warnings` содержит `FILES_NOT_INCLUDED` и `RESTORE_NOT_TESTED`.

По текущему указанию пользователя `cleanup_policy: "KEEP_REMOTE"`, `cleanup_verified: false`, а `remote_exports_retained` показывает число известных серверных SQL. `cleanup_pending: false` означает отсутствие запланированного удаления, а не факт очистки. Это манифест выгрузки аккаунта, отдельный от манифеста полного сайта в архиве.

## REST API для расширенной версии

Для управляемой FTPS-очереди добавлены `configuration.ftp_connections` и `task.ftp_connections` (целое 2–5, default 3), `progress.active_files[]` с `path/size_bytes/received_bytes`, `connections_limit`, `active_transfers`, `parallel_limit`. `GET /api/state` дополнен `ftp_connections: {used, limit: 10}` — занятые слоты исполнителя. Поля аддитивны; прежние ключи заданий, расположение копии и манифест сохраняются. [Точная семантика лимитов](22_parallel_ftps.md).

Текущая локальная панель использует `GET /api/state` и `POST /api/checks` на `127.0.0.1`. Команда `backupctl ui [--file PATH] [--mapping PATH] [--destination PATH] [--batch-id KEY] [--port N] [--open]` запускает просмотр массового прохода и управляемую очередь. Локальная сессия содержит имена и логины для оператора, секретные значения исключены. Проверка доступа выполняет вход и чтение файлового списка. Карточки серверов, каталог, управление файлами и БД, порядок и расписание используют дополнительные локальные маршруты. [Первоначальный контракт](14_local_dashboard.md), [реализованные маршруты управления](15_management_and_work_plan.md#локальные-контракты).

| Метод | Endpoint | Назначение |
| --- | --- | --- |
| `GET` | `/v1/sites` | Сайты, их готовность и диагностические статусы |
| `POST` | `/v1/backup-jobs` | Запуск `site_id` и `idempotency_key` |
| `GET` | `/v1/backup-jobs/{id}` | Статус и обезличенный журнал этапов |
| `POST` | `/v1/backup-jobs/{id}/verify` | Проверка сохранённых артефактов |
| `POST` | `/v1/restore-tests` | Изолированный тест восстановления |
| `GET` | `/v1/sites/{id}/backups` | История доступных копий |

Роли: `admin`, `operator`, `auditor`. Операции изменения прав, удаления и восстановления требуют отдельного допуска. Пароли никогда не возвращаются через API.

## Диагностика повторов управляемого FTPS

Приватные события файлового задания могут содержать `curl_error_code` (целое 1–999), `curl_error_kind` (закрытая классификация), `operation` и числовой `ftp_reply_code`, когда он известен. Необработанная строка исключения не сохраняется. Для повторов с checkpoint доступны `committed_bytes`, `progress_advanced` и `retry_total`: смещение данного файла, признак его роста после прошлой неудачи и накопленное число повторов текущей операции. `retry_attempt` сбрасывается при продвижении своего файла; соседний поток на него не влияет. Неактивное продолжение переводит этап в `WAITING`, очищает старые активные передачи и счётчик ошибки, сохраняя ключ и объёмы. [Поведение и проверки](24_large_file_recovery.md).

## Контракт импортёра MySQL JSON dump

```text
inspect_dump(file) -> SourceSchemaReport
extract_table(file, exact_table_name) -> RowSet | SOURCE_TABLE_AMBIGUOUS
validate_mapping(RowSet, Mapping) -> MappingReport
normalize_accounts(RowSet, Mapping) -> CandidatePanelAccounts
match_accounts(CandidatePanelAccounts, SiteRegistry) -> MatchReport
commit_import(approved_candidates) -> ImportedAccountRefs
```

Требования к `SourceSchemaReport`: число записей, перечень ключей/типов, ошибки JSON, пустые обязательные колонки, потенциальные дубликаты. Содержимое паролей и токенов не выводится; их форматы проверяются только в памяти и после этого значения передаются в vault.
