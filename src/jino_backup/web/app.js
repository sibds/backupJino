"use strict";
// Numeric observation history only: no names, credentials or remote paths.
globalThis.BackupActivity = class BackupActivity {
  constructor(saved) {
    this.previous = null; // A reload starts a fresh measurement; never backfill a closed tab.
    this.minutes = Array.isArray(saved?.minutes) ? saved.minutes.filter(x => x && Number.isFinite(x.time) && x.rows).slice(-60) : [];
    this.recent = [];
    this.advanced = saved?.advanced || {};
    this.started = saved?.started || null;
  }
  observe(data, now) {
    const counters = {};
    for (const server of data.servers || []) {
      for (const [kind, task] of Object.entries(server.latest_tasks || {})) {
        if (!task?.id) continue;
        const p = task.progress || {};
        counters[server.row + ':' + kind] = {
          id: task.id, row: server.row, state: task.state,
          bytes: Math.max(0, Number(p.received_bytes) || 0),
          files: kind === 'files' ? Math.max(0, Number(p.verified_files) || 0) : 0,
        };
      }
    }
    const previous = this.previous;
    this.previous = {time: now, counters};
    this.started ||= now;
    if (!previous || now <= previous.time || now - previous.time > 15000) {
      this.recent = [];
      return;
    }
    const rows = {};
    for (const [key, counter] of Object.entries(counters)) {
      const old = previous.counters[key];
      const value = rows[counter.row] ||= {bytes: 0, files: 0};
      if (!old || old.id !== counter.id || counter.bytes < old.bytes || counter.files < old.files) continue;
      if (![old.state, counter.state].some(s => ['RUNNING', 'PAUSING', 'STOPPING'].includes(s))) continue;
      value.bytes += counter.bytes - old.bytes;
      value.files += counter.files - old.files;
      if (counter.bytes > old.bytes || counter.files > old.files) this.advanced[counter.row] = now;
    }
    this.recent.push({time: now, seconds: (now - previous.time) / 1000, rows});
    this.recent = this.recent.filter(x => x.time > now - 30000);
    const minute = Math.floor(now / 60000) * 60000;
    let bucket = this.minutes.find(x => x.time === minute);
    if (!bucket) {
      bucket = {time: minute, bytes: 0, files: 0, rows: {}};
      this.minutes.push(bucket);
    }
    for (const [row, value] of Object.entries(rows)) {
      bucket.bytes += value.bytes;
      bucket.files += value.files;
      const target = bucket.rows[row] ||= {bytes: 0, files: 0};
      target.bytes += value.bytes;
      target.files += value.files;
    }
    this.minutes = this.minutes.filter(x => x.time > now - 3600000).slice(-60);
  }
  row(row, now) {
    const fresh = this.previous && now - this.previous.time <= 15000;
    const samples = fresh ? this.recent.filter(x => x.time > now - 30000 && (row === null || x.rows[row])) : [];
    const seconds = samples.reduce((n, x) => n + x.seconds, 0);
    const total = samples.reduce((n, x) => n + (row === null
      ? Object.values(x.rows).reduce((s, v) => s + v.bytes, 0) : x.rows[row].bytes), 0);
    const minute = this.minutes.find(x => x.time === Math.floor(now / 60000) * 60000);
    const values = row === null ? minute : minute?.rows[row];
    return {speed: seconds ? total / seconds : null, bytes: values?.bytes ?? null,
      files: values?.files ?? null, advanced: row === null
        ? Math.max(0, ...Object.values(this.advanced)) || null : this.advanced[row] || null};
  }
  save() { return {minutes: this.minutes, advanced: this.advanced, started: this.started}; }
};
const $ = (id) => document.getElementById(id);
let activity;
try { activity = new BackupActivity(JSON.parse(localStorage.getItem('jino-activity-v1'))); }
catch { activity = new BackupActivity(); }
const state = {
  data: null,
  selected: null,
  filter: "all",
  view: "servers",
  query: "",
  sort: "activity",
  loading: false,
  reason: "",
};
const statuses = {
  SCHEDULED: ["Запланировано", ""],
  WAITING_RATE_LIMIT: ["Ждёт допуска", "warning"],
  WAITING_OPERATOR: ["Ждёт оператора", "warning"],
  IDLE: ["Ожидает запуска", ""],
  BLOCKED: ["Заблокирован", "warning"],
  PAUSED: ["На паузе", "warning"],
  PAUSING: ["Ставится на паузу", "warning"],
  STOPPING: ["Останавливается", "warning"],
  RUNNING: ["В работе", "info"],
  QUEUED: ["В очереди", ""],
  NEEDS_REVIEW: ["Проверить данные", "warning"],
  FAILED: ["Ошибка", "danger"],
  PARTIAL: ["Частичный результат", "warning"],
  FILES_ONLY: ["Файлы получены", "good"],
  DB_ONLY: ["Получены только БД", "warning"],
  COMPLETE: ["Копия готова", "good"],
  STOPPED: ["Остановлено", "warning"],
  INTERRUPTED: ["Прервано", "warning"],
  UNKNOWN: ["Нет данных", ""],
};
const stages = {
  RETRY_WAIT: "Ожидает переподключения",
  RETRY_EXHAUSTED: "Попытки восстановления исчерпаны",
  AUTHENTICATE: "Вход",
  ACCESS_VERIFIED: "Доступ подтверждён",
  DATABASES: "Базы данных",
  FILES_DOWNLOAD: "Файлы",
  FINISHED: "Обработан",
  RECONNECT: "Переподключение",
  WAITING: "Ожидает запуска",
};
const access = {
  WAITING_RATE_LIMIT: ["Ждёт допуска", "warning"],
  WAITING_OPERATOR: ["Ждёт оператора", "warning"],
  WAITING_JOB: ["Ждёт задание", "muted"],
  BLOCKED: ["Заблокирован", "warning"],
  UNKNOWN: ["Не проверен", "muted"],
  CONFIRMED: ["Подтверждён", "good"],
  TRANSFERRING: ["Есть передача", "good"],
  AVAILABLE: ["Доступен", "good"],
  CHECKING: ["Проверяется", "warning"],
  UNAVAILABLE: ["Нет подключения", "danger"],
  AUTH_ERROR: ["Ошибка входа", "danger"],
  ATTENTION: ["Нужно действие", "warning"],
  NEEDS_REVIEW: ["Уточнить данные", "warning"],
};
function text(id, value) {
  $(id).textContent = value ?? "—";
}
function element(tag, className, value) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (value !== undefined) node.textContent = value;
  return node;
}
function bytes(value) {
  if (value === null || value === undefined) return "—";
  const units = ["Б", "КБ", "МБ", "ГБ", "ТБ"];
  let n = Math.max(0, Number(value) || 0),
    i = 0;
  while (n >= 1000 && i < units.length - 1) {
    n /= 1000;
    i++;
  }
  return (
    new Intl.NumberFormat("ru-RU", { maximumFractionDigits: i ? 2 : 0 }).format(
      n
    ) +
    " " +
    units[i]
  );
}
function count(value) {
  return new Intl.NumberFormat("ru-RU").format(value || 0);
}
function timeLabel(value) {
  if (!value) return "В текущем задании";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? "Время неизвестно"
    : date.toLocaleTimeString("ru-RU", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      });
}
function dateLabel(value) {
  if (!value) return "Ещё не определён";
  const date = new Date(value + "T12:00:00Z");
  return Number.isNaN(date.getTime())
    ? "Дата неизвестна"
    : date.toLocaleDateString("ru-RU");
}
function age(value) {
  if (!value) return "";
  const seconds = Math.max(0, (Date.now() - new Date(value).getTime()) / 1000);
  if (seconds < 60) return "только что";
  if (seconds < 3600) return Math.floor(seconds / 60) + " мин назад";
  return Math.floor(seconds / 3600) + " ч назад";
}
function needsAttention(server) {
  return (
    ["FAILED", "NEEDS_REVIEW", "STOPPED", "PARTIAL", "DB_ONLY"].includes(
      server.status
    ) ||
    ["UNAVAILABLE", "AUTH_ERROR", "ATTENTION", "NEEDS_REVIEW"].includes(
      server.availability.state
    )
  );
}
function badge(server) {
  const [label, tone] = statuses[server.status] || statuses.UNKNOWN;
  return element("span", "badge " + tone, label);
}
function percent(value) {
  return (
    new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 1 }).format(value) +
    "%"
  );
}
function current(server) {
  return server.stage === "DATABASES"
    ? server.databases.current_file
    : server.progress.current_file;
}
function saved(server) {
  return server.progress.received_bytes + server.databases.received_bytes;
}
function serverWorkProgress(server) {
  const tasks = { ...(server.latest_tasks || {}) };
  if (!tasks.files && !tasks.databases) {
    const files = server.progress || {};
    const db = server.databases || {};
    if (
      files.metadata_available ||
      files.received_bytes > 0 ||
      files.current_file
    )
      tasks.files = {
        state: server.status,
        progress: files,
        result: {
          status: ["FILES_ONLY", "COMPLETE"].includes(server.status)
            ? "FILES_ONLY"
            : null,
        },
      };
    if (db.verified_count > 0 || db.expected_count > 0 || db.current_file)
      tasks.databases = {
        state: db.state === "DB_ONLY" ? "FINISHED" : server.status,
        progress: db,
        result: { status: db.state },
      };
  }
  const parts = [];
  for (const kind of ["files", "databases"]) {
    const task = tasks[kind];
    if (!task) continue;
    const progress = task.progress || {};
    const label =
      kind === "databases"
        ? "БД"
        : task.transport === "https_panel"
        ? "HTTPS"
        : task.transport === "sftp"
        ? "SFTP"
        : "FTP";
    const successful =
      (task.state === "FINISHED" ||
        ["FILES_ONLY", "COMPLETE"].includes(task.state)) &&
      (kind === "files"
        ? ["FILES_ONLY", "COMPLETE"].includes(task.result?.status)
        : ["DB_ONLY", "COMPLETE"].includes(task.result?.status));
    let fraction = null;
    let detail = statuses[task.state]?.[0] || "Подготовка";
    if (successful) {
      fraction = 1;
      detail = "готово";
    } else if (task.result?.status === "PARTIAL") {
      detail = "частично";
    } else if (
      [
        "QUEUED",
        "SCHEDULED",
        "WAITING_RATE_LIMIT",
        "WAITING_OPERATOR",
      ].includes(task.state) &&
      !(kind === "files"
        ? progress.received_bytes > 0
        : progress.verified_count > 0)
    ) {
      fraction = 0;
    } else if (kind === "files") {
      if (progress.inventory_complete && progress.planned_bytes > 0) {
        fraction = Math.max(
          0,
          Math.min(
            0.99,
            (progress.received_bytes || 0) / progress.planned_bytes
          )
        );
        detail = percent(fraction * 100);
      } else {
        detail = "объём уточняется";
      }
    } else if (progress.expected_count > 0) {
      fraction = Math.max(
        0,
        Math.min(0.99, (progress.verified_count || 0) / progress.expected_count)
      );
      detail =
        count(progress.verified_count) + "/" + count(progress.expected_count);
    }
    parts.push({
      fraction,
      caption: label + ": " + detail,
      active: ["RUNNING", "PAUSING", "STOPPING"].includes(task.state),
      partial: task.result?.status === "PARTIAL",
    });
  }
  return {
    percent:
      parts.length && parts.every((part) => part.fraction !== null)
        ? (parts.reduce((total, part) => total + part.fraction, 0) * 100) /
          parts.length
        : null,
    parts: parts.map((part) => part.caption),
    active: parts.some((part) => part.active),
    partial: parts.some((part) => part.partial),
  };
}
function toast(message) {
  text("toast", message);
  $("toast").hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => ($("toast").hidden = true), 5000);
  text("announcement", message);
}
function displayDiagnostics(server, { table = false } = {}) {
  const task = server.latest_tasks?.files;
  const newerFtpsResult =
    task?.transport === "ftps" &&
    (task.error_code ||
      task.progress?.directories_seen > 0 ||
      task.progress?.verified_files > 0 ||
      task.progress?.inventory_complete);
  return (server.diagnostics || [])
    .filter((item) => !(table && (item.source === "Файлы HTTPS" || item.source === "FTP" && newerFtpsResult)))
    .map((item) =>
      item.source === "FTP"
        ? {
            ...item,
            historical: true,
            source: table ? "FTP ранее" : "Прежняя проверка FTP",
            message: table
              ? item.message
              : item.message +
                ". Текущее подключение этим результатом не проверено",
          }
        : item.source === "Файлы HTTPS"
        ? {
            ...item,
            historical: true,
            source: "Прежняя выгрузка HTTPS",
            message: item.message + ". Результат прежнего прохода, не состояние текущего задания",
          }
        : item
    )
    .sort((a, b) => Number(!!a.historical) - Number(!!b.historical));
}
function visibleServers() {
  if (!state.data) return [];
  let rows = state.data.servers.filter((server) => {
    const reasons = {
      AUTH: [
        "AUTH_FAILED",
        "AUTH_OR_ROOT_FAILED",
        "FTP_LOGIN_DENIED",
        "FTP_ROOT_DENIED",
      ],
      TLS: ["TLS_CERTIFICATE_INVALID", "TLS_HANDSHAKE_FAILED"],
      CONNECTION: [
        "CONNECTION_FAILED",
        "PANEL_RESPONSE_FAILED",
        "INTERNAL_ERROR",
        "OPERATION_TIMEOUT",
        "TRANSFER_TIMEOUT",
        "TRANSFER_INTERRUPTED",
      ],
      CONFIG: [
        "REMOTE_PATH_UNSAFE",
        "FTP_CREDENTIAL_MISSING",
        "ACCOUNT_AMBIGUOUS",
        "CREDENTIAL_MISSING",
      ],
    };
    if (
      state.reason &&
      !displayDiagnostics(server, { table: true }).some((item) =>
        (reasons[state.reason] || [state.reason]).includes(item.code)
      )
    )
      return false;
    if (
      state.query &&
      !`${server.name} ${server.login}`
        .toLocaleLowerCase("ru")
        .includes(state.query)
    )
      return false;
    if (state.filter === "active") return server.status === "RUNNING";
    if (state.filter === "queued")
      return [
        "QUEUED",
        "SCHEDULED",
        "WAITING_RATE_LIMIT",
        "WAITING_OPERATOR",
      ].includes(server.status);
    if (state.filter === "attention") return needsAttention(server);
    return true;
  });
  const priority = (server) =>
    server.status === "RUNNING"
      ? 0
      : needsAttention(server)
      ? 1
      : server.status === "QUEUED"
      ? 2
      : 3;
  rows.sort((a, b) =>
    state.sort === "name"
      ? a.name.localeCompare(b.name, "ru")
      : state.sort === "bytes"
      ? saved(b) - saved(a)
      : priority(a) - priority(b) || a.row - b.row
  );
  return rows;
}
const rowCommands = new Set();
const rowTransports = new Map();
const rowActiveStates = new Set([
  "QUEUED",
  "SCHEDULED",
  "WAITING_RATE_LIMIT",
  "WAITING_OPERATOR",
  "RUNNING",
  "PAUSING",
  "PAUSED",
  "STOPPING",
]);
async function rowCommand(server, kind, path, payload) {
  const key = server.row + ":" + kind;
  if (rowCommands.has(key)) return;
  rowCommands.add(key);
  renderTable();
  try {
    const response = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    if (!response.ok)
      throw new Error(result.message || "Не удалось выполнить действие");
    toast("Действие принято: " + (kind === "files" ? "файлы" : "базы данных"));
    await refresh();
  } catch (error) {
    toast(error.message);
  } finally {
    rowCommands.delete(key);
    renderTable();
  }
}
function canResumeTask(task) {
  return (
    !!task &&
    (["PAUSED", "PAUSING", "STOPPED", "INTERRUPTED", "FAILED"].includes(
      task.state
    ) ||
      (task.kind === "files" &&
        task.state === "FINISHED" &&
        task.result?.status === "PARTIAL"))
  );
}
function rowControls(server) {
  const cell = element("td", "row-controls");
  for (const kind of ["files", "databases"]) {
    const task = server.latest_tasks?.[kind];
    const active = task && rowActiveStates.has(task.state);
    const resume = canResumeTask(task);
    const fileResume = kind === "files" && resume;
    const busy = rowCommands.has(server.row + ":" + kind);
    const legacyBusy =
      server.busy && !Object.keys(server.latest_tasks || {}).length;
    const group = element("div", "row-job");
    group.dataset.kind = kind;
    group.setAttribute("role", "group");
    group.setAttribute(
      "aria-label",
      (kind === "files" ? "Файлы: " : "Базы данных: ") + server.name
    );
    let transport = "https_panel";
    if (kind === "files") {
      const protocol = server.configuration?.protocol || "ftps";
      transport = active
        ? task.transport
        : rowTransports.get(server.row) || (resume ? task.transport : protocol);
      const select = element("select", "row-transport");
      select.setAttribute("aria-label", "Транспорт файлов: " + server.name);
      for (const [value, label] of [
        [protocol, protocol.toUpperCase()],
        ["https_panel", "HTTPS"],
      ]) {
        const option = element("option", "", label);
        option.value = value;
        select.append(option);
      }
      select.value = transport;
      select.disabled = busy || !!active || server.blocked || legacyBusy;
      select.dataset.row = server.row;
      select.dataset.rowAction = "transport";
      select.addEventListener("change", () => {
        rowTransports.set(server.row, select.value);
        renderTable();
      });
      select.title =
        "Файлы · " +
        (transport === "https_panel"
          ? "HTTPS через панель"
          : transport.toUpperCase());
      group.append(select);
    } else group.append(element("span", "row-kind", "БД"));
    const configured =
      kind === "databases" || transport === "https_panel"
        ? server.can_check
        : server.configuration?.ftp_ready;
    const transportMismatch =
      resume && kind === "files" && transport !== task.transport;
    const actions = [
      [
        "start",
        "Запустить",
        "M8 5l11 7-11 7z",
        !!active ||
          server.blocked ||
          legacyBusy ||
          !configured ||
          (fileResume && !transportMismatch),
      ],
      [
        "toggle",
        resume && !fileResume ? "Продолжить" : "Пауза",
        resume && !fileResume ? "M8 5l11 7-11 7z" : "M8 5v14M16 5v14",
        !task ||
          server.blocked ||
          transportMismatch ||
          fileResume ||
          (!resume &&
            ![
              "RUNNING",
              "QUEUED",
              "SCHEDULED",
              "WAITING_RATE_LIMIT",
              "WAITING_OPERATOR",
            ].includes(task.state)),
      ],
      [
        "stop",
        "Остановить",
        "M6 6h12v12H6z",
        !active || task.state === "STOPPING",
      ],
    ];
    for (const [action, label, path, disabled] of actions) {
      const button = element("button", "button row-task-button");
      button.type = "button";
      button.disabled = busy || !!disabled;
      button.dataset.row = server.row;
      button.dataset.rowAction = action;
      button.setAttribute("aria-label", label);
      button.title =
        label +
        " · " +
        (kind === "files" ? "Файлы" : "БД") +
        " · " +
        server.name;
      if (action === "toggle" && transportMismatch)
        button.title =
          "Для продолжения выберите " +
          task.transport.replace("https_panel", "HTTPS").toUpperCase();
      const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
      svg.setAttribute("viewBox", "0 0 24 24");
      svg.setAttribute("aria-hidden", "true");
      const shape = document.createElementNS(svg.namespaceURI, "path");
      shape.setAttribute("d", path);
      svg.append(shape);
      button.append(svg);
      button.addEventListener("click", () =>
        rowCommand(
          server,
          kind,
          action === "start"
            ? "/api/servers/" + server.row + "/jobs"
            : "/api/tasks/" +
                task.id +
                "/" +
                (action === "stop" ? "stop" : resume ? "resume" : "pause"),
          action === "start" ? { kind, transport } : {}
        )
      );
      group.append(button);
    }
    if (fileResume) {
      const button = element(
        "button",
        "button primary row-continue",
        "Продолжить"
      );
      button.type = "button";
      button.dataset.row = server.row;
      button.dataset.rowAction = "resume";
      button.disabled = busy || server.blocked || transportMismatch;
      button.title = transportMismatch
        ? "Для продолжения выберите " +
          task.transport.replace("https_panel", "HTTPS").toUpperCase()
        : "Продолжить прежнюю выгрузку с сохранёнными частями";
      button.addEventListener("click", () =>
        rowCommand(server, kind, "/api/tasks/" + task.id + "/resume", {})
      );
      group.append(button);
    }
    if (task) {
      const status = element(
        "span",
        "row-task-state",
        task.stage === "RETRY_WAIT"
          ? "Переподключение " +
              task.progress.retry_attempt +
              " из " +
              task.progress.retry_limit
          : window.backupTaskLabels?.[task.state] || task.state
      );
      if (transportMismatch)
        status.textContent +=
          " · Для продолжения выберите " +
          task.transport.replace("https_panel", "HTTPS").toUpperCase();
      if (task.error_message && task.error_code !== "CANCELLED")
        status.title = task.error_message;
      group.append(status);
    }
    cell.append(group);
  }
  return cell;
}
function renderTable() {
  const rows = visibleServers();
  const focused = document.activeElement;
  const focus = focused?.dataset.row
    ? {
        row: focused.dataset.row,
        action: focused.dataset.rowAction,
        kind: focused.closest(".row-job")?.dataset.kind,
      }
    : null;
  const fragment = document.createDocumentFragment();
  for (const server of rows) {
    const tr = element("tr", server.row === state.selected ? "selected" : "");
    tr.dataset.serverRow = server.row;
    tr.setAttribute("aria-selected", String(server.row === state.selected));
    const nameCell = element("td");
    const button = element("button", "server-name", server.name);
    button.dataset.row = server.row;
    button.title = server.name;
    button.addEventListener("click", () => selectServer(server.row, true));
    nameCell.append(
      button,
      element("span", "server-login", server.login),
      element(
        "span",
        "mobile-stage",
        stages[server.stage] || statuses[server.status]?.[0] || "Ожидает"
      )
    );
    for (const diagnostic of displayDiagnostics(server, { table: true }).slice(
      0,
      2
    )) {
      nameCell.append(
        element(
          "span",
          "row-diagnostic" + (diagnostic.historical ? " historical" : ""),
          diagnostic.source + " · " + diagnostic.message
        )
      );
    }
    tr.append(nameCell);
    const accessCell = element("td");
    const [genericLabel, tone] =
      access[server.availability.state] || access.UNKNOWN;
    const label = ["ATTENTION", "AUTH_ERROR", "UNAVAILABLE"].includes(
      server.availability.state
    )
      ? server.availability.message
      : genericLabel;
    const labelNode = element("span", "availability " + tone);
    labelNode.append(
      element("span", "status-light " + tone),
      element("span", "", label)
    );
    accessCell.append(labelNode);
    if (server.availability.checked_at)
      accessCell.append(
        element("span", "availability-sub", age(server.availability.checked_at))
      );
    tr.append(accessCell);
    const stageCell = element("td", "stage-column");
    stageCell.append(badge(server));
    if (server.status === "RUNNING")
      stageCell.append(
        element(
          "span",
          "availability-sub",
          stages[server.stage] || "Подготовка"
        )
      );
    tr.append(stageCell);
    const sizeCell = element("td", "bytes-column");
    sizeCell.append(
      element("span", "availability-sub row-progress-label", "Прогресс сервера")
    );
    sizeCell.append(
      element(
        "span",
        "row-bytes",
        saved(server) > 0 ? bytes(saved(server)) : "—"
      )
    );
    const work = serverWorkProgress(server);
    if (work.parts.length) {
      const p = element("progress", "row-progress");
      p.max = 100;
      if (work.percent !== null) p.value = work.percent;
      p.setAttribute("aria-label", "Общий прогресс заданий сервера");
      const caption =
        work.percent !== null
          ? percent(work.percent)
          : work.active
          ? "Прогресс уточняется"
          : work.partial
          ? "Частичный результат"
          : "Общий объём неизвестен";
      p.setAttribute("aria-valuetext", caption + "; " + work.parts.join("; "));
      p.title =
        "Последние задания файлов и БД учитываются как равные части работы. Процент отдельного файла не используется.";
      if (work.percent !== null || work.active) sizeCell.append(p);
      sizeCell.append(
        element("div", "row-percent", caption),
        element("span", "availability-sub", work.parts.join(" · "))
      );
    }
    const movement = activity.row(server.row, Date.now());
    const speedLine = element("span", "availability-sub activity-row-speed", formatSpeed(movement.speed));
    speedLine.title = "Средняя скорость сохранения за последние 30 секунд наблюдения";
    const movementLine = element("span", "availability-sub",
      movement.files === null ? "Собираем данные о движении" :
      `За эту минуту: +${count(movement.files)} файлов · ${formatMB(movement.bytes)} МБ`);
    const lastLine = element("span", "availability-sub",
      movement.advanced ? "Последний прирост: " + age(new Date(movement.advanced).toISOString()) : "Прирост пока не зафиксирован");
    const activityCell = element("div", "server-activity");
    activityCell.append(speedLine, movementLine, lastLine);
    if (server.latest_tasks && Object.keys(server.latest_tasks).length) nameCell.append(activityCell);
    tr.append(sizeCell);
    tr.append(rowControls(server));
    fragment.append(tr);
  }
  $("server-rows").replaceChildren(fragment);
  $("empty-state").hidden = rows.length > 0;
  document.querySelector(".table-scroll").hidden = rows.length === 0;
  text(
    "visible-count",
    `Показано ${rows.length} из ${state.data?.servers.length || 0}`
  );
  if (focus) {
    const row = document.querySelector(`tr[data-server-row="${focus.row}"]`);
    const group = focus.kind
      ? row?.querySelector(`.row-job[data-kind="${focus.kind}"]`)
      : row;
    const preferred = focus.action
      ? group?.querySelector(`[data-row-action="${focus.action}"]`)
      : row?.querySelector(".server-name");
    const next =
      preferred && !preferred.disabled
        ? preferred
        : group?.querySelector("button:not(:disabled)") ||
          row?.querySelector(".server-name");
    next?.focus({ preventScroll: true });
  }
}
function selectServer(row, move = false) {
  state.selected = row;
  renderTable();
  renderDetail();
  document.dispatchEvent(new CustomEvent("backup:select", { detail: { row } }));
  if (window.location.hash.startsWith("#server/")) return;
  if (move && window.innerWidth <= 920) {
    $("detail").scrollIntoView({
      block: "start",
      behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches
        ? "instant"
        : "smooth",
    });
    $("detail").focus({ preventScroll: true });
  }
}
function renderDetail() {
  const server = state.data?.servers.find((x) => x.row === state.selected);
  $("detail-empty").hidden = !!server;
  $("detail-content").hidden = !server;
  if (!server) return;
  text("detail-name", server.name);
  text("detail-login", server.login);
  const [status, tone] = statuses[server.status] || statuses.UNKNOWN;
  text("detail-status", status);
  $("detail-status").className = "badge " + tone;
  text("detail-access", server.availability.message);
  text(
    "detail-access-time",
    server.availability.checked_at
      ? "Подтверждено в " + timeLabel(server.availability.checked_at)
      : "Отдельная проверка ещё не выполнена"
  );
  const checking = server.availability.state === "CHECKING";
  $("check-one").disabled = !server.can_check || server.busy || checking;
  $("check-one").querySelector("span").textContent = server.busy
    ? "Используем текущую передачу"
    : checking
    ? "Проверка выполняется…"
    : "Проверить доступ";
  const file = current(server);
  text(
    "current-title",
    server.stage === "DATABASES" ? "Текущий SQL-файл" : "Текущий файл"
  );
  text(
    "current-name",
    file?.name || "Подробности появятся после начала передачи"
  );
  text(
    "current-percent",
    file?.percent !== null && file?.percent !== undefined
      ? percent(file.percent)
      : ""
  );
  $("current-progress").hidden = !file || file.percent === null;
  if (file?.percent !== null && file?.percent !== undefined)
    $("current-progress").value = file.percent;
  text(
    "current-received",
    file ? "Сохранено " + bytes(file.received_bytes) : ""
  );
  text(
    "current-total",
    file
      ? file.size_bytes
        ? "из " + bytes(file.size_bytes)
        : "Объём уточняется"
      : ""
  );
  const stamp = file?.updated_at || server.progress.updated_at;
  text(
    "current-note",
    file
      ? "Последняя запись " +
          (stamp ? age(stamp) : "время неизвестно") +
          ". Проверка целого файла — после скачивания."
      : "Общий объём дерева заранее неизвестен. Ожидание в очереди не означает недоступность сервера."
  );
  text(
    "db-count",
    count(server.databases.verified_count) +
      (server.databases.expected_count
        ? " из " + count(server.databases.expected_count)
        : "")
  );
  text("db-date", dateLabel(server.databases.snapshot_date));
  $("db-stale").hidden = !server.databases.snapshot_stale;
  text(
    "db-note",
    server.databases.state === "DB_ONLY"
      ? "Сохранённые дампы прошли проверку. Восстановление сайта не проверялось."
      : server.databases.current_file
      ? server.busy && server.stage === "DATABASES"
        ? "Скачивается дамп. Он ещё не считается проверенным."
        : "Сохранена часть дампа. Этап ещё не завершён."
      : "Используются доступные снимки хостинга. Исходная дата сохраняется отдельно."
  );
  if ($("folder-path").value !== server.folder_path)
    $("folder-path").value = server.folder_path;
  const error =
    server.error_message ||
    (!server.can_check ? server.availability.message : null);
  $("detail-error").hidden = !error;
  text("detail-error", error || "");
}
function renderEvents() {
  const events = state.data?.events || [];
  const fragment = document.createDocumentFragment();
  for (const event of events) {
    const row = element("div", "event-row");
    row.append(
      element("span", "event-time", timeLabel(event.time)),
      element("span", "event-name", event.name),
      element("span", "event-message", event.message)
    );
    fragment.append(row);
  }
  $("event-list").replaceChildren(fragment);
  $("events-empty").hidden = events.length > 0;
  text("event-count", events.length ? "Последние " + events.length : "");
}
function render() {
  if (!state.data) return;
  const { summary, worker, servers } = state.data;
  if (state.selected === null)
    state.selected = (
      servers.find((s) => s.status === "RUNNING") || servers[0]
    )?.row;
  $("demo-banner").hidden = !state.data.demo;
  const running = worker.state === "RUNNING";
  text(
    "worker-label",
    running
      ? "Выгрузка работает"
      : worker.state === "WAITING"
      ? "Очередь ожидает запуска"
      : worker.state === "PAUSED"
      ? "Выгрузка на паузе"
      : worker.state === "FINISHED"
      ? "Очередь обработана"
      : "Исполнитель не запущен"
  );
  $("worker-light").className =
    "status-light " + (running ? "good" : "warning");
  text(
    "worker-description",
    state.data.report_error
      ? "Отчёт пока недоступен. Исполнитель выгрузки работает отдельно от панели."
      : running
      ? `Сейчас ${summary.active} в работе, ${summary.queued} ожидают. Предыдущие копии сохраняются.`
      : worker.state === "WAITING"
      ? "Порядок и время запуска доступны в разделе «План работ»."
      : worker.state === "PAUSED"
      ? "Продолжить или остановить задания можно в карточках серверов."
      : worker.state === "FINISHED"
      ? "Проверьте отдельные результаты и ошибки: обработанная очередь не равна полной копии."
      : "Сохранённый прогресс на месте. Просмотр панели не запускает выгрузку."
  );
  text("total-bytes", bytes(summary.received_bytes));
  text(
    "ftp-budget",
    state.data.ftp_connections
      ? "FTPS: занято слотов " +
          state.data.ftp_connections.used +
          " из " +
          state.data.ftp_connections.limit
      : "FTPS: общий предел 10 сеансов"
  );
  text("total-files", count(summary.verified_files));
  text("total-databases", count(summary.verified_databases));
  text("count-all", summary.total);
  text("count-active", summary.active);
  text("count-queued", summary.queued);
  text("count-attention", servers.filter(needsAttention).length);
  text("update-time", "Данные панели на " + timeLabel(state.data.generated_at));
  text(
    "check-queue",
    state.data.checks_pending
      ? "Проверок в очереди: " + state.data.checks_pending
      : ""
  );
  if (state.view === "events") renderEvents();
  else {
    renderTable();
    renderDetail();
  }
}
function formatMB(value) {
  return value === null ? "—" : (value / 1000000).toLocaleString("ru-RU", {maximumFractionDigits: 2});
}
function formatSpeed(value) {
  if (value === null) return "Нет замера скорости";
  if (value === 0) return "0 КБ/с";
  return value >= 1000000 ? formatMB(value) + " МБ/с" :
    (value / 1000).toLocaleString("ru-RU", {maximumFractionDigits: 1}) + " КБ/с";
}
function activityChart(id, buckets, field) {
  const svg = $(id);
  if (!svg) return;
  const ns = "http://www.w3.org/2000/svg";
  const fragment = document.createDocumentFragment();
  const max = Math.max(1, ...buckets.map(x => x?.[field] || 0));
  const description = document.createElementNS(ns, "title");
  description.textContent = field === "bytes" ? "Загруженные МБ по минутам; точные значения в таблице ниже" : "Проверенные файлы по минутам; точные значения в таблице ниже";
  fragment.append(description);
  const grid = document.createElementNS(ns, "line");
  for (const [key, value] of Object.entries({x1:0, x2:600, y1:109, y2:109})) grid.setAttribute(key, value);
  grid.setAttribute("class", "activity-baseline"); fragment.append(grid);
  const scale = document.createElementNS(ns, "text");
  scale.setAttribute("x", "0"); scale.setAttribute("y", "12"); scale.setAttribute("class", "activity-scale");
  scale.textContent = field === "bytes" ? formatMB(max === 1 ? 0 : max) + " МБ" : count(max === 1 && !buckets.some(x => x?.files) ? 0 : max) + " файлов";
  fragment.append(scale);
  buckets.forEach((bucket, index) => {
    const bar = document.createElementNS(ns, "rect");
    const height = bucket ? Math.max(2, (bucket[field] / max) * 84) : 2;
    for (const [key,value] of Object.entries({x:index * 20 + 3, y:109-height, width:14, height})) bar.setAttribute(key,value);
    bar.setAttribute("class", !bucket ? "activity-missing" : field === "files" ? "activity-file-bar" : "activity-byte-bar");
    const title = document.createElementNS(ns, "title");
    title.textContent = bucket ? new Date(bucket.time).toLocaleTimeString("ru-RU", {hour:"2-digit",minute:"2-digit"}) + ": " +
      (field === "bytes" ? formatMB(bucket.bytes) + " МБ" : count(bucket.files) + " файлов") : "Нет наблюдений";
    bar.append(title); fragment.append(bar);
  });
  svg.replaceChildren(fragment);
}
function renderActivity() {
  if (!$("activity-heading")) return;
  const now = Date.now(), current = Math.floor(now / 60000) * 60000;
  const overall = activity.row(null, now);
  text("activity-speed", formatSpeed(overall.speed));
  text("activity-volume", formatMB(overall.bytes) + " МБ");
  text("activity-files", overall.files === null ? "—" : count(overall.files) + " файлов");
  const fresh = activity.previous && now - activity.previous.time <= 15000;
  text("activity-status", !fresh ? "Нет свежих наблюдений: включите автообновление или нажмите «Обновить»." :
    overall.speed === null ? "Первый замер получен. Скорость появится после следующего обновления." :
    overall.advanced ? "Последний прирост: " + age(new Date(overall.advanced).toISOString()) : "Данные обновляются; прирост файлов и байтов пока не зафиксирован.");
  const buckets = Array.from({length:30}, (_, i) => activity.minutes.find(x => x.time === current - (29-i)*60000));
  activityChart("activity-bytes-chart", buckets, "bytes");
  activityChart("activity-files-chart", buckets, "files");
  const fragment = document.createDocumentFragment();
  for (let i=29; i>=0; i--) {
    const bucket = buckets[i];
    if (!bucket) continue;
    const tr = element("tr");
    tr.append(element("td", "", new Date(bucket.time).toLocaleTimeString("ru-RU", {hour:"2-digit", minute:"2-digit"}) + (bucket.time === current ? " · идёт" : "")),
      element("td", "", formatMB(bucket.bytes)), element("td", "", count(bucket.files)));
    fragment.append(tr);
  }
  if (!fragment.childNodes.length) {
    const tr = element("tr"), td = element("td", "", "История появится после второго замера.");
    td.colSpan=3; tr.append(td); fragment.append(tr);
  }
  $("activity-history-rows").replaceChildren(fragment);
}
async function refresh() {
  if (state.loading) return;
  state.loading = true;
  $("refresh").disabled = true;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10000);
  try {
    const response = await fetch("/api/state", {
      signal: controller.signal,
      cache: "no-store",
    });
    if (!response.ok)
      throw new Error(response.status === 401 ? "SESSION" : "READ");
    state.data = await response.json();
    activity.observe(state.data, Date.now());
    try { localStorage.setItem('jino-activity-v1', JSON.stringify(activity.save())); } catch { /* Storage is optional. */ }
    $("connection-error").hidden = true;
    render();
    renderActivity();
    document.dispatchEvent(
      new CustomEvent("backup:state", { detail: state.data })
    );
  } catch (error) {
    text(
      "connection-error",
      error.message === "SESSION"
        ? "Сессия локальной панели истекла. Перезагрузите страницу."
        : "Данные временно не обновляются. Выгрузка работает отдельно; попробуйте «Обновить»."
    );
    $("connection-error").hidden = false;
    renderActivity();
    if (state.data) renderTable();
  } finally {
    clearTimeout(timer);
    state.loading = false;
    $("refresh").disabled = false;
  }
}
async function check(payload) {
  try {
    const response = await fetch("/api/checks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    if (!response.ok)
      throw new Error(
        result.message || "Не удалось поставить проверку в очередь"
      );
    const queued = result.results.filter((x) => x.queued).length;
    toast(
      queued
        ? "Проверка поставлена в очередь: " +
            queued +
            ". Активная выгрузка продолжается."
        : "По активным серверам используем данные текущей передачи."
    );
    await refresh();
  } catch (error) {
    toast(error.message);
  }
}
function setFilter(value) {
  state.filter = value;
  for (const button of document.querySelectorAll("[data-filter]")) {
    const active = button.dataset.filter === value;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
  }
  renderTable();
}
function setView(value) {
  state.view = value;
  const events = value === "events";
  $("server-view").hidden = events;
  $("events-view").hidden = !events;
  text(
    "page-title",
    events ? "Журнал" : value === "availability" ? "Доступность" : "Серверы"
  );
  text(
    "page-description",
    events
      ? "События реальной выгрузки и последние этапы"
      : value === "availability"
      ? "Доступ к хостингу для резервного копирования"
      : "Ход выгрузки и доступ к резервному копированию"
  );
  for (const button of document.querySelectorAll("[data-view]")) {
    const active = button.dataset.view === value;
    button.classList.toggle("selected", active);
    if (active) button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
  }
  if (value === "availability") setFilter("attention");
  else if (!events) setFilter("all");
  render();
}
$("search").addEventListener("input", (event) => {
  state.query = event.target.value.trim().toLocaleLowerCase("ru");
  renderTable();
});
$("sort").addEventListener("change", (event) => {
  state.sort = event.target.value;
  renderTable();
});
for (const button of document.querySelectorAll("[data-filter]"))
  button.addEventListener("click", () => setFilter(button.dataset.filter));
for (const button of document.querySelectorAll("[data-view]"))
  button.addEventListener("click", () => setView(button.dataset.view));
$("refresh").addEventListener("click", refresh);
$("check-all").addEventListener("click", () => check({ all: true }));
$("check-one").addEventListener("click", () => check({ row: state.selected }));
$("reset-filters").addEventListener("click", () => {
  $("search").value = "";
  state.query = "";
  setFilter("all");
});
$("back-to-list").addEventListener("click", () => {
  $("search").scrollIntoView({ block: "start" });
  $("search").focus({ preventScroll: true });
});
$("copy-folder").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText($("folder-path").value);
    toast("Путь к папке скопирован");
  } catch {
    $("folder-path").focus();
    $("folder-path").select();
    toast("Путь выделен. Нажмите Ctrl+C, чтобы скопировать.");
  }
});
setInterval(() => {
  if ($("auto-refresh").checked && !document.hidden) refresh();
  else if (!document.hidden) { renderActivity(); if (state.data && state.view === "servers") renderTable(); }
}, 3000);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden && $("auto-refresh").checked) refresh();
});
refresh();
