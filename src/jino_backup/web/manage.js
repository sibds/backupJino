"use strict";

(() => {
  let data = null;
  let selected = null;
  let selectedTab = "overview";
  let detail = null;
  let fetching = false;
  let formFor = null;
  let pending = false;
  let jobFocus = null;
  let serverChoicesSignature = "";
  const labels = {
    SCHEDULED: "Запланировано",
    WAITING_RATE_LIMIT: "Ожидает часового интервала",
    WAITING_OPERATOR: "Ожидает оператора",
    QUEUE_UPDATED: "Изменён план работ",
    FILE_DOWNLOAD: "Передача файла",
    FILE_ERROR: "Ошибка передачи файла",
    DIRECTORY_ERROR: "Не удалось прочитать каталог",
    SCANNING: "Чтение списка каталога",
    FILE_SKIPPED: "Пропущено несовместимое имя",
    RETRY_WAIT: "Ожидание повторного подключения",
    RECONNECT: "Восстановление соединения",
    RETRY_EXHAUSTED: "Попытки восстановления исчерпаны",
    QUEUED: "В очереди",
    RUNNING: "Выполняется",
    PAUSING: "Ожидает точки паузы",
    PAUSED: "На паузе",
    STOPPING: "Ожидает безопасной остановки",
    STOPPED: "Остановлено",
    FINISHED: "Завершено",
    FAILED: "Ошибка",
    INTERRUPTED: "Прервано перезапуском",
    SCANNING: "Подсчёт файлов и объёма",
    FILES_DOWNLOAD: "Скачивание файлов",
    AUTHENTICATE: "Подключение и вход",
    ACCESS_VERIFIED: "Доступ подтверждён",
    SELECT_SNAPSHOT: "Выбор снимка MySQL",
    DATABASES: "Получение дампов",
    EXPORT_SUBMITTING: "Заказ SQL-выгрузки",
    EXPORT_WAIT: "Ожидание SQL-файла",
    DOWNLOAD: "Скачивание SQL",
    SQL_VERIFIED: "Дамп проверен",
    DB_ONLY: "Дампы проверены",
    REMOTE_EXPORT_RETAINED: "SQL на хостинге сохранён",
    SNAPSHOT_SELECTED: "Снимок выбран",
    PREFLIGHT_TRANSPORT: "Проверка передачи",
    TRANSPORT_READY: "Защищённая передача готова",
    SERVER_ADDED: "Сервер добавлен",
    SERVER_UPDATED: "Настройки сервера сохранены",
    SERVER_BLOCK: "Сервер заблокирован",
    SERVER_UNBLOCK: "Сервер разблокирован",
    SERVER_DELETE: "Сервер удалён из списка",
    PAUSE_REQUESTED: "Запрошена пауза",
    RESUME_REQUESTED: "Запрошено продолжение",
    STOP_REQUESTED: "Запрошена остановка",
    WAITING: "Ожидает запуска",
    LOCAL_COMMIT_RECOVERED: "Восстановлена запись о сохранённом дампе",
    EXPORT_RESPONSE_UNKNOWN: "Проверяется результат отправленного экспорта",
    ALREADY_VERIFIED: "Проверены ранее сохранённые дампы",
  };
  window.backupTaskLabels = labels;
  const activeStates = [
    "QUEUED",
    "RUNNING",
    "PAUSING",
    "PAUSED",
    "STOPPING",
    "SCHEDULED",
    "WAITING_RATE_LIMIT",
    "WAITING_OPERATOR",
  ];
  const node = element;

  async function api(path, payload) {
    const options =
      payload === undefined
        ? {}
        : {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          };
    const response = await fetch(path, options);
    const result = await response.json();
    if (!response.ok)
      throw new Error(
        result.message ||
          "Не удалось выполнить действие. Обновите страницу и повторите."
      );
    return result;
  }

  function message(value, danger = false) {
    const target = $("card-message");
    target.textContent = value;
    target.className = "notice " + (danger ? "danger" : "warning");
    target.hidden = !value;
  }

  async function command(path, payload, done) {
    if (pending) return;
    pending = true;
    message("Выполняем действие…");
    renderJobs();
    try {
      const result = await api(path, payload);
      message("Действие принято");
      if (done) done(result);
      await refresh();
      await loadDetail();
    } catch (error) {
      message(error.message, true);
    } finally {
      pending = false;
      renderJobs();
    }
  }

  function renderOverall() {
    const overall = data?.overall;
    if (!overall) return;
    text("overall-percent", percent(overall.percent));
    $("overall-bar").value = overall.percent;
    text(
      "overall-count",
      (overall.scope === "tasks"
        ? "Обработано заданий "
        : "Обработано аккаунтов ") +
        overall.completed +
        " из " +
        overall.total
    );
    text(
      "overall-errors",
      "Ошибок " +
        overall.failed +
        (overall.paused ? " · На паузе " + overall.paused : "")
    );
    text(
      "overall-note",
      overall.scope === "tasks"
        ? "Счётчик включает завершённые, остановленные и ошибочные задания. Результат каждого задания показан в карточке сервера."
        : "Это ход обработки очереди. Объёмы файлов, дампы и причины неполного копирования показаны по каждому серверу."
    );
    const transfer = overall.files;
    if (transfer) {
      text(
        "overall-file-bytes",
        bytes(transfer.received_bytes) +
          (transfer.planned_bytes !== null
            ? " из " + bytes(transfer.planned_bytes)
            : "")
      );
      $("overall-file-bar").hidden = transfer.percent === null;
      if (transfer.percent !== null)
        $("overall-file-bar").value = transfer.percent;
      text(
        "overall-file-note",
        transfer.percent === null
          ? "Общий объём уточняется при подсчёте дерева в файловых заданиях."
          : "Сохранено " +
              percent(transfer.percent) +
              " от подсчитанного объёма файловых заданий."
      );
    }
  }

  function route() {
    const match = /^#server\/(\d+|new)$/.exec(location.hash);
    const open = !!match;
    $("server-card").hidden = !open;
    if (!open) {
      selected = null;
      detail = null;
      $("server-view").hidden = state.view === "events";
      $("events-view").hidden = state.view !== "events";
      $("page-header-tools")?.removeAttribute("hidden");
      return;
    }
    selected = match[1] === "new" ? "new" : Number(match[1]);
    if (selected !== "new") state.selected = selected;
    populateServerChoices();
    $("server-view").hidden = true;
    $("events-view").hidden = true;
    $("card-block").hidden = selected === "new";
    $("card-delete").hidden = selected === "new";
    $("card-edit").hidden = selected === "new";
    text("page-title", "Карточка сервера");
    text("page-description", "Задания, доступы и журнал выбранного сервера");
    if (selected === "new") {
      detail = null;
      text("card-name", "Новый сервер");
      text("card-login", "Добавьте параметры панели и файлового подключения");
      setTab("settings");
      populateForm(null);
    } else {
      detail = data?.servers.find((server) => server.row === selected)
        ? {
            server: data.servers.find((server) => server.row === selected),
            events: [],
          }
        : null;
      setTab("overview");
      renderCard();
      loadDetail();
    }
    $("server-card").scrollIntoView({ block: "start" });
    $("card-name").focus({ preventScroll: true });
  }

  function setTab(tab) {
    selectedTab = tab;
    for (const button of document.querySelectorAll("[data-card-tab]")) {
      const active = button.dataset.cardTab === tab;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
      button.hidden = selected === "new" && !active;
    }
    for (const panel of document.querySelectorAll("[data-card-panel]"))
      panel.hidden = panel.dataset.cardPanel !== tab;
    if (tab === "settings") populateForm(detail?.server || null);
    if (tab === "logs") loadDetail();
  }

  async function loadDetail() {
    if (selected === null || selected === "new" || fetching) return;
    const row = selected;
    fetching = true;
    try {
      const result = await api("/api/servers/" + row);
      if (selected === row) {
        detail = result;
        renderCard();
      }
    } catch (error) {
      if (selected === row) message(error.message, true);
    } finally {
      fetching = false;
      if (selected !== row && selected !== null && selected !== "new")
        loadDetail();
    }
  }

  function populateServerChoices() {
    const servers = data?.servers || [];
    const signature = JSON.stringify(
      servers.map((server) => [
        server.row,
        server.name,
        server.login,
        server.blocked,
      ])
    );
    if (signature !== serverChoicesSignature) {
      serverChoicesSignature = signature;
      const options = document.createDocumentFragment();
      for (const server of servers) {
        const option = node(
          "option",
          "",
          server.name +
            " · " +
            server.login +
            (server.blocked ? " · Заблокирован" : "")
        );
        option.value = String(server.row);
        options.append(option);
      }
      $("card-server-select").replaceChildren(options);
    }
    $("card-server-select").value = selected === "new" ? "" : String(selected);
  }

  function renderCard() {
    const server = detail?.server;
    if (!server || selected === null || selected === "new") return;
    text("card-name", server.name);
    text(
      "card-login",
      server.login + (server.blocked ? " · Заблокирован" : "")
    );
    text("card-block", server.blocked ? "Разблокировать" : "Заблокировать");
    text("card-access", server.availability.message);
    $("card-check-panel").disabled =
      server.blocked || server.busy || !server.can_check || pending;
    text("card-folder", server.folder_path);
    const facts = document.createDocumentFragment();
    for (const [label, value] of [
      ["Файлов проверено", count(server.progress.verified_files)],
      ["Файлы сохранены", bytes(server.progress.received_bytes)],
      ["Дампов проверено", count(server.databases.verified_count)],
      ["Исходный снимок", dateLabel(server.databases.snapshot_date)],
    ]) {
      const item = node("div");
      item.append(node("dt", "", label), node("dd", "", value));
      facts.append(item);
    }
    $("card-backup-facts").replaceChildren(facts);
    const config = server.configuration;
    text(
      "card-ftp-endpoint",
      config?.ftp_host
        ? config.ftp_host +
            " · " +
            config.ftp_login +
            " · " +
            config.remote_root
        : "FTP-доступ требует настройки"
    );
    const diagnostics = document.createDocumentFragment();
    const diagnosticItems = displayDiagnostics(server);
    for (const item of diagnosticItems) {
      const row = node("div", "diagnostic");
      row.append(
        node("strong", "", item.source),
        node("span", "", item.message),
        node("code", "", item.code)
      );
      diagnostics.append(row);
    }
    if (!diagnosticItems.length)
      diagnostics.append(
        node("p", "secondary-text", "Зафиксированных ошибок нет")
      );
    $("card-diagnostics").replaceChildren(diagnostics);
    if ($("file-transport").dataset.server !== String(server.row)) {
      for (const option of $("file-transport").options)
        option.disabled =
          option.value !== "https_panel" && option.value !== config?.protocol;
      $("file-transport").value =
        server.latest_tasks?.files?.transport || config?.protocol || "ftps";
      $("file-transport").dataset.server = String(server.row);
    }
    const summary = document.createDocumentFragment();
    for (const [kind, label] of [
      ["files", "Файлы"],
      ["databases", "Базы данных"],
    ]) {
      const task = server.latest_tasks?.[kind];
      const button = node("button", "task-link");
      button.append(
        node("strong", "", label),
        node("span", "", task ? labels[task.state] : "Открыть управление")
      );
      button.addEventListener("click", () => setTab(kind));
      summary.append(button);
    }
    $("card-task-summary").replaceChildren(summary);
    renderJobs();
    if (selectedTab === "logs") renderLogs();
  }

  function actionButton(label, callback, disabled, primary = false) {
    const button = node(
      "button",
      "button " + (primary ? "primary" : "secondary"),
      label
    );
    button.disabled = disabled || pending;
    button.addEventListener("click", callback);
    return button;
  }

  function renderJobs() {
    const server = detail?.server;
    if (!server) return;
    const focused = document.activeElement;
    const focusedArea = focused?.closest(".job-area");
    if (focusedArea && focused.dataset.jobAction)
      jobFocus = { area: focusedArea.id, action: focused.dataset.jobAction };
    else if (!focusedArea) jobFocus = null;
    for (const id of ["card-block", "card-edit", "card-delete"])
      $(id).disabled = pending;
    if (!pending)
      $("skip-unsupported-names").checked =
        server.configuration?.skip_unsupported_names !== false;
    $("skip-unsupported-names").disabled =
      pending ||
      server.blocked ||
      (server.tasks || []).some((task) => activeStates.includes(task.state));
    if (!pending)
      $("ftp-connections").value = String(
        server.configuration?.ftp_connections || 3
      );
    $("ftp-connections").disabled =
      pending ||
      server.blocked ||
      $("file-transport").value !== "ftps" ||
      (server.tasks || []).some((task) => activeStates.includes(task.state));
    for (const kind of ["files", "databases"]) {
      const task = server.latest_tasks?.[kind];
      const target = $(kind + "-job");
      const fragment = document.createDocumentFragment();
      const controls = node("div", "job-controls");
      const active = task && activeStates.includes(task.state);
      const start = () => {
        const payload = {
          kind,
          transport:
            kind === "files" ? $("file-transport").value : "https_panel",
        };
        const startAt = $(kind + "-start-at").value;
        if (startAt) payload.scheduled_at = new Date(startAt).toISOString();
        payload.priority = $(kind + "-priority").value;
        if (kind === "databases" && $("db-snapshot-date").value)
          payload.snapshot_date = $("db-snapshot-date").value;
        command("/api/servers/" + selected + "/jobs", payload);
      };
      const resume = canResumeTask(task);
      const fileResume =
        kind === "files" &&
        resume &&
        $("file-transport").value === task.transport;
      controls.append(
        actionButton(
          fileResume ? "Продолжить" : "Запустить",
          fileResume
            ? () => command("/api/tasks/" + task.id + "/resume", {})
            : start,
          (active && !fileResume) || server.blocked,
          true
        )
      );
      controls.append(
        actionButton(
          resume && kind !== "files" ? "Продолжить" : "Пауза",
          () =>
            command(
              "/api/tasks/" + task.id + "/" + (resume ? "resume" : "pause"),
              {}
            ),
          !task ||
            server.blocked ||
            (kind === "files" && resume) ||
            (!resume &&
              ![
                "RUNNING",
                "QUEUED",
                "SCHEDULED",
                "WAITING_RATE_LIMIT",
                "WAITING_OPERATOR",
              ].includes(task.state))
        )
      );
      controls.append(
        actionButton(
          "Остановить",
          () => command("/api/tasks/" + task.id + "/stop", {}),
          !active || task.state === "STOPPING"
        )
      );
      controls.children[0].dataset.jobAction = "start";
      controls.children[1].dataset.jobAction = "toggle";
      controls.children[2].dataset.jobAction = "stop";
      fragment.append(controls);
      $(kind === "files" ? "file-transport" : "db-snapshot-date").disabled =
        !!active;
      $(kind + "-start-at").disabled = !!active;
      $(kind + "-priority").disabled = !!active;
      if (!task) {
        fragment.append(
          node("p", "secondary-text", "Задание ещё не запускалось из панели")
        );
        fragment.append(
          node(
            "p",
            "helper",
            kind === "files"
              ? "Предыдущий проход сохранил " +
                  bytes(server.progress.received_bytes) +
                  ". Проверено файлов " +
                  count(server.progress.verified_files) +
                  "."
              : "В предыдущем проходе проверено дампов " +
                  count(server.databases.verified_count) +
                  ". Исходный снимок " +
                  dateLabel(server.databases.snapshot_date) +
                  "."
          )
        );
      } else {
        fragment.append(
          node("h4", "job-state", labels[task.state] || task.state)
        );
        fragment.append(
          node("p", "secondary-text", labels[task.stage] || "Обработка задания")
        );
        if (task.next_at || task.scheduled_at)
          fragment.append(
            node(
              "p",
              "helper",
              "Начать не раньше " +
                new Date(task.next_at || task.scheduled_at).toLocaleString(
                  "ru-RU"
                )
            )
          );
        const progress = task.progress;
        if (
          ["RETRY_WAIT", "RECONNECT", "RETRY_EXHAUSTED"].includes(task.stage)
        ) {
          const message =
            task.stage === "RETRY_EXHAUSTED"
              ? "Пять попыток восстановления исчерпаны. Нажмите «Продолжить», чтобы повторить с сохранёнными частями."
              : "Попытка " +
                progress.retry_attempt +
                " из " +
                progress.retry_limit +
                (task.stage === "RETRY_WAIT"
                  ? ". Задержка " +
                    progress.retry_delay_seconds +
                    " с" +
                    (progress.retry_at
                      ? ". Следующая попытка: " +
                        new Date(progress.retry_at).toLocaleTimeString("ru-RU")
                      : "")
                  : ". Восстанавливаем соединение.");
          fragment.append(node("p", "notice warning retry-status", message));
        }
        if (kind === "files") {
          const known = progress.inventory_complete;
          const ratio = known
            ? progress.planned_bytes > 0
              ? Math.min(
                  100,
                  (100 * (progress.received_bytes || 0)) /
                    progress.planned_bytes
                )
              : 0
            : null;
          const bar = node("progress", "job-progress");
          bar.max = 100;
          if (ratio !== null) bar.value = ratio;
          bar.setAttribute("aria-label", "Общий прогресс файлов сервера");
          fragment.append(
            bar,
            node(
              "p",
              "progress-caption",
              bytes(progress.received_bytes || 0) +
                (known
                  ? " из " +
                    bytes(progress.planned_bytes) +
                    " · " +
                    percent(ratio)
                  : " · Объём подсчитывается")
            )
          );
          fragment.append(
            node(
              "p",
              "helper",
              "Найдено файлов " +
                count(progress.planned_files) +
                ", проверено " +
                count(progress.verified_files)
            )
          );
          if (task.transport === "ftps")
            fragment.append(
              node(
                "p",
                "helper",
                "Передач сейчас: " +
                  count(progress.active_transfers) +
                  " · Предел сеансов сервера: " +
                  count(progress.connections_limit || task.ftp_connections || 3)
              )
            );
          if (
            progress.parallel_limit &&
            progress.parallel_limit < (progress.connections_limit || 3) - 1
          )
            fragment.append(
              node(
                "p",
                "helper",
                "После обрывов новые передачи ограничены до " +
                  count(progress.parallel_limit) +
                  " одновременно. Текущие передачи завершаются."
              )
            );
        } else {
          const bar = node("progress", "job-progress");
          bar.max = Math.max(1, progress.expected_count || 1);
          if (progress.expected_count) bar.value = progress.verified_count || 0;
          bar.setAttribute("aria-label", "Проверенные дампы сервера");
          fragment.append(
            bar,
            node(
              "p",
              "progress-caption",
              "Проверено дампов " +
                count(progress.verified_count) +
                (progress.expected_count
                  ? " из " + count(progress.expected_count)
                  : "") +
                " · Сохранено " +
                bytes(progress.received_bytes || 0)
            )
          );
          fragment.append(
            node(
              "p",
              "helper",
              "Исходный снимок " + dateLabel(task.snapshot_date)
            )
          );
        }
        if (kind === "files" && progress.active_files?.length) {
          for (const file of progress.active_files)
            fragment.append(
              node(
                "p",
                "current-job-file",
                file.path +
                  " · " +
                  bytes(file.received_bytes) +
                  " из " +
                  bytes(file.size_bytes)
              )
            );
        } else if (progress.current_file) {
          fragment.append(
            node(
              "p",
              "current-job-file",
              progress.current_file +
                " · " +
                bytes(progress.current_bytes || 0) +
                (progress.current_size
                  ? " из " + bytes(progress.current_size)
                  : "")
            )
          );
        }
        if (task.error_message)
          fragment.append(
            node(
              "p",
              task.error_code === "CANCELLED" ? "helper" : "notice danger",
              task.error_message
            )
          );
        if (kind === "files" && progress.skipped_count)
          fragment.append(
            node(
              "p",
              "helper",
              "Пропущено файлов и каталогов: " +
                count(progress.skipped_count) +
                ". Пути и причины — в журнале сервера."
            )
          );
        if (task.result?.status === "PARTIAL")
          fragment.append(
            node(
              "p",
              "notice warning",
              "Копирование завершено частично. Пропусков имён: " +
                count(task.result.files_skipped_count) +
                ". Других ошибок: " +
                count(
                  Math.max(
                    0,
                    (task.result.file_errors_count || 0) -
                      (task.result.files_skipped_count || 0)
                  )
                )
            )
          );
        if (task.state === "PAUSING" || task.state === "STOPPING")
          fragment.append(
            node(
              "p",
              "helper",
              "Ожидаем завершения текущего сетевого запроса. Серверный экспорт может продолжаться на хостинге."
            )
          );
        if (kind === "databases" && task.state === "PAUSED")
          fragment.append(
            node(
              "p",
              "helper",
              "Приложение на паузе. Уже отправленный экспорт может продолжаться на хостинге."
            )
          );
        fragment.append(
          node(
            "p",
            "helper",
            "Обновлено " + new Date(task.updated_at).toLocaleString("ru-RU")
          )
        );
      }
      target.replaceChildren(fragment);
    }
    if (jobFocus) {
      const area = $(jobFocus.area);
      const preferred = area.querySelector(
        '[data-job-action="' + jobFocus.action + '"]'
      );
      const next =
        preferred && !preferred.disabled
          ? preferred
          : area.querySelector("button:not(:disabled)");
      if (next) next.focus({ preventScroll: true });
      else {
        area.tabIndex = -1;
        area.focus({ preventScroll: true });
      }
    }
  }

  function renderLogs() {
    const fragment = document.createDocumentFragment();
    const selectedEvents =
      $("server-log-filter").value === "skipped"
        ? detail?.skipped_events
        : $("server-log-filter").value === "directories"
        ? detail?.directory_events
        : detail?.events;
    for (const event of selectedEvents || []) {
      const row = node("div", "server-log-row");
      const source =
        event.kind === "files"
          ? "Файлы"
          : event.kind === "databases"
          ? "БД"
          : "Сервер";
      row.append(
        node("time", "", new Date(event.time).toLocaleString("ru-RU")),
        node("span", "", source),
        node(
          "span",
          event.error_code && event.stage !== "FILE_SKIPPED"
            ? "danger-text"
            : "",
          event.message || labels[event.stage] || "Этап задания"
        )
      );
      if (event.error_code) row.append(node("code", "", event.error_code));
      if (event.file) row.append(node("span", "event-file", event.file));
      if (event.directory)
        row.append(node("span", "event-file", "Каталог: " + event.directory));
      if (event.operation)
        row.append(
          node(
            "span",
            "secondary-text",
            "Операция: " +
              event.operation +
              (event.ftp_reply_code
                ? " · Ответ FTP: " + event.ftp_reply_code
                : "")
          )
        );
      if (event.stage === "RETRY_WAIT")
        if (event.committed_bytes !== undefined)
          row.append(node("span", "secondary-text",
            "Сохранено в этом файле: " + bytes(event.committed_bytes) +
            (event.progress_advanced ? ". Есть продвижение; счётчик повторов сброшен." : ". Новые данные не сохранены.")));
      if (event.curl_error_code) {
        const reasons = {
          LOW_SPEED_TIMEOUT: "Нет данных дольше допустимого времени",
          OPERATION_TIMEOUT: "Истекло время сетевой операции",
          CONNECTION_RESET: "Соединение сброшено",
          RECEIVE_ERROR: "Ошибка получения данных",
          SEND_ERROR: "Ошибка отправки данных",
          TRUNCATED_TRANSFER: "Передача завершилась раньше ожидаемого размера",
          DNS_ERROR: "Ошибка определения адреса сервера",
          CONNECT_ERROR: "Не удалось установить соединение",
          TLS_ERROR: "Ошибка защищённого соединения",
          OTHER: "Ошибка передачи",
        };
        row.append(node("span", "secondary-text", "libcurl: " + event.curl_error_code +
          " · " + (reasons[event.curl_error_kind] || reasons.OTHER)));
      }
      if (event.stage === "RETRY_WAIT")
        row.append(
          node(
            "span",
            "secondary-text",
            "Повтор " +
              event.retry_attempt +
              " из " +
              event.retry_limit +
              " через " +
              event.retry_delay_seconds +
              " с"
          )
        );
      if (event.stage === "RETRY_EXHAUSTED")
        row.append(
          node(
            "span",
            "secondary-text",
            "Автоматические попытки исчерпаны. Доступно ручное продолжение."
          )
        );
      if (event.stage === "FILE_SKIPPED") {
        const reasons = {
          WINDOWS_CHARACTERS: "Символы, недопустимые в Windows",
          TRAILING_DOT_OR_SPACE: "Точка или пробел в конце имени",
          WINDOWS_RESERVED_NAME: "Зарезервированное имя Windows",
          INVALID_UTF8_NAME: "Недопустимые байты UTF-8; имя показано escape-последовательностями",
        };
        row.append(
          node(
            "span",
            "secondary-text",
            (event.skipped_kind === "dir"
              ? "Каталог пропущен вместе с содержимым. "
              : "Файл пропущен. ") +
              (reasons[event.skip_reason] || "Имя несовместимо с Windows")
          )
        );
      }
      if (event.received_bytes !== undefined)
        row.append(
          node(
            "span",
            "secondary-text",
            "Сохранено " + bytes(event.received_bytes)
          )
        );
      fragment.append(row);
    }
    if (!selectedEvents?.length)
      fragment.append(
        node(
          "p",
          "secondary-text",
          $("server-log-filter").value === "skipped"
            ? "Пропусков имён пока нет"
            : $("server-log-filter").value === "directories"
            ? "Ошибок чтения каталогов пока нет"
            : "Событий этого сервера пока нет"
        )
      );
    $("server-log").replaceChildren(fragment);
  }

  function populateForm(server) {
    const key = server ? String(server.row) : "new";
    if (formFor === key) return;
    formFor = key;
    const form = $("server-form");
    form.reset();
    const config = server?.configuration;
    for (const name of [
      "name",
      "login",
      "ftp_host",
      "ftp_login",
      "protocol",
      "ftp_port",
      "remote_root",
      "known_hosts",
    ]) {
      if (config?.[name] !== undefined && config[name] !== null)
        form.elements[name].value = config[name];
    }
    text(
      "panel-secret-note",
      config?.panel_credentials_saved
        ? "Пароль сохранён"
        : "Пароль панели ещё не задан"
    );
    text(
      "ftp-secret-note",
      config?.ftp_credentials_saved
        ? "Пароль сохранён"
        : "FTP-пароль ещё не задан"
    );
    $("form-error").hidden = true;
  }

  $("server-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const values = Object.fromEntries(new FormData(form));
    values.ftp_port = Number(values.ftp_port);
    if (detail?.server.configuration?.revision)
      values.revision = detail.server.configuration.revision;
    const submit = form.querySelector('[type="submit"]');
    submit.disabled = true;
    $("form-error").hidden = true;
    try {
      const saved = await api(
        selected === "new" ? "/api/servers" : "/api/servers/" + selected,
        values
      );
      form.elements.panel_password.value = "";
      form.elements.ftp_password.value = "";
      formFor = null;
      await refresh();
      if (selected === "new") location.hash = "server/" + saved.row;
      else {
        await loadDetail();
        setTab("overview");
        message("Настройки сохранены");
      }
    } catch (error) {
      text("form-error", error.message);
      $("form-error").hidden = false;
    } finally {
      submit.disabled = false;
    }
  });
  for (const button of document.querySelectorAll("[data-card-tab]"))
    button.addEventListener("click", () => setTab(button.dataset.cardTab));
  $("add-server").addEventListener("click", () => {
    formFor = null;
    location.hash = "server/new";
    route();
  });
  $("open-server-card").addEventListener("click", () => {
    location.hash = "server/" + state.selected;
  });
  $("card-back").addEventListener("click", () => {
    location.hash = "";
    setView("servers");
  });
  $("card-edit").addEventListener("click", () => {
    formFor = null;
    setTab("settings");
  });
  $("card-block").addEventListener("click", () =>
    command(
      "/api/servers/" +
        selected +
        "/" +
        (detail.server.blocked ? "unblock" : "block"),
      {}
    )
  );
  $("card-delete").addEventListener("click", () => {
    if (
      confirm(
        "Удалить сервер «" +
          detail.server.name +
          "» из списка? Скачанные копии останутся в папке сохранения."
      )
    )
      command("/api/servers/" + selected + "/delete", {}, () => {
        location.hash = "";
        setView("servers");
      });
  });
  $("card-check-panel").addEventListener("click", () =>
    command("/api/checks", { row: selected })
  );
  $("card-refresh-logs").addEventListener("click", loadDetail);
  $("card-server-select").addEventListener("change", () => {
    location.hash = "server/" + $("card-server-select").value;
  });
  $("server-log-filter").addEventListener("change", renderLogs);
  $("skip-unsupported-names").addEventListener("change", () => {
    if (!detail?.server) return;
    command("/api/servers/" + selected, {
      skip_unsupported_names: $("skip-unsupported-names").checked,
      revision: detail.server.configuration.revision,
    });
  });
  $("ftp-connections").addEventListener("change", () => {
    if (!detail?.server) return;
    command("/api/servers/" + selected, {
      ftp_connections: Number($("ftp-connections").value),
      revision: detail.server.configuration.revision,
    });
  });
  $("file-transport").addEventListener("change", renderJobs);
  $("card-copy-folder").addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(detail.server.folder_path);
      message("Путь к папке скопирован");
    } catch {
      message("Выделите путь в карточке и скопируйте его через Ctrl+C");
    }
  });
  $("reason-filter").addEventListener("change", (event) => {
    state.reason = event.target.value;
    renderTable();
  });
  for (const button of document.querySelectorAll("[data-view]"))
    button.addEventListener("click", () => {
      location.hash = "";
    });
  document.addEventListener("backup:select", (event) => {
    location.hash = "server/" + event.detail.row;
  });
  document.addEventListener("backup:state", (event) => {
    data = event.detail;
    populateServerChoices();
    renderOverall();
    if (selected !== null && selected !== "new") loadDetail();
  });
  addEventListener("hashchange", route);
  if (state.data) {
    data = state.data;
    renderOverall();
  }
  route();
})();
