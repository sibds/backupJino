"use strict";

(() => {
  let data = null;
  let selected = null;
  let selectedTab = "overview";
  let detail = null;
  let fetching = false;
  let formFor = null;
  let pending = false;
  let requestedTab = null;
  let notificationSignature = '';
  const exportChoices = new Map();
  const exportAssignments = new Map();
  let jobFocus = null;
  let serverChoicesSignature = "";
  const databaseSelections = new Map();
  let databaseScope = null;
  let databaseVisible = [];
  const labels = {
    ACTION_REQUIRED: "Нужно ваше решение",
    OPERATOR_DEFERRED: "Отложено по вашему выбору",
    OPERATOR_DECISION_SKIPPED: "Разбор отложен оператором",
    OPERATOR_ACTION_REQUIRED: "Запрошено решение оператора",
    AUTO_RESUME_WAIT: "Ожидает автоматического продолжения",
    FIRST_COPY_WAIT: "Ожидает завершения первой полной копии",
    SOURCE_REFRESH: "Обновление изменившегося источника",
    SOURCE_REFRESH_WAIT: "Ожидает повторной загрузки изменившихся файлов",
    DB_REDOWNLOAD_WAIT: "Ожидает повторного скачивания дампа",
    SCHEDULED: "Запланировано",
    WAITING_RATE_LIMIT: "Ожидает часового интервала",
    WAITING_OPERATOR: "Ожидает оператора",
    QUEUE_UPDATED: "Изменён план работ",
    FILE_DOWNLOAD: "Передача файла",
    FILE_VERIFY: "Проверка сохранённых файлов",
    FILE_ERROR: "Ошибка передачи файла",
    FILE_CONFLICT: "Нужно решить конфликт файлов",
    FILE_RECONCILED: "Файлы сравнены",
    DIRECTORY_ERROR: "Не удалось прочитать каталог",
    SCANNING: "Чтение списка каталога",
    FILE_SKIPPED: "Объект пропущен",
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
    RESUMING_FILES: "Продолжение недокачанных файлов",
    VERIFICATION_WAIT: "Ожидает очереди проверки",
    AUTHENTICATE: "Подключение и вход",
    ACCESS_VERIFIED: "Доступ подтверждён",
    SELECT_SNAPSHOT: "Выбор снимка MySQL",
    DATABASES: "Получение дампов",
    EXPORT_SUBMITTING: "Заказ SQL-выгрузки",
    EXPORT_WAIT: "Ожидание SQL-файла",
    DOWNLOAD: "Скачивание SQL",
    SQL_VERIFIED: "Дамп проверен",
    SQL_WARNING: "Предупреждение проверки SQL",
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
    INSPECTED: "Список БД прочитан",
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

  let notificationRenderVersion=0;
  async function renderOperatorNotifications() {
    const target=$('operator-notifications');
    if (!target) return;
    const version=++notificationRenderVersion;
    const servers=await Promise.all((data?.servers || []).map(async server=>{
      if (!operatorDecisionRequests([server]).some(({task})=>task.operator_request.code==='FILE_CONFLICT_PENDING')) return server;
      try {
        const response=await fetch('/api/servers/'+server.row,{cache:'no-store'});
        if (!response.ok) return server;
        const current=await response.json();
        return {...server,conflict_events:current.conflict_events};
      } catch {return server;}
    }));
    if (version!==notificationRenderVersion) return;
    const requests=operatorDecisionRequests(servers);
    const signature=JSON.stringify(requests.map(({server,task})=>[server.row,server.name,task.id,task.operator_request.id]));
    if (signature===notificationSignature) return;
    const expanded=target.querySelector('details')?.open;
    notificationSignature=signature;target.hidden=!requests.length;
    const content=document.createDocumentFragment();
    content.append(node('h2','','Нужно ваше решение · '+requests.length),node('p','helper','Остальные задания продолжаются. Откройте запрос, сравните варианты и выберите действие.'));
    const list=node('ul','operator-request-list');
    const remaining=node('ul','operator-request-list');let index=0;
    for (const {server,task} of requests) {
      const item=node('li','');
      const link=node('a','',server.name+' · '+(task.kind==='databases'?'Базы данных':'Файлы'));
      link.href='#server/'+server.row;
      link.addEventListener('click',event=>{
        event.preventDefault();
        if (location.hash===link.hash) setTab(task.kind);
        else {requestedTab=task.kind;location.hash=link.hash;}
      });
      const reason=task.operator_request.inspection_pending ? 'Сверяем перечень файлов и папок' : task.error_message || (task.operator_request.code==='FILE_CONFLICT_PENDING'?'Выберите вариант файла':task.operator_request.code);
      item.append(link,node('span','secondary-text',reason));(index++<3?list:remaining).append(item);
    }
    content.append(list);
    if (requests.length>3) {
      const more=node('details','operator-more');more.open=!!expanded;
      more.append(node('summary','','Ещё запросов: '+(requests.length-3)),remaining);content.append(more);
    }
    target.replaceChildren(content);
  }

  function renderOperatorRequest(fragment,task) {
    if (detail?.server?.blocked || detail?.server?.deleted) {
      fragment.append(node('p','helper','Сервер заблокирован. Сохранённая причина: '+(task.error_message || task.operator_request?.code || task.operator_deferred?.request?.code)+'. Решение потребуется после разблокировки.'));
      return;
    }
    if (task.operator_deferred && !task.operator_request) {
      const section=node('section','operator-request');
      section.append(node('h4','','Вы отложили этот вопрос'),node('p','helper','Данные сохранены. Этот запрос не появится в общем списке снова, пока вы сами к нему не вернётесь. Остальные задания продолжаются.'));
      section.append(actionButton('Вернуться к решению',()=>command('/api/tasks/'+task.id+'/decision',{
        request_id:task.operator_deferred.request.id,action:'reopen'}),pending));
      fragment.append(section);return;
    }
    const request=task.operator_request;
    if (['CANCELLED','OPERATION_TIMEOUT','TRANSFER_TIMEOUT','LOW_SPEED_TIMEOUT','EXPORT_TIMEOUT','DB_EXPORT_TIMEOUT'].includes(request?.code)) {
      fragment.append(node('p','helper','Передача прервалась. Автоматическое продолжение поставит это же задание в очередь с сохранением скачанных частей. Ваше решение не требуется.'));
      return;
    }
    if (request?.code==='FILE_CONFLICT_PENDING' && !operatorDecisionRequests([{tasks:[task],conflict_events:detail?.conflict_events}]).length) return;
    const section=node('section','operator-request');
    section.setAttribute('aria-label','Решение по заданию');
    section.append(node('h4','','Что сделать дальше?'));
    const descriptions={
      REMOTE_DESTINATION_EXISTS:'На Jino найдены файлы экспорта, не связанные с текущим заданием. Они не удалены. Можно прочитать их список и выбрать подходящий SQL.',
      FILE_CONFLICT_PENDING:'У файлов есть разные варианты. Сравните размеры и даты ниже и выберите, какой сохранить.',
      TLS_CERTIFICATE_INVALID:'Не удалось подтвердить защищённое соединение. Проверьте доступ и сертификат, затем повторите подключение.',
      SERVER_CONFIG_CHANGED:'Привязка сервера изменилась. Сохранённые части остаются в прежней папке; новое задание требует подтверждения новой привязки.'
    };
    section.append(node('p','',descriptions[request.code] || task.error_message || 'Не удалось выполнить операцию. Проверьте причину в журнале и выберите действие.'));
    if (request.inspection_pending) section.append(node('p','notice','Сверка перечня файлов и папок поставлена в очередь'+(task.next_at?' · не раньше '+new Date(task.next_at).toLocaleString('ru-RU'):'')+'. Новый экспорт не заказывается.'));
    const decide=(action,extra={})=>command('/api/tasks/'+task.id+'/decision',{request_id:request.id,action,...extra});
    const candidates=detail?.server?.export_candidates?.[task.id] || [];
    if (request.actions.includes('apply_exports') && candidates.length) {
      if (!exportChoices.has(request.id)) exportChoices.set(request.id,new Map());
      if (!exportAssignments.has(request.id)) exportAssignments.set(request.id,new Map());
      const choices=exportChoices.get(request.id);
      const assignments=exportAssignments.get(request.id);
      section.append(node('p','helper','Дата изменения SQL-файла не подтверждает дату исходного снимка. Совпадение размеров не заменяет проверку содержимого.'));
      const list=node('ul','export-candidate-list');
      for (const candidate of candidates) {
        const item=node('li','export-candidate');
        const filename=typeof candidate.name==='string'?candidate.name:'Имя файла не получено';
        const kind=candidate.kind || 'file';
        item.append(node('strong','event-file',(kind==='dir'?'Папка: ':kind==='link'?'Ссылка: ':'')+(candidate.relative_path || filename)));
        if (kind==='missing') item.append(node('p','helper',candidate.comparison==='missing_remote'?'На Jino не найден при текущем чтении.':'Расположение на Jino не проверено полностью.'));
        else item.append(node('p','helper','На Jino: '+(kind==='file'?bytes(candidate.size_bytes):kind==='dir'?'каталог':'объект без обхода')+' · изменён '+conflictDate(candidate.mtime)));
        const comparisons={same_metadata:'Метаданные совпадают с внутренним перечнем.',different:'Метаданные отличаются от внутреннего перечня.',
          untracked:'Во внутреннем перечне выгрузок не учтён.',known_container:'Каталог связан с учтёнными выгрузками.',
          missing_remote:'Есть во внутреннем перечне, но не найден на Jino.',not_checked:'Сверка неполная: часть перечня не прочитана.'};
        item.append(node('p','helper',comparisons[candidate.comparison] || 'Перечень ещё не сверен. Обновите сверку перед выбором.'));
        if (candidate.internal) item.append(node('p','helper','Внутренний перечень: '+(Number.isFinite(candidate.internal.size_bytes)?bytes(candidate.internal.size_bytes):'размер не указан')+' · дата файла '+conflictDate(candidate.internal.mtime)));
        if (kind==='dir') item.append(node('p','helper','Учтённых вложенных объектов: '+count(candidate.known_children || 0)+' · новых: '+count(candidate.unknown_children || 0)));
        if (candidate.listing_error) item.append(node('p','notice warning','Не удалось прочитать содержимое: '+candidate.listing_error+'. Остальной перечень доступен.'));
        const local=(detail.server.database_inventory || []).find(db=>db.db_alias===candidate.db_alias && db.validation==='PASS');
        if (kind==='file' && filename.endsWith('.sql')) item.append(node('p','helper',local ? 'Локально проверено: '+bytes(local.size_bytes)+' · дата снимка '+(local.snapshot_date || 'не подтверждена')+' · скачано '+dateLabel(local.downloaded_at) : 'Проверенной локальной копии для сравнения нет.'));
        const selectable=candidate.selectable!==false && kind==='file' && !candidate.listing_error;
        const assignable=selectable && !candidate.db_alias && filename.endsWith('.sql');
        if (assignable) {
          item.append(node('p','notice warning','Назначение SQL не подтверждено. Укажите БД только если уверены в соответствии файла.'));
          const label=node('label','','Этот SQL относится к БД');const select=node('select','');
          const empty=node('option','','Выберите БД');empty.value='';select.append(empty);
          for (const db of (detail.server.database_inventory || []).filter(db=>(!task.db_aliases || task.db_aliases.includes(db.db_alias)) && (db.engine || 'mysql')===(candidate.database_engine || 'mysql'))) {
            const option=node('option','',db.name);option.value=db.db_alias;select.append(option);
          }
          select.value=assignments.get(candidate.id) || '';select.disabled=pending;
          select.dataset.jobAction='assign-'+candidate.id;
          select.addEventListener('change',()=>assignments.set(candidate.id,select.value));label.append(select);item.append(label);
        }
        if ((candidate.supported && selectable) || assignable) {
          const label=node('label','','Действие с этим SQL');const select=node('select','');
          select.dataset.jobAction='export-'+candidate.id;
          for (const [value,text] of [['','Выберите действие'],['new_version','Скачать в новую папку версии'],['use_remote','Обновить существующую копию'],['keep_local','Оставить локальную, не скачивать']]) {
            const option=node('option','',text);option.value=value;select.append(option);
          }
          select.value=choices.get(candidate.id) || '';select.disabled=pending;
          select.addEventListener('change',()=>choices.set(candidate.id,select.value));label.append(select);item.append(label);
        } else if (kind==='file') item.append(node('p','helper','Для этого объекта не выбрано скачивание SQL. Автоматическая перезапись не выполняется.'));
        list.append(item);
      }
      section.append(list,actionButton('Применить выбранные действия',()=>{
        const selections=[...choices].filter(([,decision])=>decision).map(([candidate_id,decision])=>({candidate_id,decision,...(assignments.get(candidate_id)?{db_alias:assignments.get(candidate_id)}:{})}));
        if (!selections.length) return message('Выберите действие хотя бы для одного SQL-файла.',true);
        if (selections.some(item=>!candidates.find(candidate=>candidate.id===item.candidate_id)?.db_alias && !item.db_alias)) return message('Укажите БД для каждого выбранного SQL-файла.',true);
        decide('apply_exports',{selections});
      },pending));
    }
    const actions=node('div','job-controls');
    if (request.actions.includes('inspect_exports')) actions.append(actionButton(candidates.length?'Обновить сверку':'Сверить файлы и папки',()=>decide('inspect_exports'),pending));
    if (request.actions.includes('retry')) actions.append(actionButton('Повторить после исправления',()=>decide('retry'),pending));
    if (request.actions.includes('resume_panel')) actions.append(actionButton('Доступ восстановлен — повторить',()=>decide('resume_panel'),pending));
    if (request.actions.includes('start_new_scope')) actions.append(actionButton('Подтвердить новую привязку',()=>decide('start_new_scope'),pending));
    actions.append(actionButton('Пропустить',()=>decide('skip'),pending));
    actions.append(actionButton('Настройки сервера',()=>setTab('settings'),pending));section.append(actions);
    section.append(node('p','helper','Ожидание решения не занимает рабочий поток. Сохранённые копии и части остаются на месте.'));
    fragment.append(section);
  }

  function renderOverall() {
    renderOperatorNotifications();
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
      setTab(requestedTab || "overview");
      requestedTab=null;
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
    const fileCopies = fileCopyHistory(server);
    renderFileVerification($('card-file-verification'), server.latest_tasks?.files);
    renderFileCopyHistory($('card-file-copy-history'),server);
    const facts = document.createDocumentFragment();
    for (const [label, value] of [
      [fileCopies.fromPrevious?'Ранее сохранено файлов':'Файлов сохранено',fileCopies.display.files===null?'Нет данных':count(fileCopies.display.files)],
      [fileCopies.fromPrevious?'Объём предыдущей копии':'Сохранено в текущем запуске',
        fileCopies.display.bytesKnown?bytes(fileCopies.display.bytes):'Объём неизвестен'],
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
    renderDatabaseCatalog();
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
      if (task?.auto_resume_at) fragment.append(node('p','notice warning',
        'Автопродолжение №' + task.auto_resume_count + ': ' + new Date(task.auto_resume_at).toLocaleString('ru-RU') +
        '. ' + (task.auto_resume_message || 'Временный сбой соединения') + '. Сохранённые части будут использованы повторно.'));
      if (kind==='databases' && task?.db_redownload_count && !['FINISHED','STOPPED'].includes(task.state))
        fragment.append(node('p',task.db_redownload_exhausted?'notice danger':'notice warning',
          task.db_redownload_exhausted ? 'Три повтора скачивания исчерпаны. Повреждённый файл сохранён. Проверьте SQL на сервере; затем можно продолжить вручную.' :
          'Повторное скачивание дампа: '+task.db_redownload_count+' из 3. Повреждённая версия остаётся до проверки и замены. Допуск панели сохраняется.'));
      if (kind==='databases' && task?.result?.warnings?.includes('SQL_NO_OBJECTS'))
        fragment.append(node('p','notice warning','Дамп завершён, но не содержит объектов БД. Проверьте, что база действительно пустая. Остальные БД обработаны в обычном порядке.'));
      if (kind==='databases' && (task?.source_snapshot_verified===false || task?.result?.warnings?.includes('EXISTING_EXPORT_SOURCE_DATE_UNVERIFIED')))
        fragment.append(node('p','notice warning','Скачивается ранее существовавший SQL. Дата его исходного снимка не подтверждена.'));
      if (kind === 'databases' && task?.inspect_only) fragment.append(node('p','helper','Только чтение списка БД из Jino. SQL не заказывается.'));
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
          bar.setAttribute("aria-label", "Передача файлов сервера");
          fragment.append(
            bar,
            node(
              "p",
              "progress-caption",
              'Передача: ' + bytes(progress.received_bytes || 0) +
                (known
                  ? " из " +
                    bytes(progress.planned_bytes) +
                    " · " +
                    percent(ratio)
                  : " · Объём подсчитывается")
            )
          );
          const verification = node('div','file-verification-summary');
          renderFileVerification(verification,task);
          fragment.append(verification);
          const history = node('div','file-copy-history');
          renderFileCopyHistory(history,server);fragment.append(history);
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
              "Исходный снимок " + (task.source_snapshot_verified===false ? 'не подтверждён' : dateLabel(task.snapshot_date))
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
        if (task.error_message && !task.operator_request && !task.operator_deferred)
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
        if (task.result?.status === "PARTIAL" && kind==='files')
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
        if (task.result?.status==='PARTIAL' && kind==='databases')
          fragment.append(node('p','notice warning','По вашему выбору не скачано SQL: '+count(task.result.skipped_databases_count)+'. Полнота копии БД этим заданием не подтверждена.'));
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
      const requests=document.createDocumentFragment();
      if (task?.operator_request || task?.operator_deferred) renderOperatorRequest(requests,task);
      if (kind === 'files' && !server.blocked && !server.deleted && !task?.operator_deferred) renderFileConflicts(requests,task);
      $(kind+'-operator-request').replaceChildren(requests);
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

  function conflictDate(value) {
    if (!value) return 'Дата неизвестна';
    if (/^\d{14}(?:\.\d+)?$/.test(value))
      value=value.slice(0,4)+'-'+value.slice(4,6)+'-'+value.slice(6,8)+'T'+value.slice(8,10)+':'+value.slice(10,12)+':'+value.slice(12,14)+'Z';
    else if (/^\d{9,19}(?:\.\d+)?$/.test(value)) {
      const numeric=new Date(Number(value) * (value.split('.')[0].length > 12 ? 0.000001 : 1000));
      if (Number.isNaN(numeric.getTime())) return 'Дата неизвестна';
      value=numeric.toISOString();
    }
    const date=new Date(value);
    return Number.isNaN(date.getTime()) ? 'Дата неизвестна' : date.toLocaleString('ru-RU');
  }

  function renderFileConflicts(fragment, task) {
    if (!task) return;
    const seen=new Set();
    const events=(detail?.conflict_events || []).filter(e=>e.task_id===task.id && e.conflict?.id);
    if (!events.length) return;
    const section=node('section','file-conflicts');
    const unresolved=events.some(event=>!task.conflict_decisions?.[event.conflict.id]);
    section.append(node('h4','',unresolved?'Выберите вариант файла':'Решения по файлам сохранены'),node('p','helper',unresolved?'Остальные файлы продолжают скачиваться. Выбранное действие применяется автоматически; до выбора существующая копия сохраняется.':'Дополнительных действий не требуется. Сохранённые решения будут применены исполнителем.'));
    for (const event of events) {
      const item=event.conflict;
      if (seen.has(item.id)) continue;
      seen.add(item.id);
      const card=node('details','file-conflict');
      card.open=seen.size===1;
      card.append(node('summary','','Различаются: '+item.remote_path+' ↔ '+item.local_path));
      for (const [label,path,size,date] of [['На сервере',item.remote_path,item.remote_size,item.remote_modified],['Сохранённый объект',item.local_path,item.local_size,item.local_modified]]) {
        const line=node('p','');
        line.append(node('strong','',label+': '),node('code','event-file',path),node('span','secondary-text',' · '+(Number.isFinite(size)?bytes(size):'Размер неизвестен')+' · '+conflictDate(date)));
        card.append(line);
      }
      card.append(node('p','helper',item.remote_sha256 && item.local_sha256 ? 'SHA-256 различается: содержимое файлов не совпадает.' : 'Содержимое не сравнено: конфликт типов объектов или каталогов.'));
      const decision=task.conflict_decisions?.[item.id];
      if (decision) card.append(node('p','notice','Решение сохранено: '+({keep_both:'сохранить оба',new_version:'новая папка версии',use_remote:'обновить существующую копию',keep_local:'оставить локальную'}[decision])+(['PAUSED','PAUSING','STOPPED','STOPPING'].includes(task.state)?'. Применится при продолжении задания.':'. Применится автоматически.')));
      else {
        const actions=node('div','job-controls');
        const choose=value=>command('/api/tasks/'+task.id+'/conflict',{conflict_id:item.id,decision:value});
        if (item.reason==='LOCAL_CONTENT_DIFFERS' || item.reason==='CASE_COLLISION') actions.append(actionButton('Новая папка версии',()=>choose('new_version'),pending));
        else actions.append(actionButton('Сохранить оба',()=>choose('keep_both'),pending));
        if (item.reason==='LOCAL_CONTENT_DIFFERS') actions.append(actionButton('Обновить существующую',()=>choose('use_remote'),pending));
        actions.append(actionButton('Оставить локальную',()=>choose('keep_local'),pending));
        card.append(actions);
      }
      section.append(card);
    }
    fragment.append(section);
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
      if (event.conflict) {
        const value=event.conflict;
        row.append(node('span','event-file','Сервер: '+value.remote_path+' ↔ локально: '+value.local_path),
          node('span','secondary-text','Размеры: '+(Number.isFinite(value.remote_size)?bytes(value.remote_size):'неизвестно')+' / '+(Number.isFinite(value.local_size)?bytes(value.local_size):'неизвестно')+' · '+
            (event.action==='identical'?'Содержимое идентично; повторная копия не нужна.':event.action==='updated'?'Серверная версия обновлена; прежняя сохранена.':event.action==='kept_both'?'Обе версии сохранены.':'Содержимое различается либо требует проверки.')));
      }
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
          SYMLINK: "Символическая ссылка; переход по ссылке не выполнялся",
          FIFO: "Именованный канал",
          SOCKET: "Сокет",
          CHAR_DEVICE: "Символьное устройство",
          BLOCK_DEVICE: "Блочное устройство",
        };
        row.append(
          node(
            "span",
            "secondary-text",
            (event.skipped_kind === "dir"
              ? "Каталог пропущен вместе с содержимым. "
              : event.skipped_kind === "special" ? "Специальный объект пропущен. " : "Файл пропущен. ") +
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
  function renderDatabaseCatalog() {
    const target = $("database-catalog-list");
    if (!target || !detail?.server || selected === 'new') return;
    const server = detail.server;
    const items = server.database_inventory || [];
    const busy = (server.tasks || []).some(t => t.kind === 'databases' && activeStates.includes(t.state));
    const disabled = pending || busy || server.blocked || !Array.isArray(server.database_inventory);
    $("database-catalog-refresh").disabled = disabled;
    const expected = Math.max(items.length, ...(server.tasks || []).filter(t => t.kind === 'databases').map(t => t.progress?.expected_count || t.result?.snapshot_databases_count || 0));
    const summary = databaseCatalogSummary(items, expected);
    const missing = databaseMissingAliases(items);
    text('db-download-missing', `Скачать недостающие (${missing.length})`);
    $('db-download-missing').disabled = disabled || !missing.length;
    $('db-download-missing').title = missing.length
      ? 'Все известные БД сервера без проверенной копии, независимо от поиска и выбранных строк'
      : !items.length || expected > items.length
        ? 'Обновите список из Jino, чтобы получить недостающие названия БД'
        : 'Для всех известных БД уже есть проверенные копии';
    const volume = summary.unknownSizes ? summary.unknownSizes === summary.saved ? 'размер неизвестен' : bytes(summary.bytes) + ' + неизвестный объём' : bytes(summary.bytes);
    text('database-catalog-summary', items.length ?
      `Скачано ${summary.saved} из ${summary.total} · Последние копии: ${volume}` :
      'Список ещё не получен. Обновите его из Jino или запустите выгрузку.');
    const scope = selected + ':' + server.configuration.revision;
    if (databaseScope !== scope) {
      databaseScope = scope;
      $('db-bulk-schedule').hidden = true;
      $('db-schedule-toggle').setAttribute('aria-expanded', 'false');
      $('db-search').value = ''; $('db-filter').value = 'all';
    }
    const choices = databaseSelections.get(scope) || new Set();
    const known = new Set(items.map(item => item.db_alias));
    if (Array.isArray(server.database_inventory))
      for (const alias of choices) if (!known.has(alias)) choices.delete(alias);
    databaseSelections.set(scope, choices);
    const query = $('db-search').value.trim().toLocaleLowerCase('ru-RU');
    const filter = $('db-filter').value;
    const visible = items.filter(item => {
      const copy = databaseCopyState(item);
      return item.name.toLocaleLowerCase('ru-RU').includes(query) &&
        (filter === 'all' || filter === 'saved' && copy.saved || filter === 'missing' && !copy.saved ||
          filter === 'newer' && copy.newer || filter === 'scheduled' && item.schedule?.enabled);
    });
    databaseVisible = visible.map(item => item.db_alias);
    text('db-filter-count', summary.total > items.length ? `Показано ${visible.length}; названия получены для ${items.length} из ${summary.total}` : `Показано ${visible.length} из ${items.length}`);
    text('db-selected-count', `Выбрано ${choices.size}`);
    const all = $('db-select-all');
    all.checked = visible.length > 0 && visible.every(item => choices.has(item.db_alias));
    all.indeterminate = !all.checked && visible.some(item => choices.has(item.db_alias));
    all.disabled = !visible.length || pending || server.blocked;
    $('db-download-selected').disabled = disabled || !choices.size;
    for (const id of ['db-schedule-toggle','db-schedule-apply','db-schedule-disable','db-clear-selection'])
      $(id).disabled = pending || server.blocked || !choices.size;
    $('db-schedule-preset').disabled = pending || server.blocked;
    $('db-schedule-hours').disabled = pending || server.blocked || $('db-schedule-preset').value !== 'custom';
    $('db-operation-note').hidden = !busy;
    text('db-operation-note', busy ? 'Задание БД уже выполняется или ожидает запуска. Расписание можно изменить сейчас; новую выгрузку — после завершения задания.' : '');
    const focused = target.contains(document.activeElement) ? document.activeElement.id : null;
    const fragment = document.createDocumentFragment();
    for (const item of visible) {
      const copy = databaseCopyState(item);
      const article = node('tr', 'database-item');
      article.classList.toggle('database-selected', choices.has(item.db_alias));
      const info = node('td', 'database-item-name');
      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox'; checkbox.checked = choices.has(item.db_alias);
      checkbox.disabled = pending || server.blocked;
      checkbox.id = 'db-select-' + item.db_alias;
      checkbox.setAttribute('aria-label', 'Выбрать БД ' + item.name);
      checkbox.addEventListener('change', () => {
        if (checkbox.checked) choices.add(item.db_alias); else choices.delete(item.db_alias);
        renderDatabaseCatalog();
      });
      const name = node('label', 'database-name'); name.htmlFor = checkbox.id;
      name.append(node('span','',item.name), node('span','database-copy-state ' + (copy.saved ? 'good' : ''),
        copy.saved ? 'Скачано · проверено' : 'Нет проверенной копии'));
      const nameGroup = node('div','database-name-group'); nameGroup.append(checkbox,name);
      info.append(nameGroup);
      info.append(node('span','helper',item.engine==='postgresql'?'PostgreSQL':'MySQL'));
      if (item.warnings?.includes('SQL_NO_OBJECTS'))
        info.append(node('span','database-copy-state warning','Дамп без объектов. Проверьте, что БД пустая.'));
      if (item.warnings?.includes('EXISTING_EXPORT_SOURCE_DATE_UNVERIFIED'))
        info.append(node('span','database-copy-state warning','Дата исходного снимка не подтверждена.'));
      const size = node('td','database-size', copy.saved && Number.isSafeInteger(item.size_bytes) && item.size_bytes >= 0 ? bytes(item.size_bytes) : copy.saved ? 'Неизвестен' : '—');
      size.dataset.label = 'Размер копии';
      const downloaded = node('td','database-downloaded'); downloaded.dataset.label = 'Скачано';
      const stamp = item.downloaded_at && !Number.isNaN(Date.parse(item.downloaded_at)) ? new Date(item.downloaded_at) : null;
      downloaded.append(node('span','', copy.saved ? stamp ? stamp.toLocaleDateString('ru-RU') : 'Дата неизвестна' : 'Ещё не скачано'));
      if (copy.saved && stamp) downloaded.append(node('span','helper',stamp.toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'})));
      const snapshot = node('td','database-snapshot'); snapshot.dataset.label = 'Исходный снимок';
      snapshot.append(node('span','',copy.saved ? item.snapshot_date ? dateLabel(item.snapshot_date) : 'Дата неизвестна' : '—'));
      if (copy.newer) snapshot.append(node('span','database-copy-state warning','Есть снимок новее: ' + dateLabel(item.available_snapshot_date)));
      else if (!copy.saved && item.available_snapshot_date) snapshot.append(node('span','helper','Доступен: ' + dateLabel(item.available_snapshot_date)));
      const schedule = item.schedule || {};
      const scheduled = node('td','database-schedule'); scheduled.dataset.label = 'Расписание';
      const edit = actionButton(schedule.enabled ? databaseIntervalLabel(schedule.every_hours) : 'Настроить', () => {
        choices.clear(); choices.add(item.db_alias);
        $('db-bulk-schedule').hidden = false;
        $('db-schedule-toggle').setAttribute('aria-expanded','true');
        const preset = [24,72,168].includes(schedule.every_hours) ? String(schedule.every_hours) : schedule.every_hours ? 'custom' : '168';
        $('db-schedule-preset').value = preset;
        $('db-schedule-hours').value = schedule.every_hours || 72;
        $('db-custom-period').hidden = preset !== 'custom';
        renderDatabaseCatalog(); $('db-schedule-preset').focus();
      }, pending || server.blocked);
      edit.className = 'button text-button database-schedule-action';
      edit.setAttribute('aria-label', 'Расписание БД ' + item.name);
      scheduled.append(edit, node('span','helper database-next',schedule.enabled && schedule.next_at ? 'Следующая постановка: ' + new Date(schedule.next_at).toLocaleString('ru-RU',{dateStyle:'short',timeStyle:'short'}) : 'Выключено'));
      const actions = node('td','database-row-actions');
      const download = actionButton('Скачать', () => downloadDatabases([item.db_alias]), disabled);
      download.setAttribute('aria-label','Скачать БД ' + item.name);
      actions.append(download);
      article.append(info,size,downloaded,snapshot,scheduled,actions); fragment.append(article);
    }
    if (!visible.length) {
      const row = node('tr'), cell = node('td','empty-state', items.length ? 'Нет баз по выбранному фильтру. Измените поиск или выберите «Все базы».' : 'Названия БД ещё не получены. Нажмите «Обновить список из Jino», чтобы поставить чтение списка в очередь.');
      cell.colSpan = 6; row.append(cell); fragment.append(row);
    }
    target.replaceChildren(fragment);
    if (focused) document.getElementById(focused)?.focus({preventScroll:true});
  }
  function selectedDatabases() { return Array.from(databaseSelections.get(databaseScope) || []); }
  function downloadDatabases(aliases) {
    if (!aliases.length) return;
    command('/api/servers/'+selected+'/jobs',{kind:'databases',db_aliases:aliases,new_version:true},
      () => message(`Выгрузка ${aliases.length} БД добавлена в очередь. Используется последний доступный снимок.`));
  }
  $('db-search').addEventListener('input',renderDatabaseCatalog);
  $('db-filter').addEventListener('change',renderDatabaseCatalog);
  $('db-select-all').addEventListener('change', () => {
    const choices = databaseSelections.get(databaseScope);
    for (const alias of databaseVisible) {
      if ($('db-select-all').checked) choices.add(alias); else choices.delete(alias);
    }
    renderDatabaseCatalog();
  });
  $('db-clear-selection').addEventListener('click', () => {
    databaseSelections.get(databaseScope)?.clear(); renderDatabaseCatalog();
  });
  $('db-download-selected').addEventListener('click', () => downloadDatabases(selectedDatabases()));
  $('db-download-missing').addEventListener('click', () =>
    downloadDatabases(databaseMissingAliases(detail?.server?.database_inventory || [])));
  $('db-schedule-toggle').addEventListener('click', () => {
    $('db-bulk-schedule').hidden = !$('db-bulk-schedule').hidden;
    $('db-schedule-toggle').setAttribute('aria-expanded', String(!$('db-bulk-schedule').hidden));
    if (!$('db-bulk-schedule').hidden) $('db-schedule-preset').focus();
  });
  $('db-schedule-preset').addEventListener('change', () => {
    $('db-custom-period').hidden = $('db-schedule-preset').value !== 'custom';
    renderDatabaseCatalog();
  });
  function saveDatabaseSchedule(enabled) {
    const aliases = selectedDatabases();
    if (!aliases.length) return;
    const custom = $('db-schedule-preset').value === 'custom';
    if (enabled && custom && !$('db-schedule-hours').reportValidity()) return;
    const hours = enabled ? Number(custom ? $('db-schedule-hours').value : $('db-schedule-preset').value) : 168;
    command('/api/servers/'+selected+'/database-schedules',{db_aliases:aliases,every_hours:hours,enabled},
      () => message(enabled ? `Расписание сохранено для ${aliases.length} БД. ${databaseIntervalLabel(hours)}.` : `Расписание выключено для ${aliases.length} БД. Копии сохранены.`));
  }
  $('db-bulk-schedule').addEventListener('submit', event => {event.preventDefault(); saveDatabaseSchedule(true);});
  $('db-schedule-disable').addEventListener('click', () => saveDatabaseSchedule(false));
  $("database-catalog-refresh").addEventListener('click',()=>{
    const payload={kind:'databases',inspect_only:true,new_version:true};
    if ($('db-snapshot-date').value) payload.snapshot_date=$('db-snapshot-date').value;
    command('/api/servers/'+selected+'/jobs',payload);
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
