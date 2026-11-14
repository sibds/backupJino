"use strict";

(() => {
  let plan = null;
  let loading = false;
  let pending = false;
  let serverSignature = "";
  const selectedServers = new Set();
  let bulkRequest = null;
  let expandedTasks = new Set();
  let historyLimit = 20;
  const labels = window.backupTaskLabels;
  const node = element;
  const waiting = [
    "QUEUED",
    "SCHEDULED",
    "WAITING_RATE_LIMIT",
    "WAITING_OPERATOR",
    "PAUSED",
  ];

  function note(value, error = false) {
    text("plan-message", value);
    $("plan-message").className = "notice " + (error ? "danger" : "warning");
    $("plan-message").hidden = !value;
  }

  async function request(path, payload) {
    const response = await fetch(
      path,
      payload === undefined
        ? {}
        : {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          }
    );
    const result = await response.json();
    if (!response.ok)
      throw new Error(
        result.message ||
          "Не удалось выполнить действие. Обновите страницу и повторите."
      );
    return result;
  }

  async function change(path, payload) {
    if (pending) return;
    pending = true;
    note("Сохраняем изменения плана…");
    render();
    try {
      const result=await request(path, payload);
      note(result.rules_count ? 'Расписание сохранено: '+result.rules_count+' правил.' : result.tasks_count ? 'В очереди '+result.tasks_count+' заданий. Уже активные работы не дублируются.' : 'План обновлён');
      await load();
      await refresh();
      return result;
    } catch (error) {
      note(error.message, true);
    } finally {
      pending = false;
      render();
    }
  }

  function localDate(value) {
    if (!value) return "";
    const date = new Date(value);
    const adjusted = new Date(
      date.getTime() - date.getTimezoneOffset() * 60000
    );
    return adjusted.toISOString().slice(0, 16);
  }

  function taskButton(label, task, callback, disabled = false) {
    const button = node("button", "button secondary", label);
    button.type = "button";
    button.disabled = pending || disabled;
    button.setAttribute(
      "aria-label",
      label +
        " · " +
        task.server_name +
        " · " +
        (task.kind === "files" ? "файлы" : "БД")
    );
    button.dataset.focusKey = task.id + "-" + label;
    button.addEventListener("click", callback);
    return button;
  }

  function taskRow(task, completed) {
    const row = node("article", "plan-task");
    row.dataset.taskId = task.id;
    const heading = node("div", "plan-task-heading");
    const title = node(
      task.server_deleted ? "span" : "a",
      "plan-task-name",
      task.server_name
    );
    if (!task.server_deleted) title.href = "#server/" + task.row;
    const name = node("div");
    name.append(title, node("p", "secondary-text", task.server_login));
    const stateLabel = node(
      "span",
      "badge " +
        (task.state === "FAILED"
          ? "danger"
          : task.state === "FINISHED"
          ? "good"
          : ""),
      labels[task.state] || "Ожидает"
    );
    heading.append(name, stateLabel);
    row.append(heading);
    if (task.server_deleted)
      row.append(node("p", "helper", "Сервер удалён из списка"));
    else if (task.server_blocked)
      row.append(node("p", "helper", "Сервер заблокирован"));
    const kind =
      task.kind === "files"
        ? "Файлы"
        : task.kind === "databases"
        ? "Дампы БД"
        : "Вход в панель";
    const transport = {
      ftps: "FTPS",
      sftp: "SFTP",
      https_panel: "Панель Jino",
    }[task.transport] || 'Способ не указан';
    row.append(
      node(
        "p",
        "plan-task-meta",
        kind +
          " · " +
          transport +
          (task.priority === "critical" ? " · Критичная работа" : "")
      )
    );
    if (completed) {
      row.append(
        node(
          "p",
          "helper",
          (task.report_timestamp ? "Отчёт от " : "Завершено ") +
            new Date(task.updated_at).toLocaleString("ru-RU")
        )
      );
      if (task.result?.status)
        row.append(
          node(
            "p",
            "secondary-text",
            statuses[task.result.status]?.[0] || "Обработка завершена"
          )
        );
    } else if (task.state === "WAITING_OPERATOR") {
      row.append(
        node(
          "p",
          "notice warning",
          "Входы в панель ожидают действия оператора после CAPTCHA"
        )
      );
    } else if (task.estimated_at && waiting.includes(task.state)) {
      row.append(
        node(
          "p",
          "helper",
          "Ранний допуск " + new Date(task.estimated_at).toLocaleString("ru-RU")
        )
      );
    } else {
      row.append(
        node(
          "p",
          "helper",
          task.state === "RUNNING"
            ? labels[task.stage] || "Обработка задания"
            : "Ожидает свободного исполнителя"
        )
      );
    }
    if (task.error_message)
      row.append(
        node(
          "p",
          task.error_code === "CANCELLED" ? "helper" : "notice danger",
          task.error_message
        )
      );
    const progress = task.progress || {};
    if (progress.received_bytes !== undefined)
      row.append(
        node("p", "helper", "Сохранено " + bytes(progress.received_bytes))
      );
    const controls = node("div", "plan-task-controls");
    const base = "/api/tasks/" + task.id + "/";
    if (!completed) {
      controls.append(
        taskButton(
          "В начало",
          task,
          () => change(base + "priority", { priority: "critical" }),
          !task.can_reorder
        )
      );
      controls.append(
        taskButton(
          "Выше",
          task,
          () => change(base + "move", { direction: "up" }),
          !task.can_reorder || task.queue_rank === 0
        )
      );
      controls.append(
        taskButton(
          "Ниже",
          task,
          () => change(base + "move", { direction: "down" }),
          !task.can_reorder || task.queue_rank === task.queue_total - 1
        )
      );
      const resume = task.state === "PAUSED" || task.state === "PAUSING";
      controls.append(
        taskButton(
          resume ? "Продолжить" : "Пауза",
          task,
          () => change(base + (resume ? "resume" : "pause"), {}),
          task.state === "STOPPING"
        )
      );
      controls.append(
        taskButton(
          "Отменить",
          task,
          () => change(base + "stop", {}),
          task.state === "STOPPING"
        )
      );
      const schedule = node("div", "plan-task-schedule");
      const label = node("label", "", "Начать не раньше");
      const input = node("input");
      input.type = "datetime-local";
      input.value = localDate(task.scheduled_at);
      input.disabled = !task.can_reorder || pending;
      input.setAttribute(
        "aria-label",
        "Время начала · " + task.server_name + " · " + kind
      );
      label.append(input);
      schedule.append(
        label,
        taskButton(
          "Изменить время",
          task,
          () =>
            change(base + "schedule", {
              scheduled_at: input.value
                ? new Date(input.value).toISOString()
                : null,
            }),
          !task.can_reorder
        )
      );
      const more=node('details','plan-task-more');
      more.open=expandedTasks.has(task.id);
      const disclosure=node('summary','','Управление заданием');disclosure.dataset.focusKey=task.id+'-more';
      more.append(disclosure,controls,schedule);
      row.append(more);
    } else if (
      !task.server_deleted &&
      !task.historical &&
      canResumeTask(task)
    ) {
      controls.append(
        taskButton(
          "Продолжить",
          task,
          () => change(base + "resume", {}),
          task.server_blocked
        )
      );
      row.append(controls);
    }
    return row;
  }

  function render() {
    const focused = document.activeElement?.dataset.focusKey;
    expandedTasks=new Set([...document.querySelectorAll('#plan-view .plan-task-more[open]')].map(el=>el.closest('[data-task-id]').dataset.taskId));
    renderSelectionSummary();
    $("resume-panel-queue").disabled = pending;
    if (!plan) return;
    const policy = plan.policy;
    text(
      "policy-status",
      policy.blocked
        ? "CAPTCHA остановила очередь панели. Сервер «" +
            policy.blocked_server_name +
            "». После проверки входа можно разрешить очередь."
        : policy.allowed
        ? "Вход в панель доступен"
        : "Следующий вход не раньше " +
          new Date(policy.next_at).toLocaleString("ru-RU")
    );
    $("resume-panel-queue").hidden = !policy.blocked;
    text("plan-pending-count", plan.pending.length);
    text("plan-completed-count", plan.completed.length);
    text('plan-overview', 'В работе '+plan.pending.filter(t=>t.state==='RUNNING').length+' · Ожидает '+plan.pending.filter(t=>t.state!=='RUNNING').length+' · Повторяющихся правил '+(plan.rules||[]).filter(r=>r.enabled&&!r.stale).length);
    renderRules();
    if (document.activeElement?.matches("#plan-view input")) return;
    for (const [key, completed] of [
      ["pending", false],
      ["completed", true],
    ]) {
      const fragment = document.createDocumentFragment();
      (completed ? plan[key].slice(0, historyLimit) : plan[key]).forEach(
        (task) => fragment.append(taskRow(task, completed))
      );
      if (!plan[key].length)
        fragment.append(
          node(
            "p",
            "plan-empty secondary-text",
            completed
              ? "Выполненных работ пока нет"
              : "Ожидающих работ пока нет. Выберите сервер и добавьте работу в очередь."
          )
        );
      $("plan-" + key).replaceChildren(fragment);
    }
    $("plan-more-history").hidden = plan.completed.length <= historyLimit;
    if (focused)
      for (const button of document.querySelectorAll(
        "#plan-view [data-focus-key]"
      )) {
        if (button.dataset.focusKey === focused)
          button.focus({ preventScroll: true });
      }
  }

  async function load() {
    if (loading) return;
    loading = true;
    try {
      plan = await request("/api/plan");
      render();
    } catch (error) {
      note(error.message, true);
    } finally {
      loading = false;
    }
  }

  function populateServers() {
    if (!state.data) return;
    const visible=visibleServers();
    const signature=JSON.stringify([visible.map(s=>[s.row,s.name,s.login,s.blocked,s.status,s.configuration?.revision]),[...selectedServers],$('plan-files').checked,$('plan-databases').checked,pending]);
    if (signature === serverSignature) return;
    serverSignature = signature;
    const focus=document.activeElement?.dataset.planRow;
    const fragment = document.createDocumentFragment();
    for (const server of visible) {
      const row=node('label','plan-server-choice');
      const checkbox=node('input');checkbox.type='checkbox';checkbox.dataset.planRow=server.row;
      checkbox.checked=selectedServers.has(server.row);checkbox.disabled=pending||!eligible(server)&&!checkbox.checked;
      checkbox.setAttribute('aria-label','Выбрать сервер '+server.name);
      checkbox.addEventListener('change',()=>{checkbox.checked?selectedServers.add(server.row):selectedServers.delete(server.row);populateServers();renderSelectionSummary();});
      const name=node('span','plan-server-title');name.append(node('strong','',server.name),node('span','secondary-text',server.login));
      row.append(checkbox,name,node('span','plan-server-state',server.blocked?'Заблокирован':!eligible(server)?'Нет нужного доступа':labels[server.status]||statuses[server.status]?.[0]||'Готов к планированию'));
      fragment.append(row);
    }
    if (!visible.length) fragment.append(node('p','plan-empty','Серверы не найдены. Измените запрос поиска.'));
    $('plan-servers').replaceChildren(fragment);
    if (focus) $('plan-servers').querySelector('[data-plan-row="'+focus+'"]')?.focus({preventScroll:true});
    renderSelectionSummary();
  }

  function eligible(server) {
    return !server.blocked && (!$('plan-files').checked||server.configuration?.ftp_ready!==false) && (!$('plan-databases').checked||server.can_check!==false);
  }
  function visibleServers() {
    const query=$('plan-search').value.trim().toLocaleLowerCase('ru');
    return (state.data?.servers||[]).filter(s=>!query||(s.name+' '+s.login).toLocaleLowerCase('ru').includes(query));
  }
  function renderSelectionSummary() {
    const kinds=Number($('plan-files').checked)+Number($('plan-databases').checked);
    const valid=selectedServers.size>0 && kinds>0 && [...selectedServers].every(row=>{const s=state.data?.servers.find(s=>s.row===row);return s&&eligible(s);});
    text('plan-selected-count','Выбрано '+selectedServers.size);
    const plural=(n,forms)=>forms[n%100>=11&&n%100<=14?2:n%10===1?0:n%10>=2&&n%10<=4?1:2];
    const total=selectedServers.size*kinds;
    text('plan-bulk-summary',!selectedServers.size?'Выберите хотя бы один сервер':!kinds?'Выберите файлы или БД':!valid?'В выборе есть сервер без нужного доступа. Снимите его выбор.':selectedServers.size+' '+plural(selectedServers.size,['сервер','сервера','серверов'])+' · '+(kinds===2?'файлы и БД':$('plan-files').checked?'файлы':'БД')+' · '+total+' '+plural(total,['отдельное задание','отдельных задания','отдельных заданий']));
    $('plan-submit').disabled=pending||!valid;
    $('plan-submit').textContent=$('plan-repeat').value?'Сохранить расписание':'Добавить в очередь';
    $('plan-apply-action').disabled=pending||!selectedServers.size||!kinds;
    $('plan-clear').disabled=pending||!selectedServers.size;
    const visible=visibleServers().filter(eligible);
    const checked=visible.filter(s=>selectedServers.has(s.row)).length;
    $('plan-select-all').checked=visible.length>0&&checked===visible.length;
    $('plan-select-all').indeterminate=checked>0&&checked<visible.length;
    $('plan-select-all').disabled=pending||!visible.length;
    for (const id of ['plan-files','plan-databases','plan-repeat','plan-start-at','plan-priority','plan-bulk-action']) $(id).disabled=pending;
    for (const input of $('plan-servers').querySelectorAll('input')) {
      const server=state.data?.servers.find(s=>s.row===Number(input.dataset.planRow));
      input.disabled=pending||!server||!eligible(server)&&!input.checked;
    }
  }
  function intervalLabel(hours) {
    return [...$('plan-repeat').options].find(o=>Number(o.value)===hours)?.textContent || hours+' ч';
  }
  function renderRules() {
    const fragment=document.createDocumentFragment();
    const rules=plan.rules||[];
    text('plan-rules-count',rules.length);
    for (const rule of rules) {
      const row=node('article','plan-rule');
      const name=node('a','plan-task-name',rule.server_name);name.href='#server/'+rule.row;
      const info=node('div');info.append(name,node('p','secondary-text',(rule.kind==='files'?'Файлы':'БД')+' · '+intervalLabel(rule.every_hours)));
      const timing=node('div');timing.append(node('strong','',rule.stale?'Привязка изменилась':rule.blocked?'Сервер заблокирован':!rule.enabled?'Выключено':'Следующая постановка'),node('p','helper',new Date(rule.next_at).toLocaleString('ru-RU')));
      if (rule.last_error) timing.append(node('p','danger-text',rule.last_error));
      const toggle=node('button','button secondary',rule.enabled?'Выключить':'Включить');toggle.type='button';toggle.disabled=pending||!rule.enabled&&(rule.stale||rule.blocked);
      toggle.dataset.focusKey=rule.id+'-toggle';
      toggle.setAttribute('aria-label',toggle.textContent+' расписание · '+rule.server_name+' · '+(rule.kind==='files'?'файлы':'БД'));
      toggle.addEventListener('click',()=>change('/api/plan/action',{rows:[rule.row],files:rule.kind==='files',databases:rule.kind==='databases',rule_id:rule.id,action:rule.enabled?'disable_schedule':'enable_schedule'}));
      row.append(info,timing,toggle);fragment.append(row);
    }
    if (!rules.length) fragment.append(node('p','plan-empty secondary-text','Расписаний пока нет. Выберите серверы и период повторения выше.'));
    $('plan-rules').replaceChildren(fragment);
  }

  function route() {
    const visible = location.hash === "#plan";
    document.body.classList.toggle('plan-mode',visible);
    $("plan-view").hidden = !visible;
    if (!visible) return;
    state.view = "plan";
    $("server-view").hidden = true;
    $("server-card").hidden = true;
    $("events-view").hidden = true;
    text("page-title", "План работ");
    text("page-description", "Групповые выгрузки, расписания и текущая очередь");
    for (const button of document.querySelectorAll("[data-view]")) {
      const active = button.dataset.view === "plan";
      button.classList.toggle("selected", active);
      if (active) button.setAttribute("aria-current", "page");
      else button.removeAttribute("aria-current");
    }
    populateServers();
    load();
  }

  $("plan-add-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      rows:[...selectedServers].sort((a,b)=>a-b),files:$('plan-files').checked,databases:$('plan-databases').checked,
      every_hours:$('plan-repeat').value?Number($('plan-repeat').value):null,
      priority: $("plan-priority").value,
    };
    if ($("plan-start-at").value)
      payload.scheduled_at = new Date($("plan-start-at").value).toISOString();
    const signature=JSON.stringify(payload);
    if (!bulkRequest||bulkRequest.signature!==signature) bulkRequest={signature,id:crypto.randomUUID()};
    const result=await change('/api/plan/bulk',{...payload,request_id:bulkRequest.id});
    if (result?.ok) bulkRequest=null;
  });
  $('plan-search').addEventListener('input',populateServers);
  $('plan-select-all').addEventListener('change',event=>{for(const s of visibleServers().filter(eligible)) event.target.checked?selectedServers.add(s.row):selectedServers.delete(s.row);populateServers();renderSelectionSummary();});
  $('plan-clear').addEventListener('click',()=>{selectedServers.clear();populateServers();renderSelectionSummary();});
  for(const id of ['plan-files','plan-databases','plan-repeat']) $(id).addEventListener('change',()=>{populateServers();renderSelectionSummary();});
  $('plan-apply-action').addEventListener('click',()=>change('/api/plan/action',{rows:[...selectedServers],files:$('plan-files').checked,databases:$('plan-databases').checked,action:$('plan-bulk-action').value}));
  $("plan-more-history").addEventListener("click", () => {
    historyLimit += 20;
    render();
  });
  $("resume-panel-queue").addEventListener("click", () =>
    change("/api/panel-policy/resume", {})
  );
  document.querySelector('[data-view="plan"]').addEventListener("click", () => {
    location.hash = "plan";
    route();
  });
  document.addEventListener("backup:state", () => {
    populateServers();
    if (location.hash === "#plan") load();
  });
  addEventListener("hashchange", route);
  route();
})();
