"use strict";

(() => {
  let plan = null;
  let loading = false;
  let pending = false;
  let serverSignature = "";
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
      await request(path, payload);
      note("План обновлён");
      await load();
      await refresh();
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
    }[task.transport];
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
      row.append(controls, schedule);
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
    $("plan-add-form").querySelector('[type="submit"]').disabled = pending;
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
    if (document.activeElement?.matches("#plan-view input")) return;
    const focused = document.activeElement?.dataset.focusKey;
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
        "#plan-view button[data-focus-key]"
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
    const signature = state.data.servers
      .map((server) =>
        [
          server.row,
          server.name,
          server.blocked,
          server.configuration?.revision,
        ].join("|")
      )
      .join(";");
    if (signature === serverSignature) return;
    serverSignature = signature;
    const selected = $("plan-server").value;
    const fragment = document.createDocumentFragment();
    const empty = node("option", "", "Выберите сервер");
    empty.value = "";
    fragment.append(empty);
    for (const server of state.data.servers) {
      const option = node(
        "option",
        "",
        server.name +
          " · " +
          server.login +
          (server.blocked ? " · Заблокирован" : "")
      );
      option.value = String(server.row);
      option.disabled = server.blocked;
      fragment.append(option);
    }
    $("plan-server").replaceChildren(fragment);
    $("plan-server").value = selected;
    selectTransport();
  }

  function selectTransport() {
    const server = state.data?.servers.find(
      (item) => String(item.row) === $("plan-server").value
    );
    const db = $("plan-kind").value === "databases";
    const protocol = server?.configuration?.protocol || "ftps";
    for (const option of $("plan-transport").options)
      option.disabled = db
        ? option.value !== "https_panel"
        : option.value !== "https_panel" && option.value !== protocol;
    $("plan-transport").value = db ? "https_panel" : protocol;
    $("plan-transport").disabled = db;
  }

  function route() {
    const visible = location.hash === "#plan";
    $("plan-view").hidden = !visible;
    if (!visible) return;
    state.view = "plan";
    $("server-view").hidden = true;
    $("server-card").hidden = true;
    $("events-view").hidden = true;
    text("page-title", "План работ");
    text("page-description", "Очередь, приоритет и история выполнения");
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
    const row = Number($("plan-server").value);
    const payload = {
      kind: $("plan-kind").value,
      transport: $("plan-transport").value,
      priority: $("plan-priority").value,
    };
    if ($("plan-start-at").value)
      payload.scheduled_at = new Date($("plan-start-at").value).toISOString();
    await change("/api/servers/" + row + "/jobs", payload);
  });
  $("plan-server").addEventListener("change", selectTransport);
  $("plan-more-history").addEventListener("click", () => {
    historyLimit += 20;
    render();
  });
  $("plan-kind").addEventListener("change", selectTransport);
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
