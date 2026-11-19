(() => {
  const MAX_RENDERED_MESSAGES = 200;

  const state = {
    selectedTopic: null,
    search: "",
    paused: false,
    channels: [],
    messages: [],
    retentionHours: 4,
    authenticated: false,
    eventSource: null,
    refreshTimer: null,
  };

  const els = {
    connectionPill: document.getElementById("connection-pill"),
    connectionText: document.getElementById("connection-text"),
    brokerAddress: document.getElementById("broker-address"),
    retentionLabel: document.getElementById("retention-label"),
    channelsWindowBadge: document.getElementById("channels-window-badge"),
    activeCount: document.getElementById("active-count"),
    authUserPill: document.getElementById("auth-user-pill"),
    authUsername: document.getElementById("auth-username"),
    openPasswordModalBtn: document.getElementById("open-password-modal-btn"),
    logoutBtn: document.getElementById("logout-btn"),
    loginOverlay: document.getElementById("login-overlay"),
    loginForm: document.getElementById("login-form"),
    loginUsername: document.getElementById("login-username"),
    loginPassword: document.getElementById("login-password"),
    loginError: document.getElementById("login-error"),
    loginSubmitBtn: document.getElementById("login-submit-btn"),
    passwordModal: document.getElementById("password-modal"),
    passwordForm: document.getElementById("password-form"),
    currentPassword: document.getElementById("current-password"),
    newPassword: document.getElementById("new-password"),
    confirmPassword: document.getElementById("confirm-password"),
    passwordFeedback: document.getElementById("password-feedback"),
    passwordSubmitBtn: document.getElementById("password-submit-btn"),
    closePasswordModalBtn: document.getElementById("close-password-modal-btn"),
    channelSearch: document.getElementById("channel-search"),
    channelsList: document.getElementById("channels-list"),
    clearTopicFilter: document.getElementById("clear-topic-filter"),
    activeFilterChip: document.getElementById("active-filter-chip"),
    activeFilterName: document.getElementById("active-filter-name"),
    pubTopic: document.getElementById("pub-topic"),
    pubQos: document.getElementById("pub-qos"),
    pubRetain: document.getElementById("pub-retain"),
    pubPayload: document.getElementById("pub-payload"),
    publishForm: document.getElementById("publish-form"),
    publishBtn: document.getElementById("publish-btn"),
    publishFeedback: document.getElementById("publish-feedback"),
    formatJsonBtn: document.getElementById("format-json-btn"),
    messagesList: document.getElementById("messages-list"),
    toggleStreamBtn: document.getElementById("toggle-stream-btn"),
    clearMessagesBtn: document.getElementById("clear-messages-btn"),
  };

  function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text == null ? "" : String(text);
    return div.innerHTML;
  }

  function formatTimestamp(ts) {
    const date = new Date(ts * 1000);
    return date.toLocaleTimeString("pt-BR", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function formatRelative(ts) {
    const diffSec = Math.max(0, Math.floor(Date.now() / 1000 - ts));
    if (diffSec < 60) return `há ${diffSec}s`;
    const diffMin = Math.floor(diffSec / 60);
    if (diffMin < 60) return `há ${diffMin}min`;
    const diffHours = Math.floor(diffMin / 60);
    const remMin = diffMin % 60;
    return remMin > 0 ? `há ${diffHours}h ${remMin}min` : `há ${diffHours}h`;
  }

  function formatPayload(raw) {
    if (!raw) return "(vazio)";
    const trimmed = raw.trim();
    if (
      (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
      (trimmed.startsWith("[") && trimmed.endsWith("]"))
    ) {
      try {
        return JSON.stringify(JSON.parse(trimmed), null, 2);
      } catch {
        return raw;
      }
    }
    return raw;
  }

  function updateStatusUI(status) {
    const connected = Boolean(status.connected);
    els.connectionPill.classList.toggle("connected", connected);
    els.connectionPill.classList.toggle("disconnected", !connected);
    els.connectionText.textContent = connected
      ? "Conectado"
      : status.last_error
      ? `Desconectado (${status.last_error})`
      : "Desconectado";

    els.brokerAddress.textContent = `${status.broker_host || "--"}:${status.broker_port || "--"}`;

    if (status.retention_hours) {
      state.retentionHours = status.retention_hours;
      const hoursText = `${status.retention_hours}h`;
      els.retentionLabel.textContent = `Últimas ${hoursText}`;
      els.channelsWindowBadge.textContent = hoursText;
    }
  }

  function renderChannels() {
    els.activeCount.textContent = String(state.channels.length);

    if (state.channels.length === 0) {
      els.channelsList.innerHTML = `<div class="empty-state">Nenhum canal ativo nas últimas ${escapeHtml(
        state.retentionHours
      )}h.</div>`;
      return;
    }

    els.channelsList.innerHTML = state.channels
      .map((ch) => {
        const isActive = state.selectedTopic === ch.topic;
        const preview = ch.last_payload ? ch.last_payload.replace(/\s+/g, " ") : "(vazio)";
        return `
          <button
            type="button"
            class="channel-item ${isActive ? "active" : ""}"
            data-topic="${escapeHtml(ch.topic)}"
          >
            <div class="channel-top">
              <span class="channel-name">${escapeHtml(ch.topic)}</span>
              <span class="channel-count">${ch.message_count} msg</span>
            </div>
            <div class="channel-preview">${escapeHtml(preview)}</div>
            <div class="channel-time">
              Última atividade: ${formatTimestamp(ch.last_seen)} (${formatRelative(ch.last_seen)})
            </div>
          </button>
        `;
      })
      .join("");
  }

  function renderMessages() {
    const filtered = state.selectedTopic
      ? state.messages.filter((m) => m.topic === state.selectedTopic)
      : state.messages;

    if (filtered.length === 0) {
      els.messagesList.innerHTML = `<div class="empty-state">${
        state.selectedTopic
          ? `Sem mensagens recentes no canal "${escapeHtml(state.selectedTopic)}".`
          : "Aguardando mensagens do servidor Mosquitto..."
      }</div>`;
      return;
    }

    els.messagesList.innerHTML = filtered
      .slice(0, MAX_RENDERED_MESSAGES)
      .map(
        (m) => `
          <article class="message-card">
            <div class="message-meta">
              <span class="message-topic">${escapeHtml(m.topic)}</span>
              <div class="message-badges">
                <span class="pill-tag">QoS ${m.qos}</span>
                ${m.retain ? '<span class="pill-tag pill-retain">RETAIN</span>' : ""}
                <span>${formatTimestamp(m.timestamp)}</span>
              </div>
            </div>
            <pre class="message-payload">${escapeHtml(formatPayload(m.payload))}</pre>
          </article>
        `
      )
      .join("");
  }

  function setTopicFilter(topic) {
    state.selectedTopic = topic || null;
    els.clearTopicFilter.hidden = !state.selectedTopic;
    els.activeFilterChip.hidden = !state.selectedTopic;
    if (state.selectedTopic) {
      els.activeFilterName.textContent = state.selectedTopic;
      els.pubTopic.value = state.selectedTopic;
    }
    renderChannels();
    loadMessages();
  }

  async function loadStatus() {
    try {
      const res = await fetch("/api/status");
      if (res.status === 401) {
        showLogin();
        return;
      }
      if (res.ok) {
        updateStatusUI(await res.json());
      }
    } catch {
      // Ignora erro transitório; SSE e próximo refresh atualizam
    }
  }

  async function loadChannels() {
    try {
      const params = new URLSearchParams();
      if (state.search.trim()) {
        params.set("search", state.search.trim());
      }
      const res = await fetch(`/api/channels?${params.toString()}`);
      if (res.status === 401) {
        showLogin();
        return;
      }
      if (!res.ok) return;
      const data = await res.json();
      if (data.retention_hours) {
        state.retentionHours = data.retention_hours;
      }
      state.channels = data.channels || [];
      renderChannels();
    } catch {
      // Ignora falha temporária de rede
    }
  }

  async function loadMessages() {
    try {
      const params = new URLSearchParams({ limit: "100" });
      if (state.selectedTopic) {
        params.set("topic", state.selectedTopic);
      }
      const res = await fetch(`/api/messages?${params.toString()}`);
      if (res.status === 401) {
        showLogin();
        return;
      }
      if (!res.ok) return;
      const data = await res.json();
      state.messages = data.messages || [];
      renderMessages();
    } catch {
      // Ignora falha temporária
    }
  }

  function upsertChannelFromMessage(msg) {
    const search = state.search.trim().toLowerCase();
    if (search && !msg.topic.toLowerCase().includes(search)) {
      return;
    }

    const idx = state.channels.findIndex((c) => c.topic === msg.topic);
    if (idx >= 0) {
      const existing = state.channels[idx];
      existing.message_count += 1;
      existing.last_seen = msg.timestamp;
      existing.last_payload = msg.payload;
      existing.last_retain = msg.retain;
      state.channels.splice(idx, 1);
      state.channels.unshift(existing);
    } else {
      state.channels.unshift({
        topic: msg.topic,
        message_count: 1,
        last_seen: msg.timestamp,
        last_payload: msg.payload,
        last_retain: msg.retain,
      });
    }
    renderChannels();
  }

  function stopRealtime() {
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
    if (state.refreshTimer) {
      clearInterval(state.refreshTimer);
      state.refreshTimer = null;
    }
  }

  function showLogin(message = "") {
    state.authenticated = false;
    stopRealtime();
    els.authUserPill.hidden = true;
    els.openPasswordModalBtn.hidden = true;
    els.logoutBtn.hidden = true;
    els.passwordModal.hidden = true;
    els.loginError.textContent = message;
    els.loginOverlay.hidden = false;
    els.loginUsername.focus();
  }

  function onAuthenticated(username) {
    state.authenticated = true;
    els.authUsername.textContent = username;
    els.authUserPill.hidden = false;
    els.openPasswordModalBtn.hidden = false;
    els.logoutBtn.hidden = false;
    els.loginOverlay.hidden = true;
    els.loginError.textContent = "";
    els.loginPassword.value = "";

    loadStatus();
    loadChannels();
    loadMessages();
    connectStream();
    if (!state.refreshTimer) {
      state.refreshTimer = setInterval(loadChannels, 30000);
    }
  }

  async function checkAuth() {
    try {
      const res = await fetch("/api/auth/me");
      if (res.ok) {
        const data = await res.json();
        onAuthenticated(data.username);
      } else {
        showLogin();
      }
    } catch {
      showLogin("Não foi possível verificar a sessão.");
    }
  }

  function connectStream() {
    if (state.eventSource) {
      state.eventSource.close();
    }
    const es = new EventSource("/api/stream");
    state.eventSource = es;

    es.addEventListener("status", (event) => {
      try {
        updateStatusUI(JSON.parse(event.data));
      } catch {
        // Ignora JSON inválido
      }
    });

    es.addEventListener("message", (event) => {
      try {
        const msg = JSON.parse(event.data);
        upsertChannelFromMessage(msg);
        if (!state.paused) {
          state.messages.unshift(msg);
          if (state.messages.length > MAX_RENDERED_MESSAGES) {
            state.messages.length = MAX_RENDERED_MESSAGES;
          }
          renderMessages();
        }
      } catch {
        // Ignora evento malformado
      }
    });

    es.onerror = () => {
      els.connectionPill.classList.remove("connected");
      els.connectionPill.classList.add("disconnected");
      els.connectionText.textContent = "Reconectando ao servidor web...";
    };
  }

  function showPublishFeedback(message, isError = false) {
    els.publishFeedback.textContent = message;
    els.publishFeedback.className = `publish-feedback ${isError ? "err" : "ok"}`;
  }

  els.channelsList.addEventListener("click", (event) => {
    const btn = event.target.closest(".channel-item");
    if (!btn) return;
    const topic = btn.dataset.topic;
    if (state.selectedTopic === topic) {
      setTopicFilter(null);
    } else {
      setTopicFilter(topic);
    }
  });

  els.clearTopicFilter.addEventListener("click", () => setTopicFilter(null));

  let searchTimer = null;
  els.channelSearch.addEventListener("input", (event) => {
    state.search = event.target.value;
    clearTimeout(searchTimer);
    searchTimer = setTimeout(loadChannels, 200);
  });

  els.formatJsonBtn.addEventListener("click", () => {
    const raw = els.pubPayload.value.trim();
    if (!raw) return;
    try {
      els.pubPayload.value = JSON.stringify(JSON.parse(raw), null, 2);
      showPublishFeedback("JSON formatado.", false);
    } catch {
      showPublishFeedback("Conteúdo atual não é um JSON válido.", true);
    }
  });

  els.publishForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const topic = els.pubTopic.value.trim();
    const payload = els.pubPayload.value;
    const qos = Number(els.pubQos.value);
    const retain = els.pubRetain.checked;

    if (!topic) {
      showPublishFeedback("Informe o canal (tópico) de destino.", true);
      return;
    }

    els.publishBtn.disabled = true;
    showPublishFeedback("Enviando...", false);

    try {
      const res = await fetch("/api/publish", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ topic, payload, qos, retain }),
      });
      if (res.status === 401) {
        showLogin("Sessão expirada. Faça login novamente.");
        return;
      }
      const data = await res.json();
      if (!res.ok) {
        showPublishFeedback(data.detail || "Falha ao enviar mensagem.", true);
        return;
      }
      showPublishFeedback(`Mensagem enviada para "${data.topic}" (QoS ${data.qos}).`, false);
    } catch {
      showPublishFeedback("Erro de comunicação com o servidor web.", true);
    } finally {
      els.publishBtn.disabled = false;
    }
  });

  els.toggleStreamBtn.addEventListener("click", () => {
    state.paused = !state.paused;
    els.toggleStreamBtn.textContent = state.paused ? "Retomar" : "Pausar";
    if (!state.paused) {
      loadMessages();
    }
  });

  els.clearMessagesBtn.addEventListener("click", () => {
    state.messages = [];
    renderMessages();
  });

  els.loginForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const username = els.loginUsername.value.trim();
    const password = els.loginPassword.value;
    els.loginError.textContent = "";
    els.loginSubmitBtn.disabled = true;

    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      });
      const data = await res.json();
      if (!res.ok) {
        els.loginError.textContent = data.detail || "Usuário ou senha inválidos.";
        return;
      }
      onAuthenticated(data.username);
    } catch {
      els.loginError.textContent = "Erro de conexão com o servidor.";
    } finally {
      els.loginSubmitBtn.disabled = false;
    }
  });

  els.logoutBtn.addEventListener("click", async () => {
    try {
      await fetch("/api/auth/logout", { method: "POST" });
    } catch {
      // Ignora erro de rede ao sair
    }
    state.channels = [];
    state.messages = [];
    renderChannels();
    renderMessages();
    showLogin();
  });

  els.openPasswordModalBtn.addEventListener("click", () => {
    els.passwordForm.reset();
    els.passwordFeedback.textContent = "";
    els.passwordFeedback.className = "modal-feedback";
    els.passwordModal.hidden = false;
    els.currentPassword.focus();
  });

  els.closePasswordModalBtn.addEventListener("click", () => {
    els.passwordModal.hidden = true;
  });

  els.passwordForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const currentPassword = els.currentPassword.value;
    const newPassword = els.newPassword.value;
    const confirmPassword = els.confirmPassword.value;

    if (newPassword !== confirmPassword) {
      els.passwordFeedback.textContent = "A confirmação da nova senha não confere.";
      els.passwordFeedback.className = "modal-feedback err";
      return;
    }

    els.passwordSubmitBtn.disabled = true;
    els.passwordFeedback.textContent = "Salvando...";
    els.passwordFeedback.className = "modal-feedback";

    try {
      const res = await fetch("/api/auth/change-password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          current_password: currentPassword,
          new_password: newPassword,
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        els.passwordFeedback.textContent = data.detail || "Falha ao alterar senha.";
        els.passwordFeedback.className = "modal-feedback err";
        return;
      }
      els.passwordFeedback.textContent = data.message || "Senha alterada com sucesso.";
      els.passwordFeedback.className = "modal-feedback ok";
      els.passwordForm.reset();
    } catch {
      els.passwordFeedback.textContent = "Erro de comunicação com o servidor.";
      els.passwordFeedback.className = "modal-feedback err";
    } finally {
      els.passwordSubmitBtn.disabled = false;
    }
  });

  // Inicialização autenticada
  checkAuth();
})();
