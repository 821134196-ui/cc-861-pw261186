"use strict";

const STEP_META = [
  { title: "检测心跳超时", detail: "协调器推进虚拟时钟；超过 10 秒未心跳后判定 consumer-1 失联。" },
  { title: "递增 Generation / Fence", detail: "generation 递增并颁发新 fence 令牌，旧 g1 提交随后会被拒绝。" },
  { title: "转交 p0、p1", detail: "失联消费者的分区转交给健康的 consumer-2。" },
  { title: "新旧实例重叠处理", detail: "旧实例与新所有者处理同一 offset；相同幂等键去重，副作用只执行一次。" },
  { title: "新所有者推进检查点", detail: "consumer-2 处理下一条并提交，使检查点超过旧实例准备提交的 offset。" },
  { title: "旧实例延迟提交", detail: "协调器 fence 与存储层单调提交都会阻止检查点倒退。" },
  { title: "完成重平衡", detail: "分区所有者稳定，旧僵尸实例被隔离。" },
];

let state = null;
let selectedPartition = 0;
let activeTab = "audit";
let toastTimer = null;
let localNotice = { text: "", type: "" };

const $ = (id) => document.getElementById(id);

function esc(value) {
  if (value === null || value === undefined) return "";
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function yuan(amount) {
  return `¥${(Number(amount || 0) / 100).toFixed(2)}`;
}

function outcomeLabel(outcome) {
  return {
    executed_once: "首次真实执行",
    executed: "首次真实执行",
    deduplicated: "幂等去重，未重复执行",
    ack_lost: "服务已成功，但确认丢失",
  }[outcome] || outcome;
}

function reasonLabel(reason) {
  return {
    fenced_stale_generation: "Fence：旧 generation",
    not_current_owner: "不是当前分区所有者",
    member_fenced_or_dead: "成员已 dead/fenced",
    checkpoint_regression_rejected: "存储层拒绝检查点倒退",
    idempotent_equal_commit: "相同检查点幂等提交",
    advanced: "检查点推进",
    offset_beyond_log_end: "Offset 超过日志末端",
  }[reason] || reason || "—";
}

function showToast(message, type = "") {
  const toast = $("toast");
  toast.textContent = message;
  toast.className = `toast ${type}`;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.add("hidden"), 3600);
}

function setNotice(text, type = "") {
  localNotice = { text, type };
  renderNotice();
}

function renderNotice() {
  const node = $("actionNotice");
  node.textContent = localNotice.text;
  node.className = `notice ${localNotice.type}`.trim();
}

async function refresh() {
  try {
    const response = await fetch("/api/state", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    state = await response.json();
    $("connectionBadge").textContent = "本地 API 已连接";
    $("connectionBadge").className = "badge good";
    render();
  } catch (error) {
    $("connectionBadge").textContent = "连接失败，重试中";
    $("connectionBadge").className = "badge critical";
  }
}

async function api(path, body = {}) {
  try {
    const response = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await response.json().catch(() => ({}));
    if (data.state) {
      state = data.state;
      render();
    } else {
      await refresh();
    }
    if (!response.ok && !data.state) {
      showToast(data.error || `请求失败：HTTP ${response.status}`, "error");
    }
    return data;
  } catch (error) {
    showToast(`网络错误：${error.message}`, "error");
    return { ok: false, error: error.message };
  }
}

function render() {
  if (!state) return;
  renderStats();
  renderRebalance();
  renderConsumers();
  renderOwnership();
  renderPartitionData();
  renderDeliveries();
  renderExternal();
  renderAuditAndLogs();
  renderInvariants();
  renderNotice();
}

function renderStats() {
  const totalPending = state.bus.pendingCounts.reduce((a, b) => a + b, 0);
  $("statRow").innerHTML = `
    <div class="stat">
      <span class="label">VIRTUAL CLOCK</span>
      <span class="value">${esc(state.clockSeconds)}s</span>
      <span class="sub">心跳超时 ${state.heartbeatTimeoutMs / 1000}s</span>
    </div>
    <div class="stat">
      <span class="label">GROUP GENERATION</span>
      <span class="value">${esc(state.coordinator.generation)}</span>
      <span class="sub">协调器 fence 令牌</span>
    </div>
    <div class="stat">
      <span class="label">P0 CHECKPOINT</span>
      <span class="value">${esc(state.bus.checkpoints[0])}</span>
      <span class="sub">最后已处理 offset</span>
    </div>
    <div class="stat">
      <span class="label">PENDING EVENTS</span>
      <span class="value">${totalPending}</span>
      <span class="sub">三个分区合计</span>
    </div>
  `;
  $("generationBadge").textContent = `generation ${state.coordinator.generation}`;
}

function summarizeStepResult(step) {
  if (!step.result) return "";
  const id = step.id;
  const r = step.result;
  if (id === "detect") return `timeout=${(r.timedOut || []).join(", ") || "无"}；clock=${r.clock}ms`;
  if (id === "fence") return `g${r.oldGeneration} → g${r.generation}`;
  if (id === "assign") return `新所有权：${JSON.stringify(r.assignments)}`;
  if (id === "overlap") {
    return `offset=${r.offset}\n旧 C1：${r.old?.ok ? "外部调用完成，提交暂存" : "失败"}\n新 C2：deduped=${r.new?.deduped}，提交 accepted=${r.new?.commit?.accepted}，CP=${r.new?.commit?.currentOffset}`;
  }
  if (id === "advance") {
    return `offset=${r.offset}；accepted=${r.result?.commit?.accepted}；CP=${r.result?.commit?.currentOffset}`;
  }
  if (id === "stale_commit") {
    return `协调器：${r.coordinator?.reason}\n存储层：${r.storage?.reason}\n最终 p0 CP=${r.storage?.currentOffset}`;
  }
  if (id === "complete") return `checkpoints=[${(r.checkpoints || []).join(", ")}]`;
  return JSON.stringify(r, null, 2);
}

function renderRebalance() {
  const plan = state.rebalance;
  const steps = plan?.steps || STEP_META.map((step) => ({ ...step, status: "pending" }));
  const currentIndex = plan ? Number(plan.stepIndex) : -1;

  $("stepList").innerHTML = steps
    .map((step, index) => {
      const isDone = step.status === "done";
      const isCurrent = !isDone && index === currentIndex + 1 && !(plan?.finished);
      const stateText = isDone ? "完成" : isCurrent ? "下一步" : "等待";
      const result = isDone && step.result ? `<div class="step-result">${esc(summarizeStepResult(step))}</div>` : "";
      return `
        <li class="step ${isDone ? "done" : ""} ${isCurrent ? "current" : ""}">
          <span class="step-index">${isDone ? "✓" : index + 1}</span>
          <div>
            <div class="step-title">${esc(step.title || STEP_META[index].title)}</div>
            <div class="step-detail">${esc(step.detail || STEP_META[index].detail)}</div>
            ${result}
          </div>
          <span class="step-state">${stateText}</span>
        </li>
      `;
    })
    .join("");

  const c1 = state.coordinator.members.find((m) => m.id === "consumer-1");
  const stopButton = $("stopConsumerButton");
  const stepButton = $("stepButton");

  if (c1.heartbeatRunning) {
    stopButton.disabled = false;
    stopButton.textContent = "让 consumer-1 停止回应";
  } else if (c1.coordinatorState === "dead") {
    stopButton.disabled = true;
    stopButton.textContent = "consumer-1 已被判定失联";
  } else {
    stopButton.disabled = true;
    stopButton.textContent = "consumer-1 已停止回应";
  }

  if (plan?.finished) {
    stepButton.disabled = true;
    stepButton.textContent = "✓ 重平衡完成";
  } else if (c1.heartbeatRunning) {
    stepButton.disabled = true;
    stepButton.textContent = "先停止 consumer-1，再单步推进";
  } else {
    const nextStep = steps[currentIndex + 1] || STEP_META[currentIndex + 1];
    stepButton.disabled = !nextStep;
    stepButton.textContent = nextStep ? `单步推进：${nextStep.title}` : "没有更多步骤";
  }
}

function coordinatorBadge(member) {
  const map = {
    healthy: ["good", "协调器：健康"],
    silent: ["warning", "协调器：心跳停止"],
    dead: ["critical", "协调器：已失联"],
    fenced: ["critical", "协调器：已隔离"],
  };
  const [cls, text] = map[member.coordinatorState] || ["neutral", member.coordinatorState];
  return `<span class="badge ${cls}">${esc(text)}</span>`;
}

function processBadge(member) {
  const map = {
    live: ["neutral", "进程：正常"],
    paused: ["warning", "进程：暂停"],
    zombie: ["critical", "进程：僵尸/重叠"],
    fenced: ["critical", "进程：旧令牌失效"],
  };
  const [cls, text] = map[member.processState] || ["neutral", member.processState];
  return `<span class="badge ${cls}">${esc(text)}</span>`;
}

function renderConsumers() {
  $("consumerList").innerHTML = state.coordinator.members
    .map((member) => {
      const cls = member.id === "consumer-1" ? "c1" : "c2";
      const heartbeat = member.heartbeatRunning ? "运行中" : "已停止";
      return `
        <section class="consumer-card ${cls}">
          <div class="consumer-top">
            <span class="consumer-name">${esc(member.id)}</span>
            <div class="status-line">${coordinatorBadge(member)}${processBadge(member)}</div>
          </div>
          <div class="consumer-meta">
            <span>心跳循环：<strong>${heartbeat}</strong></span>
            <span>心跳年龄：<strong class="mono">${(member.heartbeatAge / 1000).toFixed(1)}s</strong></span>
            <span>已知 Generation：<strong class="mono">g${esc(member.knownGeneration)}</strong></span>
            <span>负责分区：<strong class="mono">${member.ownedPartitions.map((p) => `p${p}`).join(" / ") || "无"}</strong></span>
          </div>
        </section>
      `;
    })
    .join("");
}

function renderOwnership() {
  $("partitionOwnership").innerHTML = state.bus.checkpoints
    .map((checkpoint, p) => {
      const owner = state.coordinator.owners[String(p)];
      const revoking = state.coordinator.revoking.includes(p);
      return `
        <div class="partition-tile ${revoking ? "revoking" : ""}">
          <div class="partition-id">orders-${p}${revoking ? " · revoking" : ""}</div>
          <span class="owner-chip ${esc(owner)}">${esc(owner)}</span>
          <span class="owner-generation">CP=${checkpoint} · end=${state.bus.latestOffsets[p]} · gen=${state.coordinator.generation}</span>
        </div>
      `;
    })
    .join("");
}

function deliveryByKey() {
  const map = new Map();
  for (const delivery of state.deliveries) map.set(delivery.idempotencyKey, delivery);
  return map;
}

function renderPartitionData() {
  const p = selectedPartition;
  $("partitionSelect").value = String(p);
  const checkpoint = state.bus.checkpoints[p];
  const latest = state.bus.latestOffsets[p];
  const pending = state.bus.pendingCounts[p];
  const owner = state.coordinator.owners[String(p)];

  $("partitionSummary").innerHTML = `
    <div class="metric-tile"><span class="label">OWNER</span><span class="value" style="font-size:15px;padding-top:4px">${esc(owner)}</span></div>
    <div class="metric-tile"><span class="label">CHECKPOINT</span><span class="value">${checkpoint}</span></div>
    <div class="metric-tile"><span class="label">LOG END / PENDING</span><span class="value">${latest} <span style="font-size:13px;color:var(--text-muted)">/ ${pending}</span></span></div>
  `;

  const deliveries = deliveryByKey();
  const events = state.bus.partitions[p] || [];
  $("eventTableBody").innerHTML = events
    .map((event) => {
      const processed = event.offset <= checkpoint;
      const key = `idem:${event.topic}:${event.partition}:${event.offset}`;
      const delivery = deliveries.get(key);
      let idempotency = `<span class="status-pill neutral">未调用外部服务</span>`;
      if (delivery) {
        const lost = delivery.status === "ack_lost";
        idempotency = `<span class="status-pill ${lost ? "critical" : "good"}">${delivery.attempts.length} 次投递 · ${esc(outcomeLabel(delivery.attempts.at(-1).outcome))}</span>`;
      }
      return `
        <tr class="${processed ? "processed" : ""}">
          <td class="mono">${event.offset}</td>
          <td>
            <div class="event-primary">${esc(event.order_id)}</div>
            <div class="event-secondary">${esc(event.event_id)} · key=${esc(event.key)}</div>
          </td>
          <td class="mono">${yuan(event.amount)}</td>
          <td><span class="status-pill ${processed ? "good" : "warning"}">${processed ? "已提交" : "待处理"}</span></td>
          <td>${idempotency}</td>
        </tr>
      `;
    })
    .join("");
}

function renderDeliveries() {
  const deliveries = state.deliveries || [];
  $("deliveryCountBadge").textContent = `${deliveries.length} 条投递`;
  const lost = deliveries.find((d) => d.status === "ack_lost");

  $("retryBanner").innerHTML = lost
    ? `
      <div class="retry-banner">
        <div><strong>有一次结果未知的投递</strong>${esc(lost.idempotencyKey)} 已成功执行，但确认丢失；检查点未前进。</div>
        <button class="btn primary small" data-action="retry" data-key="${esc(lost.idempotencyKey)}">用同一幂等键重投</button>
      </div>`
    : "";

  if (!deliveries.length) {
    $("deliveryList").innerHTML = `
      <div class="empty-state">
        还没有外部投递。<br />点击“处理下一条但丢失确认”，可观察“服务已执行 → ACK 丢失 → 同键重投 → 去重”。
      </div>`;
    return;
  }

  $("deliveryList").innerHTML = deliveries
    .map((delivery) => {
      const attention = delivery.status === "ack_lost";
      const statusClass = delivery.status === "committed" ? "good" : attention ? "critical" : "warning";
      const statusText = {
        ack_lost: "ACK 丢失 / 待重投",
        committed: "已提交检查点",
        processed: "外部成功 / 未提交",
        deduplicated: "去重成功 / 未提交",
        commit_rejected: "提交被拒绝",
        created: "已创建",
      }[delivery.status] || delivery.status;

      const attempts = delivery.attempts
        .map((attempt) => {
          return `
            <li class="attempt ${esc(attempt.outcome)}">
              <span class="attempt-dot">${attempt.attempt}</span>
              <div>
                <strong>${esc(outcomeLabel(attempt.outcome))}</strong>
                <div class="attempt-meta">
                  ${esc(attempt.actor)} · g${esc(attempt.generation)} · ${(attempt.at / 1000).toFixed(1)}s
                  ${attempt.detail ? ` · ${esc(attempt.detail)}` : ""}
                </div>
              </div>
            </li>`;
        })
        .join("");

      const commit = delivery.commit
        ? `<div class="attempt-meta" style="margin-top:8px">
             检查点提交：<strong class="${delivery.commit.accepted ? "" : "critical-text"}">${esc(reasonLabel(delivery.commit.reason))}</strong>
             ${delivery.commit.currentOffset !== undefined ? ` · 当前 CP=${esc(delivery.commit.currentOffset)}` : ""}
           </div>`
        : "";

      const actions = attention
        ? `<div class="delivery-actions">
             <button class="btn primary small" data-action="retry" data-key="${esc(delivery.idempotencyKey)}">同一幂等键重投并提交</button>
           </div>`
        : "";

      return `
        <section class="delivery-card ${attention ? "attention" : ""}">
          <div class="delivery-head">
            <div>
              <div class="delivery-title">p${delivery.partition} @ ${delivery.offset} · ${esc(delivery.orderId)}</div>
              <div class="delivery-key">${esc(delivery.idempotencyKey)}</div>
            </div>
            <span class="status-pill ${statusClass}">${esc(statusText)}</span>
          </div>
          <ul class="attempt-list">${attempts}</ul>
          ${commit}
          ${actions}
        </section>
      `;
    })
    .join("");
}

function renderExternal() {
  const external = state.external;
  if (!external) {
    $("externalStats").innerHTML = `<span class="mini-stat">外部服务不可见</span>`;
    $("ledgerTableBody").innerHTML = "";
    $("sideEffectList").innerHTML = `<div class="empty-state">未连接到幂等服务账本。</div>`;
    return;
  }

  $("externalStats").innerHTML = `
    <span class="mini-stat">真实副作用 <strong>${external.executionCount}</strong></span>
    <span class="mini-stat">去重命中 <strong>${external.duplicateHitCount}</strong></span>
    <span class="mini-stat">账本键 <strong>${external.ledgerSize}</strong></span>
  `;

  $("ledgerTableBody").innerHTML = external.records.length
    ? external.records
        .map((record) => {
          const orderId = record.result?.orderId || record.sideEffect?.orderId || "—";
          return `
            <tr>
              <td class="mono">${esc(record.idempotencyKey)}</td>
              <td>${esc(orderId)}</td>
              <td><span class="status-pill good">${record.executionCount} 次</span></td>
              <td><strong>${record.duplicateHits}</strong></td>
              <td><span class="status-pill good">${esc(record.status)}</span></td>
            </tr>
          `;
        })
        .join("")
    : `<tr><td colspan="5" style="text-align:center;color:var(--text-muted);padding:22px">尚无幂等键记录</td></tr>`;

  $("sideEffectList").innerHTML = external.sideEffects.length
    ? external.sideEffects
        .map((effect) => `<span class="side-effect">#${effect.sequence} ${esc(effect.orderId)} · ${esc(effect.action)}</span>`)
        .join("")
    : `<span class="hint" style="margin:0">尚无真实外部副作用。</span>`;
}

function renderAuditAndLogs() {
  $("auditTableBody").innerHTML = state.commitAudit.length
    ? state.commitAudit
        .map((entry) => {
          const cls = entry.accepted ? (entry.changed ? "good" : "neutral") : "critical";
          const text = entry.accepted ? (entry.changed ? "接受并推进" : "接受/相同 CP") : "拒绝";
          const cp =
            entry.previousOffset !== undefined
              ? `CP ${entry.previousOffset} → ${entry.currentOffset}`
              : `当前 CP=${entry.currentOffset ?? "?"}`;
          return `
            <tr>
              <td><span class="status-pill ${cls}">${text}</span></td>
              <td class="mono">p${entry.partition} @ ${entry.requestedOffset}</td>
              <td>${esc(entry.member)}<br><span class="event-secondary">g${esc(entry.generation)} · ${esc(entry.source)}</span></td>
              <td>${esc(entry.message || reasonLabel(entry.reason))}<br><span class="event-secondary mono">${esc(cp)} · ${esc(reasonLabel(entry.reason))}</span></td>
            </tr>
          `;
        })
        .join("")
    : `<tr><td colspan="4" style="text-align:center;color:var(--text-muted);padding:22px">尚无提交审计</td></tr>`;

  $("logList").innerHTML = state.logs
    .map(
      (entry) => `
      <div class="log-entry ${esc(entry.level)}">
        <span class="time">${(entry.at / 1000).toFixed(1)}s</span>
        <span class="source">${esc(entry.source)}</span>
        <span class="message">${esc(entry.message)}</span>
      </div>`
    )
    .join("");

  $("auditPanel").classList.toggle("hidden", activeTab !== "audit");
  $("logPanel").classList.toggle("hidden", activeTab !== "logs");
  $("auditTabButton").classList.toggle("active", activeTab === "audit");
  $("logTabButton").classList.toggle("active", activeTab === "logs");
}

function renderInvariants() {
  const stale = state.rebalance?.staleResults;
  const evidence = stale
    ? `
      <div class="invariant-card">
        <h3>✓ 已验证：新旧重叠没有让检查点倒退</h3>
        <div class="invariant-grid">
          <div class="invariant-result">
            <strong>第一层：${esc(reasonLabel(stale.coordinator.reason))}</strong>
            ${esc(stale.coordinator.message || "")}<br />当前 generation=g${esc(stale.coordinator.currentGeneration)}。
          </div>
          <div class="invariant-result">
            <strong>第二层：${esc(reasonLabel(stale.storage.reason))}</strong>
            ${esc(stale.storage.message || "")}<br />存储层 CP=${esc(stale.storage.currentOffset)}。
          </div>
        </div>
      </div>`
    : "";

  $("invariantPanel").innerHTML = `
    <div class="invariant-grid">
      <div class="invariant-result"><strong>检查点单调</strong>存储层只接受大于等于当前 offset 的提交；更小值一律拒绝。</div>
      <div class="invariant-result"><strong>Generation Fence</strong>重平衡递增 generation；旧所有者的迟到提交无法通过协调器。</div>
      <div class="invariant-result"><strong>幂等键</strong>重投复用同一键，外部服务返回缓存结果且副作用只执行一次。</div>
      <div class="invariant-result"><strong>At-least-once</strong>ACK 丢失时不提交检查点，因此事件会被重新投递。</div>
    </div>
    ${evidence}
  `;
}

function partitionFromUI() {
  return Number($("partitionSelect").value);
}

$("partitionSelect").addEventListener("change", (event) => {
  selectedPartition = Number(event.target.value);
  renderPartitionData();
});

$("resetButton").addEventListener("click", async () => {
  const data = await api("/api/reset", {});
  selectedPartition = 0;
  $("partitionSelect").value = "0";
  localNotice.text = "";
  showToast("演示状态已重置", "success");
  if (data.state) render();
});

$("stopConsumerButton").addEventListener("click", async () => {
  const data = await api("/api/consumers/consumer-1/stop", {});
  if (data.ok) {
    setNotice("consumer-1 已停止心跳。现在可反复点击右侧主按钮，单步推进重平衡。", "error");
    showToast("已注入消费者失联", "error");
  } else {
    setNotice(data.error || "操作失败", "error");
  }
});

$("stepButton").addEventListener("click", async () => {
  const data = await api("/api/rebalance/next", {});
  if (data.ok) {
    const stepId = data.result?.stepId;
    if (stepId === "stale_commit") {
      showToast("旧提交已被拒绝，检查点未倒退", "success");
      setNotice("关键验证完成：协调器 fence 与存储层单调约束均拒绝了旧提交。", "success");
    } else if (stepId === "complete") {
      showToast("重平衡完成", "success");
      setNotice("重平衡完成。可查看审计、系统事件流和幂等服务账本。", "success");
    } else {
      setNotice(`已完成步骤：${STEP_META[data.result.stepIndex]?.title || stepId}`, "success");
    }
  } else {
    setNotice(data.error || "无法推进一步", "error");
    showToast(data.error || "无法推进一步", "error");
  }
});

$("produceButton").addEventListener("click", async () => {
  const data = await api("/api/events", { partition: partitionFromUI() });
  if (data.ok) showToast("事件已生产", "success");
});

$("processButton").addEventListener("click", async () => {
  const p = partitionFromUI();
  const data = await api("/api/deliveries/process-next", { partition: p, loseAck: false, commit: true });
  if (data.ok) {
    showToast(data.result?.deduped ? "命中幂等键并提交" : "处理并提交成功", "success");
    setNotice(`p${p} 下一条事件处理完成，检查点已提交。`, "success");
  } else {
    setNotice(data.error || "没有可处理事件", "error");
    showToast(data.error || "操作未成功", "error");
  }
});

$("lostAckButton").addEventListener("click", async () => {
  const p = partitionFromUI();
  const data = await api("/api/deliveries/process-next", { partition: p, loseAck: true, commit: true });
  if (data.result?.ackLost) {
    setNotice("外部服务已执行，但响应确认丢失；检查点未提交。请点击重投按钮验证去重。", "error");
    showToast("确认已丢失：等待同一幂等键重投", "error");
  } else if (data.ok) {
    showToast("本次没有触发 ACK 丢失（可能是重复请求）", "");
  } else {
    setNotice(data.error || "操作未成功", "error");
    showToast(data.error || "操作未成功", "error");
  }
});

document.addEventListener("click", async (event) => {
  const button = event.target.closest('[data-action="retry"]');
  if (!button) return;
  button.disabled = true;
  const key = button.dataset.key;
  const data = await api("/api/deliveries/retry", { key, commit: true });
  if (data.ok) {
    showToast("重投命中幂等账本：副作用未重复，检查点已提交", "success");
    setNotice("重投返回缓存结果并完成检查点提交；外部真实执行次数没有增加。", "success");
  } else {
    button.disabled = false;
    setNotice(data.error || "重投失败", "error");
  }
});

$("auditTabButton").addEventListener("click", () => {
  activeTab = "audit";
  renderAuditAndLogs();
});

$("logTabButton").addEventListener("click", () => {
  activeTab = "logs";
  renderAuditAndLogs();
});

refresh();
setInterval(refresh, 5000);
