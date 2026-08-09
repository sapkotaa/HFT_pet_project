(() => {
  const orders = new Map();          // client_order_id -> order
  const rttSamples = [];             // rolling window, ms
  const submitTimes = new Map();     // client_order_id -> performance.now() at submit
  const notified = new Set();        // client_order_ids already toasted for a terminal status
  let selectedSide = "buy";
  let lastMaxSeq = null;
  let lastStatsAt = null;

  const $ = (id) => document.getElementById(id);
  const btnBuy = $("btn-buy"), btnSell = $("btn-sell");
  const inPrice = $("in-price"), inQty = $("in-qty");
  const submitBtn = $("submit-btn"), formError = $("form-error");
  const blotterBody = $("blotter-body"), blotterEmpty = $("blotter-empty");
  const tapeBody = $("tape-body"), tapeEmpty = $("tape-empty");
  const historyBody = $("history-body"), historyEmpty = $("history-empty"), historyMeta = $("history-meta");
  const pillBridge = $("pill-bridge"), pillGateway = $("pill-gateway");
  const statSent = $("stat-sent"), statFills = $("stat-fills"), statCancels = $("stat-cancels"), statRejects = $("stat-rejects");
  const seqOdometer = $("seq-odometer"), evtRate = $("evt-rate"), rttAvg = $("rtt-avg");
  const toasts = $("toasts");

  const RESTING = new Set(["Submitted", "Accepted", "PartialFill"]);
  const TERMINAL = new Set(["Filled", "Canceled", "Rejected"]);

  // ---------- side toggle ----------
  function setSide(side) {
    selectedSide = side;
    btnBuy.classList.toggle("active", side === "buy");
    btnSell.classList.toggle("active", side === "sell");
  }
  btnBuy.addEventListener("click", () => setSide("buy"));
  btnSell.addEventListener("click", () => setSide("sell"));

  // ---------- order entry ----------
  async function submitOrder() {
    formError.textContent = "";
    const price = Number(inPrice.value);
    const quantity = Number(inQty.value);
    if (!price || !quantity) {
      formError.textContent = "Enter a valid price and quantity.";
      return;
    }
    submitBtn.disabled = true;
    try {
      const res = await fetch("/api/order", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ side: selectedSide, price, quantity }),
      });
      const data = await res.json();
      if (!res.ok) {
        formError.textContent = data.error || "order rejected";
      } else {
        submitTimes.set(data.client_order_id, performance.now());
      }
    } catch (e) {
      formError.textContent = "request failed: " + e;
    } finally {
      submitBtn.disabled = false;
    }
  }
  submitBtn.addEventListener("click", submitOrder);

  async function cancelOrder(clientOrderId) {
    try {
      await fetch("/api/cancel", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ client_order_id: clientOrderId }),
      });
    } catch (e) {
      console.error("cancel failed", e);
    }
  }

  // ---------- keyboard shortcuts ----------
  document.addEventListener("keydown", (e) => {
    if (e.key === "F1") { e.preventDefault(); activateTab("blotter"); }
    else if (e.key === "F2") { e.preventDefault(); activateTab("tape"); }
    else if (e.key === "F3") { e.preventDefault(); activateTab("history"); }
    else if (e.key === "Enter") { submitOrder(); }
    else if ((e.key === "b" || e.key === "B") && document.activeElement.tagName !== "INPUT") setSide("buy");
    else if ((e.key === "s" || e.key === "S") && document.activeElement.tagName !== "INPUT") setSide("sell");
  });

  // ---------- tabs ----------
  function activateTab(name) {
    document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.dataset.tab === name));
    document.querySelectorAll(".tab-panel").forEach((p) => { p.hidden = p.id !== `panel-${name}`; });
    if (name === "history" && historyBody.children.length === 0) loadHistory();
  }
  document.querySelectorAll(".tab").forEach((t) => t.addEventListener("click", () => activateTab(t.dataset.tab)));

  // ---------- formatting ----------
  function fmtTime(ts) {
    const d = new Date(ts * 1000);
    return d.toLocaleTimeString("en-US", { hour12: false }) + "." + String(d.getMilliseconds()).padStart(3, "0");
  }
  function fmtSeq(n) { return String(n).padStart(6, "0"); }

  // ---------- blotter ----------
  function rowHtml(o) {
    const sideClass = o.side === "sell" ? "sell" : "buy";
    const sideLabel = o.side === "sell" ? "SELL" : "BUY";
    const cancelCell = RESTING.has(o.status)
      ? `<button class="cancel-btn" data-cancel="${o.client_order_id}">Cancel</button>`
      : "";
    return `
      <td>${fmtTime(o.ts)}</td>
      <td><span class="side-tag ${sideClass}">${sideLabel}</span></td>
      <td>${o.client_order_id}</td>
      <td>${o.exchange_order_id || "—"}</td>
      <td>${o.price}</td>
      <td>${o.quantity}</td>
      <td><span class="badge ${o.status}">${o.status}</span></td>
      <td>${o.fill_price || "—"}</td>
      <td>${o.fill_quantity || 0}</td>
      <td>${o.leaves_quantity ?? o.quantity}</td>
      <td>${cancelCell}</td>
    `;
  }

  function upsertOrder(o, flash) {
    const prevStatus = orders.get(o.client_order_id)?.status;
    orders.set(o.client_order_id, o);

    let tr = blotterBody.querySelector(`tr[data-id="${o.client_order_id}"]`);
    if (!tr) {
      tr = document.createElement("tr");
      tr.dataset.id = o.client_order_id;
      blotterBody.prepend(tr);
    } else if (flash) {
      tr.classList.remove("flash"); void tr.offsetWidth;
    }
    tr.innerHTML = rowHtml(o);
    if (flash) tr.classList.add("flash");
    blotterEmpty.style.display = orders.size === 0 ? "block" : "none";

    // Only fire tape/toast/RTT side effects for genuine live updates
    // (flash === true) — during an SSE snapshot load (initial connect or
    // reconnect), `orders` was just cleared so prevStatus is undefined
    // for every row, which would otherwise replay a toast for every
    // historical order on every reconnect.
    if (flash && prevStatus !== o.status) {
      if (o.status === "Filled" || o.status === "PartialFill") addTapeRow(o);
      if (TERMINAL.has(o.status) && !notified.has(o.client_order_id + ":" + o.status)) {
        notified.add(o.client_order_id + ":" + o.status);
        showToast(o);
      }
      if (TERMINAL.has(o.status)) {
        const t0 = submitTimes.get(o.client_order_id);
        if (t0 !== undefined) {
          recordRtt(performance.now() - t0);
          submitTimes.delete(o.client_order_id);
        }
      }
    }
  }

  blotterBody.addEventListener("click", (e) => {
    const id = e.target?.dataset?.cancel;
    if (id) cancelOrder(Number(id));
  });

  // ---------- trade tape ----------
  function addTapeRow(o) {
    tapeEmpty.style.display = "none";
    const row = document.createElement("div");
    row.className = `tape-row ${o.side} flash`;
    const mark = o.side === "sell" ? "▼" : "▲";
    row.innerHTML = `
      <span class="mark">${mark}</span>
      <span class="desc"><b>${o.fill_quantity}</b> @ <b>${o.fill_price}</b> — client #${o.client_order_id} (${o.status})</span>
      <span class="time">${fmtTime(o.ts)}</span>
    `;
    tapeBody.prepend(row);
    while (tapeBody.children.length > 60) tapeBody.removeChild(tapeBody.lastChild);
  }

  // ---------- toasts ----------
  function showToast(o) {
    const kind = o.status === "Filled" ? "fill" : o.status === "Rejected" ? "reject" : "cancel";
    const label = o.status === "Filled"
      ? `FILLED  ${o.fill_quantity} @ ${o.fill_price}`
      : o.status === "Rejected"
      ? `REJECTED  client #${o.client_order_id}`
      : `CANCELED  client #${o.client_order_id}`;
    const el = document.createElement("div");
    el.className = `toast ${kind}`;
    el.textContent = label;
    toasts.appendChild(el);
    setTimeout(() => {
      el.classList.add("out");
      setTimeout(() => el.remove(), 300);
    }, 3200);
  }

  // ---------- RTT ----------
  function recordRtt(ms) {
    rttSamples.push(ms);
    if (rttSamples.length > 20) rttSamples.shift();
    const avg = rttSamples.reduce((a, b) => a + b, 0) / rttSamples.length;
    rttAvg.textContent = avg.toFixed(2) + " ms";
  }

  // ---------- stats / sequence ticker ----------
  function setStats(stats) {
    if (!stats) return;
    statSent.textContent = stats.orders_sent ?? 0;
    statFills.textContent = stats.fills ?? 0;
    statCancels.textContent = stats.cancels ?? 0;
    statRejects.textContent = stats.rejects ?? 0;
  }

  async function pollStats() {
    try {
      const res = await fetch("/api/stats");
      const data = await res.json();
      const now = performance.now();
      seqOdometer.textContent = fmtSeq(data.max_seq);
      if (lastMaxSeq !== null && data.max_seq !== lastMaxSeq) {
        seqOdometer.classList.remove("flip"); void seqOdometer.offsetWidth;
        seqOdometer.classList.add("flip");
        setTimeout(() => seqOdometer.classList.remove("flip"), 200);
      }
      if (lastMaxSeq !== null && lastStatsAt !== null) {
        const dt = (now - lastStatsAt) / 1000;
        const rate = dt > 0 ? (data.max_seq - lastMaxSeq) / dt : 0;
        evtRate.textContent = Math.max(0, rate).toFixed(1);
      }
      lastMaxSeq = data.max_seq;
      lastStatsAt = now;
    } catch (e) { /* bridge unreachable; SSE status pill already reflects this */ }
  }
  pollStats();
  setInterval(pollStats, 1000);

  // ---------- persisted history ----------
  function historyRowHtml(o) {
    const sideClass = o.side === "sell" ? "sell" : "buy";
    const sideLabel = o.side === "sell" ? "SELL" : "BUY";
    return `
      <td>${o.seq_num}</td>
      <td><span class="side-tag ${sideClass}">${sideLabel}</span></td>
      <td>${o.session_id}</td>
      <td>${o.client_order_id}</td>
      <td>${o.price}</td>
      <td>${o.quantity}</td>
      <td><span class="badge ${o.status}">${o.status}</span></td>
      <td>${o.fill_price || "—"}</td>
      <td>${o.fill_quantity || 0}</td>
    `;
  }

  async function loadHistory() {
    historyMeta.textContent = "Loading…";
    try {
      const res = await fetch("/api/history?limit=100");
      const data = await res.json();
      historyBody.innerHTML = "";
      for (const o of data.orders || []) {
        const tr = document.createElement("tr");
        tr.innerHTML = historyRowHtml(o);
        historyBody.appendChild(tr);
      }
      historyEmpty.style.display = (data.orders || []).length === 0 ? "block" : "none";
      historyMeta.textContent = `${(data.orders || []).length} orders — loaded ${new Date().toLocaleTimeString("en-US", { hour12: false })}`;
    } catch (e) {
      historyMeta.textContent = "Failed to load: " + e;
    }
  }
  $("load-history").addEventListener("click", loadHistory);

  // ---------- connection status ----------
  function setGatewayStatus(connected) {
    pillGateway.classList.toggle("up", connected);
    pillGateway.classList.toggle("down", !connected);
  }

  // ---------- live event stream ----------
  function connect() {
    const es = new EventSource("/events");
    es.onopen = () => { pillBridge.classList.add("up"); pillBridge.classList.remove("down"); };
    es.onerror = () => { pillBridge.classList.remove("up"); pillBridge.classList.add("down"); setGatewayStatus(false); };
    es.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "snapshot") {
        blotterBody.innerHTML = "";
        orders.clear();
        for (const o of msg.orders) upsertOrder(o, false);
        setGatewayStatus(!!msg.connected);
        setStats(msg.stats);
      } else if (msg.type === "update") {
        upsertOrder(msg.order, true);
        setStats(msg.stats);
      } else if (msg.type === "status") {
        setGatewayStatus(!!msg.connected);
      }
    };
  }
  connect();
})();
