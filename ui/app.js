(() => {
  const orders = new Map();      // client_order_id -> order
  let selectedSide = "buy";

  const btnBuy = document.getElementById("btn-buy");
  const btnSell = document.getElementById("btn-sell");
  const inPrice = document.getElementById("in-price");
  const inQty = document.getElementById("in-qty");
  const submitBtn = document.getElementById("submit-btn");
  const formError = document.getElementById("form-error");
  const blotterBody = document.getElementById("blotter-body");
  const emptyState = document.getElementById("empty-state");
  const pillBridge = document.getElementById("pill-bridge");
  const pillGateway = document.getElementById("pill-gateway");
  const statSent = document.getElementById("stat-sent");
  const statFills = document.getElementById("stat-fills");
  const statCancels = document.getElementById("stat-cancels");
  const statRejects = document.getElementById("stat-rejects");

  btnBuy.addEventListener("click", () => setSide("buy"));
  btnSell.addEventListener("click", () => setSide("sell"));
  function setSide(side) {
    selectedSide = side;
    btnBuy.classList.toggle("active", side === "buy");
    btnSell.classList.toggle("active", side === "sell");
  }

  submitBtn.addEventListener("click", async () => {
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
      if (!res.ok) formError.textContent = data.error || "order rejected";
    } catch (e) {
      formError.textContent = "request failed: " + e;
    } finally {
      submitBtn.disabled = false;
    }
  });

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

  const RESTING = new Set(["Submitted", "Accepted", "PartialFill"]);

  function fmtTime(ts) {
    const d = new Date(ts * 1000);
    return d.toLocaleTimeString("en-US", { hour12: false }) + "." +
      String(d.getMilliseconds()).padStart(3, "0");
  }

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
    orders.set(o.client_order_id, o);
    let tr = blotterBody.querySelector(`tr[data-id="${o.client_order_id}"]`);
    if (!tr) {
      tr = document.createElement("tr");
      tr.dataset.id = o.client_order_id;
      blotterBody.prepend(tr);
    } else if (flash) {
      tr.classList.remove("flash");
      void tr.offsetWidth;   // restart the CSS animation
    }
    tr.innerHTML = rowHtml(o);
    if (flash) tr.classList.add("flash");
    emptyState.style.display = orders.size === 0 ? "block" : "none";
  }

  blotterBody.addEventListener("click", (e) => {
    const id = e.target?.dataset?.cancel;
    if (id) cancelOrder(Number(id));
  });

  function setGatewayStatus(connected) {
    pillGateway.classList.toggle("up", connected);
    pillGateway.classList.toggle("down", !connected);
  }

  function setStats(stats) {
    if (!stats) return;
    statSent.textContent = stats.orders_sent ?? 0;
    statFills.textContent = stats.fills ?? 0;
    statCancels.textContent = stats.cancels ?? 0;
    statRejects.textContent = stats.rejects ?? 0;
  }

  function connect() {
    const es = new EventSource("/events");

    es.onopen = () => {
      pillBridge.classList.add("up");
      pillBridge.classList.remove("down");
    };
    es.onerror = () => {
      pillBridge.classList.remove("up");
      pillBridge.classList.add("down");
      setGatewayStatus(false);
      // EventSource auto-reconnects; nothing else to do here.
    };
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
