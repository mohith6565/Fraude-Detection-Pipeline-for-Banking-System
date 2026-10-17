const merchants = [
  "Starbucks", "Target", "Electronics Store", "ATM Withdrawal", "Ride Share", "Grocery Market",
  "Online Marketplace", "Pharmacy", "Hotel Booking", "Airline Ticket", "Gas Station", "Luxury Retail"
];

const locations = [
  "New York, US", "Austin, US", "Chicago, US", "San Francisco, US", "Miami, US", "Seattle, US",
  "London, UK", "Toronto, CA", "Berlin, DE", "Singapore, SG"
];

const cards = [
  "Visa **** 4821", "Mastercard **** 1024", "Amex **** 9302", "Visa **** 7748", "Discover **** 6642", "Mastercard **** 5519"
];

const fraudTypes = [
  {
    name: "Card-Not-Present",
    signal: "Unusual merchant, amount, or billing pattern without a physical card swipe.",
    cue: "Strong checkout velocity and address mismatch"
  },
  {
    name: "Account Takeover",
    signal: "Known user profile, but new device, region, or login fingerprint.",
    cue: "Device change plus location jump"
  },
  {
    name: "Synthetic Identity",
    signal: "New profile with thin history that ramps quickly into high-value behavior.",
    cue: "Thin-file users and abrupt spend growth"
  },
  {
    name: "Refund Abuse",
    signal: "Repeated returns, chargebacks, or reversal cycles across the same merchant.",
    cue: "Repeated reversal attempts"
  },
  {
    name: "Mule Transfer",
    signal: "Rapid movement of funds to unusual destinations after an account compromise.",
    cue: "Burst transfers with velocity spikes"
  }
];

const stackLayers = [
  { name: "Kafka", role: "ingestion firehose", detail: "Buffers transaction events with burst tolerance." },
  { name: "Spark Streaming", role: "feature engineering", detail: "Builds rolling and velocity features on the fly." },
  { name: "FastAPI", role: "inference edge", detail: "Serves low-latency model scoring and control APIs." },
  { name: "Redis", role: "hot feature store", detail: "Keeps recent activity for sub-millisecond lookups." },
  { name: "Prometheus + Evidently", role: "observability", detail: "Tracks drift, latency, and model behavior changes." },
  { name: "Docker + Kubernetes", role: "delivery plane", detail: "Packages and scales the system in production." }
];

const state = {
  threshold: 80,
  autoReject: false,
  total: 0,
  flagged: 0,
  shadowDivergence: 0,
  history: new Map(),
  transactions: [],
  nextId: 1001,
  attackUntil: 0,
  attackInterval: null,
  apiOverview: null,
  alertMessage: null,
  alertTimer: null,
};

const els = {
  thresholdSlider: document.getElementById("thresholdSlider"),
  thresholdValue: document.getElementById("thresholdValue"),
  autoRejectSwitch: document.getElementById("autoRejectSwitch"),
  injectAttack: document.getElementById("injectAttack"),
  clearStream: document.getElementById("clearStream"),
  streamAlert: document.getElementById("streamAlert"),
  transactionList: document.getElementById("transactionList"),
  totalCount: document.getElementById("totalCount"),
  flaggedCount: document.getElementById("flaggedCount"),
  falsePositive: document.getElementById("falsePositive"),
  shadowDivergence: document.getElementById("shadowDivergence"),
  fraudTypeGrid: document.getElementById("fraudTypeGrid"),
  stackGrid: document.getElementById("stackGrid")
};

function randomItem(list) {
  return list[Math.floor(Math.random() * list.length)];
}

function amountForMerchant(merchant) {
  const buckets = {
    "ATM Withdrawal": [40, 80, 120, 200, 320],
    "Airline Ticket": [180, 260, 410, 650, 980],
    "Hotel Booking": [120, 220, 310, 450, 760],
    "Luxury Retail": [220, 340, 620, 950, 1450],
    "Electronics Store": [90, 180, 260, 520, 899]
  };

  const pool = buckets[merchant] || [8, 14, 22, 35, 49, 68];
  const base = randomItem(pool);
  const spread = Math.max(2, Math.round(base * 0.16));
  return +(base + (Math.random() * spread * 2 - spread)).toFixed(2);
}

function getHistory(cardId) {
  return state.history.get(cardId) || { count: 0, lastLocation: null, lastAmount: 0 };
}

function scoreTransaction(tx) {
  const history = getHistory(tx.card);
  let score = 12 + Math.random() * 18;

  if (tx.amount > 400) score += 22;
  else if (tx.amount > 180) score += 12;
  else if (tx.amount < 12) score += 5;

  if (/ATM|Luxury|Airline|Hotel/.test(tx.merchant)) score += 10;

  if (history.count === 0) score += 14;
  else if (history.count < 3) score += 8;

  if (history.lastLocation && history.lastLocation !== tx.location) score += 18;
  if (history.lastAmount > 0 && Math.abs(tx.amount - history.lastAmount) > 250) score += 12;

  if (state.attackUntil > Date.now()) score = 90 + Math.random() * 10;

  const shadowScore = Math.max(0, Math.min(100, score + (Math.random() * 14 - 7)));
  state.shadowDivergence = Math.abs(shadowScore - score);

  return Math.max(0, Math.min(100, Math.round(score)));
}

function formatTransactionTime(date) {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(date);
}

function createTransaction(forceFraud = false) {
  const merchant = forceFraud ? randomItem(["Online Marketplace", "Luxury Retail", "Airline Ticket", "Hotel Booking", "Electronics Store"]) : randomItem(merchants);
  const card = randomItem(cards);
  const location = randomItem(locations);
  const amount = amountForMerchant(merchant);
  const baseAmount = forceFraud ? amount * (1.4 + Math.random() * 1.4) : amount;
  const tx = {
    id: `TX-${state.nextId++}`,
    merchant,
    amount: +baseAmount.toFixed(2),
    location,
    card,
    timestamp: new Date()
  };

  const score = scoreTransaction(tx);
  const flagged = score > state.threshold;
  const shadowScore = Math.max(0, Math.min(100, score + Math.round(Math.random() * 12 - 6)));

  const record = {
    ...tx,
    score,
    shadowScore,
    flagged,
    rejected: flagged && state.autoReject
  };

  state.total += 1;
  if (flagged) state.flagged += 1;

  const history = getHistory(card);
  state.history.set(card, {
    count: history.count + 1,
    lastLocation: location,
    lastAmount: record.amount
  });

  state.transactions.unshift(record);
  if (state.autoReject && flagged) {
    state.transactions = state.transactions.filter((item) => !item.flagged);
  }

  state.transactions = state.transactions.slice(0, 20);
  render();
}

function falsePositiveEstimate() {
  const thresholdEffect = Math.max(0, 100 - state.threshold);
  const volumeEffect = Math.min(20, state.total / 2);
  return Math.max(0.8, (thresholdEffect * 0.32 + volumeEffect) / 2);
}

function showStreamAlert(message) {
  state.alertMessage = message;
  if (state.alertTimer) {
    clearTimeout(state.alertTimer);
  }
  state.alertTimer = setTimeout(() => {
    state.alertMessage = null;
    render();
  }, 8000);
  render();
}

function render() {
  els.totalCount.textContent = state.total.toLocaleString();
  els.flaggedCount.textContent = state.flagged.toLocaleString();
  els.falsePositive.textContent = `${falsePositiveEstimate().toFixed(1)}%`;
  els.shadowDivergence.textContent = `${state.shadowDivergence.toFixed(1)}%`;
  els.thresholdValue.textContent = state.threshold;
  els.autoRejectSwitch.classList.toggle("active", state.autoReject);
  els.autoRejectSwitch.setAttribute("aria-checked", String(state.autoReject));

  els.transactionList.innerHTML = state.transactions.map((tx) => {
    const decision = tx.flagged ? "Flagged" : "Approved";
    const badgeClass = tx.flagged ? "flagged" : "allowed";
    const amountColor = tx.flagged ? "var(--danger)" : "var(--text)";
    const rejectLabel = tx.rejected ? "Rejected" : decision;
    const badgeLabel = tx.attack ? "Attack" : rejectLabel;

    return `
      <article class="card ${tx.flagged ? 'flagged' : ''} ${tx.rejected ? 'rejected' : ''} ${tx.attack ? 'attack' : ''}">
        <div class="title">
          <div class="merchant">${tx.merchant}</div>
          <div class="meta">${tx.id} · ${tx.card} · ${tx.location}</div>
          <div class="meta">${formatTransactionTime(tx.timestamp)}</div>
        </div>
        <div class="amount" style="color:${amountColor}">₹${tx.amount.toFixed(2)}</div>
        <div class="score"><strong>${tx.score}</strong><span class="meta">Fraud Score</span></div>
        <div class="decision">
          <div class="badge ${badgeClass}">${badgeLabel}</div>
        </div>
      </article>
    `;
  }).join("");

  if (els.fraudTypeGrid) {
    els.fraudTypeGrid.innerHTML = fraudTypes.map((item) => `
      <div class="metric">
        <div class="label">${item.name}</div>
        <div class="sub" style="margin-top: 8px;">${item.signal}</div>
        <div class="sub" style="margin-top: 12px; color: var(--accent);">${item.cue}</div>
      </div>
    `).join("");
  }

  if (els.streamAlert) {
    if (state.alertMessage) {
      els.streamAlert.style.display = "block";
      els.streamAlert.textContent = state.alertMessage;
    } else {
      els.streamAlert.style.display = "none";
      els.streamAlert.textContent = "";
    }
  }

  if (els.stackGrid) {
    const source = state.apiOverview?.stack ? state.apiOverview.stack : stackLayers;
    els.stackGrid.innerHTML = source.map((item) => `
      <div class="metric">
        <div class="label">${item.name}</div>
        <div class="sub" style="margin-top: 8px;">${item.role || item.stage || ""}</div>
        <div class="sub" style="margin-top: 12px; color: var(--accent);">${item.detail || item.description || ""}</div>
      </div>
    `).join("");
  }
}

function startAttackBurst() {
  state.attackUntil = Date.now() + 5000;
  if (state.attackInterval) clearInterval(state.attackInterval);

  showStreamAlert("⚠️ Local fraud attack burst started — watch the live stream.");
  createTransaction(true);
  state.attackInterval = setInterval(() => {
    if (Date.now() > state.attackUntil) {
      clearInterval(state.attackInterval);
      state.attackInterval = null;
      return;
    }
    createTransaction(true);
  }, 500);
}

function seedStream() {
  for (let i = 0; i < 5; i += 1) {
    createTransaction(false);
  }
}

els.thresholdSlider.addEventListener("input", (event) => {
  state.threshold = Number(event.target.value);
  render();
});

els.autoRejectSwitch.addEventListener("click", () => {
  state.autoReject = !state.autoReject;
  render();
});

els.autoRejectSwitch.addEventListener("keydown", (event) => {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    state.autoReject = !state.autoReject;
    render();
  }
});

els.injectAttack.addEventListener("click", startAttackBurst);

els.clearStream.addEventListener("click", () => {
  state.transactions = [];
  state.flagged = 0;
  state.total = 0;
  state.shadowDivergence = 0;
  state.history.clear();
  render();
});

seedStream();
render();

// Prefer server-sent events for efficient live streaming when available
if (typeof EventSource !== "undefined") {
  const es = new EventSource("/events/transactions");
  es.onmessage = (ev) => {
    try {
      const tx = JSON.parse(ev.data);
      // determine flagged/rejected using client threshold and state
      const score = tx.score || 0;
      const shadowScore = tx.shadowScore || 0;
      const flagged = score > state.threshold;
      const rejected = flagged && state.autoReject;

      const record = {
        id: tx.id,
        merchant: tx.merchant,
        amount: tx.amount,
        location: tx.location,
        card: tx.card,
        timestamp: new Date(tx.timestamp),
        score: score,
        shadowScore: shadowScore,
        flagged: flagged,
        rejected: rejected,
        attack: tx.attack === true,
      };

      if (record.attack) {
        state.attackUntil = Date.now() + 6000;
        showStreamAlert("🚨 Admin attack burst injected — watch the flagged stream!");
      }

      state.total += 1;
      if (flagged) state.flagged += 1;

      const history = getHistory(record.card);
      state.history.set(record.card, {
        count: history.count + 1,
        lastLocation: record.location,
        lastAmount: record.amount,
      });

      state.transactions.unshift(record);
      if (state.autoReject && flagged) {
        state.transactions = state.transactions.filter((item) => !item.flagged);
      }

      state.transactions = state.transactions.slice(0, 20);
      render();
    } catch (err) {
      // ignore malformed events
    }
  };
  es.onerror = () => {
    // on error, close and fallback to client simulation
    es.close();
  };
} else {
  // Fallback: poll overview and keep local simulation
  setInterval(() => {
    if (state.attackUntil > Date.now()) return;
    createTransaction(false);
  }, 2000);

  fetch("/api/overview")
    .then((response) => response.ok ? response.json() : Promise.reject(new Error("overview unavailable")))
    .then((payload) => {
      state.apiOverview = payload;
      render();
    })
    .catch(() => {
      state.apiOverview = null;
      render();
    });
}