(function () {
  "use strict";

  var demos = [
    {
      id: "split-lab",
      title: "Split Lab",
      icon: "⚡",
      description: "Crash an AI research worker during summarization and watch a replacement resume from durable state without repeating completed work.",
      topics: ["Featured", "AI & workflows", "Reliability"],
      accent: "#8b5cf6"
    },
    {
      id: "workflow-explainer",
      title: "Workflows for Humans",
      icon: "🎮",
      description: "Learn durable workflows through a familiar save-point story before exploring leases, retries, and recovery.",
      topics: ["Getting started", "AI & workflows"],
      accent: "#6366f1"
    },
    {
      id: "architecture-comparison",
      title: "Architecture Comparison",
      icon: "◫",
      description: "Compare volatile scripts, queues, database polling, and durable workflow state under the same failure conditions.",
      topics: ["Featured", "Architecture", "Reliability"],
      accent: "#38bdf8"
    },
    {
      id: "ai-agent-workflow",
      title: "AI Agent & Human Approval",
      icon: "🤖",
      description: "Park a long-running agent in a durable approval state, survive a host restart, and resume from an external signal.",
      topics: ["Featured", "AI & workflows", "Reliability"],
      accent: "#34d399"
    },
    {
      id: "travel-saga",
      title: "Travel Booking Saga",
      icon: "✈",
      description: "Coordinate flight, hotel, and car reservations with explicit compensation steps when a later booking fails.",
      topics: ["AI & workflows", "Reliability"],
      accent: "#f59e0b"
    },
    {
      id: "subscription-dunning",
      title: "Subscription Dunning",
      icon: "↻",
      description: "Move a subscription through retry windows, payment recovery, and cancellation using durable timers and explicit state.",
      topics: ["AI & workflows"],
      accent: "#f472b6"
    },
    {
      id: "rate-limiting-stream",
      title: "Rate Limiting & Micro-Batching",
      icon: "≋",
      description: "Absorb a webhook burst into a durable stream, enforce throughput limits, and process bounded batches.",
      topics: ["Queues & messaging", "Architecture"],
      accent: "#22d3ee"
    },
    {
      id: "zombie-fencing",
      title: "Zombie Worker Fencing",
      icon: "⛨",
      description: "See a stale worker return after lease expiry and lose its write to a newer monotonic fencing token.",
      topics: ["Architecture", "Reliability"],
      accent: "#fb7185"
    },
    {
      id: "idempotency-determinism",
      title: "Idempotency & Determinism",
      icon: "◇",
      description: "Explore the duplicate-side-effect boundary and protect retries with stable operation identities and guarded effects.",
      topics: ["Getting started", "AI & workflows", "Reliability"],
      accent: "#a78bfa"
    },
    {
      id: "parallel-fanout",
      title: "Parallel Fan-Out",
      icon: "⑂",
      description: "Split a batch into independently recoverable children, then join their results without replaying successful work.",
      topics: ["AI & workflows", "Queues & messaging"],
      accent: "#60a5fa"
    },
    {
      id: "canary-rollback",
      title: "Canary Rollback",
      icon: "◒",
      description: "Model deployment observation, health thresholds, promotion, and rollback as visible durable states.",
      topics: ["AI & workflows", "Reliability"],
      accent: "#fbbf24"
    },
    {
      id: "ticket-reservation",
      title: "Flash-Sale Reservation",
      icon: "🎟",
      description: "Hold scarce inventory with durable expiration, guarded ownership, and a clean transition from reservation to purchase.",
      topics: ["Getting started", "AI & workflows"],
      accent: "#f97316"
    },
    {
      id: "agent-loop",
      title: "Agent Reliability Controls",
      icon: "◎",
      description: "Stop runaway agent spend with durable budgets and protect downstream services with persistent circuit state.",
      topics: ["Featured", "AI & workflows", "Reliability"],
      accent: "#ec4899"
    },
    {
      id: "hot-cold-storage",
      title: "Hot & Cold Storage",
      icon: "▦",
      description: "Follow values between memory and disk-backed storage as pressure changes, without losing the logical keyspace.",
      topics: ["Data structures", "Architecture"],
      accent: "#2dd4bf"
    },
    {
      id: "cache-stampede",
      title: "Cache Stampede Shield",
      icon: "☂",
      description: "Coordinate one recomputation while concurrent callers reuse the result instead of overwhelming the origin.",
      topics: ["Getting started", "Data structures", "Reliability"],
      accent: "#34d399"
    },
    {
      id: "beginner-queue",
      title: "Your First Durable Queue",
      icon: "→",
      description: "Enqueue, claim, lease, retry, and complete a background job with the core ownership rules made visible.",
      topics: ["Getting started", "Queues & messaging"],
      accent: "#818cf8"
    },
    {
      id: "stream-vs-pubsub",
      title: "Streams vs. Pub/Sub",
      icon: "⇄",
      description: "Contrast replayable consumer-group delivery with live broadcast messaging and choose the right primitive.",
      topics: ["Getting started", "Queues & messaging", "Architecture"],
      accent: "#38bdf8"
    },
    {
      id: "hash-field-ttl",
      title: "Hash Field TTL",
      icon: "⌛",
      description: "Expire individual hash fields independently while the surrounding object and its other fields remain available.",
      topics: ["Data structures"],
      accent: "#f59e0b"
    },
    {
      id: "probabilistic-cache",
      title: "Probabilistic Filters",
      icon: "∿",
      description: "Use Bloom and Cuckoo filters to reject definite misses early and reduce unnecessary origin lookups.",
      topics: ["Data structures", "Architecture"],
      accent: "#c084fc"
    }
  ];

  var ids = new Set();
  demos.forEach(function (demo) {
    if (ids.has(demo.id)) throw new Error("Duplicate demo id: " + demo.id);
    ids.add(demo.id);
  });

  var topicOrder = ["All", "Featured", "Getting started", "AI & workflows", "Queues & messaging", "Data structures", "Architecture", "Reliability"];
  var grid = document.querySelector("[data-demo-grid]");
  var filters = document.querySelector("[data-filters]");
  var search = document.querySelector("[data-search]");
  var visibleCount = document.querySelector("[data-visible-count]");
  var emptyState = document.querySelector("[data-empty-state]");
  var clearSearch = document.querySelector("[data-clear-search]");
  var activeTopic = "All";
  var query = "";

  function makeCard(demo, index) {
    var article = document.createElement("article");
    article.className = "demo-card";
    article.dataset.demoId = demo.id;
    article.dataset.topics = demo.topics.join("|");
    article.dataset.search = [demo.title, demo.description].concat(demo.topics).join(" ").toLowerCase();
    article.style.setProperty("--card-accent", demo.accent);

    var link = document.createElement("a");
    link.href = "./" + demo.id + "/";
    link.setAttribute("aria-label", "Open " + demo.title + " demo");

    var top = document.createElement("div");
    top.className = "card-top";
    var number = document.createElement("span");
    number.className = "card-number";
    number.textContent = String(index + 1).padStart(2, "0");
    var icon = document.createElement("span");
    icon.className = "card-icon";
    icon.setAttribute("aria-hidden", "true");
    icon.textContent = demo.icon;
    top.append(number, icon);

    var title = document.createElement("h3");
    title.textContent = demo.title;
    var description = document.createElement("p");
    description.textContent = demo.description;

    var footer = document.createElement("div");
    footer.className = "card-footer";
    var tags = document.createElement("div");
    tags.className = "card-tags";
    demo.topics.slice(0, 2).forEach(function (topic) {
      var tag = document.createElement("span");
      tag.className = "card-tag";
      tag.textContent = topic;
      tags.appendChild(tag);
    });
    var open = document.createElement("span");
    open.className = "card-open";
    open.setAttribute("aria-hidden", "true");
    open.textContent = "→";
    footer.append(tags, open);

    link.append(top, title, description, footer);
    article.appendChild(link);
    return article;
  }

  function applyFilters() {
    var normalizedQuery = query.trim().toLowerCase();
    var count = 0;

    grid.querySelectorAll(".demo-card").forEach(function (card) {
      var topics = (card.dataset.topics || "").split("|");
      var matchesTopic = activeTopic === "All" || topics.indexOf(activeTopic) !== -1;
      var matchesQuery = !normalizedQuery || (card.dataset.search || "").indexOf(normalizedQuery) !== -1;
      card.hidden = !(matchesTopic && matchesQuery);
      if (!card.hidden) count += 1;
    });

    visibleCount.textContent = String(count);
    emptyState.hidden = count !== 0;
  }

  function selectTopic(topic) {
    activeTopic = topic;
    filters.querySelectorAll("button").forEach(function (button) {
      button.setAttribute("aria-selected", String(button.dataset.topic === topic));
    });
    applyFilters();
  }

  demos.forEach(function (demo, index) { grid.appendChild(makeCard(demo, index)); });

  topicOrder.forEach(function (topic) {
    var button = document.createElement("button");
    button.type = "button";
    button.className = "filter-button";
    button.dataset.topic = topic;
    button.setAttribute("role", "tab");
    button.setAttribute("aria-selected", String(topic === activeTopic));
    button.textContent = topic;
    button.addEventListener("click", function () { selectTopic(topic); });
    filters.appendChild(button);
  });

  search.addEventListener("input", function () {
    query = search.value;
    applyFilters();
  });

  clearSearch.addEventListener("click", function () {
    search.value = "";
    query = "";
    selectTopic("All");
    search.focus();
  });

  applyFilters();
}());
