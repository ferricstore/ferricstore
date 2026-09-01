(function () {
  "use strict";

  // Lead with workflow stories and visual explanations. Code-forward comparisons
  // follow them, then lower-level messaging and data-structure demonstrations.
  var demos = [
    {
      id: "workflow-explainer",
      title: "Workflows for Humans",
      icon: "WF",
      description: "Learn durable workflows through a familiar save-point story before exploring leases, retries, and recovery.",
      inspect: "Which steps are volatile, which are committed, and who may continue after a crash.",
      topics: ["Workflows", "Start here"],
      accent: "#6366f1"
    },
    {
      id: "ai-agent-workflow",
      title: "AI Agent & Human Approval",
      icon: "AI",
      description: "Park a long-running agent in a durable approval state, survive a host restart, and resume from an external signal.",
      inspect: "The durable approval state, restart boundary, and external signal that resumes work.",
      topics: ["Workflows", "Coordination"],
      accent: "#34d399"
    },
    {
      id: "travel-saga",
      title: "Travel Booking Saga",
      icon: "SG",
      description: "Coordinate flight, hotel, and car reservations with explicit compensation steps when a later booking fails.",
      inspect: "The compensation order and which earlier bookings must be undone after a failure.",
      topics: ["Workflows", "Coordination"],
      accent: "#f59e0b"
    },
    {
      id: "subscription-dunning",
      title: "Subscription Dunning",
      icon: "DN",
      description: "Move a subscription through retry windows, payment recovery, and cancellation using durable timers and explicit state.",
      inspect: "Retry due times, recovered payment state, and the cancellation boundary.",
      topics: ["Workflows"],
      accent: "#f472b6"
    },
    {
      id: "ticket-reservation",
      title: "Flash-Sale Reservation",
      icon: "TK",
      description: "Hold scarce inventory with durable expiration, guarded ownership, and a clean transition from reservation to purchase.",
      inspect: "Lease expiry, the new owner's fence, and rejection of a stale buyer.",
      topics: ["Workflows", "Coordination"],
      accent: "#f97316"
    },
    {
      id: "canary-rollback",
      title: "Canary Rollback",
      icon: "CN",
      description: "Model deployment observation, health thresholds, promotion, and rollback as visible durable states.",
      inspect: "The durable soak state, health signal, restart, and rollback decision.",
      topics: ["Workflows", "Coordination"],
      accent: "#fbbf24"
    },
    {
      id: "parallel-fanout",
      title: "Parallel Fan-Out",
      icon: "FX",
      description: "Split a batch into independently recoverable children, then join their results without replaying successful work.",
      inspect: "Which child failed, which fifteen stayed committed, and how the join completes.",
      topics: ["Workflows", "Queues & streams"],
      accent: "#60a5fa"
    },
    {
      id: "agent-loop",
      title: "Agent Reliability Controls",
      icon: "AG",
      description: "Stop runaway agent spend with durable budgets and protect downstream services with persistent circuit state.",
      inspect: "Budget remaining, circuit state, and the stop reason for a guarded run.",
      topics: ["Workflows", "Coordination"],
      accent: "#ec4899"
    },
    {
      id: "split-lab",
      title: "Split Lab",
      icon: "SL",
      description: "Crash an AI research worker during summarization and watch a replacement resume from durable state without repeating completed work.",
      inspect: "The exact middle state reclaimed by a replacement worker after lease expiry.",
      topics: ["Workflows", "Start here"],
      accent: "#8b5cf6"
    },
    {
      id: "idempotency-determinism",
      title: "Idempotency & Determinism",
      icon: "ID",
      description: "Explore the duplicate-side-effect boundary and protect retries with stable operation identities and guarded effects.",
      inspect: "How a stable operation identity lets a retry reuse the same external-effect boundary.",
      topics: ["Workflows", "Coordination"],
      accent: "#a78bfa"
    },
    {
      id: "zombie-fencing",
      title: "Zombie Worker Fencing",
      icon: "FN",
      description: "See a stale worker return after lease expiry and lose its write to a newer monotonic fencing token.",
      inspect: "The newer fencing generation and the stale write FerricStore rejects.",
      topics: ["Coordination"],
      accent: "#fb7185"
    },
    {
      id: "architecture-comparison",
      title: "Architecture Comparison",
      icon: "CP",
      description: "Compare volatile scripts, queues, database polling, and durable workflow state under the same failure conditions.",
      inspect: "What each architecture remembers after the identical crash and how it recovers.",
      topics: ["Coordination", "Start here"],
      accent: "#38bdf8"
    },
    {
      id: "benchmark-explainer",
      title: "Workflow Benchmark, Explained",
      icon: "BM",
      description: "See what 54K worker-driven and 104K fused workflows per second actually mean, then compare their boundaries with Temporal and DBOS.",
      inspect: "Execution boundary, workload, hardware, throughput unit, and stated limitations behind each number.",
      topics: ["Coordination", "Start here"],
      accent: "#22d3ee"
    },
    {
      id: "hot-cold-storage",
      title: "Hot & Cold Storage",
      icon: "HC",
      description: "Follow values between memory and disk-backed storage as pressure changes, without losing the logical keyspace.",
      inspect: "The logical key remains available as its value moves to disk and warms back into memory.",
      topics: ["Data structures"],
      accent: "#2dd4bf"
    },
    {
      id: "rate-limiting-stream",
      title: "Rate Limiting & Micro-Batching",
      icon: "RL",
      description: "Absorb a webhook burst into a durable stream, enforce throughput limits, and process bounded batches.",
      inspect: "Buffer depth, allowed rate, batch size, and worker drain behavior during a burst.",
      topics: ["Queues & streams", "Coordination"],
      accent: "#22d3ee"
    },
    {
      id: "beginner-queue",
      title: "Your First Durable Queue",
      icon: "Q1",
      description: "Enqueue, claim, lease, retry, and complete a background job with the core ownership rules made visible.",
      inspect: "Job state, lease owner, expiry, and safe reclaim by the next worker.",
      topics: ["Queues & streams", "Start here"],
      accent: "#818cf8"
    },
    {
      id: "cache-stampede",
      title: "Cache Stampede Shield",
      icon: "CS",
      description: "Coordinate one recomputation while concurrent callers reuse the result instead of overwhelming the origin.",
      inspect: "One recompute owner, waiting callers, and the reduction in origin requests.",
      topics: ["Data structures", "Coordination"],
      accent: "#34d399"
    },
    {
      id: "stream-vs-pubsub",
      title: "Streams vs. Pub/Sub",
      icon: "SP",
      description: "Contrast replayable consumer-group delivery with live broadcast messaging and choose the right primitive.",
      inspect: "What an offline consumer misses, what can replay, and when acknowledgment matters.",
      topics: ["Queues & streams", "Coordination"],
      accent: "#38bdf8"
    },
    {
      id: "hash-field-ttl",
      title: "Hash Field TTL",
      icon: "TTL",
      description: "Expire individual hash fields independently while the surrounding object and its other fields remain available.",
      inspect: "Each field's independent expiry while the rest of the hash remains available.",
      topics: ["Data structures"],
      accent: "#f59e0b"
    },
    {
      id: "probabilistic-cache",
      title: "Probabilistic Filters",
      icon: "PF",
      description: "Use Bloom and Cuckoo filters to reject definite misses early and reduce unnecessary origin lookups.",
      inspect: "Definite misses rejected before origin and possible hits that still require a lookup.",
      topics: ["Data structures"],
      accent: "#c084fc"
    }
  ];

  var ids = new Set();
  demos.forEach(function (demo) {
    if (ids.has(demo.id)) throw new Error("Duplicate demo id: " + demo.id);
    ids.add(demo.id);
  });

  var topicOrder = ["All", "Workflows", "Queues & streams", "Coordination", "Data structures"];
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
    article.dataset.search = [demo.title, demo.description, demo.inspect].concat(demo.topics).join(" ").toLowerCase();
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

    var insight = document.createElement("div");
    insight.className = "card-insight";
    var insightLabel = document.createElement("span");
    insightLabel.textContent = "Inspect";
    var insightText = document.createElement("strong");
    insightText.textContent = demo.inspect;
    insight.append(insightLabel, insightText);

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

    link.append(top, title, description, insight, footer);
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
