(function () {
  "use strict";

  // Lead with workflow stories and visual explanations. Code-forward comparisons
  // follow them, then lower-level messaging and data-structure demonstrations.
  var demos = [
    {
      id: "workflow-explainer",
      title: "Workflows for Humans",
      icon: "WF",
      description: "See how saving progress helps a task pick up after a crash.",
      inspect: "What is saved, what may repeat, and where work continues.",
      topics: ["Workflows", "Start here"],
      accent: "#6366f1"
    },
    {
      id: "ai-agent-workflow",
      title: "AI Agent & Human Approval",
      icon: "AI",
      description: "Pause an AI task until a person approves it, even if the server restarts while it waits.",
      inspect: "The approval that lets the task continue.",
      topics: ["Workflows", "Coordination"],
      accent: "#34d399"
    },
    {
      id: "travel-saga",
      title: "When a Travel Booking Fails",
      icon: "SG",
      description: "Book a flight, hotel, and car. See what gets cancelled if one booking fails.",
      inspect: "Which earlier bookings are undone, and in what order.",
      topics: ["Workflows", "Coordination"],
      accent: "#f59e0b"
    },
    {
      id: "subscription-dunning",
      title: "When a Subscription Payment Fails",
      icon: "DN",
      description: "Try a failed payment again later, send reminders, or cancel the subscription.",
      inspect: "When the next attempt happens and what changes if payment succeeds.",
      topics: ["Workflows"],
      accent: "#f472b6"
    },
    {
      id: "ticket-reservation",
      title: "Flash-Sale Reservation",
      icon: "TK",
      description: "Hold a ticket for a buyer, then release it if they do not pay in time.",
      inspect: "Why the first buyer cannot use an expired reservation after someone else takes it.",
      topics: ["Workflows", "Coordination"],
      accent: "#f97316"
    },
    {
      id: "canary-rollback",
      title: "Undo an Unhealthy Software Update",
      icon: "CN",
      description: "Try an update on a small scale, check for problems, and decide whether to keep it.",
      inspect: "The check that triggers a return to the previous version.",
      topics: ["Workflows", "Coordination"],
      accent: "#fbbf24"
    },
    {
      id: "parallel-fanout",
      title: "Run Many Tasks Together",
      icon: "FX",
      description: "Split a large job into smaller tasks and bring their results together.",
      inspect: "One failed task runs again while the fifteen saved results stay in place.",
      topics: ["Workflows", "Queues & streams"],
      accent: "#60a5fa"
    },
    {
      id: "agent-loop",
      title: "Set Limits for an AI Agent",
      icon: "AG",
      description: "Give an AI agent a spending limit and pause requests to a service that keeps failing.",
      inspect: "How much budget is left and why the agent stops.",
      topics: ["Workflows", "Coordination"],
      accent: "#ec4899"
    },
    {
      id: "split-lab",
      title: "Start Over or Carry On?",
      icon: "SL",
      description: "Interrupt an AI research task. Compare starting from scratch with continuing from saved progress.",
      inspect: "Where the replacement picks up and which work has already been saved.",
      topics: ["Workflows", "Start here"],
      accent: "#8b5cf6"
    },
    {
      id: "idempotency-determinism",
      title: "Avoid Repeating an Outside Action",
      icon: "ID",
      description: "See why trying a task again needs care when it calls a payment provider or another service.",
      inspect: "How keeping the same action reference helps handle repeated attempts.",
      topics: ["Workflows", "Coordination"],
      accent: "#a78bfa"
    },
    {
      id: "zombie-fencing",
      title: "Stop an Old Worker Overwriting Progress",
      icon: "FN",
      description: "A worker is a part of your application that does a job. What if it comes back after its replacement has taken over?",
      inspect: "Why FerricStore rejects the old worker’s attempt to save changes.",
      topics: ["Coordination"],
      accent: "#fb7185"
    },
    {
      id: "architecture-comparison",
      title: "Four Ways to Handle a Crash",
      icon: "CP",
      description: "Compare a script, a queue, a database-based approach, and a workflow when the same failure happens.",
      inspect: "What each approach remembers and how work starts again.",
      topics: ["Coordination", "Start here"],
      accent: "#38bdf8"
    },
    {
      id: "benchmark-explainer",
      title: "Workflow Speed Tests, Explained",
      icon: "BM",
      description: "Understand the 54K and 104K workflows-per-second results from two different tests, with context from Temporal and DBOS.",
      inspect: "What each test measures, which machines it uses, and what the numbers can tell you.",
      topics: ["Coordination", "Start here"],
      accent: "#22d3ee"
    },
    {
      id: "hot-cold-storage",
      title: "Move Data Between Memory and Disk",
      icon: "HC",
      description: "See how data moves to disk when memory fills up, and comes back when needed.",
      inspect: "The same data stays available even when its storage location changes.",
      topics: ["Data structures"],
      accent: "#2dd4bf"
    },
    {
      id: "rate-limiting-stream",
      title: "Handle a Sudden Rush of Jobs",
      icon: "RL",
      description: "Keep incoming jobs waiting safely, then process them in small groups at a set pace.",
      inspect: "How many jobs are waiting and how quickly they are handled.",
      topics: ["Queues & streams", "Coordination"],
      accent: "#22d3ee"
    },
    {
      id: "beginner-queue",
      title: "How a Job Queue Works",
      icon: "Q1",
      description: "Add a job to a waiting list, pick it up, and try it again if something goes wrong.",
      inspect: "Who is doing the job and when a replacement can take over.",
      topics: ["Queues & streams", "Start here"],
      accent: "#818cf8"
    },
    {
      id: "cache-stampede",
      title: "Share One Result with Many Requests",
      icon: "CS",
      description: "When many people ask for the same missing result, calculate it once and share it.",
      inspect: "One task does the calculation while the other requests wait.",
      topics: ["Data structures", "Coordination"],
      accent: "#34d399"
    },
    {
      id: "stream-vs-pubsub",
      title: "What Happens to Missed Messages?",
      icon: "SP",
      description: "Compare messages saved for later with live messages that only reach connected listeners.",
      inspect: "Which messages a listener can catch up on after reconnecting.",
      topics: ["Queues & streams", "Coordination"],
      accent: "#38bdf8"
    },
    {
      id: "hash-field-ttl",
      title: "Let Parts of a Record Expire",
      icon: "TTL",
      description: "Set different time limits for pieces of information in the same record.",
      inspect: "One piece expires while the rest of the record stays available.",
      topics: ["Data structures"],
      accent: "#f59e0b"
    },
    {
      id: "probabilistic-cache",
      title: "Skip Searches That Cannot Match",
      icon: "PF",
      description: "Use a quick check to rule out missing items before searching the main data store.",
      inspect: "A definite no skips the search. A possible yes still needs checking.",
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
  var topicLabels = {"Queues & streams": "Jobs & messages", "Coordination": "Working together", "Data structures": "Storing data"};
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
    article.dataset.search = [demo.id.replace(/-/g, " "), demo.title, demo.description, demo.inspect].concat(demo.topics, demo.topics.map(function (topic) { return topicLabels[topic] || topic; })).join(" ").toLowerCase();
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
    insightLabel.textContent = "Watch for";
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
      tag.textContent = topicLabels[topic] || topic;
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
      button.setAttribute("aria-pressed", String(button.dataset.topic === topic));
    });
    applyFilters();
  }

  demos.forEach(function (demo, index) { grid.appendChild(makeCard(demo, index)); });

  topicOrder.forEach(function (topic) {
    var button = document.createElement("button");
    button.type = "button";
    button.className = "filter-button";
    button.dataset.topic = topic;
    button.setAttribute("aria-pressed", String(topic === activeTopic));
    button.textContent = topicLabels[topic] || topic;
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
