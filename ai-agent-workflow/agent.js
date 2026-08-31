(function () {
  "use strict";

  var currentStep = 0;
  var isPaused = false;
  var timer = null;

  var stepsData = [
  {
    "badge": "\ud83d\udd0d RESEARCH AGENT ACTIVE",
    "badgeClass": "good",
    "title": "1. Multi-Source Competitor Research",
    "desc": "Scraping 24 competitor ad accounts and keyword bids. Step result is durably cached in FerricStore disk log.",
    "code": "return transition('draft_plan', payload={'competitors': competitors})",
    "cpu": "14.2%",
    "ram": "42 MB",
    "wait": "0s",
    "resume": "After claim"
  },
  {
    "badge": "\ud83d\udcdd LLM INFERENCE ACTIVE",
    "badgeClass": "good",
    "title": "2. Drafting $50,000 Marketing Campaign",
    "desc": "LLM generates campaign strategy. Posts Slack notification containing job.id and transitions to 'await_approval' state.",
    "code": "slack.post_approval_request(channel='#finance', job_id=job.id, plan=plan)\\nreturn transition('await_approval', payload={'plan': plan})",
    "cpu": "28.5%",
    "ram": "68 MB",
    "wait": "0s",
    "resume": "After claim"
  },
  {
    "badge": "\u23f8\ufe0f STATE: AWAIT_APPROVAL",
    "badgeClass": "warn",
    "title": "3. Waiting for Slack Webhook Signal",
    "desc": "Workflow is persisted in 'await_approval'. No application handler remains blocked while a durable external signal is pending.",
    "code": "# Persisted in state 'await_approval'; handler returned",
    "cpu": "No handler",
    "ram": "Durable state",
    "wait": "Awaiting Webhook",
    "resume": "After signal"
  },
  {
    "badge": "\ud83d\udca5 CRASH RESILIENCE TEST",
    "badgeClass": "warn",
    "title": "4. Simulating Cloud Host Crash While Parked",
    "desc": "The host server was killed. The committed approval state remains durable. A compatible worker can reclaim the workflow after the signal and lease rules allow it.",
    "code": "# Host crashed - State safe in Raft log. Ready for incoming signal webhook",
    "cpu": "No handler",
    "ram": "Durable state",
    "wait": "Safe on Disk",
    "resume": "Lease-dependent"
  },
  {
    "badge": "\ud83d\ude80 SIGNAL RECEIVED \u2794 LAUNCHING",
    "badgeClass": "good",
    "title": "5. Webhook Sends a State-Guarded Approval Signal",
    "desc": "The CFO approved in Slack. The webhook records a deduplicated signal only while the workflow is still awaiting approval, then advances it to launch_campaign.",
    "code": "client.signal(job_id, signal='approved', idempotency_key=f'{job_id}:approval:v1', if_state='await_approval', transition_to='launch_campaign', values={'approved_by': '@cfo'})",
    "cpu": "6.4%",
    "ram": "24 MB",
    "wait": "Signal Processed",
    "resume": "Next claim"
  }
];

  var nodes = document.querySelectorAll("[data-agent-node]");
  var narrativeBadge = document.querySelector("[data-narrative-badge]");
  var narrativeTitle = document.querySelector("[data-narrative-title]");
  var narrativeDesc = document.querySelector("[data-narrative-desc]");
  var narrativeCode = document.querySelector("[data-narrative-code]");

  var valCpu = document.querySelector("[data-val-cpu]");
  var valRam = document.querySelector("[data-val-ram]");
  var valWait = document.querySelector("[data-val-wait]");
  var valResume = document.querySelector("[data-val-resume]");

  var prevBtn = document.querySelector("[data-prev]");
  var pauseBtn = document.querySelector("[data-pause]");
  var nextBtn = document.querySelector("[data-next]");
  var replayBtn = document.querySelector("[data-replay]");
  var liveStatus = document.querySelector("[data-live-status]");
  var killBtn = document.querySelector("[data-kill-btn]");

  var slackApprove = document.querySelector("[data-slack-approve]");
  var slackReject = document.querySelector("[data-slack-reject]");

  function render() {
    var data = stepsData[currentStep];

    nodes.forEach(function (node, idx) {
      var isDone = idx < currentStep;
      var isActive = idx === currentStep;
      node.classList.toggle("is-done", isDone);
      node.classList.toggle("is-active", isActive);

      var pill = node.querySelector(".node-pill");
      if (pill) {
        if (isDone) pill.textContent = "✓ DONE";
        else if (isActive) pill.textContent = (idx === 2 ? "PARKED" : (idx === 3 ? "CRASH TEST" : "ACTIVE"));
        else pill.textContent = "PENDING";
      }
    });

    if (narrativeBadge) {
      narrativeBadge.textContent = data.badge;
      narrativeBadge.className = "agent-badge " + data.badgeClass;
    }
    if (narrativeTitle) narrativeTitle.textContent = data.title;
    if (narrativeDesc) narrativeDesc.textContent = data.desc;
    if (narrativeCode) narrativeCode.textContent = data.code;

    if (valCpu) valCpu.textContent = data.cpu;
    if (valRam) valRam.textContent = data.ram;
    if (valWait) valWait.textContent = data.wait;
    if (valResume) valResume.textContent = data.resume;

    var canSignal = currentStep === 2 || currentStep === 3;
    if (slackApprove) slackApprove.disabled = !canSignal;
    if (slackReject) slackReject.disabled = !canSignal;
    if (killBtn) {
      killBtn.disabled = currentStep !== 2;
      killBtn.title = currentStep === 2
        ? "Kill the host while the workflow is waiting for approval"
        : "Available when the workflow reaches the approval wait";
    }

    if (pauseBtn) pauseBtn.textContent = isPaused ? "▶ Play" : "⏸ Pause";
    if (liveStatus) liveStatus.textContent = isPaused ? "Simulation Paused" : "Auto-advancing simulation";
  }

  function clearTimer() {
    if (timer !== null) {
      window.clearTimeout(timer);
      timer = null;
    }
  }

  function schedule() {
    clearTimer();
    if (isPaused) return;
    var wait = (currentStep === 2) ? 5000 : 3000;
    timer = window.setTimeout(function () {
      currentStep = (currentStep + 1) % stepsData.length;
      render();
      schedule();
    }, wait);
  }

  nodes.forEach(function (node, idx) {
    node.addEventListener("click", function () {
      currentStep = idx;
      render();
      schedule();
    });
  });

  if (slackApprove) {
    slackApprove.addEventListener("click", function () {
      currentStep = 4;
      render();
      schedule();
    });
  }

  if (slackReject) {
    slackReject.addEventListener("click", function () {
      currentStep = 1;
      render();
      schedule();
    });
  }

  if (killBtn) {
    killBtn.addEventListener("click", function () {
      currentStep = 3;
      render();
      schedule();
    });
  }

  if (pauseBtn) {
    pauseBtn.addEventListener("click", function () {
      isPaused = !isPaused;
      render();
      schedule();
    });
  }

  if (prevBtn) {
    prevBtn.addEventListener("click", function () {
      currentStep = (currentStep - 1 + stepsData.length) % stepsData.length;
      render();
      schedule();
    });
  }

  if (nextBtn) {
    nextBtn.addEventListener("click", function () {
      currentStep = (currentStep + 1) % stepsData.length;
      render();
      schedule();
    });
  }

  if (replayBtn) {
    replayBtn.addEventListener("click", function () {
      currentStep = 0;
      isPaused = false;
      render();
      schedule();
    });
  }

  render();
  schedule();
})();
