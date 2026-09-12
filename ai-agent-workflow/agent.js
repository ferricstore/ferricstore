(function () {
  "use strict";

  var currentStep = 0;
  var isPaused = false;
  var hasStarted = false;
  var timer = null;

  var stepsData = [
  {
    "badge": "RESEARCH IN PROGRESS",
    "badgeClass": "good",
    "title": "1. Research campaign",
    "desc": "The workflow collects competitor data. This step is saved durably so a replacement worker can continue from it.",
    "code": "return transition('draft_plan', payload={'competitors': competitors})",
    "cpu": "14.2%",
    "ram": "42 MB",
    "wait": "0s",
    "resume": "After claim"
  },
  {
    "badge": "DRAFT IN PROGRESS",
    "badgeClass": "good",
    "title": "2. Draft campaign",
    "desc": "The model creates the campaign and sends a Slack approval request. The workflow then moves to its saved approval state.",
    "code": "slack.post_approval_request(channel='#finance-approvals', job_id=ctx.id, plan=plan, idempotency_key=f'{ctx.id}:approval:v1')\nreturn transition('await_approval', payload={'plan': plan})",
    "cpu": "28.5%",
    "ram": "68 MB",
    "wait": "0s",
    "resume": "After claim"
  },
  {
    "badge": "WAITING FOR APPROVAL",
    "badgeClass": "warn",
    "title": "3. Wait for approval",
    "desc": "The workflow is saved in await_approval. No application handler stays blocked while the person decides.",
    "code": "# Persisted in state 'await_approval'; handler returned",
    "cpu": "No handler",
    "ram": "Durable state",
    "wait": "Awaiting Webhook",
    "resume": "After signal"
  },
  {
    "badge": "HOST RESTARTED",
    "badgeClass": "warn",
    "title": "4. Host restarts while paused",
    "desc": "The host stops while the workflow is waiting. The saved approval state remains; a compatible worker can reclaim it when the signal and lease rules allow.",
    "code": "# Host crashed - State safe in Raft log. Ready for incoming signal webhook",
    "cpu": "No handler",
    "ram": "Durable state",
    "wait": "Safe on Disk",
    "resume": "Lease-dependent"
  },
  {
    "badge": "APPROVAL RECEIVED · LAUNCHING",
    "badgeClass": "good",
    "title": "5. Launch after approval",
    "desc": "The approval signal arrives from Slack. It is accepted only while the workflow is waiting, then advances the workflow to launch_campaign.",
    "code": "action_id = payload['action_id']\nclient.signal(job_id, signal='approved', if_state='await_approval', transition_to='launch_campaign', values={'approved_by': payload['user']}, idempotency_key=f'{job_id}:approve:{action_id}:v1')",
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
  var currentRunStep = document.querySelector("[data-current-run-step]");

  function render() {
    var data = stepsData[currentStep];

    nodes.forEach(function (node, idx) {
      var isDone = idx < currentStep;
      var isActive = idx === currentStep;
      node.classList.toggle("is-done", isDone);
      node.classList.toggle("is-active", isActive);
      node.setAttribute("aria-pressed", String(isActive));
      if (isActive) node.setAttribute("aria-current", "step");
      else node.removeAttribute("aria-current");
      node.setAttribute("aria-label", stepsData[idx].title.replace(/^\d+\.\s*/, "") + ". " + (isActive ? "Active" : (isDone ? "Complete" : "Pending")));

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
    if (currentRunStep) {
      currentRunStep.textContent = (!hasStarted ? "Preview · " : "Step ") + (currentStep + 1) + " of " + stepsData.length + " · " + data.title.replace(/^\d+\.\s*/, "");
    }

    var canSignal = currentStep === 2 || currentStep === 3;
    if (slackApprove) slackApprove.disabled = !canSignal;
    if (slackReject) slackReject.disabled = !canSignal;
    if (killBtn) {
      killBtn.disabled = currentStep !== 2;
      killBtn.title = currentStep === 2
        ? "Simulate a host restart while the workflow is waiting for approval"
        : "Available when the workflow reaches the approval wait";
    }

    if (prevBtn) prevBtn.disabled = currentStep === 0;
    if (nextBtn) nextBtn.disabled = currentStep >= stepsData.length - 1;
    if (pauseBtn) {
      var finished = hasStarted && currentStep >= stepsData.length - 1 && isPaused;
      pauseBtn.textContent = finished ? "Finished" : (!hasStarted || isPaused ? "Play" : "Pause");
      pauseBtn.disabled = finished;
      pauseBtn.setAttribute("aria-label", finished ? "Workflow finished; choose Replay to run it again" : "Pause or play the workflow");
    }
    if (liveStatus) liveStatus.textContent = !hasStarted
      ? "Preview ready · choose Run to play"
      : (finished ? "Finished · choose Replay to run again" : (isPaused ? "Paused · choose Play to continue" : "Playing automatically · Pause to inspect"));
  }

  function clearTimer() {
    if (timer !== null) {
      window.clearTimeout(timer);
      timer = null;
    }
  }

  function schedule() {
    clearTimer();
    if (!hasStarted || isPaused || currentStep >= stepsData.length - 1) {
      if (hasStarted && currentStep >= stepsData.length - 1) {
        isPaused = true;
        render();
      }
      return;
    }
    var wait = (currentStep === 2) ? 5000 : 3000;
    timer = window.setTimeout(function () {
      currentStep += 1;
      render();
      schedule();
    }, wait);
  }

  nodes.forEach(function (node, idx) {
    function selectNode() {
      currentStep = idx;
      hasStarted = true;
      isPaused = true;
      clearTimer();
      render();
      schedule();
    }
    node.addEventListener("click", selectNode);
    node.addEventListener("keydown", function (event) {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      selectNode();
    });
  });

  if (slackApprove) {
    slackApprove.addEventListener("click", function () {
      currentStep = 4;
      hasStarted = true;
      isPaused = false;
      render();
      schedule();
    });
  }

  if (slackReject) {
    slackReject.addEventListener("click", function () {
      currentStep = 1;
      hasStarted = true;
      isPaused = false;
      render();
      schedule();
    });
  }

  if (killBtn) {
    killBtn.addEventListener("click", function () {
      currentStep = 3;
      hasStarted = true;
      isPaused = false;
      render();
      schedule();
    });
  }

  if (pauseBtn) {
    pauseBtn.addEventListener("click", function () {
      if (!hasStarted) {
        hasStarted = true;
        isPaused = false;
      } else {
        isPaused = !isPaused;
      }
      render();
      schedule();
    });
  }

  if (prevBtn) {
    prevBtn.addEventListener("click", function () {
      hasStarted = true;
      isPaused = true;
      clearTimer();
      currentStep = Math.max(0, currentStep - 1);
      render();
    });
  }

  if (nextBtn) {
    nextBtn.addEventListener("click", function () {
      hasStarted = true;
      isPaused = true;
      clearTimer();
      currentStep = Math.min(stepsData.length - 1, currentStep + 1);
      render();
    });
  }

  if (replayBtn) {
    replayBtn.addEventListener("click", function () {
      currentStep = 0;
      hasStarted = true;
      isPaused = false;
      render();
      schedule();
    });
  }

  render();
})();
