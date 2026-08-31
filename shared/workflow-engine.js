(function (global) {
  "use strict";

  var MODES = Object.freeze({
    DURABLE: "durable",
    UNMANAGED: "unmanaged"
  });

  var PHASES = Object.freeze({
    IDLE: "idle",
    PLAN: "plan",
    SEARCH: "search",
    SUMMARIZE: "summarize",
    CRASHED: "crashed",
    RECOVERING: "recovering",
    RESTARTING: "restarting",
    REPLAN: "replan",
    RESEARCH: "research",
    RESUMMARIZE: "resummarize",
    COMPLETE: "complete"
  });

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function nowLabel() {
    return new Date().toISOString().slice(11, 23);
  }

  function initialState(mode) {
    return {
      mode: mode,
      phase: PHASES.IDLE,
      status: "IDLE",
      stepIndex: -1,
      activeWorker: "worker-a",
      previousWorker: null,
      workerState: "ready",
      fencingToken: 41,
      previousFencingToken: null,
      crashReady: false,
      persisted: {},
      volatile: {},
      repeatedSteps: [],
      message: "Ready to run the AI research workflow.",
      logs: []
    };
  }

  function WorkflowEngine(options) {
    options = options || {};
    this.delay = Math.max(120, Number(options.delay) || 900);
    this.mode = options.mode === MODES.UNMANAGED ? MODES.UNMANAGED : MODES.DURABLE;
    this.state = initialState(this.mode);
    this.listeners = [];
    this.timer = null;
    this.autoPlay = false;
  }

  WorkflowEngine.prototype.snapshot = function () {
    return clone(this.state);
  };

  WorkflowEngine.prototype.subscribe = function (listener) {
    if (typeof listener !== "function") return function () {};
    this.listeners.push(listener);
    listener({ type: "engine:ready", state: this.snapshot(), payload: {} });
    var self = this;
    return function () {
      self.listeners = self.listeners.filter(function (candidate) {
        return candidate !== listener;
      });
    };
  };

  WorkflowEngine.prototype._emit = function (type, payload, announcement) {
    var event = {
      id: this.state.logs.length + 1,
      at: nowLabel(),
      type: type,
      message: announcement || this.state.message,
      payload: payload || {}
    };
    this.state.logs.push(event);
    var envelope = { type: type, state: this.snapshot(), payload: clone(payload || {}) };
    this.listeners.slice().forEach(function (listener) {
      listener(envelope);
    });
  };

  WorkflowEngine.prototype._clearTimer = function () {
    if (this.timer) global.clearTimeout(this.timer);
    this.timer = null;
  };

  WorkflowEngine.prototype._schedule = function () {
    var self = this;
    this._clearTimer();
    if (!this.autoPlay || this.state.status !== "RUNNING") return;
    this.timer = global.setTimeout(function () {
      self.timer = null;
      self._advance();
      self._schedule();
    }, this.delay);
  };

  WorkflowEngine.prototype.setMode = function (mode) {
    var nextMode = mode === MODES.UNMANAGED ? MODES.UNMANAGED : MODES.DURABLE;
    this._clearTimer();
    this.autoPlay = false;
    this.mode = nextMode;
    this.state = initialState(nextMode);
    this._emit(
      "mode:changed",
      { mode: nextMode },
      nextMode === MODES.DURABLE
        ? "Durable mode selected. Completed states survive the worker crash."
        : "Unmanaged baseline selected. Process memory is lost on crash."
    );
  };

  WorkflowEngine.prototype.start = function () {
    if (this.state.phase === PHASES.COMPLETE) this.reset(false);
    if (this.state.status === "AWAITING_CRASH") return;
    this.autoPlay = true;
    this.state.status = "RUNNING";
    this.state.message = this.state.phase === PHASES.IDLE
      ? "Workflow started. Planning the research task."
      : "Workflow resumed.";
    this._emit("run:started", { phase: this.state.phase }, this.state.message);
    this._advance();
    this._schedule();
  };

  WorkflowEngine.prototype.pause = function () {
    if (this.state.status !== "RUNNING") return;
    this.autoPlay = false;
    this._clearTimer();
    this.state.status = "PAUSED";
    this.state.message = "Playback paused. The workflow state is unchanged.";
    this._emit("run:paused", { phase: this.state.phase }, this.state.message);
  };

  WorkflowEngine.prototype.step = function () {
    if (this.state.status === "AWAITING_CRASH") return;
    if (this.state.phase === PHASES.COMPLETE) {
      this.reset(false);
      return;
    }
    this.autoPlay = false;
    this._clearTimer();
    this.state.status = "PAUSED";
    this._advance();
    if (this.state.status === "RUNNING") {
      this.state.status = "PAUSED";
      this.state.message = "Advanced one state. Playback remains paused.";
      this._emit("run:stepped", { phase: this.state.phase }, this.state.message);
    }
  };

  WorkflowEngine.prototype.crashWorker = function () {
    if (this.state.phase !== PHASES.SUMMARIZE || !this.state.crashReady) return false;
    this._clearTimer();
    this.state.phase = PHASES.CRASHED;
    this.state.stepIndex = 3;
    this.state.status = "CRASHED";
    this.state.crashReady = false;
    this.state.workerState = "crashed";
    this.state.message = this.mode === MODES.DURABLE
      ? "Worker A crashed. Plan and search state remain persisted."
      : "Worker A crashed. Its in-memory plan and search results were lost.";
    if (this.mode === MODES.UNMANAGED) this.state.volatile = {};
    this._emit(
      "worker:crashed",
      { worker: "worker-a", retained: Object.keys(this.state.persisted) },
      this.state.message
    );
    if (this.autoPlay) {
      this.state.status = "RUNNING";
      this._schedule();
    }
    return true;
  };

  WorkflowEngine.prototype.reset = function (announce) {
    this._clearTimer();
    this.autoPlay = false;
    this.state = initialState(this.mode);
    if (announce !== false) {
      this._emit("run:reset", {}, "Workflow reset. Ready to run again.");
    } else {
      var envelope = { type: "run:reset", state: this.snapshot(), payload: {} };
      this.listeners.slice().forEach(function (listener) { listener(envelope); });
    }
  };

  WorkflowEngine.prototype._advance = function () {
    switch (this.state.phase) {
      case PHASES.IDLE:
        this._enterPlan(false);
        break;
      case PHASES.PLAN:
        this._enterSearch(false);
        break;
      case PHASES.SEARCH:
        this._enterSummarize(false);
        break;
      case PHASES.CRASHED:
        if (this.mode === MODES.DURABLE) this._enterRecovery();
        else this._enterRestart();
        break;
      case PHASES.RECOVERING:
        this._enterComplete(false);
        break;
      case PHASES.RESTARTING:
        this._enterPlan(true);
        break;
      case PHASES.REPLAN:
        this._enterSearch(true);
        break;
      case PHASES.RESEARCH:
        this._enterSummarize(true);
        break;
      case PHASES.RESUMMARIZE:
        this._enterComplete(true);
        break;
      default:
        break;
    }
  };

  WorkflowEngine.prototype._enterPlan = function (repeated) {
    this.state.phase = repeated ? PHASES.REPLAN : PHASES.PLAN;
    this.state.stepIndex = 0;
    this.state.status = "RUNNING";
    this.state.workerState = "running";
    this.state.activeWorker = repeated ? "worker-b" : "worker-a";
    this.state.volatile.plan = { queryCount: 3, ref: "plan:report-204" };
    if (this.mode === MODES.DURABLE) this.state.persisted.plan = clone(this.state.volatile.plan);
    if (repeated) this.state.repeatedSteps.push("plan");
    this.state.message = repeated
      ? "Worker B repeated planning because no durable state was available."
      : "Plan created: three research queries are ready.";
    this._emit(repeated ? "step:repeated" : "step:completed", { step: "plan" }, this.state.message);
  };

  WorkflowEngine.prototype._enterSearch = function (repeated) {
    this.state.phase = repeated ? PHASES.RESEARCH : PHASES.SEARCH;
    this.state.stepIndex = 1;
    this.state.status = "RUNNING";
    this.state.volatile.search = { sourceCount: 6, ref: "facts:report-204" };
    if (this.mode === MODES.DURABLE) this.state.persisted.search = clone(this.state.volatile.search);
    if (repeated) this.state.repeatedSteps.push("search");
    this.state.message = repeated
      ? "Worker B repeated the source search after losing process memory."
      : "Search completed: six source records are available.";
    this._emit(repeated ? "step:repeated" : "step:completed", { step: "search" }, this.state.message);
  };

  WorkflowEngine.prototype._enterSummarize = function (repeated) {
    if (repeated) {
      this.state.phase = PHASES.RESUMMARIZE;
      this.state.stepIndex = 2;
      this.state.status = "RUNNING";
      this.state.message = "Worker B can now summarize the repeated results.";
      this._emit("step:repeated", { step: "summarize" }, this.state.message);
      return;
    }
    this.state.phase = PHASES.SUMMARIZE;
    this.state.stepIndex = 2;
    this.state.status = "AWAITING_CRASH";
    this.state.crashReady = true;
    this.state.message = "Summarization is active. Crash Worker A to test recovery.";
    this._emit("crash:ready", { worker: this.state.activeWorker }, this.state.message);
  };

  WorkflowEngine.prototype._enterRecovery = function () {
    this.state.phase = PHASES.RECOVERING;
    this.state.stepIndex = 4;
    this.state.status = "RUNNING";
    this.state.previousWorker = "worker-a";
    this.state.activeWorker = "worker-b";
    this.state.workerState = "reclaimed";
    this.state.previousFencingToken = this.state.fencingToken;
    this.state.fencingToken += 1;
    this.state.message = "Worker B reclaimed the summarize state with a newer fencing token.";
    this._emit(
      "lease:reclaimed",
      {
        previousWorker: "worker-a",
        activeWorker: "worker-b",
        previousFencingToken: this.state.previousFencingToken,
        fencingToken: this.state.fencingToken,
        retained: Object.keys(this.state.persisted)
      },
      this.state.message
    );
  };

  WorkflowEngine.prototype._enterRestart = function () {
    this.state.phase = PHASES.RESTARTING;
    this.state.stepIndex = 4;
    this.state.status = "RUNNING";
    this.state.previousWorker = "worker-a";
    this.state.activeWorker = "worker-b";
    this.state.workerState = "restarting";
    this.state.message = "Worker B restarted from the beginning because no checkpoint exists.";
    this._emit("run:restarted", { activeWorker: "worker-b" }, this.state.message);
  };

  WorkflowEngine.prototype._enterComplete = function (repeated) {
    this.state.phase = PHASES.COMPLETE;
    this.state.stepIndex = 5;
    this.state.status = "COMPLETED";
    this.state.workerState = "complete";
    this.state.crashReady = false;
    this.state.volatile.summary = { ref: "summary:report-204" };
    if (this.mode === MODES.DURABLE) this.state.persisted.summary = clone(this.state.volatile.summary);
    this.autoPlay = false;
    this._clearTimer();
    this.state.message = repeated
      ? "Workflow completed after repeating Plan and Search."
      : "Workflow completed from the persisted summarize state. Plan and Search were not rerun.";
    this._emit("run:completed", { repeatedSteps: this.state.repeatedSteps.slice() }, this.state.message);
  };

  global.FerricWorkflow = Object.freeze({
    MODES: MODES,
    PHASES: PHASES,
    createEngine: function (options) { return new WorkflowEngine(options); }
  });
})(window);
