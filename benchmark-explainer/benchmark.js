(function () {
  "use strict";

  var WORKFLOWS_PER_SECOND = 54060;
  var stepSlider = document.querySelector("[data-step-slider]");
  var stepCount = document.querySelector("[data-step-count]");
  var stepWord = document.querySelector("[data-step-word]");
  var stepCountCopy = document.querySelector("[data-step-count-copy]");
  var equationSteps = document.querySelector("[data-equation-steps]");
  var stateActions = document.querySelector("[data-state-actions]");
  var commandRate = document.querySelector("[data-command-rate]");
  var actionTrack = document.querySelector("[data-action-track]");

  function formatInteger(value) {
    return Math.round(value).toLocaleString("en-US");
  }

  function actionLabel(index, total) {
    if (index === total) return "STEP " + index + " · COMPLETE";
    return "STEP " + index;
  }

  function renderTrack(steps) {
    actionTrack.replaceChildren();

    var create = document.createElement("span");
    create.className = "create-action";
    create.textContent = "CREATE";
    actionTrack.appendChild(create);

    for (var index = 1; index <= steps; index += 1) {
      var action = document.createElement("span");
      action.textContent = actionLabel(index, steps);
      actionTrack.appendChild(action);
    }

    actionTrack.setAttribute(
      "aria-label",
      formatInteger(steps) + " worker-applied state actions plus workflow creation"
    );
  }

  function updateMath() {
    var steps = Number(stepSlider.value);
    var stateActionRate = WORKFLOWS_PER_SECOND * steps;
    var totalCommandRate = WORKFLOWS_PER_SECOND * (steps + 1);
    stepCount.textContent = formatInteger(steps);
    stepWord.textContent = steps === 1 ? "step" : "steps";
    stepCountCopy.textContent = formatInteger(steps);
    equationSteps.textContent = formatInteger(steps);
    stateActions.textContent = formatInteger(stateActionRate) + "/s";
    commandRate.textContent = formatInteger(totalCommandRate) + " durable commands/s";
    renderTrack(steps);
  }

  stepSlider.addEventListener("input", updateMath);
  updateMath();
}());
