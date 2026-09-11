defmodule FerricstoreServer.Health.Dashboard.Render.DetailActionScripts do
  @moduledoc false

  def render_rewind_script do
    """
    <script>
    (function () {
      function bind() {
        document.querySelectorAll('[data-flow-rewind-form]').forEach(function (form) {
          if (form.dataset.rewindReviewBound === '1') { return; }
          form.dataset.rewindReviewBound = '1';
          var target = form.elements.to_event;
          var mode = form.elements.schedule_mode;
          var time = form.elements.run_at_utc;
          var targetReview = form.querySelector('[data-flow-rewind-target-review]');
          var scheduleReview = form.querySelector('[data-flow-rewind-schedule-review]');
          function update(changed) {
            var option = target && target.options ? target.options[target.selectedIndex] : null;
            if (targetReview && option) { targetReview.textContent = option.textContent; }
            if (mode && time) {
              var scheduled = mode.value === 'at';
              time.disabled = !scheduled;
              time.required = scheduled;
              form.querySelector('[data-flow-rewind-time-field]').hidden = !scheduled;
              var schedule = option && option.getAttribute('data-flow-event-schedule');
              scheduleReview.textContent = mode.value === 'now' ? 'Run now, when the server accepts the rewind' :
                scheduled ? (time.value ? time.value.replace('T', ' ') + ' UTC' : 'Choose a UTC date and time') :
                'Keep event schedule: ' + (schedule || (target.value ? 'refresh to review the submitted event schedule' : 'choose a target event'));
            }
            if (changed && form.elements.confirm_rewind) { form.elements.confirm_rewind.checked = false; }
          }
          form.addEventListener('input', function (event) {
            if (event.target === target || event.target === mode || event.target === time) { update(true); }
          });
          form.addEventListener('change', function (event) {
            if (event.target === target || event.target === mode || event.target === time) { update(true); }
          });
          update(false);
        });
      }
      if (document.readyState === 'loading') { document.addEventListener('DOMContentLoaded', bind); } else { bind(); }
    })();
    </script>
    """
  end

  def render_signal_review_script do
    """
    <script>
    (function () {
      function bind() {
        document.querySelectorAll('[data-flow-signal-form]').forEach(function (form) {
          var review = form.querySelector('[data-flow-signal-review]');
          if (!review || form.dataset.signalReviewBound === '1') { return; }
          form.dataset.signalReviewBound = '1';
          function update() {
            var name = form.elements.signal.value;
            var transition = form.elements.transition_to.value;
            review.textContent = (name ? 'Signal: ' + name : 'Choose a signal name') +
              (transition ? '; transition to ' + transition : '; no state transition');
          }
          form.addEventListener('input', update);
          update();
        });
      }
      if (document.readyState === 'loading') { document.addEventListener('DOMContentLoaded', bind); } else { bind(); }
    })();
    </script>
    """
  end
end
