defmodule FerricstoreServer.Health.Dashboard.Render.FlowFormScripts do
  def render_flow_signal_form_script do
    """
    <script>
    (() => {
      const form = document.currentScript.previousElementSibling;
      if (!form?.matches('[data-flow-signal-form]')) return;
      const transition = form.elements.namedItem('transition_to');
      const state = form.elements.namedItem('if_state');
      const error = form.querySelector('#flow-signal-state-error');
      const validate = () => {
        state.required = transition.value !== '';
        const invalid = state.required && state.value === '';
        state.setCustomValidity(invalid ? 'Enter If State when Transition To is set.' : '');
        state.setAttribute('aria-invalid', String(invalid));
        error.hidden = !invalid;
      };
      transition.addEventListener('input', validate);
      state.addEventListener('input', validate);
      window.addEventListener('pageshow', validate);
      validate();
    })();
    </script>
    """
  end

  @moduledoc false

  def duration_script do
    ~S"""
    <script>
    (() => {
      const form = document.querySelector('form[data-policy-editor], #flow-schedule-create-panel form');
      if (!form) return;
      const sizes = { milliseconds: 1n, seconds: 1000n, minutes: 60000n, hours: 3600000n, days: 86400000n };
      const units = Array.from(form.querySelectorAll('[data-duration-unit]'));
      const decimal = (raw, unit) => {
        if (!sizes[unit]) return { error: 'Select milliseconds, seconds, minutes, hours, or days.' };
        if (raw.trim() === '') return { value: '' };
        const match = /^\+?(\d{1,30})(?:\.(\d{1,12}))?$/.exec(raw.trim());
        if (!match || !sizes[unit]) return { error: 'Enter a non-negative decimal duration without exponents.' };
        const fraction = match[2] || '';
        const scale = 10n ** BigInt(fraction.length);
        const numerator = (BigInt(match[1]) * scale + BigInt(fraction || '0')) * sizes[unit];
        if (numerator % scale) return { error: 'Duration must resolve to a whole number of milliseconds.' };
        return { value: String(numerator / scale) };
      };
      form.dashboardDurationValue = input => {
        const unit = form.elements.namedItem(input.name + '_unit');
        return unit ? decimal(input.value, unit.value) : { value: input.value.trim() };
      };
      const validate = input => {
        if (input.disabled) { input.setCustomValidity(''); return true; }
        const result = form.dashboardDurationValue(input);
        let message = result.error || '';
        if (!message && result.value !== '') {
          if (input.dataset.durationMin && BigInt(result.value) < BigInt(input.dataset.durationMin)) message = 'Duration must be at least ' + input.dataset.durationMin + ' ms.';
          if (input.dataset.durationMax && BigInt(result.value) > BigInt(input.dataset.durationMax)) message = 'Duration must not exceed ' + input.dataset.durationMax + ' ms.';
        }
        input.setCustomValidity(message);
        input.setAttribute('aria-invalid', String(message !== ''));
        const error = (input.getAttribute('aria-describedby') || '').split(/\s+/).map(id => document.getElementById(id)).find(node => node?.classList.contains('flow-field-error'));
        if (error) { error.textContent = message; error.hidden = message === ''; }
        return !message;
      };
      const amount = (ms, unit) => {
        const size = sizes[unit];
        let result = String(ms / size), remainder = ms % size;
        if (remainder) result += '.';
        for (let count = 0; remainder && count < 12; count++) {
          remainder *= 10n;
          result += String(remainder / size);
          remainder %= size;
        }
        return remainder ? null : result;
      };
      units.forEach(unit => {
        const input = form.elements.namedItem(unit.dataset.durationUnit);
        if (unit.value === 'milliseconds' && /^\d{1,30}$/.test(input.value)) {
          const ms = BigInt(input.value);
          const preferred = ['days', 'hours', 'minutes', 'seconds'].find(name => ms >= sizes[name] && ms % sizes[name] === 0n);
          if (preferred) { input.value = String(ms / sizes[preferred]); unit.value = preferred; }
        }
        let previous = unit.value;
        unit.addEventListener('change', () => {
          if (!sizes[unit.value]) { previous = unit.value; validate(input); return; }
          const original = decimal(input.value, previous);
          if (!original.error && original.value !== '') {
            const converted = amount(BigInt(original.value), unit.value);
            if (converted === null) {
              unit.value = previous;
              input.setCustomValidity('Use milliseconds to preserve this exact duration.');
              input.reportValidity();
              return;
            }
            input.value = converted;
          }
          previous = unit.value;
          validate(input);
        });
        input.addEventListener('input', () => validate(input));
      });
      form.addEventListener('submit', event => {
        const invalid = units.map(unit => form.elements.namedItem(unit.dataset.durationUnit)).filter(input => !validate(input));
        if (invalid.length) {
          event.preventDefault();
          event.stopImmediatePropagation();
          invalid[0].focus();
          invalid[0].reportValidity();
        }
      });
    })();
    </script>
    """
  end

  def schedule_script do
    ~S"""
    <script>
    (() => {
      const form = document.querySelector('#flow-schedule-create-panel form');
      if (!form) return;
      const dirtyStatus = form.querySelector('[data-schedule-dirty-status]');
      const initialDraft = Array.from(form.elements)
        .filter(input => input.name && !['hidden', 'submit', 'button'].includes(input.type) && input.name !== 'confirm_replace')
        .map(input => [input, input.value, input.checked]);
      let submitting = false;
      const isDirty = () => form.dataset.scheduleDraft === 'true' ||
        initialDraft.some(([input, value, checked]) => input.value !== value || input.checked !== checked);
      const updateDirty = () => { if (dirtyStatus) dirtyStatus.hidden = !isDirty(); };
      form.querySelector('[data-discard-draft]')?.addEventListener('click', event => {
        if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        if (isDirty() && !window.confirm('Discard unsaved changes?')) event.preventDefault();
        else submitting = true;
      });
      window.addEventListener('beforeunload', event => {
        if (!submitting && isDirty()) {
          event.preventDefault();
          event.returnValue = '';
        }
      });
      const select = form.elements.namedItem('schedule_kind');
      const overlap = form.elements.namedItem('overlap_policy');
      const overlapDescription = form.querySelector('#schedule-overlap-description');
      const updateOverlap = () => {
        if (!overlapDescription) return;
        overlapDescription.textContent = overlap.disabled
          ? 'Overlap policy applies only to cron and interval schedules.'
          : overlap.selectedOptions[0]?.dataset.overlapDescription || 'Select an overlap policy.';
      };
      overlap.addEventListener('change', updateOverlap);
      const timings = [['cron', 'cron'], ['interval', 'every_ms'], ['delay', 'delay_ms'], ['one_shot', 'at_utc']];
      const update = () => {
        const kind = select.value;
        timings.forEach(([mode, name]) => {
          const input = form.elements.namedItem(name);
          const active = kind === mode;
          input.disabled = !active;
          input.required = active;
          document.getElementById('schedule-field-' + mode).style.display = active ? '' : 'none';
          const unit = form.elements.namedItem(name + '_unit');
          if (unit) unit.disabled = !active;
        });
        ['overlap_policy', 'max_fires', 'start_at_utc', 'end_at_utc'].forEach(name => {
          form.elements.namedItem(name).disabled = !['cron', 'interval'].includes(kind);
        });
        form.elements.namedItem('timezone').disabled = kind !== 'cron';
        form.querySelector('[data-schedule-timezone]').hidden = kind !== 'cron';
        form.querySelectorAll('[data-schedule-bound]').forEach(field => { field.hidden = !['cron', 'interval'].includes(kind); });
        form.querySelector('[data-schedule-recurrence]').hidden = !['cron', 'interval'].includes(kind);
        updateOverlap();
        updateDirty();
      };
      select.addEventListener('change', update);
      const invalidateReview = (event) => {
        if (event.target.name === 'confirm_replace' || event.target.hasAttribute('data-schedule-review-field')) return;
        const review = form.querySelector('[data-schedule-review]');
        if (!review) return;
        review.hidden = true;
        review.querySelectorAll('input, button').forEach(input => { input.disabled = true; });
        const stale = form.querySelector('[data-schedule-review-stale]');
        if (stale) stale.hidden = false;
      };
      form.addEventListener('input', invalidateReview);
      form.addEventListener('change', invalidateReview);
      const payload = form.elements.namedItem('target_payload');
      const payloadError = form.querySelector('#schedule-create-target_payload-error');
      const validatePayload = () => {
        let message = '';
        if (payload.value.trim() !== '') {
          try { JSON.parse(payload.value); }
          catch (_error) { message = 'Enter valid JSON for Target Payload, or leave it blank.'; }
        }
        payload.setCustomValidity(message);
        payload.setAttribute('aria-invalid', String(message !== ''));
        payloadError.textContent = message;
        payloadError.hidden = message === '';
        return message === '';
      };
      form.addEventListener('input', event => {
        const input = event.target;
        if (input.getAttribute('aria-invalid') === 'true' && !input.hasAttribute('data-duration-min')) {
          input.setAttribute('aria-invalid', 'false');
          input.setCustomValidity('');
          (input.getAttribute('aria-describedby') || '').split(/\s+/).forEach(id => {
            const error = document.getElementById(id);
            if (error?.hasAttribute('data-schedule-field-error')) error.hidden = true;
          });
        }
        updateDirty();
      });
      form.addEventListener('change', updateDirty);
      form.addEventListener('submit', event => {
        if (!validatePayload()) {
          event.preventDefault();
          event.stopPropagation();
          payload.focus();
        } else if (!event.defaultPrevented && (event.submitter?.formNoValidate || form.checkValidity())) {
          submitting = true;
        }
      });
      form.addEventListener('reset', () => queueMicrotask(update));
      window.addEventListener('pageshow', () => { submitting = false; update(); });
      update();
      const firstError = form.querySelector('[aria-invalid="true"]:not(:disabled)') ||
        document.getElementById('flow-schedule-create-error');
      if (firstError) firstError.focus();
    })();
    </script>
    """
  end

  def policy_script do
    ~S"""
    <script>
    (() => {
      const form = document.querySelector('#flow-policy-editor form[data-policy-editor]');
      if (!form) return;
      const scope = document.querySelector('#flow-policy-editor form[method="get"]');
      const scopeStatus = form.querySelector('[data-policy-scope-status]');
      const save = form.querySelector('button[type="submit"]');
      const dirtyStatus = form.querySelector('[data-policy-dirty-status]');
      const draft = () => JSON.stringify(Array.from(new FormData(form)).filter(([name]) => name !== '_csrf_token'));
      const initialDraft = draft();
      let submitting = false;
      const isDirty = () => form.dataset.policyDraft === 'true' || draft() !== initialDraft;
      const updateDirty = () => { if (dirtyStatus) dirtyStatus.hidden = !isDirty(); };
      form.querySelector('[data-discard-draft]')?.addEventListener('click', event => {
        if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        if (isDirty() && !window.confirm('Discard unsaved changes?')) event.preventDefault();
        else submitting = true;
      });
      window.addEventListener('beforeunload', event => {
        if (!submitting && isDirty()) {
          event.preventDefault();
          event.returnValue = '';
        }
      });
      const scopeChanged = () => scope &&
        (scope.elements.namedItem('edit').value !== form.elements.namedItem('type').value ||
         scope.elements.namedItem('edit_state').value !== form.elements.namedItem('state').value);
      const updateScope = () => {
        const changed = scopeChanged();
        scopeStatus.hidden = !changed;
        save.disabled = changed || form.dataset.dashboardSubmitting === '1';
      };
      if (scope) {
        scope.addEventListener('input', updateScope);
        scope.addEventListener('change', updateScope);
        scope.addEventListener('reset', () => queueMicrotask(updateScope));
      }
      form.addEventListener('submit', event => {
        if (scopeChanged()) {
          event.preventDefault();
          event.stopPropagation();
          updateScope();
        } else if (!event.defaultPrevented && form.checkValidity()) {
          submitting = true;
        }
      });
      const value = name => form.elements.namedItem(name).value;
      const number = (name, blank = null) => {
        const input = form.elements.namedItem(name);
        const normalized = form.dashboardDurationValue ? form.dashboardDurationValue(input) : { value: input.value.trim() };
        const raw = normalized.value || '';
        let message = normalized.error || '';
        if (!(blank !== null && raw === '' && !input.validity.badInput)) {
          if (message) {}
          else if (!/^\+?\d+$/.test(raw)) message = 'Enter a whole decimal number, without fractions or exponents.';
          else if (input.min !== '' && BigInt(raw) < BigInt(input.min)) message = 'Enter a value of at least ' + input.min + '.';
          else if (input.max !== '' && BigInt(raw) > BigInt(input.max)) message = 'Enter a value no greater than ' + input.max + '.';
          else if (input.dataset.durationMin && BigInt(raw) < BigInt(input.dataset.durationMin)) message = 'Duration must be at least ' + input.dataset.durationMin + ' ms.';
          else if (input.dataset.durationMax && BigInt(raw) > BigInt(input.dataset.durationMax)) message = 'Duration must not exceed ' + input.dataset.durationMax + ' ms.';
        }
        input.setCustomValidity(message);
        input.setAttribute('aria-invalid', String(message !== ''));
        const error = document.getElementById('policy-' + name + '-error');
        error.textContent = message;
        error.hidden = message === '';
        if (message !== '') return 'invalid';
        return raw === '' ? blank : raw;
      };
      const put = (name, text) => {
        const target = form.querySelector('[data-policy-preview="' + name + '"]');
        if (target.textContent !== text) target.textContent = text;
      };
      const duration = raw => {
        if (!/^\+?\d+$/.test(raw)) return raw;
        const ms = BigInt(raw);
        const units = [[86400000n, 'd'], [3600000n, 'h'], [60000n, 'm'], [1000n, 's']];
        for (const [size, label] of units) {
          if (ms >= size) {
            const remainder = ms % size;
            return String(ms / size) + label + (remainder ? ' ' + String(remainder) + ' ms' : '') + ' (' + raw + ' ms)';
          }
        }
        return raw + ' ms';
      };
      const update = () => {
        const state = value('state');
        put('scope', (value('type') || '(type required)') + ' / ' +
          (state ? 'state override: ' + state : 'type defaults'));
        put('mode', state ? value('mode') : 'unchanged (no state override)');
        put('indexes', state ? 'unchanged (type-level indexes)' :
          'attributes ' + (value('indexed_attributes') || 'none') +
          ', state meta ' + (value('indexed_state_meta') || 'none'));
        put('retry', number('max_retries') + ' retries, ' + value('backoff_kind') +
          ' backoff, base ' + duration(number('base_ms')) + ', max ' + duration(number('max_ms')) +
          ', jitter ' + number('jitter_pct') + '%, exhausted to ' + (value('exhausted_to') || '(state required)'));
        put('max-active', state ? 'unchanged (type defaults)' :
          duration(number('max_active_ms', 'unlimited')));
        put('retention', duration(number('retention_ttl_ms')) + '; up to ' +
          number('history_max_events') + ' history events before cleanup');
        updateDirty();
      };
      form.addEventListener('input', update);
      form.addEventListener('change', update);
      form.addEventListener('reset', () => queueMicrotask(update));
      window.addEventListener('pageshow', event => {
        if (event.persisted) delete form.dataset.dashboardSubmitting;
        submitting = false;
        update();
        updateScope();
      });
      update();
      updateScope();
      form.querySelector('.flow-policy-preview').style.display = '';
      if (document.getElementById('flow-policy-error')) {
        const firstError = form.querySelector('[aria-invalid="true"]');
        if (firstError) firstError.focus();
      }
    })();
    </script>
    """
  end
end
