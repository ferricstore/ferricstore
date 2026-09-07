defmodule FerricstoreServer.Health.Dashboard.Render.FlowFormScripts do
  @moduledoc false

  def schedule_script do
    ~S"""
    <script>
    (() => {
      const form = document.querySelector('#flow-schedule-create-panel form');
      if (!form) return;
      const select = form.elements.namedItem('schedule_kind');
      const timings = [['cron', 'cron'], ['interval', 'every_ms'], ['delay', 'delay_ms']];
      const update = () => {
        const kind = select.value;
        timings.forEach(([mode, name]) => {
          const input = form.elements.namedItem(name);
          const active = kind === mode;
          input.disabled = !active;
          input.required = active;
          input.closest('label').style.display = active ? '' : 'none';
        });
        ['overlap_policy', 'max_fires'].forEach(name => {
          form.elements.namedItem(name).disabled = !['cron', 'interval'].includes(kind);
        });
        form.elements.namedItem('timezone').disabled = kind !== 'cron';
      };
      select.addEventListener('change', update);
      form.addEventListener('reset', () => queueMicrotask(update));
      window.addEventListener('pageshow', update);
      update();
    })();
    </script>
    """
  end

  def policy_script do
    ~S"""
    <script>
    (() => {
      const form = document.querySelector('#flow-policy-editor form');
      if (!form) return;
      const value = name => form.elements.namedItem(name).value.trim();
      const number = (name, blank = null) => {
        const input = form.elements.namedItem(name);
        if (blank !== null && input.value === '' && !input.validity.badInput) return blank;
        return input.validity.valid && /^\+?\d+$/.test(input.value) ? input.value : 'invalid';
      };
      const put = (name, text) => {
        const target = form.querySelector('[data-policy-preview="' + name + '"]');
        if (target.textContent !== text) target.textContent = text;
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
          ' backoff, base ' + number('base_ms') + ' ms, max ' + number('max_ms') +
          ' ms, jitter ' + number('jitter_pct') + '%, exhausted to ' + (value('exhausted_to') || '(state required)'));
        const active = number('max_active_ms', 'unlimited');
        put('max-active', active === 'unlimited' ? active : active + ' ms');
        put('retention', number('retention_ttl_ms') + ' ms; up to ' +
          number('history_max_events') + ' history events before cleanup');
      };
      form.addEventListener('input', update);
      form.addEventListener('change', update);
      form.addEventListener('reset', () => queueMicrotask(update));
      window.addEventListener('pageshow', update);
      update();
      form.querySelector('.flow-policy-preview').style.display = '';
    })();
    </script>
    """
  end
end
