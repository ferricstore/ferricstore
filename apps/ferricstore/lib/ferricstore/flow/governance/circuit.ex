defmodule Ferricstore.Flow.Governance.Circuit do
  @moduledoc false

  alias Ferricstore.Flow.Governance.Decision

  @max_events 64

  defstruct [
    :scope,
    :failure_threshold,
    :open_ms,
    :opened_at_ms,
    :window_ms,
    :min_calls,
    :failure_rate_pct,
    :latency_threshold_ms,
    error_classes: [],
    half_open_max_probes: 1,
    half_open_success_threshold: 1,
    half_open_in_flight: 0,
    half_open_successes: 0,
    half_open_started_at_ms: nil,
    last_failure_ms: nil,
    last_success_ms: nil,
    updated_at_ms: nil,
    events: [],
    status: :closed,
    failures: 0
  ]

  def new(scope, opts) when is_binary(scope) do
    %__MODULE__{
      scope: scope,
      failure_threshold: Keyword.get(opts, :failure_threshold, 5),
      open_ms: Keyword.get(opts, :open_ms, 30_000),
      window_ms: Keyword.get(opts, :window_ms),
      min_calls: Keyword.get(opts, :min_calls),
      failure_rate_pct: Keyword.get(opts, :failure_rate_pct),
      latency_threshold_ms: Keyword.get(opts, :latency_threshold_ms),
      error_classes: Keyword.get(opts, :error_classes, []),
      half_open_max_probes: Keyword.get(opts, :half_open_max_probes, 1),
      half_open_success_threshold: Keyword.get(opts, :half_open_success_threshold, 1)
    }
  end

  def allow?(%__MODULE__{status: :open} = circuit, now_ms) do
    if probe_due?(circuit, now_ms) do
      :allow
    else
      elapsed_ms = elapsed_ms(circuit, now_ms)

      {:deny,
       Decision.circuit_open(%{
         scope: circuit.scope,
         status: circuit.status,
         opened_at_ms: circuit.opened_at_ms,
         retry_after_ms: max(circuit.open_ms - elapsed_ms, 0)
       })}
    end
  end

  def allow?(%__MODULE__{status: :half_open} = circuit, _now_ms) do
    {:deny,
     Decision.circuit_open(%{
       scope: circuit.scope,
       status: circuit.status,
       opened_at_ms: circuit.opened_at_ms,
       retry_after_ms: 0,
       half_open_in_flight: circuit.half_open_in_flight,
       half_open_max_probes: circuit.half_open_max_probes
     })}
  end

  def allow?(%__MODULE__{}, _now_ms), do: :allow

  def configure(%__MODULE__{} = circuit, opts) when is_list(opts) do
    circuit
    |> maybe_put_positive(:failure_threshold, Keyword.get(opts, :failure_threshold))
    |> maybe_put_positive(:open_ms, Keyword.get(opts, :open_ms))
    |> maybe_put_positive(:window_ms, Keyword.get(opts, :window_ms))
    |> maybe_put_positive(:min_calls, Keyword.get(opts, :min_calls))
    |> maybe_put_percent(:failure_rate_pct, Keyword.get(opts, :failure_rate_pct))
    |> maybe_put_positive(:latency_threshold_ms, Keyword.get(opts, :latency_threshold_ms))
    |> maybe_put_string_list(:error_classes, Keyword.get(opts, :error_classes))
    |> maybe_put_positive(:half_open_max_probes, Keyword.get(opts, :half_open_max_probes))
    |> maybe_put_positive(
      :half_open_success_threshold,
      Keyword.get(opts, :half_open_success_threshold)
    )
  end

  def normalize(%__MODULE__{} = circuit) do
    defaults = new(Map.get(circuit, :scope, ""), [])

    defaults
    |> Map.from_struct()
    |> Map.merge(Map.delete(circuit, :__struct__))
    |> then(&struct(__MODULE__, &1))
    |> normalize_probe_limits()
    |> normalize_events()
  end

  def probe_due?(%__MODULE__{status: :open, opened_at_ms: opened_at_ms} = circuit, now_ms)
      when is_integer(opened_at_ms) do
    now_ms - opened_at_ms >= circuit.open_ms
  end

  def probe_due?(%__MODULE__{}, _now_ms), do: false

  def probe_available?(%__MODULE__{status: :open} = circuit, now_ms),
    do: probe_due?(circuit, now_ms)

  def probe_available?(%__MODULE__{status: :half_open} = circuit, _now_ms) do
    circuit.half_open_in_flight < circuit.half_open_max_probes
  end

  def probe_available?(%__MODULE__{}, _now_ms), do: false

  def start_probe(%__MODULE__{} = circuit), do: claim_probe(circuit, nil)

  def claim_probe(%__MODULE__{status: :open} = circuit, now_ms) do
    circuit
    |> Map.merge(%{
      status: :half_open,
      half_open_started_at_ms: now_ms,
      half_open_in_flight: 1,
      half_open_successes: 0,
      updated_at_ms: now_ms
    })
    |> append_event(now_ms, :half_open_probe, counted: false)
  end

  def claim_probe(%__MODULE__{status: :half_open} = circuit, now_ms) do
    if probe_available?(circuit, now_ms) do
      circuit
      |> Map.update!(:half_open_in_flight, &(&1 + 1))
      |> Map.put(:updated_at_ms, now_ms)
      |> append_event(now_ms, :half_open_probe, counted: false)
    else
      circuit
    end
  end

  defp elapsed_ms(%__MODULE__{opened_at_ms: opened_at_ms}, now_ms)
       when is_integer(opened_at_ms) do
    now_ms - opened_at_ms
  end

  defp elapsed_ms(%__MODULE__{}, _now_ms), do: 0

  def record_manual_open(%__MODULE__{} = circuit, now_ms) do
    circuit
    |> open(now_ms)
    |> append_event(now_ms, :manual_open, counted: false)
  end

  def record_manual_close(%__MODULE__{} = circuit, now_ms) do
    circuit
    |> close(now_ms)
    |> append_event(now_ms, :manual_close, counted: false)
  end

  def record_failure(%__MODULE__{} = circuit, now_ms, opts \\ []) do
    error_class = Keyword.get(opts, :error_class)

    if ignored_error_class?(circuit, error_class) do
      circuit
      |> Map.put(:updated_at_ms, now_ms)
      |> append_event(now_ms, :ignored_failure,
        counted: false,
        error_class: error_class,
        latency_ms: Keyword.get(opts, :latency_ms)
      )
    else
      circuit
      |> decrement_probe()
      |> append_event(now_ms, :failure,
        counted: true,
        error_class: error_class,
        latency_ms: Keyword.get(opts, :latency_ms)
      )
      |> after_failure(now_ms)
    end
  end

  def record_success(%__MODULE__{} = circuit, now_ms, opts \\ []) do
    latency_ms = Keyword.get(opts, :latency_ms)

    if slow_call?(circuit, latency_ms) do
      circuit
      |> decrement_probe()
      |> append_event(now_ms, :slow_call, counted: true, latency_ms: latency_ms)
      |> after_failure(now_ms)
    else
      do_record_success(circuit, now_ms, latency_ms)
    end
  end

  defp do_record_success(%__MODULE__{status: :half_open} = circuit, now_ms, latency_ms) do
    circuit =
      circuit
      |> decrement_probe()
      |> Map.update!(:half_open_successes, &(&1 + 1))
      |> append_event(now_ms, :success, counted: true, latency_ms: latency_ms)

    if circuit.half_open_successes >= circuit.half_open_success_threshold do
      close(circuit, now_ms)
    else
      %{circuit | last_success_ms: now_ms, updated_at_ms: now_ms}
    end
  end

  defp do_record_success(%__MODULE__{status: :open} = circuit, _now_ms, _latency_ms), do: circuit

  defp do_record_success(
         %__MODULE__{status: :closed, failure_rate_pct: nil, failures: 0} = circuit,
         _now_ms,
         _latency_ms
       ) do
    circuit
  end

  defp do_record_success(
         %__MODULE__{status: :closed, failure_rate_pct: nil} = circuit,
         now_ms,
         _latency_ms
       ) do
    close(circuit, now_ms)
  end

  defp do_record_success(%__MODULE__{status: :closed} = circuit, now_ms, latency_ms) do
    circuit =
      circuit
      |> append_event(now_ms, :success, counted: true, latency_ms: latency_ms)
      |> Map.put(:last_success_ms, now_ms)
      |> Map.put(:updated_at_ms, now_ms)

    failures = count_recent_failures(circuit, now_ms)
    total = count_recent_calls(circuit, now_ms)
    circuit = %{circuit | failures: failures}

    if failures >= circuit.failure_threshold or rate_exceeded?(circuit, failures, total) do
      open(circuit, now_ms)
    else
      circuit
    end
  end

  defp after_failure(%__MODULE__{status: :half_open} = circuit, now_ms), do: open(circuit, now_ms)

  defp after_failure(%__MODULE__{} = circuit, now_ms) do
    failures = count_recent_failures(circuit, now_ms)
    total = count_recent_calls(circuit, now_ms)

    circuit =
      %{circuit | failures: failures, last_failure_ms: now_ms, updated_at_ms: now_ms}

    if failures >= circuit.failure_threshold or rate_exceeded?(circuit, failures, total) do
      open(circuit, now_ms)
    else
      circuit
    end
  end

  defp open(%__MODULE__{} = circuit, now_ms) do
    circuit
    |> Map.merge(%{
      status: :open,
      opened_at_ms: now_ms,
      half_open_in_flight: 0,
      half_open_successes: 0,
      half_open_started_at_ms: nil,
      updated_at_ms: now_ms
    })
    |> append_event(now_ms, :opened, counted: false)
  end

  defp close(%__MODULE__{} = circuit, now_ms) do
    circuit
    |> Map.merge(%{
      status: :closed,
      failures: 0,
      opened_at_ms: nil,
      half_open_in_flight: 0,
      half_open_successes: 0,
      half_open_started_at_ms: nil,
      last_success_ms: now_ms,
      updated_at_ms: now_ms
    })
    |> append_event(now_ms, :closed, counted: false)
  end

  defp append_event(circuit, nil, _kind, _opts), do: circuit

  defp append_event(%__MODULE__{} = circuit, now_ms, kind, opts) do
    event =
      %{
        at_ms: now_ms,
        kind: kind,
        status: circuit.status,
        failures: circuit.failures,
        counted: Keyword.get(opts, :counted, false)
      }
      |> maybe_put_event(:latency_ms, Keyword.get(opts, :latency_ms))
      |> maybe_put_event(:error_class, Keyword.get(opts, :error_class))

    events =
      [event | circuit.events]
      |> Enum.take(@max_events)
      |> trim_window(circuit, now_ms)

    %{circuit | events: events}
  end

  defp trim_window(events, %{window_ms: window_ms}, now_ms) when is_integer(window_ms) do
    Enum.filter(events, fn event -> now_ms - Map.get(event, :at_ms, now_ms) <= window_ms end)
  end

  defp trim_window(events, _circuit, _now_ms), do: events

  defp count_recent_failures(circuit, now_ms) do
    circuit.events
    |> events_since_reset()
    |> recent_events(circuit, now_ms)
    |> Enum.count(&failure_event?/1)
  end

  defp count_recent_calls(circuit, now_ms) do
    circuit.events
    |> events_since_reset()
    |> recent_events(circuit, now_ms)
    |> Enum.count(&counted_call?/1)
  end

  defp events_since_reset(events) do
    Enum.take_while(events, fn event -> Map.get(event, :kind) not in [:closed, :manual_close] end)
  end

  defp recent_events(events, %{window_ms: window_ms}, now_ms) when is_integer(window_ms) do
    Enum.filter(events, fn event -> now_ms - Map.get(event, :at_ms, now_ms) <= window_ms end)
  end

  defp recent_events(events, _circuit, _now_ms), do: events

  defp failure_event?(%{counted: true, kind: kind}) when kind in [:failure, :slow_call], do: true
  defp failure_event?(_event), do: false

  defp counted_call?(%{counted: true, kind: kind})
       when kind in [:failure, :success, :slow_call],
       do: true

  defp counted_call?(_event), do: false

  defp rate_exceeded?(%__MODULE__{failure_rate_pct: nil}, _failures, _total), do: false

  defp rate_exceeded?(%__MODULE__{} = circuit, failures, total) do
    min_calls = circuit.min_calls || circuit.failure_threshold
    total >= min_calls and failures * 100 >= circuit.failure_rate_pct * total
  end

  defp slow_call?(%__MODULE__{latency_threshold_ms: nil}, _latency_ms), do: false

  defp slow_call?(%__MODULE__{} = circuit, latency_ms) when is_integer(latency_ms),
    do: latency_ms >= circuit.latency_threshold_ms

  defp slow_call?(_circuit, _latency_ms), do: false

  defp ignored_error_class?(%__MODULE__{error_classes: []}, _error_class), do: false
  defp ignored_error_class?(%__MODULE__{}, nil), do: true

  defp ignored_error_class?(%__MODULE__{} = circuit, error_class) when is_binary(error_class) do
    error_class not in circuit.error_classes
  end

  defp ignored_error_class?(%__MODULE__{}, _error_class), do: true

  defp decrement_probe(%__MODULE__{status: :half_open} = circuit) do
    %{circuit | half_open_in_flight: max(circuit.half_open_in_flight - 1, 0)}
  end

  defp decrement_probe(%__MODULE__{} = circuit), do: circuit

  defp normalize_probe_limits(%__MODULE__{} = circuit) do
    max_probes = max(circuit.half_open_max_probes || 1, 1)
    success_threshold = max(circuit.half_open_success_threshold || max_probes, 1)

    %{
      circuit
      | half_open_max_probes: max_probes,
        half_open_success_threshold: success_threshold,
        error_classes: circuit.error_classes || [],
        events: circuit.events || []
    }
  end

  defp normalize_events(%__MODULE__{} = circuit) do
    %{circuit | events: Enum.take(circuit.events || [], @max_events)}
  end

  defp maybe_put_positive(circuit, _key, nil), do: circuit

  defp maybe_put_positive(circuit, key, value) when is_integer(value) and value > 0,
    do: Map.put(circuit, key, value)

  defp maybe_put_positive(circuit, _key, _value), do: circuit

  defp maybe_put_percent(circuit, _key, nil), do: circuit

  defp maybe_put_percent(circuit, key, value)
       when is_integer(value) and value >= 1 and value <= 100,
       do: Map.put(circuit, key, value)

  defp maybe_put_percent(circuit, _key, _value), do: circuit

  defp maybe_put_string_list(circuit, _key, nil), do: circuit

  defp maybe_put_string_list(circuit, key, values) when is_list(values),
    do: Map.put(circuit, key, values)

  defp maybe_put_string_list(circuit, _key, _values), do: circuit

  defp maybe_put_event(event, _key, nil), do: event
  defp maybe_put_event(event, key, value), do: Map.put(event, key, value)
end
