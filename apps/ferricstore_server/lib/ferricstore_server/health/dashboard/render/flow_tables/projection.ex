defmodule FerricstoreServer.Health.Dashboard.Render.FlowTables.Projection do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format

  def default_flow_projection_health do
    %{
      lmdb_projection: :lagged,
      lmdb_flush_interval_ms: 0,
      history_flush_interval_ms: 0,
      metrics: []
    }
  end

  def flow_projection_rollup(metrics) do
    rows = projection_metric_rows(metrics)

    totals =
      Enum.reduce(
        rows,
        %{lag: 0, pending_ops: 0, oldest_pending_age_us: 0, degraded: 0, failures: 0},
        fn row, acc ->
          %{
            lag: acc.lag + row.lag,
            pending_ops: acc.pending_ops + row.pending_ops,
            oldest_pending_age_us: max(acc.oldest_pending_age_us, row.oldest_pending_age_us),
            degraded: acc.degraded + row.degraded,
            failures: acc.failures + row.failures
          }
        end
      )

    health =
      cond do
        totals.failures > 0 -> "failures"
        totals.degraded > 0 -> "degraded"
        totals.lag > 0 or totals.pending_ops > 0 -> "pending"
        true -> "healthy"
      end

    Map.merge(totals, %{health: health, shards: length(rows)})
  end

  def flow_projection_health_class(%{failures: failures, degraded: degraded})
      when failures > 0 or degraded > 0,
      do: "c-red"

  def flow_projection_health_class(%{lag: lag, pending_ops: pending_ops})
      when lag > 0 or pending_ops > 0,
      do: "c-yellow"

  def flow_projection_health_class(_rollup), do: "c-green"

  defp projection_metric_rows(metrics) do
    metrics
    |> Enum.reduce(%{}, fn metric, acc ->
      shard = projection_metric_shard(metric.name)
      field = projection_metric_field(metric.name)
      value = numeric_metric_value(metric.value)

      row =
        Map.get(acc, shard, %{
          shard: shard,
          replay_safe_index: 0,
          requested_index: 0,
          lag: 0,
          pending_ops: 0,
          oldest_pending_age_us: 0,
          degraded: 0,
          persist_failures: 0,
          enqueue_failures: 0,
          flush_failures: 0,
          failures: 0
        })

      row =
        row
        |> Map.put(field, value)
        |> then(fn row ->
          failures =
            Map.get(row, :persist_failures, 0) + Map.get(row, :enqueue_failures, 0) +
              Map.get(row, :flush_failures, 0) + Map.get(row, :degraded, 0)

          %{row | failures: failures}
        end)

      Map.put(acc, shard, row)
    end)
    |> Map.values()
    |> Enum.sort_by(& &1.shard)
  end

  defp projection_metric_shard(name) do
    case Regex.run(~r/shard_index="([^"]+)"/, name) do
      [_all, shard] -> shard
      _ -> "all"
    end
  end

  defp projection_metric_field(name) do
    cond do
      String.contains?(name, "replay_safe_index") -> :replay_safe_index
      String.contains?(name, "requested_index") -> :requested_index
      String.contains?(name, "lag") -> :lag
      String.contains?(name, "pending_ops") -> :pending_ops
      String.contains?(name, "oldest_pending_age_us") -> :oldest_pending_age_us
      String.contains?(name, "persist_failures") -> :persist_failures
      String.contains?(name, "enqueue_failures") -> :enqueue_failures
      String.contains?(name, "flush_failures") -> :flush_failures
      String.contains?(name, "degraded") -> :degraded
      true -> :unknown
    end
  end

  defp numeric_metric_value(value) when is_integer(value), do: value
  defp numeric_metric_value(value) when is_float(value), do: round(value)

  defp numeric_metric_value(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, _rest} -> integer
      :error -> 0
    end
  end

  defp numeric_metric_value(_value), do: 0

  def render_flow_projection_health(%{restricted: true}), do: ""

  def render_flow_projection_health(data) do
    data = Map.merge(default_flow_projection_health(), data)
    rollup = flow_projection_rollup(Map.get(data, :metrics, []))
    health_class = flow_projection_health_class(rollup)

    """
    <div class="section-title">Projection Health</div>
    <div class="flow-card-grid">
      <div class="flow-card">
        <div class="flow-card-label">LMDB</div>
        <div class="flow-card-value" style="font-size:1.2rem;">#{escape(to_string(data.lmdb_projection))}</div>
        <div class="flow-card-detail">cold/query projection runs after durable Flow writes</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Health</div>
        <div class="flow-card-value #{health_class}" style="font-size:1.2rem;">#{escape(rollup.health)}</div>
        <div class="flow-card-detail">#{format_number(rollup.shards)} shard projection row(s)</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Lag</div>
        <div class="flow-card-value" style="font-size:1.2rem;">#{format_number(rollup.lag)}</div>
        <div class="flow-card-detail">requested index minus durable projected index</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Pending</div>
        <div class="flow-card-value" style="font-size:1.2rem;">#{format_number(rollup.pending_ops)}</div>
        <div class="flow-card-detail">writer queue ops, oldest #{format_number(rollup.oldest_pending_age_us)}us</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Failures</div>
        <div class="flow-card-value #{if rollup.failures > 0, do: "c-red", else: "c-green"}" style="font-size:1.2rem;">#{format_number(rollup.failures)}</div>
        <div class="flow-card-detail">enqueue, flush, persist, or degraded projection events</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Flush Windows</div>
        <div class="flow-card-value" style="font-size:1.2rem;">#{format_duration_ms(data.lmdb_flush_interval_ms)} / #{format_duration_ms(data.history_flush_interval_ms)}</div>
        <div class="flow-card-detail">state and history projector batching</div>
      </div>
    </div>
    """
  end
end
