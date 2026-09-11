defmodule FerricstoreServer.Health.Dashboard.Render.FlowTables.Projection do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format

  def default_flow_projection_health do
    %{
      lmdb_projection: :asynchronous,
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
        totals.degraded > 0 -> "degraded"
        totals.lag > 0 or totals.pending_ops > 0 -> "pending"
        rows == [] or Enum.any?(rows, &(not &1.current_observed)) -> "unavailable"
        true -> "healthy"
      end

    Map.merge(totals, %{health: health, shards: length(rows)})
  end

  def flow_projection_health_class(%{health: "unavailable"}), do: "c-muted"

  def flow_projection_health_class(%{degraded: degraded})
      when degraded > 0,
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
          failures: 0,
          current_observed: false
        })

      row =
        row
        |> Map.put(field, value)
        |> Map.put(
          :current_observed,
          row.current_observed or field in [:lag, :pending_ops, :degraded]
        )
        |> then(fn row ->
          failures =
            Map.get(row, :persist_failures, 0) + Map.get(row, :enqueue_failures, 0) +
              Map.get(row, :flush_failures, 0)

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
    metrics = Map.get(data, :metrics, [])
    rollup = flow_projection_rollup(metrics)
    health_class = flow_projection_health_class(rollup)
    fields = MapSet.new(metrics, &projection_metric_field(&1.name))
    lag = if MapSet.member?(fields, :lag), do: rollup.lag, else: "Unavailable"
    pending = if MapSet.member?(fields, :pending_ops), do: rollup.pending_ops, else: "Unavailable"

    oldest =
      if MapSet.member?(fields, :oldest_pending_age_us),
        do: "#{format_number(rollup.oldest_pending_age_us)}us",
        else: "not reported"

    failures =
      if Enum.any?(
           [:persist_failures, :enqueue_failures, :flush_failures],
           &MapSet.member?(fields, &1)
         ),
         do: rollup.failures,
         else: "Unavailable"

    """
    <h2 class="section-title">Projection Health</h2>
    <dl class="flow-projection-ledger" aria-label="Projection health metrics">
      #{render_projection_metric("LMDB", "asynchronous", "consistency model; current lag is measured separately")}
      #{render_projection_metric("Health", rollup.health, "#{format_number(rollup.shards)} shard projection row(s)", health_class)}
      #{render_projection_metric("Lag", lag, "requested index minus durable projected index")}
      #{render_projection_metric("Pending", pending, "writer queue ops, oldest #{oldest}")}
      #{render_projection_metric("Historical failures", failures, "enqueue, flush and persist failures since start; not current health")}
      #{render_projection_metric("Flush Windows", "#{format_duration_ms(data.lmdb_flush_interval_ms)} / #{format_duration_ms(data.history_flush_interval_ms)}", "state and history projector batching")}
    </dl>
    """
  end

  defp render_projection_metric(label, value, detail, value_class \\ "") do
    rendered_value =
      case value do
        value when is_integer(value) -> format_number(value)
        value -> escape(to_string(value))
      end

    """
    <div>
      <dt>#{escape(label)}</dt>
      <dd class="#{escape_attr(value_class)}">#{rendered_value}<span>#{escape(detail)}</span></dd>
    </div>
    """
  end
end
