defmodule Ferricstore.StandaloneTxMetrics do
  @moduledoc """
  Low-cardinality Prometheus counters for standalone cross-shard tx-log recovery.
  """

  use GenServer

  @table :ferricstore_standalone_tx_metrics
  @handler_id {__MODULE__, :telemetry}

  @events [
    [:ferricstore, :standalone_tx_log, :prepare],
    [:ferricstore, :standalone_tx_log, :commit],
    [:ferricstore, :standalone_tx_log, :recover],
    [:ferricstore, :standalone_tx_log, :corrupt_entry]
  ]

  @metric_defs [
    prepare_total:
      {"ferricstore_standalone_tx_prepare_total", :counter,
       "Total standalone cross-shard tx-log prepare attempts"},
    prepare_groups_total:
      {"ferricstore_standalone_tx_prepare_groups_total", :counter,
       "Total shard groups recorded by standalone cross-shard tx-log prepares"},
    prepare_ops_total:
      {"ferricstore_standalone_tx_prepare_ops_total", :counter,
       "Total Bitcask operations recorded by standalone cross-shard tx-log prepares"},
    commit_total:
      {"ferricstore_standalone_tx_commit_total", :counter,
       "Total standalone cross-shard tx-log commit attempts"},
    recover_total:
      {"ferricstore_standalone_tx_recover_total", :counter,
       "Total standalone cross-shard tx-log recovery attempts"},
    recover_pending_total:
      {"ferricstore_standalone_tx_recover_pending_total", :counter,
       "Total pending transactions observed by standalone tx-log recovery"},
    recover_replayed_total:
      {"ferricstore_standalone_tx_recover_replayed_total", :counter,
       "Total pending transactions replayed by standalone tx-log recovery"},
    recover_groups_total:
      {"ferricstore_standalone_tx_recover_groups_total", :counter,
       "Total shard groups replayed by standalone tx-log recovery"},
    recover_ops_total:
      {"ferricstore_standalone_tx_recover_ops_total", :counter,
       "Total Bitcask operations replayed by standalone tx-log recovery"},
    corrupt_entries_skipped_total:
      {"ferricstore_standalone_tx_corrupt_entries_skipped_total", :counter,
       "Total corrupt standalone tx-log entries skipped during decode"}
  ]

  @type labels :: keyword()
  @type metric_id :: atom()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec prometheus_text() :: binary()
  def prometheus_text do
    entries = table_entries()

    @metric_defs
    |> Enum.map(fn metric_def -> format_family(metric_def, entries) end)
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("\n")
  end

  @spec reset() :: :ok
  def reset do
    try do
      :ets.delete_all_objects(@table)
      :ok
    rescue
      ArgumentError -> :ok
    end
  end

  @doc false
  def handle_event([:ferricstore, :standalone_tx_log, :prepare], measurements, metadata, _config) do
    labels = [status: status_label(metadata)]
    increment(:prepare_total, labels, 1)
    increment(:prepare_groups_total, labels, measurement(measurements, :groups))
    increment(:prepare_ops_total, labels, measurement(measurements, :ops))
  end

  def handle_event([:ferricstore, :standalone_tx_log, :commit], _measurements, metadata, _config) do
    increment(:commit_total, [status: status_label(metadata)], 1)
  end

  def handle_event([:ferricstore, :standalone_tx_log, :recover], measurements, metadata, _config) do
    labels = [status: status_label(metadata)]
    increment(:recover_total, labels, 1)
    increment(:recover_pending_total, labels, measurement(measurements, :pending))
    increment(:recover_replayed_total, labels, measurement(measurements, :replayed))
    increment(:recover_groups_total, labels, measurement(measurements, :groups))
    increment(:recover_ops_total, labels, measurement(measurements, :ops))
  end

  def handle_event(
        [:ferricstore, :standalone_tx_log, :corrupt_entry],
        measurements,
        _metadata,
        _config
      ) do
    increment(:corrupt_entries_skipped_total, [], measurement(measurements, :count))
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  @impl true
  def init(_opts) do
    ensure_table()
    attach_handler()
    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    :telemetry.detach(@handler_id)
    :ok
  end

  defp ensure_table do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [
          :named_table,
          :public,
          :set,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

        :ok

      _tid ->
        :ok
    end
  end

  defp attach_handler do
    :telemetry.detach(@handler_id)
    :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, nil)
  end

  defp increment(metric_id, labels, value) do
    amount = counter_value(value)
    key = {metric_id, labels}

    try do
      :ets.update_counter(@table, key, {2, amount}, {key, 0})
      :ok
    rescue
      ArgumentError -> :ok
    end
  end

  defp measurement(measurements, key), do: counter_value(Map.get(measurements, key, 0))

  defp status_label(%{status: status}) when status in [:ok, :error], do: status
  defp status_label(_metadata), do: :unknown

  defp counter_value(value) when is_integer(value) and value >= 0, do: value
  defp counter_value(value) when is_float(value) and value >= 0, do: trunc(value)
  defp counter_value(_value), do: 0

  defp table_entries do
    try do
      :ets.tab2list(@table)
    rescue
      ArgumentError -> []
    end
  end

  defp format_family({metric_id, {name, type, help}}, entries) do
    samples =
      entries
      |> Enum.flat_map(fn
        {{^metric_id, labels}, value} -> [{labels, value}]
        _entry -> []
      end)
      |> Enum.sort_by(fn {labels, _value} -> labels end)
      |> Enum.map(fn {labels, value} -> "#{name}#{format_labels(labels)} #{value}" end)

    case samples do
      [] -> ""
      _ -> "# HELP #{name} #{help}\n# TYPE #{name} #{type}\n" <> Enum.join(samples, "\n")
    end
  end

  defp format_labels([]), do: ""

  defp format_labels(labels) do
    rendered =
      Enum.map_join(labels, ",", fn {key, value} ->
        "#{key}=\"#{escape_label(label_value(value))}\""
      end)

    "{#{rendered}}"
  end

  defp label_value(value) when is_binary(value), do: value
  defp label_value(value) when is_atom(value), do: Atom.to_string(value)
  defp label_value(value) when is_integer(value), do: Integer.to_string(value)
  defp label_value(value), do: inspect(value)

  defp escape_label(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
  end
end
