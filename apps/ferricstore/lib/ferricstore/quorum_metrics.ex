defmodule Ferricstore.QuorumMetrics do
  @moduledoc """
  Low-cardinality Prometheus counters for the quorum write path.

  The write path emits raw `:telemetry` events. This GenServer owns a small ETS
  table and aggregates those events into counters that `Ferricstore.Metrics`
  exposes through `FERRICSTORE.METRICS`.
  """

  use GenServer

  @table :ferricstore_quorum_metrics
  @handler_id {__MODULE__, :telemetry}

  @events [
    [:ferricstore, :batcher, :slot_flush],
    [:ferricstore, :batcher, :quorum_submit],
    [:ferricstore, :batcher, :quorum_applied],
    [:ferricstore, :batcher, :local_apply_waiters],
    [:ferricstore, :batcher, :local_apply_gate],
    [:ferricstore, :batcher, :local_apply_timeout],
    [:ferricstore, :wal, :sync],
    [:ferricstore, :raft, :apply],
    [:ferricstore, :bitcask, :append]
  ]

  @metric_defs [
    slot_flush_total:
      {"ferricstore_quorum_slot_flush_total", :counter,
       "Total number of quorum write-path batcher slot flushes"},
    slot_flush_queue_wait_us_total:
      {"ferricstore_quorum_slot_flush_queue_wait_us_total", :counter,
       "Total queued microseconds observed before slot flush"},
    slot_flush_queue_wait_us_max:
      {"ferricstore_quorum_slot_flush_queue_wait_us_max", :gauge,
       "Maximum queued microseconds observed before slot flush"},
    slot_flush_batch_size_total:
      {"ferricstore_quorum_slot_flush_batch_size_total", :counter,
       "Total commands flushed from batcher slots"},
    slot_flush_caller_count_total:
      {"ferricstore_quorum_slot_flush_caller_count_total", :counter,
       "Total callers represented by flushed batcher slots"},
    submit_total:
      {"ferricstore_quorum_submit_total", :counter, "Total number of quorum submissions to Raft"},
    submit_duration_us_total:
      {"ferricstore_quorum_submit_duration_us_total", :counter,
       "Total microseconds spent submitting quorum commands to Raft"},
    submit_duration_us_max:
      {"ferricstore_quorum_submit_duration_us_max", :gauge,
       "Maximum microseconds spent submitting quorum commands to Raft"},
    submit_batch_size_total:
      {"ferricstore_quorum_submit_batch_size_total", :counter,
       "Total commands submitted through the quorum path"},
    submit_caller_count_total:
      {"ferricstore_quorum_submit_caller_count_total", :counter,
       "Total callers represented by quorum submissions"},
    submit_command_bytes_total:
      {"ferricstore_quorum_submit_command_bytes_total", :counter,
       "Total serialized command bytes submitted through the quorum path"},
    applied_total:
      {"ferricstore_quorum_applied_total", :counter,
       "Total quorum submissions observed as Raft-applied by the batcher"},
    applied_duration_us_total:
      {"ferricstore_quorum_applied_duration_us_total", :counter,
       "Total microseconds from quorum submission tracking to Raft applied event"},
    applied_duration_us_max:
      {"ferricstore_quorum_applied_duration_us_max", :gauge,
       "Maximum microseconds from quorum submission tracking to Raft applied event"},
    applied_caller_count_total:
      {"ferricstore_quorum_applied_caller_count_total", :counter,
       "Total callers represented by Raft-applied quorum submissions"},
    local_apply_waiters:
      {"ferricstore_batcher_local_apply_waiters", :gauge,
       "Current number of quorum replies waiting for local Raft apply"},
    local_apply_waiter_oldest_age_ms:
      {"ferricstore_batcher_local_apply_waiter_oldest_age_ms", :gauge,
       "Age in milliseconds of the oldest quorum reply waiting for local Raft apply"},
    local_apply_gate_total:
      {"ferricstore_batcher_local_apply_gate_total", :counter,
       "Total quorum replies released after waiting for local Raft apply"},
    local_apply_gate_duration_us_total:
      {"ferricstore_batcher_local_apply_gate_duration_us_total", :counter,
       "Total microseconds quorum replies spent gated on local Raft apply"},
    local_apply_gate_duration_us_max:
      {"ferricstore_batcher_local_apply_gate_duration_us_max", :gauge,
       "Maximum microseconds quorum replies spent gated on local Raft apply"},
    local_apply_gate_caller_count_total:
      {"ferricstore_batcher_local_apply_gate_caller_count_total", :counter,
       "Total callers represented by local Raft apply gate releases"},
    local_apply_timeout_total:
      {"ferricstore_batcher_local_apply_timeout_total", :counter,
       "Total timeouts while waiting for local Raft apply"},
    wal_sync_total:
      {"ferricstore_wal_sync_total", :counter, "Total async Ra WAL sync completions"},
    wal_sync_duration_us_total:
      {"ferricstore_wal_sync_duration_us_total", :counter,
       "Total microseconds spent waiting for async Ra WAL sync"},
    wal_sync_duration_us_max:
      {"ferricstore_wal_sync_duration_us_max", :gauge,
       "Maximum observed async Ra WAL sync duration in microseconds"},
    wal_sync_delay_us_total:
      {"ferricstore_wal_sync_delay_us_total", :counter,
       "Total adaptive Ra WAL sync delay selected in microseconds"},
    wal_sync_delay_us_max:
      {"ferricstore_wal_sync_delay_us_max", :gauge,
       "Maximum adaptive Ra WAL sync delay selected in microseconds"},
    wal_sync_pending_batches_total:
      {"ferricstore_wal_sync_pending_batches_total", :counter,
       "Total Ra WAL batches released by async sync completions"},
    wal_sync_pending_batches_max:
      {"ferricstore_wal_sync_pending_batches_max", :gauge,
       "Maximum observed number of Ra WAL batches released by one async sync completion"},
    wal_sync_queued_batches_total:
      {"ferricstore_wal_sync_queued_batches_total", :counter,
       "Total Ra WAL batches left queued behind async sync completions"},
    wal_sync_queued_batches_max:
      {"ferricstore_wal_sync_queued_batches_max", :gauge,
       "Maximum observed number of Ra WAL batches left queued behind one async sync completion"},
    apply_total:
      {"ferricstore_quorum_apply_total", :counter,
       "Total number of Raft state-machine apply calls"},
    apply_duration_us_total:
      {"ferricstore_quorum_apply_duration_us_total", :counter,
       "Total microseconds spent in Raft state-machine apply"},
    apply_duration_us_max:
      {"ferricstore_quorum_apply_duration_us_max", :gauge,
       "Maximum microseconds spent in Raft state-machine apply"},
    bitcask_append_total:
      {"ferricstore_quorum_bitcask_append_total", :counter,
       "Total number of Bitcask append attempts from Raft apply"},
    bitcask_append_duration_us_total:
      {"ferricstore_quorum_bitcask_append_duration_us_total", :counter,
       "Total microseconds spent appending Raft-applied writes to Bitcask"},
    bitcask_append_duration_us_max:
      {"ferricstore_quorum_bitcask_append_duration_us_max", :gauge,
       "Maximum microseconds spent appending Raft-applied writes to Bitcask"},
    bitcask_append_batch_size_total:
      {"ferricstore_quorum_bitcask_append_batch_size_total", :counter,
       "Total records appended to Bitcask from Raft apply"},
    bitcask_append_batch_bytes_total:
      {"ferricstore_quorum_bitcask_append_batch_bytes_total", :counter,
       "Total key/value bytes appended to Bitcask from Raft apply"}
  ]

  @type metric_id :: atom()
  @type labels :: keyword()

  @doc """
  Starts the quorum metrics collector.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns Prometheus text for currently collected quorum write-path counters.
  """
  @spec prometheus_text() :: binary()
  def prometheus_text do
    entries = table_entries()

    @metric_defs
    |> Enum.map(fn metric_def -> format_family(metric_def, entries) end)
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("\n")
  end

  @doc """
  Clears collected counters.

  This is primarily used by tests. In production counters normally live for the
  lifetime of the supervised collector process.
  """
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
  @spec handle_event([atom()], map(), map(), term()) :: :ok
  def handle_event([:ferricstore, :batcher, :slot_flush], measurements, metadata, _config) do
    labels = [
      shard_index: shard_label(Map.get(metadata, :shard_index)),
      write_path: enum_label(Map.get(metadata, :write_path), [:quorum, :origin_replay])
    ]

    increment(:slot_flush_total, labels, 1)
    increment(:slot_flush_queue_wait_us_total, labels, measurement(measurements, :queue_wait_us))
    max_metric(:slot_flush_queue_wait_us_max, labels, measurement(measurements, :queue_wait_us))
    increment(:slot_flush_batch_size_total, labels, measurement(measurements, :batch_size))
    increment(:slot_flush_caller_count_total, labels, measurement(measurements, :caller_count))
  end

  def handle_event([:ferricstore, :batcher, :quorum_submit], measurements, metadata, _config) do
    labels = [
      shard_index: shard_label(Map.get(metadata, :shard_index)),
      kind: enum_label(Map.get(metadata, :kind), [:single, :batch]),
      status: enum_label(Map.get(metadata, :status), [:ok, :error, :unknown])
    ]

    increment(:submit_total, labels, 1)
    increment(:submit_duration_us_total, labels, measurement(measurements, :duration_us))
    max_metric(:submit_duration_us_max, labels, measurement(measurements, :duration_us))
    increment(:submit_batch_size_total, labels, measurement(measurements, :batch_size))
    increment(:submit_caller_count_total, labels, measurement(measurements, :caller_count))
    increment(:submit_command_bytes_total, labels, measurement(measurements, :command_bytes))
  end

  def handle_event([:ferricstore, :batcher, :quorum_applied], measurements, metadata, _config) do
    labels = [
      shard_index: shard_label(Map.get(metadata, :shard_index)),
      kind: enum_label(Map.get(metadata, :kind), [:single, :batch]),
      result: enum_label(Map.get(metadata, :result), [:ok, :error])
    ]

    increment(:applied_total, labels, 1)
    increment(:applied_duration_us_total, labels, measurement(measurements, :duration_us))
    max_metric(:applied_duration_us_max, labels, measurement(measurements, :duration_us))
    increment(:applied_caller_count_total, labels, measurement(measurements, :caller_count))
  end

  def handle_event(
        [:ferricstore, :batcher, :local_apply_waiters],
        measurements,
        metadata,
        _config
      ) do
    labels = [shard_index: shard_label(Map.get(metadata, :shard_index))]

    set_metric(:local_apply_waiters, labels, measurement(measurements, :depth))

    set_metric(
      :local_apply_waiter_oldest_age_ms,
      labels,
      measurement(measurements, :oldest_age_ms)
    )
  end

  def handle_event(
        [:ferricstore, :batcher, :local_apply_gate],
        measurements,
        metadata,
        _config
      ) do
    labels = [
      shard_index: shard_label(Map.get(metadata, :shard_index)),
      kind: enum_label(Map.get(metadata, :kind), [:single, :batch])
    ]

    increment(:local_apply_gate_total, labels, 1)

    increment(
      :local_apply_gate_duration_us_total,
      labels,
      measurement(measurements, :duration_us)
    )

    max_metric(
      :local_apply_gate_duration_us_max,
      labels,
      measurement(measurements, :duration_us)
    )

    increment(
      :local_apply_gate_caller_count_total,
      labels,
      measurement(measurements, :caller_count)
    )
  end

  def handle_event(
        [:ferricstore, :batcher, :local_apply_timeout],
        measurements,
        metadata,
        _config
      ) do
    labels = [shard_index: shard_label(Map.get(metadata, :shard_index))]
    increment(:local_apply_timeout_total, labels, measurement(measurements, :count))
  end

  def handle_event([:ferricstore, :wal, :sync], measurements, metadata, _config) do
    labels = [status: enum_label(Map.get(metadata, :status), [:ok, :error])]

    increment(:wal_sync_total, labels, 1)
    increment(:wal_sync_duration_us_total, labels, measurement(measurements, :duration_us))
    max_metric(:wal_sync_duration_us_max, labels, measurement(measurements, :duration_us))
    increment(:wal_sync_delay_us_total, labels, measurement(measurements, :delay_us))
    max_metric(:wal_sync_delay_us_max, labels, measurement(measurements, :delay_us))

    increment(
      :wal_sync_pending_batches_total,
      labels,
      measurement(measurements, :pending_batches)
    )

    max_metric(
      :wal_sync_pending_batches_max,
      labels,
      measurement(measurements, :pending_batches)
    )

    increment(
      :wal_sync_queued_batches_total,
      labels,
      measurement(measurements, :queued_batches)
    )

    max_metric(
      :wal_sync_queued_batches_max,
      labels,
      measurement(measurements, :queued_batches)
    )
  end

  def handle_event([:ferricstore, :raft, :apply], measurements, metadata, _config) do
    labels = [
      shard_index: shard_label(Map.get(metadata, :shard_index)),
      result: enum_label(Map.get(metadata, :result), [:ok, :error]),
      disk: enum_label(Map.get(metadata, :disk), [:ok, :error, :unknown])
    ]

    increment(:apply_total, labels, 1)
    increment(:apply_duration_us_total, labels, measurement(measurements, :duration_us))
    max_metric(:apply_duration_us_max, labels, measurement(measurements, :duration_us))
  end

  def handle_event([:ferricstore, :bitcask, :append], measurements, metadata, _config) do
    labels = [
      shard_index: shard_label(Map.get(metadata, :shard_index)),
      status: enum_label(Map.get(metadata, :status), [:ok, :error, :stale, :unknown])
    ]

    increment(:bitcask_append_total, labels, 1)

    increment(
      :bitcask_append_duration_us_total,
      labels,
      measurement(measurements, :duration_us)
    )

    max_metric(
      :bitcask_append_duration_us_max,
      labels,
      measurement(measurements, :duration_us)
    )

    increment(
      :bitcask_append_batch_size_total,
      labels,
      measurement(measurements, :batch_size)
    )

    increment(
      :bitcask_append_batch_bytes_total,
      labels,
      measurement(measurements, :batch_bytes)
    )
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

  @spec ensure_table() :: :ok
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

  @spec attach_handler() :: :ok
  defp attach_handler do
    :telemetry.detach(@handler_id)
    :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, nil)
  end

  @spec increment(metric_id(), labels(), integer()) :: :ok
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

  @spec set_metric(metric_id(), labels(), integer()) :: :ok
  defp set_metric(metric_id, labels, value) do
    key = {metric_id, labels}

    try do
      :ets.insert(@table, {key, counter_value(value)})
      :ok
    rescue
      ArgumentError -> :ok
    end
  end

  @spec max_metric(metric_id(), labels(), integer()) :: :ok
  defp max_metric(metric_id, labels, value) do
    amount = counter_value(value)
    key = {metric_id, labels}

    try do
      case :ets.lookup(@table, key) do
        [{^key, current}] when current >= amount -> :ok
        _ -> :ets.insert(@table, {key, amount})
      end

      :ok
    rescue
      ArgumentError -> :ok
    end
  end

  @spec measurement(map(), atom()) :: non_neg_integer()
  defp measurement(measurements, key), do: counter_value(Map.get(measurements, key, 0))

  @spec shard_label(term()) :: non_neg_integer() | :unknown
  defp shard_label(value) when is_integer(value) and value >= 0, do: value
  defp shard_label(_value), do: :unknown

  @spec enum_label(term(), [atom()]) :: atom()
  defp enum_label(value, allowed) when is_atom(value) do
    if value in allowed, do: value, else: :unknown
  end

  defp enum_label(_value, _allowed), do: :unknown

  @spec counter_value(term()) :: non_neg_integer()
  defp counter_value(value) when is_integer(value) and value >= 0, do: value
  defp counter_value(value) when is_float(value) and value >= 0, do: trunc(value)
  defp counter_value(_value), do: 0

  @spec table_entries() :: [{{metric_id(), labels()}, non_neg_integer()}]
  defp table_entries do
    try do
      :ets.tab2list(@table)
    rescue
      ArgumentError -> []
    end
  end

  @spec format_family({metric_id(), {binary(), atom(), binary()}}, list()) :: binary()
  defp format_family({metric_id, {name, type, help}}, entries) do
    samples =
      entries
      |> Enum.flat_map(fn
        {{^metric_id, labels}, value} -> [{labels, value}]
        _entry -> []
      end)
      |> Enum.sort_by(fn {labels, _value} -> labels end)
      |> Enum.map(fn {labels, value} ->
        "#{name}#{format_labels(labels)} #{value}"
      end)

    case samples do
      [] ->
        ""

      _ ->
        type_str = Atom.to_string(type)

        "# HELP #{name} #{help}\n# TYPE #{name} #{type_str}\n" <>
          Enum.join(samples, "\n")
    end
  end

  @spec format_labels(labels()) :: binary()
  defp format_labels([]), do: ""

  defp format_labels(labels) do
    rendered =
      Enum.map_join(labels, ",", fn {key, value} ->
        "#{key}=\"#{escape_label(label_value(value))}\""
      end)

    "{#{rendered}}"
  end

  @spec label_value(term()) :: binary()
  defp label_value(value) when is_binary(value), do: value
  defp label_value(value) when is_atom(value), do: Atom.to_string(value)
  defp label_value(value) when is_integer(value), do: Integer.to_string(value)
  defp label_value(value), do: inspect(value)

  @spec escape_label(binary()) :: binary()
  defp escape_label(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
  end
end
