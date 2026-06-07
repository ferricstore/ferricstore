defmodule Ferricstore.Flow.LMDBRebuilder.ActiveIndexes do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Store.Shard.ZSetIndex

  @active_index_rebuild_batch_size 4096
  @startup_active_rebuild_slots :ferricstore_flow_lmdb_startup_active_rebuild_slots

  def rebuild_score_indexes(zset_score_index, zset_score_lookup, record) do
    do_rebuild_score_indexes(
      zset_score_index,
      zset_score_lookup,
      active_flow_index_entries(record)
    )
  end

  def rebuild_flow_indexes(nil, _flow_lookup, _record), do: :ok
  def rebuild_flow_indexes(_flow_index, nil, _record), do: :ok

  def rebuild_flow_indexes(flow_index, flow_lookup, record) do
    entries = active_flow_index_entries(record)

    case NativeFlowIndex.get(flow_index, flow_lookup) do
      nil -> :ok
      native -> NativeFlowIndex.put_entries(native, entries)
    end
  end

  def rebuild_flow_indexes_from_lmdb(
        lmdb_path,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup
      ) do
    if not LMDB.env_present?(lmdb_path) do
      0
    else
      do_rebuild_flow_indexes_from_lmdb(
        lmdb_path,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup
      )
    end
  end

  def with_startup_active_rebuild_slot(fun) when is_function(fun, 0) do
    table = ensure_startup_active_rebuild_slots!()
    acquire_startup_active_rebuild_slot(table, startup_active_rebuild_concurrency())

    try do
      fun.()
    after
      release_startup_active_rebuild_slot(table)
    end
  end

  def startup_active_rebuild_concurrency do
    System.schedulers_online()
    |> div(8)
    |> max(1)
    |> min(2)
  end

  defp do_rebuild_score_indexes(nil, _zset_score_lookup, _entries), do: :ok
  defp do_rebuild_score_indexes(_zset_score_index, nil, _entries), do: :ok

  defp do_rebuild_score_indexes(zset_score_index, zset_score_lookup, entries) do
    entries
    |> Enum.group_by(fn {index_key, _id, _score} -> index_key end, fn {_index_key, id, score} ->
      {id, score_string(score)}
    end)
    |> Enum.each(fn {index_key, member_score_pairs} ->
      ZSetIndex.put_members(zset_score_index, zset_score_lookup, index_key, member_score_pairs)
    end)
  end

  defp rebuild_score_index_entry(
         zset_score_index,
         zset_score_lookup,
         index_key,
         id,
         score
       ) do
    do_rebuild_score_indexes(zset_score_index, zset_score_lookup, [{index_key, id, score}])
  end

  defp do_rebuild_flow_indexes_from_lmdb(
         lmdb_path,
         zset_score_index,
         zset_score_lookup,
         flow_index,
         flow_lookup
       ) do
    now_ms = Ferricstore.CommandTime.now_ms()

    with_startup_active_rebuild_slot(fn ->
      rebuild_flow_indexes_from_lmdb_page(
        lmdb_path,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup,
        now_ms,
        <<>>,
        0
      )
    end)
  end

  defp ensure_startup_active_rebuild_slots! do
    case :ets.whereis(@startup_active_rebuild_slots) do
      :undefined ->
        try do
          table =
            :ets.new(@startup_active_rebuild_slots, [
              :set,
              :public,
              :named_table,
              {:read_concurrency, true},
              {:write_concurrency, true}
            ])

          :ets.insert_new(table, {:active, 0})
          table
        rescue
          ArgumentError ->
            @startup_active_rebuild_slots
        end

      table ->
        table
    end
  end

  defp acquire_startup_active_rebuild_slot(table, limit) do
    count = :ets.update_counter(table, :active, {2, 1}, {:active, 0})

    if count <= limit do
      :ok
    else
      _ = :ets.update_counter(table, :active, {2, -1}, {:active, 0})
      Process.sleep(10)
      acquire_startup_active_rebuild_slot(table, limit)
    end
  rescue
    ArgumentError ->
      Process.sleep(10)
      acquire_startup_active_rebuild_slot(ensure_startup_active_rebuild_slots!(), limit)
  end

  defp release_startup_active_rebuild_slot(table) do
    _ = :ets.update_counter(table, :active, {2, -1}, {:active, 0})
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp rebuild_flow_indexes_from_lmdb_page(
         lmdb_path,
         zset_score_index,
         zset_score_lookup,
         flow_index,
         flow_lookup,
         now_ms,
         after_key,
         total_active
       ) do
    case LMDB.prefix_entries_after(
           lmdb_path,
           LMDB.active_index_global_prefix(),
           after_key,
           @active_index_rebuild_batch_size
         ) do
      {:ok, []} ->
        total_active

      {:ok, entries} ->
        active_entries =
          Enum.flat_map(entries, fn {_key, blob} ->
            case LMDB.decode_active_index_value(blob) do
              {:ok, {index_key, id, score, expire_at_ms, _state_key}}
              when expire_at_ms <= 0 or expire_at_ms > now_ms ->
                [{index_key, id, score}]

              _ ->
                []
            end
          end)

        Enum.each(active_entries, fn {index_key, id, score} ->
          rebuild_score_index_entry(
            zset_score_index,
            zset_score_lookup,
            index_key,
            id,
            score
          )
        end)

        rebuild_flow_index_entries(flow_index, flow_lookup, active_entries)

        next_total = total_active + length(active_entries)

        :telemetry.execute(
          [:ferricstore, :flow, :lmdb_startup_active_index_chunk],
          %{
            entries: length(entries),
            active: length(active_entries),
            total_active: next_total
          },
          %{path: lmdb_path}
        )

        {last_key, _last_value} = List.last(entries)

        rebuild_flow_indexes_from_lmdb_page(
          lmdb_path,
          zset_score_index,
          zset_score_lookup,
          flow_index,
          flow_lookup,
          now_ms,
          last_key,
          next_total
        )

      _ ->
        total_active
    end
  end

  defp rebuild_flow_index_entries(nil, _flow_lookup, _entries), do: :ok
  defp rebuild_flow_index_entries(_flow_index, nil, _entries), do: :ok

  defp rebuild_flow_index_entries(flow_index, flow_lookup, entries) do
    case NativeFlowIndex.get(flow_index, flow_lookup) do
      nil -> :ok
      native -> NativeFlowIndex.put_entries(native, entries)
    end
  end

  defp active_flow_index_entries(record) do
    partition_key = Map.get(record, :partition_key)
    updated_score = Map.get(record, :updated_at_ms, 0)
    state_index_key = Flow.Keys.state_index_key(record.type, record.state, partition_key)

    [{state_index_key, record.id, updated_score}]
    |> maybe_add_due_index_entry(record, partition_key)
    |> maybe_add_running_index_entries(record, partition_key)
  end

  defp maybe_add_due_index_entry(
         entries,
         %{next_run_at_ms: next_run_at_ms} = record,
         partition_key
       )
       when is_integer(next_run_at_ms) do
    priority = Map.get(record, :priority, 0)
    due_key = Flow.Keys.due_key(record.type, record.state, priority, partition_key)

    [{due_key, record.id, next_run_at_ms} | entries]
  end

  defp maybe_add_due_index_entry(entries, _record, _partition_key), do: entries

  defp maybe_add_running_index_entries(
         entries,
         %{state: "running", lease_deadline_ms: lease_deadline_ms} = record,
         partition_key
       )
       when is_integer(lease_deadline_ms) do
    inflight_key = Flow.Keys.inflight_index_key(record.type, partition_key)
    worker_key = Flow.Keys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key)

    [
      {worker_key, record.id, lease_deadline_ms},
      {inflight_key, record.id, lease_deadline_ms}
      | entries
    ]
  end

  defp maybe_add_running_index_entries(entries, _record, _partition_key), do: entries

  defp score_string(value) when is_integer(value), do: Float.to_string(value * 1.0)
  defp score_string(value) when is_float(value), do: Float.to_string(value)
  defp score_string(_value), do: "0.0"
end
