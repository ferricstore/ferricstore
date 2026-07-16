defmodule Ferricstore.Flow.LMDBRebuilder.ActiveIndexes do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Store.Shard.ZSetIndex

  @active_index_rebuild_batch_size 4096
  @startup_active_rebuild_slots_key {__MODULE__, :startup_active_rebuild_slots}

  def init_startup_active_rebuild_limiter do
    case :persistent_term.get(@startup_active_rebuild_slots_key, nil) do
      ref when is_reference(ref) ->
        :ok

      _missing_or_invalid ->
        ref = :atomics.new(1, signed: false)
        :persistent_term.put(@startup_active_rebuild_slots_key, ref)
    end

    :ok
  end

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
      {:ok, 0}
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
    ref = :persistent_term.get(@startup_active_rebuild_slots_key)
    lease = acquire_startup_active_rebuild_slot(ref, startup_active_rebuild_concurrency())

    try do
      fun.()
    after
      release_startup_active_rebuild_slot(lease)
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
      fetch_page = fn after_key ->
        LMDB.prefix_entries_after(
          lmdb_path,
          LMDB.active_index_global_prefix(),
          after_key,
          @active_index_rebuild_batch_size
        )
      end

      rebuild_flow_indexes_from_lmdb_page(
        lmdb_path,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup,
        now_ms,
        <<>>,
        0,
        fetch_page
      )
    end)
  end

  @doc false
  def __rebuild_flow_indexes_from_lmdb_pages_for_test__(fetch_page)
      when is_function(fetch_page, 1) do
    rebuild_flow_indexes_from_lmdb_page(
      "test",
      nil,
      nil,
      nil,
      nil,
      0,
      <<>>,
      0,
      fetch_page
    )
  end

  defp acquire_startup_active_rebuild_slot(ref, limit) do
    owner = self()
    token = make_ref()

    # The guardian owns the counter lease so an untrappable owner exit still releases it.
    {guardian, monitor_ref} =
      spawn_monitor(fn ->
        startup_active_rebuild_slot_guardian(owner, token, ref, limit)
      end)

    receive do
      {:startup_active_rebuild_slot_acquired, ^guardian, ^token} ->
        {guardian, monitor_ref, token}

      {:DOWN, ^monitor_ref, :process, ^guardian, reason} ->
        raise "startup active LMDB rebuild slot acquisition failed: #{inspect(reason)}"
    end
  end

  defp startup_active_rebuild_slot_guardian(owner, token, ref, limit) do
    owner_monitor = Process.monitor(owner)

    case guardian_acquire_startup_active_rebuild_slot(ref, limit, owner, owner_monitor) do
      :acquired ->
        send(owner, {:startup_active_rebuild_slot_acquired, self(), token})
        guardian_await_startup_active_rebuild_release(ref, owner, owner_monitor, token)

      :owner_down ->
        :ok
    end
  end

  defp guardian_acquire_startup_active_rebuild_slot(ref, limit, owner, owner_monitor) do
    receive do
      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        :owner_down
    after
      0 ->
        guardian_try_startup_active_rebuild_slot(ref, limit, owner, owner_monitor)
    end
  end

  defp guardian_try_startup_active_rebuild_slot(ref, limit, owner, owner_monitor) do
    count = :atomics.get(ref, 1)

    cond do
      count >= limit ->
        receive do
          {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
            :owner_down
        after
          10 -> guardian_acquire_startup_active_rebuild_slot(ref, limit, owner, owner_monitor)
        end

      :atomics.compare_exchange(ref, 1, count, count + 1) == :ok ->
        :acquired

      true ->
        guardian_acquire_startup_active_rebuild_slot(ref, limit, owner, owner_monitor)
    end
  end

  defp guardian_await_startup_active_rebuild_release(ref, owner, owner_monitor, token) do
    receive do
      {:release_startup_active_rebuild_slot, ^owner, ^token} ->
        Process.demonitor(owner_monitor, [:flush])
        release_startup_active_rebuild_counter(ref)

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        release_startup_active_rebuild_counter(ref)
    end
  end

  defp release_startup_active_rebuild_slot({guardian, monitor_ref, token}) do
    send(guardian, {:release_startup_active_rebuild_slot, self(), token})

    receive do
      {:DOWN, ^monitor_ref, :process, ^guardian, :normal} ->
        :ok

      {:DOWN, ^monitor_ref, :process, ^guardian, reason} ->
        raise "startup active LMDB rebuild slot release failed: #{inspect(reason)}"
    end
  end

  defp release_startup_active_rebuild_counter(ref) do
    case :atomics.sub_get(ref, 1, 1) do
      count when count >= 0 ->
        :ok

      _negative ->
        _ = :atomics.add_get(ref, 1, 1)
        raise "startup active LMDB rebuild slot released without a matching acquire"
    end
  rescue
    ArgumentError ->
      raise "startup active LMDB rebuild slot released without a matching acquire"
  end

  defp rebuild_flow_indexes_from_lmdb_page(
         lmdb_path,
         zset_score_index,
         zset_score_lookup,
         flow_index,
         flow_lookup,
         now_ms,
         after_key,
         total_active,
         fetch_page
       ) do
    case fetch_page.(after_key) do
      {:ok, []} ->
        {:ok, total_active}

      {:ok, entries} when is_list(entries) ->
        with {:ok, active_entries} <- decode_active_index_page(entries, now_ms),
             {:ok, last_key} <- active_index_page_last_key(entries),
             :ok <- ensure_active_index_cursor_advanced(after_key, last_key) do
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

          rebuild_flow_indexes_from_lmdb_page(
            lmdb_path,
            zset_score_index,
            zset_score_lookup,
            flow_index,
            flow_lookup,
            now_ms,
            last_key,
            next_total,
            fetch_page
          )
        end

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_active_index_page, invalid}}
    end
  end

  defp decode_active_index_page(entries, now_ms) do
    Enum.reduce_while(entries, {:ok, []}, fn
      {key, blob}, {:ok, acc} when is_binary(key) and is_binary(blob) ->
        case LMDB.decode_active_index_value(blob) do
          {:ok, {index_key, id, score, expire_at_ms, _state_key}}
          when expire_at_ms <= 0 or expire_at_ms > now_ms ->
            {:cont, {:ok, [{index_key, id, score} | acc]}}

          {:ok, {_index_key, _id, _score, _expire_at_ms, _state_key}} ->
            {:cont, {:ok, acc}}

          :error ->
            {:halt, {:error, {:invalid_active_index_value, key}}}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_active_index_entry, invalid}}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp active_index_page_last_key(entries) do
    case List.last(entries) do
      {last_key, _last_value} when is_binary(last_key) -> {:ok, last_key}
      invalid -> {:error, {:invalid_active_index_entry, invalid}}
    end
  end

  defp ensure_active_index_cursor_advanced(after_key, last_key) when last_key > after_key, do: :ok

  defp ensure_active_index_cursor_advanced(after_key, last_key),
    do: {:error, {:active_index_scan_stalled, after_key, last_key}}

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
    |> maybe_add_active_timeout_entry(record)
    |> maybe_add_terminal_retention_entry(record)
  end

  defp maybe_add_active_timeout_entry(entries, record) do
    case {Map.get(record, :state), Map.get(record, :created_at_ms),
          Map.get(record, :max_active_ms)} do
      {flow_state, created_at_ms, max_active_ms}
      when is_integer(created_at_ms) and is_integer(max_active_ms) and max_active_ms > 0 ->
        if LMDB.terminal_state?(flow_state) do
          entries
        else
          state_key = Flow.Keys.state_key(record.id, Map.get(record, :partition_key))

          [
            {Flow.Keys.active_timeout_index_key(), state_key, created_at_ms + max_active_ms}
            | entries
          ]
        end

      _other ->
        entries
    end
  end

  defp maybe_add_terminal_retention_entry(entries, record) do
    case {Map.get(record, :state), Map.get(record, :terminal_retention_until_ms)} do
      {flow_state, retention_until_ms} when is_integer(retention_until_ms) ->
        if LMDB.terminal_state?(flow_state) do
          state_key = Flow.Keys.state_key(record.id, Map.get(record, :partition_key))

          [
            {Flow.Keys.terminal_retention_index_key(), state_key, retention_until_ms}
            | entries
          ]
        else
          entries
        end

      _other ->
        entries
    end
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
