defmodule Ferricstore.Flow.LMDBWriter.Outbox do
  @moduledoc false

  alias Ferricstore.Flow.LMDBWriter
  alias Ferricstore.Flow.LMDBWriter.ProjectionOps

  @projection_outbox_batch_size 1_024
  @end_of_table :"$end_of_table"

  def drain_projection_outbox(state) do
    capacity = max(@projection_outbox_batch_size - Map.get(state, :count, 0), 0)
    entries = take_projection_outbox_entries(state, capacity)

    state =
      case projection_outbox_items(state, entries) do
        {[], []} ->
          state

        {ops, after_flush} ->
          now = System.monotonic_time()
          state = LMDBWriter.enqueue_ops(ops, after_flush, state, now)
          LMDBWriter.emit_backlog(state, now)
          state
      end

    {state, projection_outbox_pending?(state)}
  end

  def take_projection_outbox_entries(state) do
    take_projection_outbox_entries(state, @projection_outbox_batch_size)
  end

  def projection_outbox_pending?(state) do
    table = LMDBWriter.projection_outbox_name(state.instance_name, state.shard_index)

    case :ets.whereis(table) do
      :undefined -> false
      tid -> first_projection_entry_key(tid) != @end_of_table
    end
  rescue
    ArgumentError -> false
  end

  defp take_projection_outbox_entries(_state, limit) when limit <= 0, do: []

  defp take_projection_outbox_entries(state, limit) do
    table = LMDBWriter.projection_outbox_name(state.instance_name, state.shard_index)

    case :ets.whereis(table) do
      :undefined -> []
      tid -> take_projection_entries(tid, first_projection_entry_key(tid), limit, [])
    end
  rescue
    ArgumentError -> []
  end

  defp take_projection_entries(_tid, @end_of_table, _remaining, acc), do: Enum.reverse(acc)
  defp take_projection_entries(_tid, _key, 0, acc), do: Enum.reverse(acc)

  defp take_projection_entries(tid, key, remaining, acc) do
    next_key = next_projection_entry_key(tid, key)

    case :ets.take(tid, key) do
      [{^key, _state_key, _version} = entry] ->
        take_projection_entries(tid, next_key, remaining - 1, [entry | acc])

      _other ->
        take_projection_entries(tid, next_key, remaining, acc)
    end
  end

  defp first_projection_entry_key(tid), do: find_projection_entry_key(tid, :ets.first(tid))

  defp next_projection_entry_key(tid, key) do
    find_projection_entry_key(tid, :ets.next(tid, key))
  end

  defp find_projection_entry_key(_tid, @end_of_table), do: @end_of_table
  defp find_projection_entry_key(_tid, key) when is_integer(key), do: key

  defp find_projection_entry_key(tid, marker) do
    find_projection_entry_key(tid, :ets.next(tid, marker))
  end

  def projection_outbox_items(state, entries) do
    entries
    |> Enum.reduce({[], []}, fn {_seq, state_key, version}, {ops, after_flush} ->
      action = projection_outbox_after_flush(state, state_key, version)

      after_flush =
        case action do
          nil -> after_flush
          action -> [action | after_flush]
        end

      {[{:project_flow_state_from_source, state_key} | ops], after_flush}
    end)
    |> then(fn {ops, after_flush} -> {Enum.reverse(ops), Enum.reverse(after_flush)} end)
  end

  def projection_outbox_after_flush(state, state_key, version) do
    case ProjectionOps.source_keydir(state) do
      nil ->
        nil

      ets ->
        {zset_index, zset_lookup} =
          Ferricstore.Store.Shard.ZSetIndex.table_names(state.instance_name, state.shard_index)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.NativeOrderedIndex.table_names(state.instance_name, state.shard_index)

        {:defer_after_flush, LMDBWriter.terminal_hot_ttl_ms(),
         {:prune_terminal_flow_from_source, state.data_dir, state.shard_index, ets, zset_index,
          zset_lookup, flow_index, flow_lookup, state_key, version}}
    end
  end

  def maybe_reconcile_dirty_projection_with_reply(state, :ok) do
    case reconcile_dirty_projection(state) do
      :ok ->
        clear_projection_dirty_marker(state)
        {%{state | projection_dirty?: false}, :ok}

      {:error, reason} ->
        LMDBWriter.record_flush_failure(state.instance_ctx, state.shard_index)
        LMDBWriter.mark_mirror_degraded(state.instance_ctx, state.shard_index, reason)
        {%{state | projection_dirty?: true}, {:error, reason}}
    end
  end

  def maybe_reconcile_dirty_projection_with_reply(state, reply), do: {state, reply}

  def reconcile_dirty_projection(%{projection_dirty?: true, mode: :lagged} = state) do
    case ProjectionOps.source_keydir(state) do
      nil ->
        {:error, :source_keydir_unavailable}

      keydir ->
        {zset_index, zset_lookup} =
          Ferricstore.Store.Shard.ZSetIndex.table_names(state.instance_name, state.shard_index)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.NativeOrderedIndex.table_names(state.instance_name, state.shard_index)

        Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
          state.shard_data_path,
          keydir,
          state.shard_index,
          state.instance_ctx,
          zset_index,
          zset_lookup,
          flow_index,
          flow_lookup,
          prune_terminal_keydir?: true
        )
    end
  rescue
    error -> {:error, {:lagged_projection_reconcile_failed, error}}
  catch
    kind, reason -> {:error, {:lagged_projection_reconcile_failed, {kind, reason}}}
  end

  def reconcile_dirty_projection(_state), do: :ok

  def clear_projection_dirty_marker(state) do
    case :ets.whereis(LMDBWriter.projection_outbox_name(state.instance_name, state.shard_index)) do
      :undefined -> :ok
      tid -> :ets.delete(tid, :dirty)
    end
  rescue
    ArgumentError -> :ok
  end

  def clear_projection_outbox(state) do
    case :ets.whereis(LMDBWriter.projection_outbox_name(state.instance_name, state.shard_index)) do
      :undefined -> :ok
      tid -> :ets.delete_all_objects(tid)
    end
  rescue
    ArgumentError -> :ok
  end
end
