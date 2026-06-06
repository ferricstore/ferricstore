defmodule Ferricstore.Flow.LMDBWriter.Registry do
  @moduledoc false

  @owner Ferricstore.Flow.LMDBWriter
  @enqueue_seq_queued 1

  def name(shard_index), do: :"Ferricstore.Flow.LMDBWriter.#{shard_index}"

  def name(:default, shard_index), do: name(shard_index)

  def name(instance_name, shard_index) do
    :"Ferricstore.Flow.LMDBWriter.#{instance_name}.#{shard_index}"
  end

  def projection_outbox_name(instance_name, shard_index) do
    :"Ferricstore.Flow.LMDBWriter.ProjectionOutbox.#{instance_name}.#{shard_index}"
  end

  def ensure_projection_outbox!(instance_name, shard_index) do
    table = projection_outbox_name(instance_name, shard_index)

    case :ets.whereis(table) do
      :undefined ->
        :ets.new(table, [
          :ordered_set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      tid ->
        tid
    end
  end

  def normalize_projection_outbox_entries(entries) do
    Enum.flat_map(entries, fn
      {state_key, version} when is_binary(state_key) and is_integer(version) -> [{state_key, version}]
      _other -> []
    end)
  end

  def projection_outbox_rows(entries) do
    Enum.map(entries, fn {state_key, version} ->
      {System.unique_integer([:monotonic, :positive]), state_key, version}
    end)
  end

  def mark_instance_suspended(instance_name) when is_atom(instance_name) do
    :persistent_term.put(suspend_key(instance_name), true)
    :ok
  end

  def clear_instance_suspended(instance_name) when is_atom(instance_name) do
    :persistent_term.erase(suspend_key(instance_name))
    :ok
  rescue
    ArgumentError -> :ok
  end

  def instance_suspended?(instance_name) when is_atom(instance_name),
    do: :persistent_term.get(suspend_key(instance_name), false)

  def publish_enqueue_seq(instance_name, shard_index, ref) when is_reference(ref) do
    :persistent_term.put(enqueue_seq_key(instance_name, shard_index), ref)
  end

  def reserve_enqueue_seq(instance_name, shard_index) do
    case :persistent_term.get(enqueue_seq_key(instance_name, shard_index), nil) do
      ref when is_reference(ref) -> :atomics.add_get(ref, @enqueue_seq_queued, 1)
      _ -> 0
    end
  rescue
    _ -> 0
  end

  defp suspend_key(instance_name), do: {@owner, :suspended, instance_name}

  def enqueue_seq_key(instance_name, shard_index),
    do: {@owner, :enqueue_seq, instance_name, shard_index}
end
