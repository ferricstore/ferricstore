defmodule Ferricstore.Flow.LMDBWriter.EnqueueControl do
  @moduledoc false

  alias Ferricstore.Flow.LMDBWriter

  @default_max_mailbox_messages 50_000
  @default_max_enqueue_ops 100_000
  @enqueue_seq_queued 1
  @enqueue_seq_processed 2
  @enqueue_ops_queued 3

  def publish_enqueue_seq(instance_name, shard_index, ref) when is_reference(ref) do
    Ferricstore.Flow.LMDBWriter.Registry.publish_enqueue_seq(instance_name, shard_index, ref)
  end

  def enqueue_seq_key(instance_name, shard_index) do
    Ferricstore.Flow.LMDBWriter.Registry.enqueue_seq_key(instance_name, shard_index)
  end

  def reserve_enqueue_seq(instance_name, shard_index) do
    Ferricstore.Flow.LMDBWriter.Registry.reserve_enqueue_seq(instance_name, shard_index)
  end

  def reserve_enqueue_seq({ref, _reserved_ops}) when is_reference(ref) do
    :atomics.add_get(ref, @enqueue_seq_queued, 1)
  rescue
    _ -> 0
  end

  def previous_writer_generation(instance_name, shard_index) do
    case :persistent_term.get(enqueue_seq_key(instance_name, shard_index), nil) do
      ref when is_reference(ref) ->
        queued = :atomics.get(ref, @enqueue_seq_queued)
        processed = :atomics.get(ref, @enqueue_seq_processed)
        queued_ops = queued_ops(ref)

        if queued > processed or queued_ops > 0 do
          {:writer_restarted_with_unprocessed_enqueues, queued, processed, queued_ops}
        else
          {:writer_restarted_with_volatile_state, queued, processed}
        end

      _other ->
        :none
    end
  rescue
    _ -> :none
  end

  def mark_previous_writer_generation(_instance_ctx, _shard_index, :none), do: :ok

  def mark_previous_writer_generation(instance_ctx, shard_index, reason) do
    LMDBWriter.mark_mirror_degraded(instance_ctx, shard_index, reason)
  end

  def enqueue_guard(instance_name, shard_index, pid, op_count) do
    with :ok <- enqueue_ops_capacity(op_count),
         {:ok, reservation} <- reserve_queued_ops(instance_name, shard_index, op_count) do
      case enqueue_mailbox_capacity(pid) do
        :ok ->
          {:ok, reservation}

        {:error, _reason} = error ->
          _ = release_queued_ops(reservation)
          error
      end
    end
  end

  def enqueue_async_guard(instance_name, shard_index, op_count) do
    with :ok <- enqueue_ops_capacity(op_count),
         {:ok, reservation} <- reserve_queued_ops(instance_name, shard_index, op_count) do
      case enqueue_async_mailbox_capacity(instance_name, shard_index) do
        :ok ->
          {:ok, reservation}

        {:error, _reason} = error ->
          _ = release_queued_ops(reservation)
          error
      end
    end
  end

  def reserve_queued_ops(instance_name, shard_index, 0) do
    with {:ok, ref} <- enqueue_seq_ref(instance_name, shard_index) do
      {:ok, {ref, 0}}
    end
  end

  def reserve_queued_ops(instance_name, shard_index, op_count)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(op_count) and op_count > 0 do
    case Ferricstore.MemoryBudget.limit(
           :flow_lmdb_writer_max_enqueue_ops,
           @default_max_enqueue_ops
         ) do
      :infinity ->
        with {:ok, ref} <- enqueue_seq_ref(instance_name, shard_index) do
          {:ok, {ref, 0}}
        end

      max_ops when is_integer(max_ops) and max_ops >= 0 ->
        with {:ok, ref} <- enqueue_seq_ref(instance_name, shard_index) do
          do_reserve_queued_ops(ref, op_count, max_ops)
        end
    end
  end

  def release_queued_ops({ref, reserved_ops})
      when is_reference(ref) and is_integer(reserved_ops) and reserved_ops >= 0,
      do: release_queued_ops(ref, reserved_ops)

  def release_queued_ops(_ref, 0), do: :ok

  def release_queued_ops(ref, op_count)
      when is_reference(ref) and is_integer(op_count) and op_count > 0 do
    if :atomics.info(ref).size >= @enqueue_ops_queued do
      case :atomics.sub_get(ref, @enqueue_ops_queued, op_count) do
        remaining when remaining >= 0 ->
          :ok

        _negative ->
          _ = :atomics.add_get(ref, @enqueue_ops_queued, op_count)
          {:error, :invalid_queued_ops_release}
      end
    else
      {:error, :invalid_enqueue_reservation_state}
    end
  rescue
    _ -> {:error, :invalid_enqueue_reservation_state}
  end

  def current_generation?(instance_name, shard_index, pid, {ref, _reserved_ops})
      when is_atom(instance_name) and is_integer(shard_index) and is_pid(pid) and
             is_reference(ref) do
    Process.whereis(Ferricstore.Flow.LMDBWriter.Registry.name(instance_name, shard_index)) == pid and
      :persistent_term.get(enqueue_seq_key(instance_name, shard_index), nil) == ref
  rescue
    _ -> false
  end

  def enqueue_ops_capacity(op_count) do
    case Ferricstore.MemoryBudget.limit(
           :flow_lmdb_writer_max_enqueue_ops,
           @default_max_enqueue_ops
         ) do
      :infinity -> :ok
      max_ops when is_integer(max_ops) and op_count <= max_ops -> :ok
      _max_ops -> {:error, :queue_full}
    end
  end

  def enqueue_mailbox_capacity(pid) do
    case Ferricstore.MemoryBudget.limit(
           :flow_lmdb_writer_max_mailbox_messages,
           @default_max_mailbox_messages
         ) do
      :infinity ->
        :ok

      max_messages when is_integer(max_messages) ->
        case Process.info(pid, :message_queue_len) do
          {:message_queue_len, len} when len < max_messages -> :ok
          {:message_queue_len, _len} -> {:error, :queue_full}
          nil -> {:error, :writer_not_started}
        end
    end
  end

  def enqueue_async_mailbox_capacity(instance_name, shard_index) do
    case Ferricstore.MemoryBudget.limit(
           :flow_lmdb_writer_max_mailbox_messages,
           @default_max_mailbox_messages
         ) do
      :infinity ->
        :ok

      max_messages when is_integer(max_messages) ->
        case :persistent_term.get(enqueue_seq_key(instance_name, shard_index), nil) do
          ref when is_reference(ref) ->
            queued = :atomics.get(ref, @enqueue_seq_queued)
            processed = :atomics.get(ref, @enqueue_seq_processed)

            if queued - processed < max_messages do
              :ok
            else
              {:error, :queue_full}
            end

          _missing ->
            :ok
        end
    end
  rescue
    _ -> {:error, :writer_not_started}
  end

  def enqueue_seq_target(%{enqueue_seq: ref}) when is_reference(ref) do
    :atomics.get(ref, @enqueue_seq_queued)
  rescue
    _ -> 0
  end

  def enqueue_seq_target(_state), do: 0

  def queue_flush_waiter(state, from) do
    target = enqueue_seq_target(state)
    %{state | flush_waiters: [{from, target} | state.flush_waiters]}
  end

  def mark_enqueue_processed(state, seq) when is_integer(seq) and seq > 0 do
    {processed, gaps} =
      advance_processed_enqueue_seq(
        state.processed_enqueue_seq,
        state.processed_enqueue_gaps,
        seq
      )

    publish_processed_enqueue_seq(state.enqueue_seq, processed)
    %{state | processed_enqueue_seq: processed, processed_enqueue_gaps: gaps}
  end

  def mark_enqueue_processed(state, _seq), do: state

  def advance_processed_enqueue_seq(current, gaps, seq) when seq <= current do
    {current, gaps}
  end

  def advance_processed_enqueue_seq(current, gaps, seq) when seq == current + 1 do
    consume_processed_enqueue_gaps(seq, gaps)
  end

  def advance_processed_enqueue_seq(current, gaps, seq) do
    {current, MapSet.put(gaps, seq)}
  end

  def consume_processed_enqueue_gaps(current, gaps) do
    next = current + 1

    if MapSet.member?(gaps, next) do
      consume_processed_enqueue_gaps(next, MapSet.delete(gaps, next))
    else
      {current, gaps}
    end
  end

  def publish_processed_enqueue_seq(ref, processed) when is_reference(ref) do
    :atomics.put(ref, @enqueue_seq_processed, processed)
  rescue
    _ -> :ok
  end

  def publish_processed_enqueue_seq(_ref, _processed), do: :ok

  defp enqueue_seq_ref(instance_name, shard_index) do
    case :persistent_term.get(enqueue_seq_key(instance_name, shard_index), nil) do
      ref when is_reference(ref) ->
        if :atomics.info(ref).size >= @enqueue_ops_queued do
          {:ok, ref}
        else
          {:error, :invalid_enqueue_reservation_state}
        end

      _missing ->
        {:error, :writer_not_started}
    end
  rescue
    _ -> {:error, :writer_not_started}
  end

  defp do_reserve_queued_ops(ref, op_count, max_ops) do
    current = :atomics.get(ref, @enqueue_ops_queued)

    if current + op_count > max_ops do
      {:error, :queue_full}
    else
      case :atomics.compare_exchange(
             ref,
             @enqueue_ops_queued,
             current,
             current + op_count
           ) do
        :ok -> {:ok, {ref, op_count}}
        _actual -> do_reserve_queued_ops(ref, op_count, max_ops)
      end
    end
  end

  defp queued_ops(ref) do
    if :atomics.info(ref).size >= @enqueue_ops_queued do
      :atomics.get(ref, @enqueue_ops_queued)
    else
      0
    end
  rescue
    _ -> 0
  end

  def maybe_reply_flush_waiters(%{flush_waiters: []} = state), do: state

  def maybe_reply_flush_waiters(state) do
    {ready, waiting} =
      state.flush_waiters
      |> Enum.reverse()
      |> Enum.split_with(fn {_from, target} -> state.processed_enqueue_seq >= target end)

    state = %{state | flush_waiters: Enum.reverse(waiting)}

    case ready do
      [] ->
        state

      _ ->
        {state, reply} = LMDBWriter.flush_pending_with_reply(state)
        Enum.each(ready, fn {from, _target} -> GenServer.reply(from, reply) end)
        state
    end
  end
end
