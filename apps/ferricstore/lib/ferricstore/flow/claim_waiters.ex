defmodule Ferricstore.Flow.ClaimWaiters do
  @moduledoc """
  Waiter registry for `FLOW.CLAIM_DUE ... BLOCK`.

  This table is intentionally separate from list and stream waiters. Flow waiter
  notifications are only scheduling hints: a woken process must rerun the real
  `claim_due` command, which remains atomic through the replicated write path.
  """

  @table :ferricstore_flow_claim_waiters
  @any_state :any_state
  @any_priority :any_priority
  @any_partition :any_partition
  @message :flow_claim_due_wake

  @type waiter_key :: {binary(), binary() | atom(), non_neg_integer() | atom(), binary() | atom()}

  @spec init() :: :ok
  def init do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [
          :duplicate_bag,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, :auto},
          {:decentralized_counters, true}
        ])

        :ok

      _tid ->
        :ok
    end
  end

  @spec message() :: atom()
  def message, do: @message

  @spec wait_keys(binary(), term(), term(), term()) :: [waiter_key()]
  def wait_keys(type, state, priority, partition_key) do
    for state_key <- wait_state_keys(state),
        priority_key <- wait_priority_keys(priority),
        partition_key <- wait_partition_keys(partition_key),
        uniq: true do
      {type, state_key, priority_key, partition_key}
    end
  end

  @spec ready_keys(binary(), term(), term(), term()) :: [waiter_key()]
  def ready_keys(type, state, priority, partition_key) do
    for state_key <- ready_state_keys(state),
        priority_key <- ready_priority_keys(priority),
        partition_key <- ready_partition_keys(partition_key),
        uniq: true do
      {type, state_key, priority_key, partition_key}
    end
  end

  @spec register([waiter_key()], pid(), non_neg_integer(), keyword()) :: :ok
  def register(keys, pid, deadline_ms, opts \\ []) when is_list(keys) and is_pid(pid) do
    registered_at = System.monotonic_time(:microsecond)
    limit = waiter_limit(opts)

    Enum.each(keys, fn key ->
      :ets.insert(@table, {key, pid, deadline_ms, registered_at, limit})
    end)

    :ok
  end

  @spec unregister([waiter_key()], pid()) :: :ok
  def unregister(keys, pid) when is_list(keys) and is_pid(pid) do
    Enum.each(keys, fn key ->
      :ets.match_delete(@table, {key, pid, :_, :_, :_})
      :ets.match_delete(@table, {key, pid, :_, :_})
    end)

    :ok
  end

  @spec cleanup(pid()) :: :ok
  def cleanup(pid) when is_pid(pid) do
    :ets.match_delete(@table, {:_, pid, :_, :_, :_})
    :ets.match_delete(@table, {:_, pid, :_, :_})
    :ok
  end

  @spec notify_ready(binary(), term(), term(), term(), pos_integer()) :: non_neg_integer()
  def notify_ready(type, state, priority, partition_key, count \\ 1)
      when is_binary(type) and is_integer(count) and count > 0 do
    type
    |> ready_keys(state, priority, partition_key)
    |> notify(count)
  end

  @spec notify_ready_many(
          [{binary(), term(), term(), term(), pos_integer()}],
          pos_integer()
        ) :: non_neg_integer()
  def notify_ready_many(hints, max_wake_per_bucket \\ 8)
      when is_list(hints) and is_integer(max_wake_per_bucket) and max_wake_per_bucket > 0 do
    hints
    |> Enum.reduce(%{}, fn
      {type, state, priority, partition_key, count}, acc when is_binary(type) ->
        count = if is_integer(count) and count > 0, do: count, else: 1
        Map.update(acc, {type, state, priority, partition_key}, count, &(&1 + count))

      _hint, acc ->
        acc
    end)
    |> Enum.reduce(0, fn {{type, state, priority, partition_key}, count}, notified ->
      notified +
        notify_ready_capped(type, state, priority, partition_key, count, max_wake_per_bucket)
    end)
  end

  @spec notify([waiter_key()], pos_integer()) :: non_neg_integer()
  def notify(keys, count) when is_list(keys) and is_integer(count) and count > 0 do
    notify(keys, count, count)
  end

  defp notify_ready_capped(type, state, priority, partition_key, ready_count, max_wake)
       when is_binary(type) and is_integer(ready_count) and ready_count > 0 do
    type
    |> ready_keys(state, priority, partition_key)
    |> notify(ready_count, max_wake)
  end

  defp notify(keys, ready_count, max_wake)
       when is_list(keys) and is_integer(ready_count) and ready_count > 0 and is_integer(max_wake) and
              max_wake > 0 do
    keys
    |> Enum.flat_map(&:ets.lookup(@table, &1))
    |> Enum.flat_map(&normalize_entry/1)
    |> Enum.sort_by(fn {_key, _pid, _deadline, registered_at, _limit} -> registered_at end)
    |> notify_entries(ready_count, max_wake, MapSet.new(), 0)
  end

  @spec count(waiter_key()) :: non_neg_integer()
  def count(key), do: :ets.lookup(@table, key) |> length()

  @spec total_count() :: non_neg_integer()
  def total_count do
    case :ets.whereis(@table) do
      :undefined -> 0
      _tid -> :ets.info(@table, :size)
    end
  end

  defp notify_entries(_entries, remaining_ready, _remaining_wakes, _seen, notified)
       when remaining_ready <= 0,
       do: notified

  defp notify_entries(_entries, _remaining_ready, remaining_wakes, _seen, notified)
       when remaining_wakes <= 0,
       do: notified

  defp notify_entries([], _remaining_ready, _remaining_wakes, _seen, notified), do: notified

  defp notify_entries(
         [{_key, pid, _deadline, _registered_at, limit} | rest],
         remaining_ready,
         remaining_wakes,
         seen,
         notified
       ) do
    cond do
      MapSet.member?(seen, pid) ->
        notify_entries(rest, remaining_ready, remaining_wakes, seen, notified)

      Process.alive?(pid) ->
        cleanup(pid)
        send(pid, {@message, :ready})

        notify_entries(
          rest,
          remaining_ready - limit,
          remaining_wakes - 1,
          MapSet.put(seen, pid),
          notified + 1
        )

      true ->
        cleanup(pid)
        notify_entries(rest, remaining_ready, remaining_wakes, MapSet.put(seen, pid), notified)
    end
  end

  defp normalize_entry({key, pid, deadline, registered_at, limit}) do
    [{key, pid, deadline, registered_at, max(1, limit)}]
  end

  defp normalize_entry({key, pid, deadline, registered_at}) do
    [{key, pid, deadline, registered_at, 1}]
  end

  defp normalize_entry(_entry), do: []

  defp waiter_limit(opts) when is_list(opts) do
    case Keyword.get(opts, :limit, 1) do
      limit when is_integer(limit) and limit > 0 -> limit
      _other -> 1
    end
  end

  defp waiter_limit(_opts), do: 1

  defp wait_state_keys(states) when is_list(states), do: broad_wait_keys(states, @any_state)
  defp wait_state_keys(:any), do: [@any_state]
  defp wait_state_keys(state) when is_binary(state), do: [state]
  defp wait_state_keys(_state), do: [@any_state]

  defp wait_priority_keys(nil), do: [@any_priority]
  defp wait_priority_keys(priority) when is_integer(priority), do: [priority]
  defp wait_priority_keys(_priority), do: [@any_priority]

  defp wait_partition_keys(partitions) when is_list(partitions),
    do: broad_wait_keys(partitions, @any_partition)

  defp wait_partition_keys(partition) when is_binary(partition), do: [partition]
  defp wait_partition_keys(_partition), do: [@any_partition]

  defp ready_state_keys(state) when is_binary(state), do: [state, @any_state]
  defp ready_state_keys(_state), do: [@any_state]

  defp ready_priority_keys(priority) when is_integer(priority), do: [priority, @any_priority]
  defp ready_priority_keys(_priority), do: [@any_priority]

  defp ready_partition_keys(partition) when is_binary(partition), do: [partition, @any_partition]
  defp ready_partition_keys(_partition), do: [@any_partition]

  # Broad claims intentionally register only wildcard keys. A wake is not the
  # claim itself; it only asks the waiter to rerun the replicated claim_due.
  # Keeping multi-state/multi-partition waiters compact avoids one blocked
  # worker inserting thousands of duplicate-bag rows.
  defp broad_wait_keys([single], _fallback) when is_binary(single), do: [single]
  defp broad_wait_keys(_many_or_invalid, fallback), do: [fallback]
end
