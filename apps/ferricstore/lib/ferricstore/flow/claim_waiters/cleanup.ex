defmodule Ferricstore.Flow.ClaimWaiters.Cleanup do
  @moduledoc false

  @table :ferricstore_flow_claim_waiters
  @timer_table :ferricstore_flow_claim_waiter_timers
  @any_state :any_state
  @any_priority :any_priority
  @any_partition :any_partition
  @scheduled_prune_page_size 64

  @spec cleanup(pid()) :: :ok
  def cleanup(pid) when is_pid(pid) do
    if :ets.whereis(@table) != :undefined do
      :ets.match_delete(@table, {:_, pid, :_, :_, :_})
      :ets.match_delete(@table, {:_, pid, :_, :_})
    end

    prune_scheduled_ready_without_waiters()

    :ok
  end

  defp prune_scheduled_ready_without_waiters do
    case :ets.whereis(@timer_table) do
      :undefined ->
        :ok

      _tid ->
        @timer_table
        |> select_scheduled_ready_timer_keys(@scheduled_prune_page_size)
        |> prune_scheduled_ready_timer_keys()

        :ok
    end
  end

  defp select_scheduled_ready_timer_keys(table, limit) do
    :ets.select(table, [{{:"$1", :_}, [], [:"$1"]}], limit)
  rescue
    ArgumentError -> :"$end_of_table"
  end

  defp prune_scheduled_ready_timer_keys(:"$end_of_table"), do: :ok

  defp prune_scheduled_ready_timer_keys({timer_keys, _continuation}) when is_list(timer_keys) do
    Enum.each(timer_keys, &maybe_prune_scheduled_ready_timer/1)
    :ok
  end

  defp prune_scheduled_ready_timer_keys(timer_keys) when is_list(timer_keys) do
    Enum.each(timer_keys, &maybe_prune_scheduled_ready_timer/1)
    :ok
  end

  defp maybe_prune_scheduled_ready_timer(
         {type, state, priority, partition_key, _due_at_ms} = timer_key
       )
       when is_binary(type) do
    unless scheduled_ready_has_waiters?(type, state, priority, partition_key) do
      :ets.delete(@timer_table, timer_key)
    end
  end

  defp maybe_prune_scheduled_ready_timer(_timer_key), do: :ok

  defp scheduled_ready_has_waiters?(type, state, priority, partition_key) do
    now_ms = System.monotonic_time(:millisecond)

    type
    |> ready_keys(state, priority, partition_key)
    |> Enum.any?(&waiter_key_has_live_entry?(&1, now_ms))
  end

  defp waiter_key_has_live_entry?(key, now_ms) do
    @table
    |> :ets.lookup(key)
    |> Enum.flat_map(&normalize_entry/1)
    |> Enum.any?(fn {_key, pid, deadline, _registered_at, _limit} ->
      Process.alive?(pid) and not expired_deadline?(deadline, now_ms)
    end)
  rescue
    ArgumentError ->
      false
  end

  defp ready_keys(type, state, priority, partition_key) do
    for state_key <- ready_state_keys(state),
        priority_key <- ready_priority_keys(priority),
        partition_key <- ready_partition_keys(partition_key),
        uniq: true do
      {type, state_key, priority_key, partition_key}
    end
  end

  defp ready_state_keys(state) when is_binary(state), do: [state, @any_state]
  defp ready_state_keys(_state), do: [@any_state]

  defp ready_priority_keys(priority) when is_integer(priority), do: [priority, @any_priority]
  defp ready_priority_keys(_priority), do: [@any_priority]

  defp ready_partition_keys(partition) when is_binary(partition), do: [partition, @any_partition]
  defp ready_partition_keys(_partition), do: [@any_partition]

  defp expired_deadline?(deadline, now_ms) when is_integer(deadline) and deadline != 0,
    do: deadline <= now_ms

  defp expired_deadline?(_deadline, _now_ms), do: false

  defp normalize_entry({key, pid, deadline, registered_at, limit}) do
    [{key, pid, deadline, registered_at, max(1, limit)}]
  end

  defp normalize_entry({key, pid, deadline, registered_at}) do
    [{key, pid, deadline, registered_at, 1}]
  end

  defp normalize_entry(_entry), do: []
end
