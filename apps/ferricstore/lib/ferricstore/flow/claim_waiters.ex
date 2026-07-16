defmodule Ferricstore.Flow.ClaimWaiters do
  @moduledoc """
  Waiter registry for `FLOW.CLAIM_DUE ... BLOCK`.

  This table is intentionally separate from list and stream waiters. Flow waiter
  notifications are only scheduling hints: a woken process must rerun the real
  `claim_due` command, which remains atomic through the replicated write path.
  """

  @table :ferricstore_flow_claim_waiters
  @timer_table :ferricstore_flow_claim_waiter_timers
  @capacity_lock_table :ferricstore_flow_claim_waiter_capacity_lock
  @capacity_lock_key :registration
  @any_state :any_state
  @any_priority :any_priority
  @any_partition :any_partition
  @message :flow_claim_due_wake
  @max_wait_key_rows 64
  @default_max_waiter_rows 100_000
  @max_waiters_error "ERR max blocked claim_due waiters reached"
  @max_wait_keys_error "ERR blocked claim_due waiter has too many keys"
  @scheduled_due_bucket_ms 10
  @scheduled_prune_page_size 64
  @capacity_prune_page_size 256

  @type waiter_key :: {binary(), binary() | atom(), non_neg_integer() | atom(), binary() | atom()}

  @spec init() :: :ok
  def init do
    ensure_waiter_table()
    ensure_timer_table()
    ensure_capacity_lock_table()
    :ok
  end

  @spec message() :: atom()
  def message, do: @message

  @spec wait_keys(binary(), term(), term(), term()) :: [waiter_key()]
  def wait_keys(type, state, priority, partition_key) do
    state_keys = wait_state_keys(state)
    priority_keys = wait_priority_keys(priority)
    partition_keys = wait_partition_keys(partition_key)
    {state_keys, partition_keys} = compact_wait_key_dimensions(state_keys, partition_keys)

    for state_key <- state_keys,
        priority_key <- priority_keys,
        partition_key <- partition_keys,
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

  @spec register([waiter_key()], pid(), integer(), keyword()) :: :ok | {:error, binary()}
  def register(keys, pid, deadline_ms, opts \\ []) when is_list(keys) and is_pid(pid) do
    with {:ok, keys} <- bounded_unique_waiter_keys(keys) do
      registered_at = System.unique_integer([:monotonic])
      limit = waiter_limit(opts)

      result =
        with_capacity_lock(fn ->
          with :ok <- ensure_waiter_capacity(keys) do
            entries =
              Enum.map(keys, fn key ->
                {key, pid, deadline_ms, registered_at, limit}
              end)

            true = :ets.insert(@table, entries)
            :ok
          end
        end)

      if result == :ok, do: Ferricstore.Waiters.Monitor.track(pid)
      result
    end
  end

  @spec unregister([waiter_key()], pid()) :: :ok
  def unregister(keys, pid) when is_list(keys) and is_pid(pid) do
    Enum.each(keys, fn key ->
      :ets.match_delete(@table, {key, pid, :_, :_, :_})
      :ets.match_delete(@table, {key, pid, :_, :_})
    end)

    prune_scheduled_ready_without_waiters()

    :ok
  end

  @spec cleanup(pid()) :: :ok
  def cleanup(pid) when is_pid(pid) do
    Ferricstore.Flow.ClaimWaiters.Cleanup.cleanup(pid)
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
  def notify_ready_many(hints, min_wake_budget_per_bucket \\ 8)
      when is_list(hints) and is_integer(min_wake_budget_per_bucket) and
             min_wake_budget_per_bucket > 0 do
    hints
    |> ready_count_map()
    |> notify_ready_map(min_wake_budget_per_bucket)
  end

  defp ready_count_map(hints) do
    Enum.reduce(hints, %{}, fn
      {type, state, priority, partition_key, count}, acc when is_binary(type) ->
        count = if is_integer(count) and count > 0, do: count, else: 1
        Map.update(acc, {type, state, priority, partition_key}, count, &(&1 + count))

      _hint, acc ->
        acc
    end)
  end

  defp notify_ready_map(ready_counts, _min_wake_budget) when map_size(ready_counts) == 0,
    do: 0

  defp notify_ready_map(ready_counts, min_wake_budget) do
    Enum.reduce(ready_counts, 0, fn {{type, state, priority, partition_key}, count}, notified ->
      notified +
        notify_ready_budgeted(
          type,
          state,
          priority,
          partition_key,
          count,
          min_wake_budget
        )
    end)
  end

  @spec schedule_ready(binary(), term(), term(), term(), integer(), pos_integer()) :: :ok
  def schedule_ready(type, state, priority, partition_key, due_at_ms, count \\ 1)

  def schedule_ready(type, state, priority, partition_key, due_at_ms, count)
      when is_binary(type) and is_integer(due_at_ms) do
    count = if is_integer(count) and count > 0, do: count, else: 1
    due_at_ms = scheduled_due_at_ms(due_at_ms)
    now_ms = Ferricstore.CommandTime.now_ms()

    if due_at_ms <= now_ms do
      notify_ready(type, state, priority, partition_key, count)
      :ok
    else
      ensure_timer_table()
      timer_key = {type, state, priority, partition_key, due_at_ms}
      pending_token = make_ref()

      case :ets.insert_new(@timer_table, {timer_key, count, {:pending, pending_token}}) do
        true ->
          start_scheduled_ready_timer(
            timer_key,
            pending_token,
            max(due_at_ms - now_ms, 0)
          )

          :ok

        false ->
          try do
            :ets.update_counter(@timer_table, timer_key, {2, count})
            :ok
          rescue
            ArgumentError ->
              schedule_ready(type, state, priority, partition_key, due_at_ms, count)
          end
      end
    end
  end

  def schedule_ready(_type, _state, _priority, _partition_key, _due_at_ms, _count), do: :ok

  @spec notify_scheduled_ready(tuple()) :: non_neg_integer()
  def notify_scheduled_ready(timer_key) when is_tuple(timer_key) do
    ensure_timer_table()

    case :ets.take(@timer_table, timer_key) do
      [{^timer_key, count, _timer_ref}] ->
        case timer_key do
          {type, state, priority, partition_key, _due_at_ms} ->
            notify_ready(type, state, priority, partition_key, max(count, 1))

          _other ->
            0
        end

      [] ->
        0
    end
  end

  def notify_scheduled_ready(_timer_key), do: 0

  @doc false
  @spec prune_scheduled_ready_for_cleanup() :: :ok
  def prune_scheduled_ready_for_cleanup, do: prune_scheduled_ready_without_waiters()

  @spec scheduled_count() :: non_neg_integer()
  def scheduled_count do
    case :ets.whereis(@timer_table) do
      :undefined -> 0
      _tid -> :ets.info(@timer_table, :size)
    end
  end

  @spec notify([waiter_key()], pos_integer()) :: non_neg_integer()
  def notify(keys, count) when is_list(keys) and is_integer(count) and count > 0 do
    notify(keys, count, count)
  end

  defp notify_ready_budgeted(type, state, priority, partition_key, ready_count, min_wake_budget)
       when is_binary(type) and is_integer(ready_count) and ready_count > 0 do
    # The ready count and each waiter's claim limit are the real herd control.
    # A fixed hard cap can strand work forever when one event makes many jobs
    # ready, so the small budget is only a floor for coarse low-count hints.
    wake_budget = max(min_wake_budget, ready_count)

    type
    |> ready_keys(state, priority, partition_key)
    |> notify(ready_count, wake_budget)
  end

  defp notify(keys, ready_count, max_wake)
       when is_list(keys) and is_integer(ready_count) and ready_count > 0 and is_integer(max_wake) and
              max_wake > 0 do
    now_ms = System.monotonic_time(:millisecond)

    keys
    |> Enum.flat_map(&:ets.lookup(@table, &1))
    |> Enum.flat_map(&normalize_entry/1)
    |> Enum.sort_by(fn {_key, _pid, _deadline, registered_at, _limit} -> registered_at end)
    |> notify_entries(ready_count, max_wake, MapSet.new(), 0, now_ms)
  end

  @spec count(waiter_key()) :: non_neg_integer()
  def count(key) do
    prune_stale_entries()
    :ets.lookup(@table, key) |> length()
  end

  @spec has_live_waiter?(waiter_key()) :: boolean()
  def has_live_waiter?(key) do
    waiter_key_has_live_entry?(key, System.monotonic_time(:millisecond))
  end

  @spec total_count() :: non_neg_integer()
  def total_count do
    case :ets.whereis(@table) do
      :undefined ->
        0

      _tid ->
        prune_stale_entries()
        :ets.info(@table, :size)
    end
  end

  @spec any_waiters?() :: boolean()
  def any_waiters?, do: waiter_row_count() > 0

  @spec prune_stale_entries() :: :ok
  def prune_stale_entries do
    case :ets.whereis(@table) do
      :undefined ->
        :ok

      _tid ->
        now_ms = System.monotonic_time(:millisecond)

        pruned? =
          :ets.foldl(
            fn entry, pruned? ->
              prune_stale_entry(entry, now_ms) or pruned?
            end,
            false,
            @table
          )

        if pruned?, do: prune_scheduled_ready_without_waiters()

        :ok
    end
  end

  defp prune_stale_entry(entry, now_ms) do
    case normalize_entry(entry) do
      [{_key, pid, deadline, _registered_at, _limit}] ->
        if expired_deadline?(deadline, now_ms) or not Process.alive?(pid) do
          :ets.delete_object(@table, entry)
          true
        else
          false
        end

      _other ->
        false
    end
  end

  defp notify_entries(_entries, remaining_ready, _remaining_wakes, _seen, notified, _now_ms)
       when remaining_ready <= 0,
       do: notified

  defp notify_entries(_entries, _remaining_ready, remaining_wakes, _seen, notified, _now_ms)
       when remaining_wakes <= 0,
       do: notified

  defp notify_entries([], _remaining_ready, _remaining_wakes, _seen, notified, _now_ms),
    do: notified

  defp notify_entries(
         [{_key, pid, deadline, _registered_at, limit} | rest],
         remaining_ready,
         remaining_wakes,
         seen,
         notified,
         now_ms
       ) do
    cond do
      MapSet.member?(seen, pid) ->
        notify_entries(rest, remaining_ready, remaining_wakes, seen, notified, now_ms)

      expired_deadline?(deadline, now_ms) ->
        cleanup(pid)

        notify_entries(
          rest,
          remaining_ready,
          remaining_wakes,
          MapSet.put(seen, pid),
          notified,
          now_ms
        )

      Process.alive?(pid) ->
        cleanup(pid)
        send(pid, {@message, :ready})

        notify_entries(
          rest,
          remaining_ready - limit,
          remaining_wakes - 1,
          MapSet.put(seen, pid),
          notified + 1,
          now_ms
        )

      true ->
        cleanup(pid)

        notify_entries(
          rest,
          remaining_ready,
          remaining_wakes,
          MapSet.put(seen, pid),
          notified,
          now_ms
        )
    end
  end

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

  defp waiter_limit(opts) when is_list(opts) do
    case Keyword.get(opts, :limit, 1) do
      limit when is_integer(limit) and limit > 0 -> limit
      _other -> 1
    end
  end

  defp waiter_limit(_opts), do: 1

  defp bounded_unique_waiter_keys(keys) do
    result =
      Enum.reduce_while(keys, {[], MapSet.new()}, fn key, {unique, seen} ->
        if MapSet.member?(seen, key) do
          {:cont, {unique, seen}}
        else
          seen = MapSet.put(seen, key)
          unique = [key | unique]

          if MapSet.size(seen) > @max_wait_key_rows do
            {:halt, :too_many}
          else
            {:cont, {unique, seen}}
          end
        end
      end)

    case result do
      :too_many -> {:error, @max_wait_keys_error}
      {unique, _seen} -> {:ok, Enum.reverse(unique)}
    end
  end

  defp ensure_waiter_capacity([]), do: :ok

  defp ensure_waiter_capacity(keys) do
    max_rows = max_waiter_rows()

    if max_rows == :infinity do
      :ok
    else
      needed_rows = length(keys)

      if waiter_row_count() + needed_rows <= max_rows do
        :ok
      else
        prune_stale_entries(@capacity_prune_page_size)
        current_rows = waiter_row_count()

        if current_rows + needed_rows <= max_rows do
          :ok
        else
          :telemetry.execute(
            [:ferricstore, :flow, :claim_due, :waiter_rejected],
            %{count: needed_rows, current: current_rows, max: max_rows},
            %{reason: :max_waiters}
          )

          {:error, @max_waiters_error}
        end
      end
    end
  end

  defp waiter_row_count do
    case :ets.whereis(@table) do
      :undefined ->
        0

      _tid ->
        case :ets.info(@table, :size) do
          size when is_integer(size) and size >= 0 -> size
          _other -> 0
        end
    end
  rescue
    ArgumentError -> 0
  end

  defp prune_stale_entries(limit) when is_integer(limit) and limit > 0 do
    case :ets.whereis(@table) do
      :undefined ->
        :ok

      _tid ->
        now_ms = System.monotonic_time(:millisecond)

        pruned? =
          @table
          |> select_waiter_entries(limit)
          |> Enum.reduce(false, fn entry, pruned? ->
            prune_stale_entry(entry, now_ms) or pruned?
          end)

        if pruned?, do: prune_scheduled_ready_without_waiters()

        :ok
    end
  end

  defp max_waiter_rows do
    case Application.get_env(
           :ferricstore,
           :flow_claim_due_max_waiter_rows,
           @default_max_waiter_rows
         ) do
      value when is_integer(value) and value > 0 -> value
      :infinity -> :infinity
      _other -> @default_max_waiter_rows
    end
  end

  defp select_waiter_entries(table, limit) do
    match_spec = [
      {{:"$1", :"$2", :"$3", :"$4", :"$5"}, [], [:"$_"]},
      {{:"$1", :"$2", :"$3", :"$4"}, [], [:"$_"]}
    ]

    case :ets.select(table, match_spec, limit) do
      {entries, _continuation} -> entries
      :"$end_of_table" -> []
    end
  rescue
    ArgumentError -> []
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
    :ets.select(table, [{{:"$1", :_, :_}, [], [:"$1"]}], limit)
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
      cancel_scheduled_ready_timer(timer_key)
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

  defp wait_state_keys(states) when is_list(states), do: wait_binary_list_keys(states, @any_state)
  defp wait_state_keys(:any), do: [@any_state]
  defp wait_state_keys(state) when is_binary(state), do: [state]
  defp wait_state_keys(_state), do: [@any_state]

  defp wait_priority_keys(nil), do: [@any_priority]
  defp wait_priority_keys(priority) when is_integer(priority), do: [priority]
  defp wait_priority_keys(_priority), do: [@any_priority]

  defp wait_partition_keys(partitions) when is_list(partitions),
    do: wait_binary_list_keys(partitions, @any_partition)

  defp wait_partition_keys(partition) when is_binary(partition), do: [partition]
  defp wait_partition_keys(_partition), do: [@any_partition]

  defp ready_state_keys(state) when is_binary(state), do: [state, @any_state]
  defp ready_state_keys(_state), do: [@any_state]

  defp ready_priority_keys(priority) when is_integer(priority), do: [priority, @any_priority]
  defp ready_priority_keys(_priority), do: [@any_priority]

  defp ready_partition_keys(partition) when is_binary(partition), do: [partition, @any_partition]
  defp ready_partition_keys(_partition), do: [@any_partition]

  defp wait_binary_list_keys(items, fallback) when is_list(items) do
    keys = Enum.uniq(items)

    if keys != [] and Enum.all?(keys, &is_binary/1) do
      keys
    else
      [fallback]
    end
  end

  # A waiter with both many states and many partitions can otherwise expand to
  # N*M ETS rows. Prefer preserving partition specificity because unrelated
  # partition wakeups are the common expensive false-positive in worker polls.
  defp compact_wait_key_dimensions(state_keys, partition_keys) do
    if length(state_keys) * length(partition_keys) <= @max_wait_key_rows do
      {state_keys, partition_keys}
    else
      cond do
        length(state_keys) > 1 ->
          compact_wait_key_dimensions([@any_state], partition_keys)

        length(partition_keys) > 1 ->
          compact_wait_key_dimensions(state_keys, [@any_partition])

        true ->
          {state_keys, partition_keys}
      end
    end
  end

  defp scheduled_due_at_ms(due_at_ms) when is_integer(due_at_ms) do
    bucket = @scheduled_due_bucket_ms
    div(due_at_ms + bucket - 1, bucket) * bucket
  end

  defp start_scheduled_ready_timer(timer_key, pending_token, delay_ms) do
    case :timer.apply_after(delay_ms, __MODULE__, :notify_scheduled_ready, [timer_key]) do
      {:ok, timer_ref} ->
        unless publish_scheduled_timer_ref(timer_key, pending_token, timer_ref) do
          cancel_timer(timer_ref)
        end

      {:error, _reason} ->
        delete_pending_scheduled_timer(timer_key, pending_token)
    end
  rescue
    ArgumentError -> :ok
  end

  defp publish_scheduled_timer_ref(timer_key, pending_token, timer_ref) do
    match_spec = [
      {
        {timer_key, :"$1", {:pending, pending_token}},
        [],
        [{{{:const, timer_key}, :"$1", {:const, timer_ref}}}]
      }
    ]

    :ets.select_replace(@timer_table, match_spec) == 1
  end

  defp delete_pending_scheduled_timer(timer_key, pending_token) do
    match_spec = [
      {{timer_key, :_, {:pending, pending_token}}, [], [true]}
    ]

    _deleted = :ets.select_delete(@timer_table, match_spec)
    :ok
  end

  defp cancel_scheduled_ready_timer(timer_key) do
    case :ets.take(@timer_table, timer_key) do
      [{^timer_key, _count, {:pending, _token}}] -> :ok
      [{^timer_key, _count, timer_ref}] -> cancel_timer(timer_ref)
      [] -> :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp cancel_timer(timer_ref) do
    _ = :timer.cancel(timer_ref)
    :ok
  catch
    :exit, _reason -> :ok
    :error, _reason -> :ok
  end

  defp with_capacity_lock(fun) when is_function(fun, 0) do
    ensure_capacity_lock_table()
    lock = {@capacity_lock_key, self(), make_ref()}
    acquire_capacity_lock(lock)

    try do
      fun.()
    after
      delete_capacity_lock(lock)
    end
  end

  defp acquire_capacity_lock(lock) do
    case :ets.insert_new(@capacity_lock_table, lock) do
      true ->
        :ok

      false ->
        clear_stale_capacity_lock()

        receive do
        after
          1 -> acquire_capacity_lock(lock)
        end
    end
  rescue
    ArgumentError ->
      ensure_capacity_lock_table()
      acquire_capacity_lock(lock)
  end

  defp clear_stale_capacity_lock do
    case :ets.lookup(@capacity_lock_table, @capacity_lock_key) do
      [{@capacity_lock_key, owner, _ref} = lock] when is_pid(owner) ->
        unless Process.alive?(owner), do: :ets.delete_object(@capacity_lock_table, lock)

      _other ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp delete_capacity_lock(lock) do
    :ets.delete_object(@capacity_lock_table, lock)
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp ensure_waiter_table do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [
            :duplicate_bag,
            :public,
            :named_table,
            {:read_concurrency, true},
            {:write_concurrency, :auto},
            {:decentralized_counters, true}
          ])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _tid ->
        :ok
    end
  end

  defp ensure_capacity_lock_table do
    case :ets.whereis(@capacity_lock_table) do
      :undefined ->
        try do
          :ets.new(@capacity_lock_table, [
            :set,
            :public,
            :named_table,
            {:write_concurrency, true}
          ])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _tid ->
        :ok
    end
  end

  defp ensure_timer_table do
    case :ets.whereis(@timer_table) do
      :undefined ->
        try do
          :ets.new(@timer_table, [
            :set,
            :public,
            :named_table,
            {:read_concurrency, true},
            {:write_concurrency, :auto}
          ])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _tid ->
        :ok
    end
  end
end
