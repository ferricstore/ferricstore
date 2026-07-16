defmodule FerricstoreServer.Native.ResourceBudget do
  @moduledoc false

  use GenServer

  @resources [
    :executions,
    :lanes,
    :blocking_requests,
    :chunk_streams,
    :chunk_bytes,
    :inbound_bytes,
    :subscription_bytes
  ]
  @resource_indexes @resources |> Enum.with_index(1) |> Map.new()

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    opts = Keyword.put(opts, :budget_name, name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec acquire(atom(), pid(), non_neg_integer()) :: {:ok, reference()} | {:error, term()}
  def acquire(resource, owner \\ self(), amount \\ 1) do
    acquire(__MODULE__, resource, owner, amount)
  end

  @spec acquire(GenServer.server(), atom(), pid(), non_neg_integer()) ::
          {:ok, reference()} | {:error, term()}
  def acquire(server, resource, owner, amount)
      when is_atom(resource) and is_pid(owner) and is_integer(amount) and amount >= 0 do
    with {:ok, budget} <- lookup_budget(server) do
      grant_fast_lease(budget, resource, owner, amount, true)
    end
  end

  @spec acquire_wait(atom(), pid(), non_neg_integer()) ::
          {:ok, reference()} | {:error, term()}
  def acquire_wait(resource, owner \\ self(), amount \\ 1) do
    acquire_wait(__MODULE__, resource, owner, amount)
  end

  @spec acquire_wait(GenServer.server(), atom(), pid(), non_neg_integer()) ::
          {:ok, reference()} | {:error, term()}
  def acquire_wait(server, resource, owner, amount)
      when is_atom(resource) and is_pid(owner) and is_integer(amount) and amount >= 0 do
    with {:ok, budget} <- lookup_budget(server) do
      cond do
        resource not in @resources ->
          {:error, {:unknown_resource, resource}}

        owner != self() ->
          {:error, :waiter_must_own_lease}

        amount > Map.fetch!(budget.limits, resource) ->
          {:error, {:limit, resource}}

        waiter_count(budget, resource) > 0 ->
          GenServer.call(server, {:acquire_wait, resource, owner, amount}, :infinity)

        true ->
          case grant_fast_lease(budget, resource, owner, amount, true) do
            {:ok, token} = granted ->
              if waiter_count(budget, resource) > 0 do
                release_fast_lease(budget, token, true)
                GenServer.call(server, {:acquire_wait, resource, owner, amount}, :infinity)
              else
                granted
              end

            {:error, {:limit, ^resource}} ->
              GenServer.call(server, {:acquire_wait, resource, owner, amount}, :infinity)

            result ->
              result
          end
      end
    end
  catch
    :exit, _reason -> {:error, :resource_budget_unavailable}
  end

  @spec resize(GenServer.server(), reference(), non_neg_integer()) :: :ok | {:error, term()}
  def resize(server \\ __MODULE__, token, amount)
      when is_reference(token) and is_integer(amount) and amount >= 0 do
    with {:ok, budget} <- lookup_budget(server) do
      resize_fast_lease(budget, token, amount)
    end
  end

  @spec release(GenServer.server(), reference()) :: :ok
  def release(server \\ __MODULE__, token) when is_reference(token) do
    case lookup_budget(server) do
      {:ok, budget} -> release_fast_lease(budget, token, true)
      {:error, :resource_budget_unavailable} -> :ok
    end
  end

  @spec release_async(GenServer.server(), reference()) :: :ok
  def release_async(server \\ __MODULE__, token) when is_reference(token),
    do: release(server, token)

  @doc false
  def usage(server \\ __MODULE__) do
    case lookup_budget(server) do
      {:ok, budget} -> usage_snapshot(budget)
      {:error, reason} -> exit(reason)
    end
  end

  @doc false
  def waiting(server \\ __MODULE__), do: GenServer.call(server, :waiting)

  @doc false
  def waiter_queue_depths(server \\ __MODULE__), do: GenServer.call(server, :waiter_queue_depths)

  @impl true
  def init(opts) do
    limits = default_limits() |> Map.merge(Map.new(Keyword.get(opts, :limits, %{})))

    with :ok <- validate_limits(limits) do
      counters = :atomics.new(length(@resources), signed: true)
      waiter_counters = :atomics.new(length(@resources), signed: false)

      leases =
        :ets.new(:native_resource_budget_leases, [
          :set,
          :public,
          {:read_concurrency, true},
          {:write_concurrency, :auto}
        ])

      owner_leases =
        :ets.new(:native_resource_budget_owner_leases, [
          :bag,
          :public,
          {:read_concurrency, true},
          {:write_concurrency, :auto}
        ])

      tracked_owners =
        :ets.new(:native_resource_budget_owners, [
          :set,
          :public,
          {:read_concurrency, true}
        ])

      budget = %{
        coordinator: self(),
        counters: counters,
        waiter_counters: waiter_counters,
        leases: leases,
        owner_leases: owner_leases,
        tracked_owners: tracked_owners,
        limits: limits
      }

      registry_keys =
        [self(), Keyword.fetch!(opts, :budget_name)]
        |> Enum.uniq()
        |> Enum.map(&registry_key/1)

      Enum.each(registry_keys, &:persistent_term.put(&1, budget))

      {:ok,
       %{
         budget: budget,
         registry_keys: registry_keys,
         owner_monitors: %{},
         monitors: %{},
         waiter_queues: Map.new(@resources, &{&1, :queue.new()}),
         waiting: %{},
         waiting_by_owner: %{},
         waiting_counts: Map.new(@resources, &{&1, 0})
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:acquire, resource, owner, amount}, _from, state) do
    case grant_fast_lease(state.budget, resource, owner, amount, false) do
      {:ok, _token} = granted -> {:reply, granted, ensure_owner_monitor(state, owner)}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  def handle_call({:acquire_wait, resource, owner, amount}, {caller, _tag} = from, state) do
    cond do
      resource not in @resources ->
        {:reply, {:error, {:unknown_resource, resource}}, state}

      caller != owner ->
        {:reply, {:error, :waiter_must_own_lease}, state}

      amount > Map.fetch!(state.budget.limits, resource) ->
        {:reply, {:error, {:limit, resource}}, state}

      Map.fetch!(state.waiting_counts, resource) > 0 ->
        {:noreply, enqueue_waiter(state, resource, owner, amount, from)}

      true ->
        case grant_fast_lease(state.budget, resource, owner, amount, false) do
          {:ok, token} ->
            {:reply, {:ok, token}, ensure_owner_monitor(state, owner)}

          {:error, {:limit, ^resource}} ->
            {:noreply, enqueue_waiter(state, resource, owner, amount, from)}

          {:error, _reason} = error ->
            {:reply, error, state}
        end
    end
  end

  def handle_call({:resize, token, amount}, _from, state),
    do: {:reply, resize_fast_lease(state.budget, token, amount), state}

  def handle_call({:release, token}, _from, state),
    do: {:reply, :ok, release_and_grant_waiters(state, token)}

  def handle_call(:usage, _from, state),
    do: {:reply, usage_snapshot(state.budget), state}

  def handle_call(:waiting, _from, state), do: {:reply, state.waiting_counts, state}

  def handle_call(:waiter_queue_depths, _from, state) do
    depths =
      Map.new(state.waiter_queues, fn {resource, queue} -> {resource, :queue.len(queue)} end)

    {:reply, depths, state}
  end

  @impl true
  def handle_cast({:track_owner, owner}, state) do
    {:noreply, ensure_owner_monitor(state, owner)}
  end

  def handle_cast({:capacity_available, resource}, state) do
    {:noreply, grant_waiters(state, resource)}
  end

  def handle_cast({:release, token}, state) do
    {:noreply, release_and_grant_waiters(state, token)}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, owner, _reason}, state) do
    case Map.get(state.monitors, monitor_ref) do
      ^owner ->
        state = %{
          state
          | monitors: Map.delete(state.monitors, monitor_ref),
            owner_monitors: Map.delete(state.owner_monitors, owner)
        }

        :ets.delete(state.budget.tracked_owners, owner)

        {:noreply, release_owner(state, owner)}

      _stale ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.registry_keys, fn key ->
      case :persistent_term.get(key, :missing) do
        %{coordinator: coordinator} when coordinator == self() -> :persistent_term.erase(key)
        _replaced_or_missing -> :ok
      end
    end)

    Enum.each(state.owner_monitors, fn {_owner, monitor_ref} ->
      Process.demonitor(monitor_ref, [:flush])
    end)

    :ok
  end

  defp grant_fast_lease(budget, resource, owner, amount, track_owner?) do
    cond do
      resource not in @resources ->
        {:error, {:unknown_resource, resource}}

      not Process.alive?(owner) ->
        {:error, :lease_owner_dead}

      true ->
        case reserve(budget, resource, amount) do
          :ok ->
            token = make_ref()

            try do
              true = :ets.insert_new(budget.leases, {token, owner, resource, amount})
              true = :ets.insert(budget.owner_leases, {owner, token})

              if track_owner? and not tracked_owner?(budget, owner),
                do: GenServer.cast(budget.coordinator, {:track_owner, owner})

              {:ok, token}
            rescue
              ArgumentError ->
                safe_take_lease(budget.leases, token)
                safe_delete_owner_lease(budget.owner_leases, owner, token)
                release_amount(budget, resource, amount)
                {:error, :resource_budget_unavailable}
            end

          {:error, :limit} ->
            {:error, {:limit, resource}}
        end
    end
  end

  defp resize_fast_lease(budget, token, amount) do
    case safe_lookup_lease(budget.leases, token) do
      [{^token, owner, resource, previous_amount} = previous] ->
        next = {token, owner, resource, amount}
        resize_fast_lease(budget, resource, previous_amount, amount, previous, next)

      [] ->
        {:error, :unknown_lease}

      :unavailable ->
        {:error, :resource_budget_unavailable}
    end
  end

  defp resize_fast_lease(_budget, _resource, amount, amount, _previous, _next), do: :ok

  defp resize_fast_lease(budget, resource, previous_amount, amount, previous, next)
       when amount > previous_amount do
    delta = amount - previous_amount

    case reserve(budget, resource, delta) do
      :ok ->
        case replace_lease(budget.leases, previous, next) do
          :ok ->
            :ok

          :stale ->
            release_amount(budget, resource, delta)
            {:error, :unknown_lease}

          :unavailable ->
            release_amount(budget, resource, delta)
            {:error, :resource_budget_unavailable}
        end

      {:error, :limit} ->
        {:error, {:limit, resource}}
    end
  end

  defp resize_fast_lease(budget, resource, previous_amount, amount, previous, next) do
    case replace_lease(budget.leases, previous, next) do
      :ok ->
        release_amount(budget, resource, previous_amount - amount)
        notify_capacity(budget, resource)
        :ok

      :stale ->
        {:error, :unknown_lease}

      :unavailable ->
        {:error, :resource_budget_unavailable}
    end
  end

  defp replace_lease(table, previous, next) do
    case :ets.select_replace(table, [{previous, [], [{:const, next}]}]) do
      1 -> :ok
      0 -> :stale
    end
  rescue
    ArgumentError -> :unavailable
  end

  defp release_fast_lease(budget, token, notify?) do
    case safe_take_lease(budget.leases, token) do
      [{^token, owner, resource, amount}] ->
        safe_delete_owner_lease(budget.owner_leases, owner, token)
        release_amount(budget, resource, amount)
        if notify?, do: notify_capacity(budget, resource)
        :ok

      [] ->
        :ok

      :unavailable ->
        :ok
    end
  end

  defp release_and_grant_waiters(state, token) do
    case safe_take_lease(state.budget.leases, token) do
      [{^token, owner, resource, amount}] ->
        safe_delete_owner_lease(state.budget.owner_leases, owner, token)
        release_amount(state.budget, resource, amount)
        grant_waiters(state, resource)

      _missing_or_unavailable ->
        state
    end
  end

  defp reserve(budget, resource, amount) do
    index = Map.fetch!(@resource_indexes, resource)
    limit = Map.fetch!(budget.limits, resource)
    reserve_counter(budget.counters, index, limit, amount)
  end

  defp reserve_counter(counters, index, limit, amount) do
    used = :atomics.get(counters, index)

    if amount <= limit - used do
      case :atomics.compare_exchange(counters, index, used, used + amount) do
        :ok -> :ok
        _changed -> reserve_counter(counters, index, limit, amount)
      end
    else
      {:error, :limit}
    end
  end

  defp release_amount(_budget, _resource, 0), do: :ok

  defp release_amount(budget, resource, amount) do
    :atomics.sub(budget.counters, Map.fetch!(@resource_indexes, resource), amount)
    :ok
  end

  defp notify_capacity(budget, resource) do
    GenServer.cast(budget.coordinator, {:capacity_available, resource})
  end

  defp usage_snapshot(budget) do
    Map.new(@resources, fn resource ->
      {resource, :atomics.get(budget.counters, Map.fetch!(@resource_indexes, resource))}
    end)
  end

  defp waiter_count(budget, resource) do
    :atomics.get(budget.waiter_counters, Map.fetch!(@resource_indexes, resource))
  end

  defp tracked_owner?(budget, owner) do
    :ets.member(budget.tracked_owners, owner)
  rescue
    ArgumentError -> false
  end

  defp safe_lookup_lease(table, token) do
    :ets.lookup(table, token)
  rescue
    ArgumentError -> :unavailable
  end

  defp safe_take_lease(table, token) do
    :ets.take(table, token)
  rescue
    ArgumentError -> :unavailable
  end

  defp safe_delete_owner_lease(table, owner, token) do
    :ets.delete_object(table, {owner, token})
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp lookup_budget(server) do
    case :persistent_term.get(registry_key(server), :missing) do
      %{coordinator: coordinator, leases: leases} = budget ->
        if Process.alive?(coordinator) and :ets.info(leases) != :undefined do
          {:ok, budget}
        else
          {:error, :resource_budget_unavailable}
        end

      :missing ->
        {:error, :resource_budget_unavailable}
    end
  rescue
    ArgumentError -> {:error, :resource_budget_unavailable}
  end

  defp registry_key(server), do: {__MODULE__, :budget, server}

  defp ensure_owner_monitor(%{owner_monitors: owner_monitors} = state, owner) do
    case Map.fetch(owner_monitors, owner) do
      {:ok, _monitor_ref} ->
        state

      :error ->
        monitor_ref = Process.monitor(owner)
        true = :ets.insert(state.budget.tracked_owners, {owner})

        %{
          state
          | owner_monitors: Map.put(owner_monitors, owner, monitor_ref),
            monitors: Map.put(state.monitors, monitor_ref, owner)
        }
    end
  end

  defp enqueue_waiter(state, resource, owner, amount, from) do
    waiter_ref = make_ref()
    state = ensure_owner_monitor(state, owner)
    :atomics.add(state.budget.waiter_counters, Map.fetch!(@resource_indexes, resource), 1)

    %{
      state
      | waiter_queues: Map.update!(state.waiter_queues, resource, &:queue.in(waiter_ref, &1)),
        waiting:
          Map.put(state.waiting, waiter_ref, %{
            owner: owner,
            resource: resource,
            amount: amount,
            from: from
          }),
        waiting_by_owner:
          Map.update(
            state.waiting_by_owner,
            owner,
            MapSet.new([waiter_ref]),
            &MapSet.put(&1, waiter_ref)
          ),
        waiting_counts: Map.update!(state.waiting_counts, resource, &(&1 + 1))
    }
  end

  defp grant_waiters(state, resource) do
    queue = Map.fetch!(state.waiter_queues, resource)

    case :queue.out(queue) do
      {:empty, _queue} ->
        state

      {{:value, waiter_ref}, queue} ->
        state = %{state | waiter_queues: Map.put(state.waiter_queues, resource, queue)}

        case Map.get(state.waiting, waiter_ref) do
          nil ->
            grant_waiters(state, resource)

          %{owner: owner, amount: amount, from: from} ->
            case grant_fast_lease(state.budget, resource, owner, amount, false) do
              {:ok, token} ->
                state = remove_waiter(state, waiter_ref)
                GenServer.reply(from, {:ok, token})
                grant_waiters(state, resource)

              {:error, :lease_owner_dead} ->
                state
                |> remove_waiter(waiter_ref)
                |> grant_waiters(resource)

              {:error, {:limit, ^resource}} ->
                %{
                  state
                  | waiter_queues:
                      Map.put(state.waiter_queues, resource, :queue.in_r(waiter_ref, queue))
                }

              {:error, _reason} = error ->
                state = remove_waiter(state, waiter_ref)
                GenServer.reply(from, error)
                grant_waiters(state, resource)
            end
        end
    end
  end

  defp remove_waiter(state, waiter_ref) do
    case Map.pop(state.waiting, waiter_ref) do
      {nil, _waiting} ->
        state

      {%{owner: owner, resource: resource}, waiting} ->
        :atomics.sub(
          state.budget.waiter_counters,
          Map.fetch!(@resource_indexes, resource),
          1
        )

        %{
          state
          | waiting: waiting,
            waiting_by_owner: delete_owner_waiter(state.waiting_by_owner, owner, waiter_ref),
            waiting_counts: Map.update!(state.waiting_counts, resource, &max(&1 - 1, 0))
        }
    end
  end

  defp delete_owner_waiter(waiting_by_owner, owner, waiter_ref) do
    case Map.fetch(waiting_by_owner, owner) do
      {:ok, waiter_refs} ->
        remaining = MapSet.delete(waiter_refs, waiter_ref)

        if MapSet.size(remaining) == 0 do
          Map.delete(waiting_by_owner, owner)
        else
          Map.put(waiting_by_owner, owner, remaining)
        end

      :error ->
        waiting_by_owner
    end
  end

  defp release_owner(state, owner) do
    {state, canceled_resources} = cancel_owner_waiters(state, owner)

    state =
      Enum.reduce(canceled_resources, state, fn resource, acc ->
        maybe_compact_waiter_queue(acc, resource)
      end)

    released_resources = release_owner_leases(state.budget, owner)

    Enum.reduce(released_resources, state, fn resource, acc ->
      grant_waiters(acc, resource)
    end)
  end

  defp cancel_owner_waiters(state, owner) do
    state.waiting_by_owner
    |> Map.get(owner, MapSet.new())
    |> Enum.reduce({state, MapSet.new()}, fn waiter_ref, {acc, resources} ->
      case Map.get(acc.waiting, waiter_ref) do
        %{resource: resource} ->
          {remove_waiter(acc, waiter_ref), MapSet.put(resources, resource)}

        nil ->
          {acc, resources}
      end
    end)
  end

  defp release_owner_leases(budget, owner) do
    rows =
      try do
        :ets.lookup(budget.owner_leases, owner)
      rescue
        ArgumentError -> []
      end

    Enum.reduce(rows, MapSet.new(), fn {^owner, token}, resources ->
      safe_delete_owner_lease(budget.owner_leases, owner, token)

      case safe_take_lease(budget.leases, token) do
        [{^token, ^owner, resource, amount}] ->
          release_amount(budget, resource, amount)
          MapSet.put(resources, resource)

        _already_released ->
          resources
      end
    end)
  end

  defp maybe_compact_waiter_queue(state, resource) do
    queue = Map.fetch!(state.waiter_queues, resource)
    physical_count = :queue.len(queue)
    live_count = Map.fetch!(state.waiting_counts, resource)

    if physical_count > 64 and physical_count > live_count * 2 do
      compacted = :queue.filter(&Map.has_key?(state.waiting, &1), queue)
      %{state | waiter_queues: Map.put(state.waiter_queues, resource, compacted)}
    else
      state
    end
  end

  defp default_limits do
    schedulers = max(System.schedulers_online(), 1)

    %{
      executions:
        Application.get_env(:ferricstore, :native_max_global_executions, schedulers * 8),
      lanes: Application.get_env(:ferricstore, :native_max_global_lanes, schedulers * 32),
      blocking_requests:
        Application.get_env(:ferricstore, :native_max_global_blocking_requests, 4_096),
      chunk_streams: Application.get_env(:ferricstore, :native_max_global_pending_chunks, 4_096),
      chunk_bytes:
        Application.get_env(
          :ferricstore,
          :native_max_global_pending_chunk_bytes,
          256 * 1024 * 1024
        ),
      inbound_bytes:
        Application.get_env(
          :ferricstore,
          :native_max_global_inbound_buffer_bytes,
          256 * 1024 * 1024
        ),
      subscription_bytes:
        Application.get_env(
          :ferricstore,
          :native_max_global_subscription_bytes,
          256 * 1024 * 1024
        )
    }
  end

  defp validate_limits(limits) do
    case Enum.find(@resources, fn resource ->
           value = Map.get(limits, resource)
           not (is_integer(value) and value > 0)
         end) do
      nil -> :ok
      resource -> {:error, {:invalid_resource_limit, resource, Map.get(limits, resource)}}
    end
  end
end
