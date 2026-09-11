defmodule FerricstoreHttp.Auth.Cache do
  @moduledoc false

  use GenServer

  @table __MODULE__
  @runtime_key {__MODULE__, :runtime}
  @default_task_supervisor __MODULE__.TaskSupervisor

  @type source :: :hit | :miss | :coalesced | :timeout
  @type fetch_result :: {:ok, term(), source()} | {:error, term(), source()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts),
    do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @spec reference(module(), module(), term()) :: binary()
  def reference(provider, backend, material) when is_atom(provider) and is_atom(backend) do
    %{hmac_key: key} = runtime!()
    encoded = :erlang.term_to_binary({provider, backend, material}, [:deterministic])
    :crypto.mac(:hmac, :sha256, key, encoded)
  end

  @spec fetch(binary(), term(), (-> {:ok, term()} | {:error, term()}), timeout()) ::
          fetch_result()
  def fetch(cache_key, flight_scope, authenticate, timeout)
      when is_binary(cache_key) and is_function(authenticate, 0) do
    case lookup(cache_key) do
      {:ok, session} -> {:ok, session, :hit}
      :miss -> call_fetch(cache_key, flight_scope, authenticate, timeout)
    end
  end

  @spec invalidate(binary(), term()) :: :ok
  def invalidate(cache_key, session) when is_binary(cache_key) do
    case :ets.lookup(@table, cache_key) do
      [{^cache_key, cached, _expires_at_ms, _last_used_ms} = entry] ->
        if cached_session(cached) == session do
          _deleted = :ets.delete_object(@table, entry)
        end

        :ok

      _missing_or_replaced ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp cached_session(%{session: session}), do: session
  defp cached_session(session), do: session

  @spec stats() :: %{
          entries: non_neg_integer(),
          max_entries: non_neg_integer(),
          pending: non_neg_integer()
        }
  def stats do
    GenServer.call(__MODULE__, :stats)
  catch
    :exit, _reason -> %{entries: 0, max_entries: 0, pending: 0}
  end

  @impl GenServer
  def init(opts) do
    table =
      :ets.new(@table, [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])

    state = %{
      table: table,
      hmac_key: :crypto.strong_rand_bytes(32),
      ttl_ms: Keyword.fetch!(opts, :ttl_ms),
      touch_interval_ms: Keyword.get(opts, :touch_interval_ms, 1_000),
      max_entries: Keyword.fetch!(opts, :max_entries),
      sweep_interval_ms: Keyword.fetch!(opts, :sweep_interval_ms),
      task_supervisor: Keyword.get(opts, :task_supervisor, @default_task_supervisor),
      clock: Keyword.get(opts, :clock, fn -> System.monotonic_time(:millisecond) end),
      pending: %{},
      flights: %{},
      waiters: %{},
      timer: nil
    }

    :persistent_term.put(@runtime_key, %{
      owner: self(),
      hmac_key: state.hmac_key,
      clock: state.clock,
      touch_interval_ms: state.touch_interval_ms
    })

    {:ok, schedule_sweep(state)}
  end

  @impl GenServer
  def handle_call({:fetch, cache_key, flight_key, waiter_ref, authenticate}, from, state) do
    case lookup(cache_key) do
      {:ok, session} ->
        {:reply, {:ok, session, :hit}, state}

      :miss ->
        join_or_start_flight(cache_key, flight_key, waiter_ref, authenticate, from, state)
    end
  end

  def handle_call(:stats, _from, state) do
    stats = %{
      entries: :ets.info(state.table, :size),
      max_entries: state.max_entries,
      pending: map_size(state.pending)
    }

    {:reply, stats, state}
  end

  @impl GenServer
  def handle_cast({:cancel_waiter, waiter_ref}, state) do
    case Map.pop(state.waiters, waiter_ref) do
      {nil, _waiters} ->
        {:noreply, state}

      {task_ref, waiters} ->
        pending = Map.fetch!(state.pending, task_ref)
        flight_waiters = Map.delete(pending.waiters, waiter_ref)

        if map_size(flight_waiters) == 0 do
          _terminated = Task.Supervisor.terminate_child(state.task_supervisor, pending.task_pid)

          {:noreply,
           %{
             state
             | pending: Map.delete(state.pending, task_ref),
               flights: Map.delete(state.flights, pending.flight_key),
               waiters: waiters
           }}
        else
          pending = Map.put(state.pending, task_ref, %{pending | waiters: flight_waiters})
          {:noreply, %{state | pending: pending, waiters: waiters}}
        end
    end
  end

  @impl GenServer
  def handle_info({ref, result}, %{pending: pending} = state) when is_map_key(pending, ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, complete(ref, result, state)}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{pending: pending} = state)
      when is_map_key(pending, ref) do
    {:noreply, complete(ref, {:error, :authentication_unavailable}, state)}
  end

  def handle_info(:sweep, state) do
    prune(state)
    {:noreply, schedule_sweep(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, _state) do
    case :persistent_term.get(@runtime_key, nil) do
      %{owner: owner} when owner == self() -> :persistent_term.erase(@runtime_key)
      _other_owner -> :ok
    end

    :ok
  end

  defp join_or_start_flight(cache_key, flight_key, waiter_ref, authenticate, from, state) do
    case Map.fetch(state.flights, flight_key) do
      {:ok, ref} ->
        pending =
          Map.update!(state.pending, ref, fn flight ->
            %{flight | waiters: Map.put(flight.waiters, waiter_ref, {from, :coalesced})}
          end)

        {:noreply, %{state | pending: pending, waiters: Map.put(state.waiters, waiter_ref, ref)}}

      :error ->
        start_flight(cache_key, flight_key, waiter_ref, authenticate, from, state)
    end
  end

  defp start_flight(_cache_key, _flight_key, _waiter_ref, _authenticate, _from, state)
       when map_size(state.pending) >= state.max_entries,
       do: {:reply, {:error, :authentication_unavailable, :miss}, state}

  defp start_flight(cache_key, flight_key, waiter_ref, authenticate, from, state) do
    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        safe_authenticate(authenticate)
      end)

    pending = %{
      cache_key: cache_key,
      flight_key: flight_key,
      task_pid: task.pid,
      waiters: %{waiter_ref => {from, :miss}}
    }

    {:noreply,
     %{
       state
       | pending: Map.put(state.pending, task.ref, pending),
         flights: Map.put(state.flights, flight_key, task.ref),
         waiters: Map.put(state.waiters, waiter_ref, task.ref)
     }}
  end

  defp call_fetch(cache_key, flight_scope, authenticate, timeout) do
    flight_key = {cache_key, flight_scope}
    waiter_ref = make_ref()

    try do
      GenServer.call(
        __MODULE__,
        {:fetch, cache_key, flight_key, waiter_ref, authenticate},
        timeout
      )
    catch
      :exit, {:timeout, _call} ->
        GenServer.cast(__MODULE__, {:cancel_waiter, waiter_ref})
        {:error, :authentication_unavailable, :timeout}

      :exit, _reason ->
        GenServer.cast(__MODULE__, {:cancel_waiter, waiter_ref})
        {:error, :authentication_unavailable, :timeout}
    end
  end

  defp lookup(cache_key) do
    %{clock: clock, touch_interval_ms: touch_interval_ms} = runtime!()
    now_ms = clock.()

    case :ets.lookup(@table, cache_key) do
      [{^cache_key, session, expires_at_ms, last_used_ms}] when expires_at_ms > now_ms ->
        maybe_touch(cache_key, now_ms, last_used_ms, touch_interval_ms)
        {:ok, session}

      [{^cache_key, _session, _expires_at_ms, _last_used_ms}] ->
        :ets.delete(@table, cache_key)
        :miss

      [] ->
        :miss
    end
  rescue
    ArgumentError -> :miss
  end

  defp maybe_touch(cache_key, now_ms, last_used_ms, touch_interval_ms)
       when now_ms - last_used_ms >= touch_interval_ms do
    _updated = :ets.update_element(@table, cache_key, {4, now_ms})
    :ok
  end

  defp maybe_touch(_cache_key, _now_ms, _last_used_ms, _touch_interval_ms), do: :ok

  defp complete(ref, result, state) do
    {pending, remaining} = Map.pop!(state.pending, ref)

    waiters =
      Enum.reduce(pending.waiters, state.waiters, fn {waiter_ref, _waiter}, waiters ->
        Map.delete(waiters, waiter_ref)
      end)

    state = %{
      state
      | pending: remaining,
        flights: Map.delete(state.flights, pending.flight_key),
        waiters: waiters
    }

    state = maybe_cache(result, pending.cache_key, state)

    pending.waiters
    |> Enum.each(fn {_waiter_ref, {from, source}} ->
      GenServer.reply(from, tagged_result(result, source))
    end)

    state
  end

  defp maybe_cache({:ok, session}, cache_key, state) do
    now_ms = state.clock.()
    true = :ets.insert(state.table, {cache_key, session, now_ms + state.ttl_ms, now_ms})
    if :ets.info(state.table, :size) > state.max_entries, do: prune(state)
    state
  end

  defp maybe_cache(_error, _cache_key, state), do: state

  defp tagged_result({:ok, session}, source), do: {:ok, session, source}
  defp tagged_result({:error, reason}, source), do: {:error, reason, source}

  defp safe_authenticate(authenticate) do
    case authenticate.() do
      {:ok, _session} = success -> success
      {:error, _reason} = error -> error
      _invalid -> {:error, :authentication_unavailable}
    end
  rescue
    _error -> {:error, :authentication_unavailable}
  catch
    _kind, _reason -> {:error, :authentication_unavailable}
  end

  defp prune(state) do
    now_ms = state.clock.()

    Enum.each(:ets.tab2list(state.table), fn
      {cache_key, _session, expires_at_ms, _last_used_ms} when expires_at_ms <= now_ms ->
        :ets.delete(state.table, cache_key)

      _active ->
        :ok
    end)

    overflow = max(:ets.info(state.table, :size) - state.max_entries, 0)

    evict_lru(state.table, overflow)
  end

  defp evict_lru(_table, 0), do: :ok

  defp evict_lru(table, 1) do
    case :ets.foldl(&least_recent/2, nil, table) do
      {cache_key, _session, _expires_at_ms, _last_used_ms} -> :ets.delete(table, cache_key)
      nil -> :ok
    end
  end

  defp evict_lru(table, count) do
    table
    |> :ets.tab2list()
    |> Enum.sort_by(fn {cache_key, _session, _expires_at_ms, last_used_ms} ->
      {last_used_ms, cache_key}
    end)
    |> Enum.take(count)
    |> Enum.each(fn {cache_key, _session, _expires_at_ms, _last_used_ms} ->
      :ets.delete(table, cache_key)
    end)
  end

  defp least_recent(entry, nil), do: entry

  defp least_recent(
         {cache_key, _session, _expires_at_ms, last_used_ms} = entry,
         {least_key, _least_session, _least_expiry, least_used_ms} = least
       ) do
    if {last_used_ms, cache_key} < {least_used_ms, least_key}, do: entry, else: least
  end

  defp schedule_sweep(state) do
    if is_reference(state.timer), do: Process.cancel_timer(state.timer)
    %{state | timer: Process.send_after(self(), :sweep, state.sweep_interval_ms)}
  end

  defp runtime! do
    case :persistent_term.get(@runtime_key, nil) do
      %{owner: owner} = runtime when is_pid(owner) -> runtime
      _unavailable -> raise ArgumentError, "authentication cache is not running"
    end
  end
end
