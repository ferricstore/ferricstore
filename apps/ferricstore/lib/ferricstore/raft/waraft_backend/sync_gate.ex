defmodule Ferricstore.Raft.WARaftBackend.SyncGate do
  @moduledoc false

  use GenServer

  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Raft.WARaftBackend.SyncGate.TableOwner

  @pause_key {__MODULE__, :pause}

  @type token :: {non_neg_integer(), reference()}
  @type pause_lease :: {pid(), reference()}

  @spec lease(pid()) :: pause_lease()
  def lease(owner_pid \\ self()) when is_pid(owner_pid), do: {owner_pid, make_ref()}

  @spec init_shards(pos_integer()) :: :ok
  def init_shards(shard_count) when is_integer(shard_count) and shard_count > 0 do
    TableOwner.ensure_table()
  end

  def init_shards(_shard_count), do: TableOwner.ensure_table()

  @spec clear_shards(non_neg_integer()) :: :ok
  def clear_shards(shard_count) when is_integer(shard_count) and shard_count > 0 do
    Enum.each(0..(shard_count - 1), fn shard_index ->
      _ = force_resume(shard_index, 5_000)
      delete_shard_admissions(shard_index)
      :persistent_term.erase(pause_key(shard_index))
    end)

    :ok
  end

  def clear_shards(_shard_count), do: :ok

  @spec enter(non_neg_integer()) :: {:ok, token()} | {:error, term()}
  def enter(shard_index) when is_integer(shard_index) and shard_index >= 0 do
    with {:ok, token} <- claim(shard_index) do
      admit_or_wait(token)
    end
  end

  def enter(shard_index), do: {:error, {:invalid_shard_index, shard_index}}

  defp admit_or_wait(token) do
    if WARaftBackend.starting?() do
      reject_startup_admission(token)
    else
      wait_for_pause_or_admit(token)
    end
  end

  defp reject_startup_admission(token) do
    leave(token)
    {:error, :backend_unavailable}
  end

  defp wait_for_pause_or_admit({shard_index, _admission_ref} = token) do
    case pause_pid(shard_index) do
      nil -> {:ok, token}
      pid -> wait_for_pause(shard_index, token, pid)
    end
  end

  defp wait_for_pause(shard_index, token, pid) do
    leave(token)

    case await(pid, :infinity) do
      :ok -> enter(shard_index)
      {:error, _reason} = error -> error
    end
  end

  @spec enter_many([non_neg_integer()]) :: {:ok, [token()]} | {:error, term()}
  def enter_many(shard_indexes) when is_list(shard_indexes) do
    indexes = Enum.uniq(shard_indexes)

    if Enum.all?(indexes, &(is_integer(&1) and &1 >= 0)) do
      do_enter_many(indexes)
    else
      {:error, {:invalid_shard_indexes, shard_indexes}}
    end
  end

  def enter_many(shard_indexes), do: {:error, {:invalid_shard_indexes, shard_indexes}}

  @spec leave(token()) :: :ok
  def leave({shard_index, admission_ref})
      when is_integer(shard_index) and shard_index >= 0 and is_reference(admission_ref) do
    _ = take_admission(shard_index, admission_ref)
    notify_admission_changed(shard_index)
    :ok
  catch
    :error, _reason -> :ok
  end

  def leave(_token), do: :ok

  @spec transfer(token(), pid()) :: :ok | {:error, term()}
  def transfer({shard_index, admission_ref}, owner_pid)
      when is_integer(shard_index) and shard_index >= 0 and is_reference(admission_ref) and
             is_pid(owner_pid) do
    case update_admission_owner(shard_index, admission_ref, owner_pid) do
      true ->
        notify_admission_changed(shard_index)
        :ok

      false ->
        {:error, :unknown_sync_admission}
    end
  catch
    :error, reason -> {:error, {:sync_gate_transfer_failed, reason}}
  end

  def transfer(_token, _owner_pid), do: {:error, :invalid_sync_admission}

  defp do_enter_many([]), do: {:ok, []}

  defp do_enter_many(shard_indexes) do
    with {:ok, tokens} <- claim_many(shard_indexes) do
      admit_many_or_wait(shard_indexes, tokens)
    end
  end

  defp admit_many_or_wait(shard_indexes, tokens) do
    if WARaftBackend.starting?() do
      reject_startup_admissions(tokens)
    else
      wait_for_pauses_or_admit(shard_indexes, tokens)
    end
  end

  defp reject_startup_admissions(tokens) do
    Enum.each(tokens, &leave/1)
    {:error, :backend_unavailable}
  end

  defp wait_for_pauses_or_admit(shard_indexes, tokens) do
    case active_pauses(tokens) do
      [] -> {:ok, tokens}
      pauses -> wait_for_pauses(shard_indexes, tokens, pauses)
    end
  end

  defp wait_for_pauses(shard_indexes, tokens, pauses) do
    Enum.each(tokens, &leave/1)

    case await_pauses(pauses) do
      :ok -> do_enter_many(shard_indexes)
      {:error, _reason} = error -> error
    end
  end

  defp claim_many(shard_indexes) do
    shard_indexes
    |> Enum.reduce_while({:ok, []}, fn shard_index, {:ok, tokens} ->
      case claim(shard_index) do
        {:ok, token} ->
          {:cont, {:ok, [token | tokens]}}

        {:error, _reason} = error ->
          Enum.each(tokens, &leave/1)
          {:halt, error}
      end
    end)
    |> case do
      {:ok, tokens} -> {:ok, Enum.reverse(tokens)}
      {:error, _reason} = error -> error
    end
  end

  defp claim(shard_index) do
    admission_ref = make_ref()
    token = {shard_index, admission_ref}

    case insert_admission(shard_index, admission_ref, self()) do
      true -> {:ok, token}
      false -> {:error, :sync_gate_admission_conflict}
    end
  catch
    :error, reason -> {:error, {:sync_gate_enter_failed, reason}}
  end

  defp active_pauses(tokens) do
    Enum.flat_map(tokens, fn {shard_index, _admission_ref} ->
      case pause_pid(shard_index) do
        nil -> []
        pid -> [{shard_index, pid}]
      end
    end)
  end

  defp await_pauses(pauses) do
    Enum.reduce_while(pauses, :ok, fn {_shard_index, pid}, :ok ->
      case await(pid, :infinity) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @spec pause(non_neg_integer()) :: {:ok, pid()} | {:error, term()}
  def pause(shard_index) when is_integer(shard_index) and shard_index >= 0 do
    case GenServer.start(__MODULE__, {:unscoped, shard_index}, name: name(shard_index)) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> acquire_existing_unscoped_pause(shard_index, pid)
      {:error, reason} -> {:error, reason}
    end
  end

  def pause(shard_index), do: {:error, {:invalid_shard_index, shard_index}}

  @spec pause(non_neg_integer(), pause_lease()) :: {:ok, pid()} | {:error, term()}
  def pause(shard_index, {owner_pid, lease_ref} = pause_lease)
      when is_integer(shard_index) and shard_index >= 0 and is_pid(owner_pid) and
             is_reference(lease_ref) do
    case GenServer.start(__MODULE__, {shard_index, pause_lease}, name: name(shard_index)) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> acquire_existing_pause(shard_index, pid, pause_lease)
      {:error, reason} -> {:error, reason}
    end
  end

  def pause(shard_index, _pause_lease), do: {:error, {:invalid_shard_index, shard_index}}

  @spec pause_many([non_neg_integer()]) ::
          {:ok, [{non_neg_integer(), pid()}]} | {:error, term()}
  def pause_many(shard_indexes) when is_list(shard_indexes) do
    shard_indexes
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, []}, fn shard_index, {:ok, paused} ->
      case pause(shard_index) do
        {:ok, pid} ->
          {:cont, {:ok, [{shard_index, pid} | paused]}}

        {:error, reason} ->
          _ = resume_many(Enum.map(paused, &elem(&1, 0)), 5_000)
          {:halt, {:error, {:sync_pause_many_failed, shard_index, reason}}}
      end
    end)
    |> case do
      {:ok, paused} -> {:ok, Enum.reverse(paused)}
      {:error, _reason} = error -> error
    end
  end

  def pause_many(shard_indexes), do: {:error, {:invalid_shard_indexes, shard_indexes}}

  @spec pause_many([non_neg_integer()], pause_lease()) ::
          {:ok, [{non_neg_integer(), pid()}]} | {:error, term()}
  def pause_many(shard_indexes, {owner_pid, lease_ref} = pause_lease)
      when is_list(shard_indexes) and is_pid(owner_pid) and is_reference(lease_ref) do
    shard_indexes
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, []}, fn shard_index, {:ok, paused} ->
      case pause(shard_index, pause_lease) do
        {:ok, pid} ->
          {:cont, {:ok, [{shard_index, pid} | paused]}}

        {:error, reason} ->
          _ = resume_many(Enum.map(paused, &elem(&1, 0)), pause_lease, 5_000)
          {:halt, {:error, {:sync_pause_many_failed, shard_index, reason}}}
      end
    end)
    |> case do
      {:ok, paused} -> {:ok, Enum.reverse(paused)}
      {:error, _reason} = error -> error
    end
  end

  def pause_many(shard_indexes, _pause_lease),
    do: {:error, {:invalid_shard_indexes, shard_indexes}}

  @spec await_many_drained([{non_neg_integer(), pid()}], timeout()) :: :ok | {:error, term()}
  def await_many_drained(pauses, timeout) when is_list(pauses) do
    deadline = sync_gate_deadline(timeout)

    Enum.reduce_while(pauses, :ok, fn {shard_index, pid}, :ok ->
      case await_drained(pid, sync_gate_remaining(deadline)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:sync_pause_drain_failed, shard_index, reason}}}
      end
    end)
  end

  @spec resume_many([non_neg_integer()], timeout()) :: :ok | {:error, term()}
  def resume_many(shard_indexes, timeout) when is_list(shard_indexes) do
    deadline = sync_gate_deadline(timeout)

    failures =
      shard_indexes
      |> Enum.uniq()
      |> Enum.reduce([], fn shard_index, failures ->
        case resume(shard_index, sync_gate_remaining(deadline)) do
          :ok -> failures
          {:error, reason} -> [{shard_index, reason} | failures]
        end
      end)

    case Enum.reverse(failures) do
      [] -> :ok
      failures -> {:error, {:sync_resume_many_failed, failures}}
    end
  end

  def resume_many(shard_indexes, _timeout),
    do: {:error, {:invalid_shard_indexes, shard_indexes}}

  @spec resume_many([non_neg_integer()], pause_lease(), timeout()) :: :ok | {:error, term()}
  def resume_many(shard_indexes, {owner_pid, lease_ref} = pause_lease, timeout)
      when is_list(shard_indexes) and is_pid(owner_pid) and is_reference(lease_ref) do
    deadline = sync_gate_deadline(timeout)

    failures =
      shard_indexes
      |> Enum.uniq()
      |> Enum.reduce([], fn shard_index, failures ->
        case resume(shard_index, pause_lease, sync_gate_remaining(deadline)) do
          :ok -> failures
          {:error, reason} -> [{shard_index, reason} | failures]
        end
      end)

    case Enum.reverse(failures) do
      [] -> :ok
      failures -> {:error, {:sync_resume_many_failed, failures}}
    end
  end

  def resume_many(shard_indexes, _pause_lease, _timeout),
    do: {:error, {:invalid_shard_indexes, shard_indexes}}

  @spec paused?(non_neg_integer()) :: boolean()
  def paused?(shard_index) when is_integer(shard_index) and shard_index >= 0,
    do: is_pid(pause_pid(shard_index))

  def paused?(_shard_index), do: false

  @spec await(pid(), timeout()) :: :ok | {:error, term()}
  def await(pid, timeout) when is_pid(pid) do
    GenServer.call(pid, :await, timeout)
  catch
    :exit, {:timeout, _} -> {:error, :sync_pause_timeout}
    :exit, {:noproc, _} -> :ok
    :exit, {:normal, _} -> :ok
    :exit, reason -> {:error, {:sync_pause_wait_failed, reason}}
  end

  @spec await_drained(pid(), timeout()) :: :ok | {:error, term()}
  def await_drained(pid, timeout) when is_pid(pid) do
    GenServer.call(pid, :await_drained, timeout)
  catch
    :exit, {:timeout, _} -> {:error, :sync_pause_drain_timeout}
    :exit, {:noproc, _} -> {:error, :sync_pause_released}
    :exit, {:normal, _} -> {:error, :sync_pause_released}
    :exit, reason -> {:error, {:sync_pause_drain_failed, reason}}
  end

  @spec resume(non_neg_integer(), timeout()) :: :ok | {:error, term()}
  def resume(shard_index, timeout \\ 5_000) when is_integer(shard_index) and shard_index >= 0 do
    case Process.whereis(name(shard_index)) do
      nil -> :ok
      _pid -> GenServer.call(name(shard_index), :resume, timeout)
    end
  catch
    :exit, {:noproc, _} -> :ok
    :exit, {:normal, _} -> :ok
    :exit, reason -> {:error, {:sync_pause_resume_failed, reason}}
  end

  @spec resume(non_neg_integer(), pause_lease(), timeout()) :: :ok | {:error, term()}
  def resume(shard_index, {owner_pid, lease_ref} = pause_lease, timeout)
      when is_integer(shard_index) and shard_index >= 0 and is_pid(owner_pid) and
             is_reference(lease_ref) do
    case Process.whereis(name(shard_index)) do
      nil -> :ok
      _pid -> GenServer.call(name(shard_index), {:resume, pause_lease}, timeout)
    end
  catch
    :exit, {:noproc, _} -> :ok
    :exit, {:normal, _} -> :ok
    :exit, reason -> {:error, {:sync_pause_resume_failed, reason}}
  end

  def resume(shard_index, _pause_lease, _timeout),
    do: {:error, {:invalid_shard_index, shard_index}}

  @spec force_resume(non_neg_integer(), timeout()) :: :ok | {:error, term()}
  def force_resume(shard_index, timeout \\ 5_000)

  def force_resume(shard_index, timeout) when is_integer(shard_index) and shard_index >= 0 do
    case Process.whereis(name(shard_index)) do
      nil -> :ok
      _pid -> GenServer.call(name(shard_index), :force_resume, timeout)
    end
  catch
    :exit, {:noproc, _} -> :ok
    :exit, {:normal, _} -> :ok
    :exit, reason -> {:error, {:sync_pause_force_resume_failed, reason}}
  end

  def force_resume(shard_index, _timeout), do: {:error, {:invalid_shard_index, shard_index}}

  @spec name(non_neg_integer()) :: atom()
  def name(shard_index), do: :"ferricstore_waraft_sync_gate_#{shard_index}"

  @impl true
  def init({:unscoped, shard_index}) do
    state = initial_pause_state(shard_index)
    {:ok, %{state | unscoped_holds: 1}}
  end

  def init({shard_index, pause_lease}) do
    state = initial_pause_state(shard_index)

    case add_pause_lease(state, pause_lease) do
      {:ok, state} -> {:ok, state}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:acquire_unscoped, _from, state) do
    {:reply, :ok, %{state | unscoped_holds: state.unscoped_holds + 1}}
  end

  def handle_call({:acquire, pause_lease}, _from, state) do
    case add_pause_lease(state, pause_lease) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:await, from, state) do
    {:noreply, %{state | waiters: [from | state.waiters]}}
  end

  def handle_call(:await_drained, from, state) do
    state = refresh_admission_monitors(state)

    if admissions_empty?(state.shard_index) do
      {:reply, :ok, state}
    else
      {:noreply, %{state | drain_waiters: [from | state.drain_waiters]}}
    end
  end

  def handle_call(:resume, _from, %{unscoped_holds: unscoped_holds} = state)
      when unscoped_holds > 0 do
    state = %{state | unscoped_holds: unscoped_holds - 1}
    maybe_release_pause(state)
  end

  def handle_call(:resume, _from, state), do: {:reply, :ok, state}

  def handle_call({:resume, pause_lease}, _from, state) do
    state = remove_pause_lease(state, pause_lease)
    maybe_release_pause(state)
  end

  def handle_call(:force_resume, _from, state) do
    state = demonitor_pause_owners(state)
    release_pause(state)
  end

  defp release_pause(state) do
    state = demonitor_admission_owners(state)
    unpublish_pause(state.shard_index)
    Enum.each(state.waiters, &GenServer.reply(&1, :ok))
    Enum.each(state.drain_waiters, &GenServer.reply(&1, {:error, :sync_pause_released}))

    {:stop, :normal, :ok,
     %{state | waiters: [], drain_waiters: [], admission_owners: %{}, admission_monitors: %{}}}
  end

  defp maybe_release_pause(%{unscoped_holds: 0, leases: leases} = state)
       when map_size(leases) == 0,
       do: release_pause(state)

  defp maybe_release_pause(state), do: {:reply, :ok, state}

  @impl true
  def handle_cast(:drained, state) do
    state = maybe_reply_drained(state)
    {:noreply, state}
  end

  def handle_cast(:admission_changed, state) do
    state = maybe_reply_drained(state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, owner_pid, _reason}, state) do
    case Map.get(state.monitor_owners, monitor_ref) do
      ^owner_pid ->
        state = remove_owner_leases(state, owner_pid, false)

        if state.unscoped_holds == 0 and map_size(state.leases) == 0 do
          state = demonitor_admission_owners(state)
          unpublish_pause(state.shard_index)
          Enum.each(state.waiters, &GenServer.reply(&1, :ok))

          Enum.each(
            state.drain_waiters,
            &GenServer.reply(&1, {:error, :sync_pause_released})
          )

          {:stop, :normal,
           %{
             state
             | waiters: [],
               drain_waiters: [],
               admission_owners: %{},
               admission_monitors: %{}
           }}
        else
          {:noreply, state}
        end

      _unknown_monitor ->
        handle_admission_owner_down(state, monitor_ref, owner_pid)
    end
  end

  @impl true
  def terminate(_reason, state) do
    _ = demonitor_admission_owners(state)
    unpublish_pause(state.shard_index)
    :ok
  end

  defp maybe_reply_drained(state) do
    state = refresh_admission_monitors(state)

    if admissions_empty?(state.shard_index) do
      Enum.each(state.drain_waiters, &GenServer.reply(&1, :ok))
      %{state | drain_waiters: []}
    else
      state
    end
  end

  defp notify_admission_changed(shard_index) do
    case pause_pid(shard_index) do
      nil -> :ok
      pid -> GenServer.cast(pid, :admission_changed)
    end
  catch
    :exit, _reason -> :ok
  end

  defp sync_gate_deadline(:infinity), do: :infinity

  defp sync_gate_deadline(timeout) when is_integer(timeout) and timeout >= 0,
    do: System.monotonic_time(:millisecond) + timeout

  defp sync_gate_remaining(:infinity), do: :infinity

  defp sync_gate_remaining(deadline),
    do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp initial_pause_state(shard_index) do
    :persistent_term.put(pause_key(shard_index), self())

    %{
      shard_index: shard_index,
      waiters: [],
      drain_waiters: [],
      unscoped_holds: 0,
      leases: %{},
      owners: %{},
      monitor_owners: %{},
      admission_owners: %{},
      admission_monitors: %{}
    }
  end

  defp acquire_existing_unscoped_pause(shard_index, pid) do
    case GenServer.call(pid, :acquire_unscoped, 5_000) do
      :ok -> {:ok, pid}
      other -> {:error, {:sync_pause_acquire_failed, other}}
    end
  catch
    :exit, {:noproc, _} -> pause(shard_index)
    :exit, {:normal, _} -> pause(shard_index)
    :exit, reason -> {:error, {:sync_pause_acquire_failed, reason}}
  end

  defp acquire_existing_pause(shard_index, pid, pause_lease) do
    case GenServer.call(pid, {:acquire, pause_lease}, 5_000) do
      :ok -> {:ok, pid}
      {:error, reason} -> {:error, {:sync_pause_acquire_failed, reason}}
      other -> {:error, {:sync_pause_acquire_failed, other}}
    end
  catch
    :exit, {:noproc, _} -> pause(shard_index, pause_lease)
    :exit, {:normal, _} -> pause(shard_index, pause_lease)
    :exit, reason -> {:error, {:sync_pause_acquire_failed, reason}}
  end

  defp add_pause_lease(
         state,
         {owner_pid, lease_ref}
       )
       when is_pid(owner_pid) and is_reference(lease_ref) do
    case Map.get(state.leases, lease_ref) do
      nil ->
        {owner, monitor_owners} =
          case Map.get(state.owners, owner_pid) do
            nil ->
              monitor_ref = Process.monitor(owner_pid)

              {%{monitor_ref: monitor_ref, leases: MapSet.new()},
               Map.put(state.monitor_owners, monitor_ref, owner_pid)}

            owner ->
              {owner, state.monitor_owners}
          end

        owner = %{owner | leases: MapSet.put(owner.leases, lease_ref)}

        {:ok,
         %{
           state
           | leases: Map.put(state.leases, lease_ref, owner_pid),
             owners: Map.put(state.owners, owner_pid, owner),
             monitor_owners: monitor_owners
         }}

      ^owner_pid ->
        {:ok, state}

      _different_owner ->
        {:error, :sync_pause_lease_conflict}
    end
  end

  defp add_pause_lease(_state, _pause_lease), do: {:error, :invalid_sync_pause_lease}

  defp remove_pause_lease(state, {owner_pid, lease_ref})
       when is_pid(owner_pid) and is_reference(lease_ref) do
    case Map.get(state.leases, lease_ref) do
      ^owner_pid ->
        leases = Map.delete(state.leases, lease_ref)
        owner = Map.fetch!(state.owners, owner_pid)
        owner_leases = MapSet.delete(owner.leases, lease_ref)

        if MapSet.size(owner_leases) == 0 do
          Process.demonitor(owner.monitor_ref, [:flush])

          %{
            state
            | leases: leases,
              owners: Map.delete(state.owners, owner_pid),
              monitor_owners: Map.delete(state.monitor_owners, owner.monitor_ref)
          }
        else
          owner = %{owner | leases: owner_leases}
          %{state | leases: leases, owners: Map.put(state.owners, owner_pid, owner)}
        end

      _missing_or_different_owner ->
        state
    end
  end

  defp remove_pause_lease(state, _pause_lease), do: state

  defp remove_owner_leases(state, owner_pid, demonitor?) do
    case Map.get(state.owners, owner_pid) do
      nil ->
        state

      owner ->
        if demonitor?, do: Process.demonitor(owner.monitor_ref, [:flush])

        leases = Enum.reduce(owner.leases, state.leases, &Map.delete(&2, &1))

        %{
          state
          | leases: leases,
            owners: Map.delete(state.owners, owner_pid),
            monitor_owners: Map.delete(state.monitor_owners, owner.monitor_ref)
        }
    end
  end

  defp demonitor_pause_owners(state) do
    Enum.each(state.owners, fn {_owner_pid, owner} ->
      Process.demonitor(owner.monitor_ref, [:flush])
    end)

    %{state | unscoped_holds: 0, leases: %{}, owners: %{}, monitor_owners: %{}}
  end

  defp handle_admission_owner_down(state, monitor_ref, owner_pid) do
    case Map.get(state.admission_monitors, monitor_ref) do
      ^owner_pid ->
        delete_owner_admissions(state.shard_index, owner_pid)

        state =
          %{
            state
            | admission_owners: Map.delete(state.admission_owners, owner_pid),
              admission_monitors: Map.delete(state.admission_monitors, monitor_ref)
          }

        {:noreply, maybe_reply_drained(state)}

      _unknown_monitor ->
        {:noreply, state}
    end
  end

  defp refresh_admission_monitors(state) do
    owners =
      state.shard_index
      |> live_admission_owners()
      |> MapSet.new()

    state =
      Enum.reduce(state.admission_owners, state, fn {owner_pid, monitor_ref}, acc ->
        if MapSet.member?(owners, owner_pid) do
          acc
        else
          Process.demonitor(monitor_ref, [:flush])

          %{
            acc
            | admission_owners: Map.delete(acc.admission_owners, owner_pid),
              admission_monitors: Map.delete(acc.admission_monitors, monitor_ref)
          }
        end
      end)

    Enum.reduce(owners, state, fn owner_pid, acc ->
      if Map.has_key?(acc.admission_owners, owner_pid) do
        acc
      else
        monitor_ref = Process.monitor(owner_pid)

        %{
          acc
          | admission_owners: Map.put(acc.admission_owners, owner_pid, monitor_ref),
            admission_monitors: Map.put(acc.admission_monitors, monitor_ref, owner_pid)
        }
      end
    end)
  end

  defp demonitor_admission_owners(state) do
    Enum.each(state.admission_owners, fn {_owner_pid, monitor_ref} ->
      Process.demonitor(monitor_ref, [:flush])
    end)

    %{state | admission_owners: %{}, admission_monitors: %{}}
  end

  defp live_admission_owners(shard_index) do
    shard_index
    |> shard_admissions()
    |> Enum.reduce(MapSet.new(), fn {key, owner_pid}, owners ->
      if Process.alive?(owner_pid) do
        MapSet.put(owners, owner_pid)
      else
        delete_admission(key)
        owners
      end
    end)
  end

  defp admissions_empty?(shard_index) do
    shard_admissions(shard_index) == []
  end

  defp insert_admission(shard_index, admission_ref, owner_pid) do
    with_admission_table(fn table ->
      :ets.insert_new(table, {{shard_index, admission_ref}, owner_pid})
    end)
  end

  defp update_admission_owner(shard_index, admission_ref, owner_pid) do
    with_admission_table(fn table ->
      :ets.update_element(table, {shard_index, admission_ref}, {2, owner_pid})
    end)
  end

  defp take_admission(shard_index, admission_ref) do
    with_admission_table(fn table ->
      :ets.take(table, {shard_index, admission_ref})
    end)
  end

  defp shard_admissions(shard_index) do
    with_admission_table(fn table ->
      :ets.match_object(table, {{shard_index, :_}, :_})
    end)
  end

  defp delete_owner_admissions(shard_index, owner_pid) do
    with_admission_table(fn table ->
      :ets.match_delete(table, {{shard_index, :_}, owner_pid})
    end)
  end

  defp delete_shard_admissions(shard_index) do
    case :ets.whereis(TableOwner.table()) do
      :undefined -> :ok
      table -> :ets.match_delete(table, {{shard_index, :_}, :_})
    end
  end

  defp delete_admission(key) do
    with_admission_table(fn table ->
      :ets.delete(table, key)
    end)
  end

  defp with_admission_table(fun) when is_function(fun, 1) do
    table = TableOwner.table()

    try do
      fun.(table)
    rescue
      ArgumentError ->
        case TableOwner.ensure_table() do
          :ok -> fun.(table)
          {:error, reason} -> :erlang.error({:sync_admission_table_unavailable, reason})
        end
    end
  end

  defp pause_pid(shard_index) do
    case :persistent_term.get(pause_key(shard_index), nil) do
      pid when is_pid(pid) ->
        if Process.alive?(pid) do
          pid
        else
          unpublish_pause(shard_index)
          nil
        end

      _other ->
        nil
    end
  end

  defp unpublish_pause(shard_index) do
    case :persistent_term.get(pause_key(shard_index), nil) do
      pid when pid == self() -> :persistent_term.erase(pause_key(shard_index))
      _other -> :ok
    end
  catch
    :error, :badarg -> :ok
  end

  defp pause_key(shard_index), do: {@pause_key, shard_index}
end
