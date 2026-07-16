defmodule Ferricstore.Raft.WARaftBackend.SyncGate do
  @moduledoc false

  use GenServer

  @active_index 1
  @state_key {__MODULE__, :state}
  @pause_key {__MODULE__, :pause}

  @type token :: {non_neg_integer(), :atomics.atomics_ref()}

  @spec init_shards(pos_integer()) :: :ok
  def init_shards(shard_count) when is_integer(shard_count) and shard_count > 0 do
    Enum.each(0..(shard_count - 1), &ensure_state/1)
    :ok
  end

  def init_shards(_shard_count), do: :ok

  @spec clear_shards(non_neg_integer()) :: :ok
  def clear_shards(shard_count) when is_integer(shard_count) and shard_count > 0 do
    Enum.each(0..(shard_count - 1), fn shard_index ->
      _ = force_resume(shard_index, 5_000)
      :persistent_term.erase(state_key(shard_index))
      :persistent_term.erase(pause_key(shard_index))
    end)

    :ok
  end

  def clear_shards(_shard_count), do: :ok

  @spec enter(non_neg_integer()) :: {:ok, token()} | {:error, term()}
  def enter(shard_index) when is_integer(shard_index) and shard_index >= 0 do
    counters = ensure_state(shard_index)
    :atomics.add(counters, @active_index, 1)

    case pause_pid(shard_index) do
      nil ->
        {:ok, {shard_index, counters}}

      pid ->
        leave({shard_index, counters})

        case await(pid, :infinity) do
          :ok -> enter(shard_index)
          {:error, _reason} = error -> error
        end
    end
  catch
    :error, reason -> {:error, {:sync_gate_enter_failed, reason}}
  end

  def enter(shard_index), do: {:error, {:invalid_shard_index, shard_index}}

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
  def leave({shard_index, counters})
      when is_integer(shard_index) and shard_index >= 0 do
    case :atomics.sub_get(counters, @active_index, 1) do
      0 -> notify_drained(shard_index)
      active when is_integer(active) and active > 0 -> :ok
    end
  catch
    :error, _reason -> :ok
  end

  def leave(_token), do: :ok

  defp do_enter_many([]), do: {:ok, []}

  defp do_enter_many(shard_indexes) do
    with {:ok, tokens} <- claim_many(shard_indexes) do
      case active_pauses(tokens) do
        [] ->
          {:ok, tokens}

        pauses ->
          Enum.each(tokens, &leave/1)

          case await_pauses(pauses) do
            :ok -> do_enter_many(shard_indexes)
            {:error, _reason} = error -> error
          end
      end
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
    counters = ensure_state(shard_index)
    :atomics.add(counters, @active_index, 1)
    {:ok, {shard_index, counters}}
  catch
    :error, reason -> {:error, {:sync_gate_enter_failed, reason}}
  end

  defp active_pauses(tokens) do
    Enum.flat_map(tokens, fn {shard_index, _counters} ->
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
    case GenServer.start_link(__MODULE__, shard_index, name: name(shard_index)) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> acquire_existing_pause(shard_index, pid)
      {:error, reason} -> {:error, reason}
    end
  end

  def pause(shard_index), do: {:error, {:invalid_shard_index, shard_index}}

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
    :exit, {:noproc, _} -> :ok
    :exit, {:normal, _} -> :ok
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
  def init(shard_index) do
    counters = ensure_state(shard_index)
    :persistent_term.put(pause_key(shard_index), self())

    {:ok,
     %{shard_index: shard_index, counters: counters, waiters: [], drain_waiters: [], holds: 1}}
  end

  @impl true
  def handle_call(:acquire, _from, state) do
    {:reply, :ok, %{state | holds: state.holds + 1}}
  end

  def handle_call(:await, from, state) do
    {:noreply, %{state | waiters: [from | state.waiters]}}
  end

  def handle_call(:await_drained, from, state) do
    if active_count(state.counters) == 0 do
      {:reply, :ok, state}
    else
      {:noreply, %{state | drain_waiters: [from | state.drain_waiters]}}
    end
  end

  def handle_call(:resume, _from, %{holds: holds} = state) when holds > 1 do
    {:reply, :ok, %{state | holds: holds - 1}}
  end

  def handle_call(:resume, _from, state) do
    release_pause(state)
  end

  def handle_call(:force_resume, _from, state) do
    release_pause(state)
  end

  defp release_pause(state) do
    unpublish_pause(state.shard_index)
    Enum.each(state.waiters, &GenServer.reply(&1, :ok))
    {:stop, :normal, :ok, %{state | waiters: []}}
  end

  @impl true
  def handle_cast(:drained, state) do
    state = maybe_reply_drained(state)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    unpublish_pause(state.shard_index)
    :ok
  end

  defp maybe_reply_drained(state) do
    if active_count(state.counters) == 0 do
      Enum.each(state.drain_waiters, &GenServer.reply(&1, :ok))
      %{state | drain_waiters: []}
    else
      state
    end
  end

  defp notify_drained(shard_index) do
    case pause_pid(shard_index) do
      nil -> :ok
      pid -> GenServer.cast(pid, :drained)
    end
  catch
    :exit, _reason -> :ok
  end

  defp active_count(counters), do: :atomics.get(counters, @active_index)

  defp sync_gate_deadline(:infinity), do: :infinity

  defp sync_gate_deadline(timeout) when is_integer(timeout) and timeout >= 0,
    do: System.monotonic_time(:millisecond) + timeout

  defp sync_gate_remaining(:infinity), do: :infinity

  defp sync_gate_remaining(deadline),
    do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp ensure_state(shard_index) do
    case :persistent_term.get(state_key(shard_index), nil) do
      nil ->
        initialize_state(shard_index)

      counters ->
        counters
    end
  end

  defp initialize_state(shard_index) do
    :global.trans(
      {{__MODULE__, {:state, shard_index}}, self()},
      fn ->
        case :persistent_term.get(state_key(shard_index), nil) do
          nil ->
            counters = :atomics.new(1, signed: false)
            :persistent_term.put(state_key(shard_index), counters)
            counters

          counters ->
            counters
        end
      end,
      [node()]
    )
  end

  defp acquire_existing_pause(shard_index, pid) do
    case GenServer.call(pid, :acquire, 5_000) do
      :ok -> {:ok, pid}
      other -> {:error, {:sync_pause_acquire_failed, other}}
    end
  catch
    :exit, {:noproc, _} -> pause(shard_index)
    :exit, {:normal, _} -> pause(shard_index)
    :exit, reason -> {:error, {:sync_pause_acquire_failed, reason}}
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

  defp state_key(shard_index), do: {@state_key, shard_index}
  defp pause_key(shard_index), do: {@pause_key, shard_index}
end
