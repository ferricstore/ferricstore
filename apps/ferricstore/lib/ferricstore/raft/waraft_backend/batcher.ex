defmodule Ferricstore.Raft.WARaftBackend.Batcher do
  @moduledoc """
  Namespace-window batcher for the WARaft replacement backend.

  The normal WARaft write path stays direct. This process is used only when a
  namespace explicitly configures a `window_ms` larger than the default, so the
  common SET/DEL hot path does not pay a GenServer hop just to discover there is
  nothing to coalesce.
  """

  use GenServer

  alias Ferricstore.Raft.WARaftBackend.SyncGate
  alias Ferricstore.Raft.WARaftBackend.Batcher.Telemetry

  @call_timeout 30_000
  @default_max_batch_size 10_000
  @default_hot_batch_window_ms 1

  defstruct [
    :shard_index,
    :max_batch_size,
    :hot_batch_window_ms,
    :hot_max_batch_size,
    slots: %{},
    batch_slot: nil,
    put_slot: nil,
    delete_slot: nil,
    in_flight: %{},
    flush_waiters: [],
    stop_waiters: [],
    stopping?: false
  ]

  @type slot :: %{
          commands: [tuple()],
          froms: [GenServer.from()],
          count: non_neg_integer(),
          timer_ref: reference() | nil,
          timer_token: reference() | nil,
          window_ms: pos_integer(),
          created_mono: integer()
        }

  @type hot_slot :: %{
          groups: [{GenServer.from(), [term()]}],
          count: non_neg_integer(),
          timer_ref: reference() | nil,
          timer_token: reference() | nil,
          window_ms: non_neg_integer(),
          created_mono: integer()
        }

  @spec start_link(non_neg_integer(), keyword()) :: GenServer.on_start()
  def start_link(shard_index, opts \\ []) when is_integer(shard_index) and shard_index >= 0 do
    GenServer.start_link(__MODULE__, {shard_index, opts}, name: name(shard_index))
  end

  @spec stop(non_neg_integer()) :: :ok
  def stop(shard_index) do
    case Process.whereis(name(shard_index)) do
      nil -> :ok
      _pid -> GenServer.call(name(shard_index), :stop, @call_timeout)
    end
  catch
    :exit, _reason -> :ok
  end

  @spec flush(non_neg_integer(), timeout()) :: :ok | {:error, term()}
  def flush(shard_index, timeout \\ @call_timeout) do
    case Process.whereis(name(shard_index)) do
      nil -> :ok
      _pid -> GenServer.call(name(shard_index), :flush, timeout)
    end
  catch
    :exit, reason -> {:error, reason}
  end

  @spec write(non_neg_integer(), binary(), tuple(), pos_integer()) :: term()
  def write(shard_index, prefix, command, window_ms)
      when is_integer(shard_index) and is_binary(prefix) and is_tuple(command) and
             is_integer(window_ms) and window_ms > 0 do
    case Process.whereis(name(shard_index)) do
      nil ->
        commit_single_direct(shard_index, command)

      _pid ->
        GenServer.call(name(shard_index), {:write, prefix, command, window_ms}, @call_timeout)
    end
  catch
    :exit, {:noproc, _} -> commit_single_direct(shard_index, command)
    :exit, {:normal, _} -> commit_single_direct(shard_index, command)
  end

  @spec write_batch(non_neg_integer(), [tuple()]) :: term()
  def write_batch(shard_index, commands)
      when is_integer(shard_index) and is_list(commands) do
    case generic_batch_window_ms() do
      window_ms when window_ms > 0 ->
        call_or_commit_batch_direct(shard_index, commands, window_ms)

      _disabled ->
        if generic_batch_during_flush?() do
          call_or_commit_batch_direct(shard_index, commands, 0)
        else
          backend_call(:__commit_batch_direct__, [shard_index, commands])
        end
    end
  end

  @spec write_put_batch(non_neg_integer(), [{binary(), binary(), non_neg_integer()}]) :: term()
  def write_put_batch(shard_index, entries)
      when is_integer(shard_index) and is_list(entries) do
    case hot_batch_window_ms() do
      window_ms when window_ms > 0 ->
        call_or_commit_put_batch_direct(shard_index, entries, window_ms)

      _disabled ->
        backend_call(:__commit_put_batch_direct__, [shard_index, entries])
    end
  end

  @spec write_put_batch_async(
          non_neg_integer(),
          [{binary(), binary(), non_neg_integer()}],
          GenServer.from(),
          SyncGate.token() | nil
        ) :: :ok | {:direct, term()}
  def write_put_batch_async(shard_index, entries, from, sync_token \\ nil)
      when is_integer(shard_index) and is_list(entries) do
    case hot_batch_window_ms() do
      window_ms when window_ms > 0 ->
        cast_or_commit_put_batch_direct(shard_index, entries, window_ms, from, sync_token)

      _disabled ->
        {:direct, backend_call(:__commit_put_batch_direct__, [shard_index, entries])}
    end
  end

  @spec write_delete_batch(non_neg_integer(), [binary()]) :: term()
  def write_delete_batch(shard_index, keys) when is_integer(shard_index) and is_list(keys) do
    case hot_batch_window_ms() do
      window_ms when window_ms > 0 ->
        call_or_commit_delete_batch_direct(shard_index, keys, window_ms)

      _disabled ->
        backend_call(:__commit_delete_batch_direct__, [shard_index, keys])
    end
  end

  @spec write_delete_batch_async(
          non_neg_integer(),
          [binary()],
          GenServer.from(),
          SyncGate.token() | nil
        ) ::
          :ok | {:direct, term()}
  def write_delete_batch_async(shard_index, keys, from, sync_token \\ nil)
      when is_integer(shard_index) and is_list(keys) do
    case hot_batch_window_ms() do
      window_ms when window_ms > 0 ->
        cast_or_commit_delete_batch_direct(shard_index, keys, window_ms, from, sync_token)

      _disabled ->
        {:direct, backend_call(:__commit_delete_batch_direct__, [shard_index, keys])}
    end
  end

  @spec name(non_neg_integer()) :: atom()
  def name(shard_index), do: :"ferricstore_waraft_backend_batcher_#{shard_index}"

  @impl true
  def init({shard_index, opts}) do
    {:ok,
     %__MODULE__{
       shard_index: shard_index,
       max_batch_size: Keyword.get(opts, :namespace_batch_max, @default_max_batch_size),
       hot_batch_window_ms: Keyword.get(opts, :hot_batch_window_ms, hot_batch_window_ms()),
       hot_max_batch_size: Keyword.get(opts, :hot_batch_max, @default_max_batch_size)
     }}
  end

  @impl true
  def handle_call({:write, _prefix, _command, _window_ms}, _from, %{stopping?: true} = state),
    do: {:reply, {:error, :shutting_down}, state}

  def handle_call({:write, prefix, command, window_ms}, from, state) do
    slot = Map.get(state.slots, prefix, new_slot(window_ms))

    slot =
      if slot.timer_ref == nil do
        token = make_ref()
        ref = Process.send_after(self(), {:flush, prefix, token}, window_ms)
        %{slot | timer_ref: ref, timer_token: token, window_ms: window_ms}
      else
        slot
      end

    slot = %{
      slot
      | commands: [command | slot.commands],
        froms: [from | slot.froms],
        count: slot.count + 1
    }

    state = %{state | slots: Map.put(state.slots, prefix, slot)}

    if slot.count >= state.max_batch_size and not in_flight?(state, {:prefix, prefix}) do
      {:noreply, flush_slot(state, prefix)}
    else
      {:noreply, state}
    end
  end

  def handle_call({:write_batch, _commands, _window_ms}, _from, %{stopping?: true} = state),
    do: {:reply, {:error, :shutting_down}, state}

  def handle_call({:write_batch, commands, window_ms}, from, state) do
    state = enqueue_hot_batch(state, from, commands, window_ms)

    cond do
      in_flight?(state, :batch) ->
        {:noreply, state}

      window_ms == 0 or state.batch_slot.count >= state.hot_max_batch_size ->
        {:noreply, flush_hot_batch_slot(state)}

      true ->
        {:noreply, state}
    end
  end

  def handle_call({:write_put_batch, _entries, _window_ms}, _from, %{stopping?: true} = state),
    do: {:reply, {:error, :shutting_down}, state}

  def handle_call({:write_put_batch, entries, window_ms}, from, state) do
    state = enqueue_hot_put_batch(state, from, entries, window_ms)

    if state.put_slot.count >= state.hot_max_batch_size and not in_flight?(state, :put_batch) do
      {:noreply, flush_hot_put_slot(state)}
    else
      {:noreply, state}
    end
  end

  def handle_call({:write_delete_batch, _keys, _window_ms}, _from, %{stopping?: true} = state),
    do: {:reply, {:error, :shutting_down}, state}

  def handle_call({:write_delete_batch, keys, window_ms}, from, state) do
    state = enqueue_hot_delete_batch(state, from, keys, window_ms)

    if state.delete_slot.count >= state.hot_max_batch_size and
         not in_flight?(state, :delete_batch) do
      {:noreply, flush_hot_delete_slot(state)}
    else
      {:noreply, state}
    end
  end

  def handle_call(:stop, from, state) do
    state = flush_all_slots(state, :sync)
    state = %{state | stopping?: true, stop_waiters: [from | state.stop_waiters]}
    maybe_finish_control_waiters(state)
  end

  def handle_call(:flush, from, state) do
    state = flush_all_slots(state, :sync)
    state = %{state | flush_waiters: [from | state.flush_waiters]}
    maybe_finish_control_waiters(state)
  end

  @impl true
  def handle_cast(
        {:write_put_batch, _entries, _window_ms, from, sync_token},
        %{stopping?: true} = state
      ) do
    try do
      GenServer.reply(from, {:error, :shutting_down})
      {:noreply, state}
    after
      release_sync_token(sync_token)
    end
  end

  def handle_cast({:write_put_batch, entries, window_ms, from, sync_token}, state) do
    try do
      state = enqueue_hot_put_batch(state, from, entries, window_ms)

      if state.put_slot.count >= state.hot_max_batch_size and not in_flight?(state, :put_batch) do
        {:noreply, flush_hot_put_slot(state)}
      else
        {:noreply, state}
      end
    after
      release_sync_token(sync_token)
    end
  end

  def handle_cast(
        {:write_delete_batch, _keys, _window_ms, from, sync_token},
        %{stopping?: true} = state
      ) do
    try do
      GenServer.reply(from, {:error, :shutting_down})
      {:noreply, state}
    after
      release_sync_token(sync_token)
    end
  end

  def handle_cast({:write_delete_batch, keys, window_ms, from, sync_token}, state) do
    try do
      state = enqueue_hot_delete_batch(state, from, keys, window_ms)

      if state.delete_slot.count >= state.hot_max_batch_size and
           not in_flight?(state, :delete_batch) do
        {:noreply, flush_hot_delete_slot(state)}
      else
        {:noreply, state}
      end
    after
      release_sync_token(sync_token)
    end
  end

  @impl true
  def handle_info({:flush, prefix, token}, state) do
    case Map.get(state.slots, prefix) do
      %{timer_token: ^token} ->
        if in_flight?(state, {:prefix, prefix}) do
          {:noreply, state}
        else
          {:noreply, flush_slot(state, prefix)}
        end

      _stale_or_missing ->
        {:noreply, state}
    end
  end

  def handle_info({:flush_hot_batch, token}, state) do
    case state.batch_slot do
      %{timer_token: ^token} ->
        if in_flight?(state, :batch) do
          {:noreply, state}
        else
          {:noreply, flush_hot_batch_slot(state)}
        end

      _stale_or_missing ->
        {:noreply, state}
    end
  end

  def handle_info({:flush_hot_put_batch, token}, state) do
    case state.put_slot do
      %{timer_token: ^token} ->
        if in_flight?(state, :put_batch) do
          {:noreply, state}
        else
          {:noreply, flush_hot_put_slot(state)}
        end

      _stale_or_missing ->
        {:noreply, state}
    end
  end

  def handle_info({:flush_hot_delete_batch, token}, state) do
    case state.delete_slot do
      %{timer_token: ^token} ->
        if in_flight?(state, :delete_batch) do
          {:noreply, state}
        else
          {:noreply, flush_hot_delete_slot(state)}
        end

      _stale_or_missing ->
        {:noreply, state}
    end
  end

  def handle_info({:async_flush_done, kind, ref}, state) do
    case state.in_flight do
      %{^kind => ^ref} ->
        state = %{state | in_flight: Map.delete(state.in_flight, kind)}
        state = flush_after_inflight(kind, state)
        maybe_finish_control_waiters(state)

      _stale ->
        {:noreply, state}
    end
  end

  def handle_info({:flush, _prefix}, state), do: {:noreply, state}
  def handle_info(:flush_hot_batch, state), do: {:noreply, state}
  def handle_info(:flush_hot_put_batch, state), do: {:noreply, state}
  def handle_info(:flush_hot_delete_batch, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, _state) do
    drain_unhandled_async_sync_tokens()
    :ok
  end

  defp call_or_commit_batch_direct(shard_index, commands, window_ms) do
    case Process.whereis(name(shard_index)) do
      nil ->
        backend_call(:__commit_batch_direct__, [shard_index, commands])

      _pid ->
        GenServer.call(name(shard_index), {:write_batch, commands, window_ms}, @call_timeout)
    end
  catch
    :exit, {:noproc, _} -> backend_call(:__commit_batch_direct__, [shard_index, commands])
    :exit, {:normal, _} -> backend_call(:__commit_batch_direct__, [shard_index, commands])
  end

  defp call_or_commit_put_batch_direct(shard_index, entries, window_ms) do
    case Process.whereis(name(shard_index)) do
      nil ->
        backend_call(:__commit_put_batch_direct__, [shard_index, entries])

      _pid ->
        GenServer.call(name(shard_index), {:write_put_batch, entries, window_ms}, @call_timeout)
    end
  catch
    :exit, {:noproc, _} -> backend_call(:__commit_put_batch_direct__, [shard_index, entries])
    :exit, {:normal, _} -> backend_call(:__commit_put_batch_direct__, [shard_index, entries])
  end

  defp cast_or_commit_put_batch_direct(shard_index, entries, window_ms, from, sync_token) do
    case Process.whereis(name(shard_index)) do
      nil ->
        {:direct, backend_call(:__commit_put_batch_direct__, [shard_index, entries])}

      pid ->
        cast_hot_batch(pid, {:write_put_batch, entries, window_ms, from, sync_token})
        :ok
    end
  catch
    :exit, {:noproc, _} ->
      {:direct, backend_call(:__commit_put_batch_direct__, [shard_index, entries])}

    :exit, {:normal, _} ->
      {:direct, backend_call(:__commit_put_batch_direct__, [shard_index, entries])}
  end

  defp call_or_commit_delete_batch_direct(shard_index, keys, window_ms) do
    case Process.whereis(name(shard_index)) do
      nil ->
        backend_call(:__commit_delete_batch_direct__, [shard_index, keys])

      _pid ->
        GenServer.call(name(shard_index), {:write_delete_batch, keys, window_ms}, @call_timeout)
    end
  catch
    :exit, {:noproc, _} -> backend_call(:__commit_delete_batch_direct__, [shard_index, keys])
    :exit, {:normal, _} -> backend_call(:__commit_delete_batch_direct__, [shard_index, keys])
  end

  defp cast_or_commit_delete_batch_direct(shard_index, keys, window_ms, from, sync_token) do
    case Process.whereis(name(shard_index)) do
      nil ->
        {:direct, backend_call(:__commit_delete_batch_direct__, [shard_index, keys])}

      pid ->
        cast_hot_batch(pid, {:write_delete_batch, keys, window_ms, from, sync_token})
        :ok
    end
  catch
    :exit, {:noproc, _} ->
      {:direct, backend_call(:__commit_delete_batch_direct__, [shard_index, keys])}

    :exit, {:normal, _} ->
      {:direct, backend_call(:__commit_delete_batch_direct__, [shard_index, keys])}
  end

  defp cast_hot_batch(pid, message) do
    case Application.get_env(:ferricstore, :waraft_backend_batcher_cast_hook) do
      {:defer, notify} when is_pid(notify) ->
        ref = make_ref()

        {:ok, _pid} =
          Task.start(fn ->
            send(notify, {:waraft_backend_batcher_cast_deferred, ref, self()})

            receive do
              {^ref, :continue} -> maybe_cast_hot_batch(pid, message)
            after
              @call_timeout -> handle_lost_hot_batch(message)
            end
          end)

        :ok

      _ ->
        maybe_cast_hot_batch(pid, message)
    end
  end

  defp maybe_cast_hot_batch(pid, message) do
    if Process.alive?(pid) do
      GenServer.cast(pid, message)
    else
      handle_lost_hot_batch(message)
    end
  end

  defp handle_lost_hot_batch({:write_put_batch, _entries, _window_ms, from, sync_token}) do
    reply_unhandled_async_batch(from)
    release_sync_token(sync_token)
  end

  defp handle_lost_hot_batch({:write_delete_batch, _keys, _window_ms, from, sync_token}) do
    reply_unhandled_async_batch(from)
    release_sync_token(sync_token)
  end

  defp release_sync_token(nil), do: :ok
  defp release_sync_token(token), do: SyncGate.leave(token)

  defp drain_unhandled_async_sync_tokens do
    receive do
      {:"$gen_cast", {:write_put_batch, _entries, _window_ms, from, sync_token}} ->
        reply_unhandled_async_batch(from)
        release_sync_token(sync_token)
        drain_unhandled_async_sync_tokens()

      {:"$gen_cast", {:write_delete_batch, _keys, _window_ms, from, sync_token}} ->
        reply_unhandled_async_batch(from)
        release_sync_token(sync_token)
        drain_unhandled_async_sync_tokens()
    after
      0 -> :ok
    end
  end

  defp reply_unhandled_async_batch(from) do
    GenServer.reply(from, {:error, :shutting_down})
  catch
    _kind, _reason -> :ok
  end

  defp new_slot(window_ms) do
    %{
      commands: [],
      froms: [],
      count: 0,
      timer_ref: nil,
      timer_token: nil,
      window_ms: window_ms,
      created_mono: System.monotonic_time()
    }
  end

  defp flush_all_slots(state, mode) do
    state =
      state.slots
      |> Map.keys()
      |> Enum.reduce(state, fn prefix, acc -> flush_slot(acc, prefix, mode) end)

    state
    |> flush_hot_batch_slot(mode)
    |> flush_hot_put_slot(mode)
    |> flush_hot_delete_slot(mode)
  end

  defp flush_slot(state, prefix, mode \\ :async) do
    kind = {:prefix, prefix}

    case Map.pop(state.slots, prefix) do
      {nil, _slots} ->
        state

      {slot, slots} ->
        if in_flight?(state, kind) do
          state
        else
          cancel_timer(slot.timer_ref)

          commands = Enum.reverse(slot.commands)
          froms = Enum.reverse(slot.froms)

          flush_fun = fn ->
            flush_started = System.monotonic_time()
            {function, args} = compact_slot_commit(state.shard_index, commands)
            result = safe_backend_call(function, args)
            flush_finished = System.monotonic_time()
            replies = replies_for_batch(result, length(commands))

            Enum.zip(froms, replies)
            |> Enum.each(fn {from, reply} -> GenServer.reply(from, reply) end)

            Telemetry.emit_flush_telemetry(
              state,
              prefix,
              slot,
              result,
              flush_started,
              flush_finished
            )
          end

          state
          |> Map.put(:slots, slots)
          |> run_flush(mode, kind, flush_fun)
        end
    end
  end

  defp enqueue_hot_batch(state, from, commands, window_ms) do
    slot = state.batch_slot || new_hot_slot(window_ms, :flush_hot_batch)

    %{
      state
      | batch_slot: %{
          slot
          | groups: [{from, commands} | slot.groups],
            count: slot.count + hot_batch_reply_count(commands)
        }
    }
  end

  defp enqueue_hot_put_batch(state, from, entries, window_ms) do
    slot = state.put_slot || new_hot_slot(window_ms, :flush_hot_put_batch)

    %{
      state
      | put_slot: %{
          slot
          | groups: [{from, entries} | slot.groups],
            count: slot.count + length(entries)
        }
    }
  end

  defp enqueue_hot_delete_batch(state, from, keys, window_ms) do
    slot = state.delete_slot || new_hot_slot(window_ms, :flush_hot_delete_batch)

    %{
      state
      | delete_slot: %{
          slot
          | groups: [{from, keys} | slot.groups],
            count: slot.count + length(keys)
        }
    }
  end

  defp new_hot_slot(window_ms, flush_message) do
    token = make_ref()

    timer_ref =
      if window_ms > 0 do
        Process.send_after(self(), {flush_message, token}, window_ms)
      end

    %{
      groups: [],
      count: 0,
      timer_ref: timer_ref,
      timer_token: token,
      window_ms: window_ms,
      created_mono: System.monotonic_time()
    }
  end

  defp flush_hot_batch_slot(state, mode \\ :async)
  defp flush_hot_batch_slot(%{batch_slot: nil} = state, _mode), do: state

  defp flush_hot_batch_slot(%{batch_slot: slot} = state, mode) do
    if in_flight?(state, :batch) do
      state
    else
      cancel_timer(slot.timer_ref)
      groups = Enum.reverse(slot.groups)
      commands = hot_batch_items(groups)

      flush_fun = fn ->
        flush_started = System.monotonic_time()
        result = safe_backend_call(:__commit_batch_direct__, [state.shard_index, commands])
        flush_finished = System.monotonic_time()
        reply_hot_batch_groups(groups, result)

        Telemetry.emit_hot_flush_telemetry(
          state,
          :batch,
          slot,
          result,
          flush_started,
          flush_finished
        )
      end

      %{state | batch_slot: nil}
      |> run_flush(mode, :batch, flush_fun)
    end
  end

  defp flush_hot_put_slot(state, mode \\ :async)
  defp flush_hot_put_slot(%{put_slot: nil} = state, _mode), do: state

  defp flush_hot_put_slot(%{put_slot: slot} = state, mode) do
    if in_flight?(state, :put_batch) do
      state
    else
      cancel_timer(slot.timer_ref)
      groups = Enum.reverse(slot.groups)
      entries = hot_batch_items(groups)

      flush_fun = fn ->
        flush_started = System.monotonic_time()
        result = safe_backend_call(:__commit_put_batch_direct__, [state.shard_index, entries])
        flush_finished = System.monotonic_time()
        reply_hot_batch_groups(groups, result)

        Telemetry.emit_hot_flush_telemetry(
          state,
          :put_batch,
          slot,
          result,
          flush_started,
          flush_finished
        )
      end

      %{state | put_slot: nil}
      |> run_flush(mode, :put_batch, flush_fun)
    end
  end

  defp flush_hot_delete_slot(state, mode \\ :async)
  defp flush_hot_delete_slot(%{delete_slot: nil} = state, _mode), do: state

  defp flush_hot_delete_slot(%{delete_slot: slot} = state, mode) do
    if in_flight?(state, :delete_batch) do
      state
    else
      cancel_timer(slot.timer_ref)
      groups = Enum.reverse(slot.groups)
      keys = hot_batch_items(groups)

      flush_fun = fn ->
        flush_started = System.monotonic_time()
        result = safe_backend_call(:__commit_delete_batch_direct__, [state.shard_index, keys])
        flush_finished = System.monotonic_time()
        reply_hot_batch_groups(groups, result)

        Telemetry.emit_hot_flush_telemetry(
          state,
          :delete_batch,
          slot,
          result,
          flush_started,
          flush_finished
        )
      end

      %{state | delete_slot: nil}
      |> run_flush(mode, :delete_batch, flush_fun)
    end
  end

  defp run_flush(state, :sync, _kind, fun) do
    fun.()
    state
  end

  defp run_flush(state, :async, kind, fun) do
    owner = self()
    ref = make_ref()

    {:ok, _pid} =
      Task.start(fn ->
        try do
          fun.()
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        after
          send(owner, {:async_flush_done, kind, ref})
        end
      end)

    %{state | in_flight: Map.put(state.in_flight, kind, ref)}
  end

  defp in_flight?(state, kind), do: Map.has_key?(state.in_flight, kind)

  defp flush_after_inflight({:prefix, prefix}, state), do: flush_slot(state, prefix)
  defp flush_after_inflight(:batch, state), do: flush_hot_batch_slot(state)
  defp flush_after_inflight(:put_batch, state), do: flush_hot_put_slot(state)
  defp flush_after_inflight(:delete_batch, state), do: flush_hot_delete_slot(state)

  defp maybe_finish_control_waiters(state) do
    if batcher_idle?(state) do
      Enum.each(state.flush_waiters, &GenServer.reply(&1, :ok))

      if state.stopping? do
        Enum.each(state.stop_waiters, &GenServer.reply(&1, :ok))

        {:stop, :normal, %{state | flush_waiters: [], stop_waiters: [], stopping?: false}}
      else
        {:noreply, %{state | flush_waiters: []}}
      end
    else
      {:noreply, state}
    end
  end

  defp batcher_idle?(state) do
    map_size(state.slots) == 0 and is_nil(state.batch_slot) and is_nil(state.put_slot) and
      is_nil(state.delete_slot) and map_size(state.in_flight) == 0
  end

  defp hot_batch_items(groups) do
    Enum.flat_map(groups, fn {_from, items} -> items end)
  end

  defp reply_hot_batch_groups(groups, {:ok, replies}) when is_list(replies) do
    expected = total_hot_batch_replies(groups)

    if length(replies) == expected do
      {group_replies, []} =
        Enum.map_reduce(groups, replies, fn {from, items}, remaining ->
          {reply, rest} = Enum.split(remaining, hot_batch_reply_count(items))
          {{from, {:ok, reply}}, rest}
        end)

      Enum.each(group_replies, fn {from, reply} -> GenServer.reply(from, reply) end)
    else
      error = {:error, {:batch_result_mismatch, expected, length(replies)}}
      Enum.each(groups, fn {from, _items} -> GenServer.reply(from, error) end)
    end
  end

  defp reply_hot_batch_groups(groups, result) do
    Enum.each(groups, fn {from, _items} -> GenServer.reply(from, result) end)
  end

  defp total_hot_batch_replies(groups) do
    Enum.reduce(groups, 0, fn {_from, items}, acc -> acc + hot_batch_reply_count(items) end)
  end

  defp hot_batch_reply_count(items) when is_list(items) do
    Enum.reduce(items, 0, fn item, acc -> acc + hot_batch_item_reply_count(item) end)
  end

  defp hot_batch_item_reply_count({:put_batch, entries}) when is_list(entries),
    do: length(entries)

  defp hot_batch_item_reply_count({:delete_batch, keys}) when is_list(keys), do: length(keys)

  defp hot_batch_item_reply_count({:batch, commands}) when is_list(commands),
    do: hot_batch_reply_count(commands)

  defp hot_batch_item_reply_count(_command), do: 1

  defp cancel_timer(nil), do: :ok

  defp cancel_timer(ref) do
    _ = Process.cancel_timer(ref)
    :ok
  end

  defp replies_for_batch({:ok, replies}, expected) when is_list(replies) do
    if length(replies) == expected do
      replies
    else
      List.duplicate({:error, {:batch_result_mismatch, expected, length(replies)}}, expected)
    end
  end

  defp replies_for_batch(result, expected), do: List.duplicate(result, expected)

  defp compact_slot_commit(shard_index, [{:put, key, value, expire_at_ms} | rest] = commands)
       when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) do
    case compact_put_entries(rest, [{key, value, expire_at_ms}]) do
      {:ok, entries} -> {:__commit_put_batch_direct__, [shard_index, entries]}
      :fallback -> {:__commit_batch_direct__, [shard_index, commands]}
    end
  end

  defp compact_slot_commit(shard_index, [{:delete, key} | rest] = commands)
       when is_binary(key) do
    case compact_delete_keys(rest, [key]) do
      {:ok, keys} -> {:__commit_delete_batch_direct__, [shard_index, keys]}
      :fallback -> {:__commit_batch_direct__, [shard_index, commands]}
    end
  end

  defp compact_slot_commit(shard_index, commands),
    do: {:__commit_batch_direct__, [shard_index, commands]}

  defp compact_put_entries([], acc), do: {:ok, Enum.reverse(acc)}

  defp compact_put_entries([{:put, key, value, expire_at_ms} | rest], acc)
       when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) do
    compact_put_entries(rest, [{key, value, expire_at_ms} | acc])
  end

  defp compact_put_entries(_commands, _acc), do: :fallback

  defp compact_delete_keys([], acc), do: {:ok, Enum.reverse(acc)}

  defp compact_delete_keys([{:delete, key} | rest], acc) when is_binary(key) do
    compact_delete_keys(rest, [key | acc])
  end

  defp compact_delete_keys(_commands, _acc), do: :fallback

  defp commit_single_direct(shard_index, command) do
    case backend_call(:__commit_batch_direct__, [shard_index, [command]]) do
      {:ok, [reply]} -> reply
      other -> other
    end
  end

  defp backend_call(function, args) do
    maybe_run_backend_call_hook(function)
    apply(Ferricstore.Raft.WARaftBackend, function, args)
  end

  defp safe_backend_call(function, args) do
    backend_call(function, args)
  catch
    :exit, reason -> {:error, {:backend_exit, reason}}
    kind, reason -> {:error, {:backend_error, kind, reason}}
  end

  defp maybe_run_backend_call_hook(function) do
    case Application.get_env(:ferricstore, :waraft_backend_batcher_call_hook) do
      {:block, notify} when is_pid(notify) ->
        ref = make_ref()
        send(notify, {:waraft_backend_batcher_call, function, ref, self()})

        receive do
          {^ref, :continue} -> :ok
        after
          @call_timeout -> exit({:backend_call_hook_timeout, function})
        end

      _ ->
        :ok
    end
  end

  defp hot_batch_window_ms do
    Application.get_env(:ferricstore, :waraft_hot_batch_window_ms, @default_hot_batch_window_ms)
  end

  defp generic_batch_window_ms do
    Application.get_env(:ferricstore, :waraft_generic_batch_window_ms, 0)
  end

  defp generic_batch_during_flush? do
    # Keep zero-window generic batches on the batcher so callers arriving behind
    # an in-flight flush coalesce without adding a fixed latency window.
    Application.get_env(:ferricstore, :waraft_generic_batch_during_flush, true) == true
  end
end
