defmodule Ferricstore.Raft.WARaftBackend.Batcher do
  @moduledoc """
  Namespace-window batcher for the WARaft replacement backend.

  The normal WARaft write path stays direct. This process is used only when a
  namespace explicitly configures a `window_ms` larger than the default, so the
  common SET/DEL hot path does not pay a GenServer hop just to discover there is
  nothing to coalesce.
  """

  use GenServer

  alias Ferricstore.Raft.WARaftBackend

  @call_timeout 30_000
  @default_max_batch_size 1024
  @default_hot_batch_window_ms 1

  defstruct [
    :shard_index,
    :max_batch_size,
    :hot_batch_window_ms,
    :hot_max_batch_size,
    slots: %{},
    put_slot: nil,
    delete_slot: nil
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

  @spec write_put_batch(non_neg_integer(), [{binary(), binary(), non_neg_integer()}]) :: term()
  def write_put_batch(shard_index, entries)
      when is_integer(shard_index) and is_list(entries) do
    case hot_batch_window_ms() do
      window_ms when window_ms > 0 ->
        call_or_commit_put_batch_direct(shard_index, entries, window_ms)

      _disabled ->
        WARaftBackend.__commit_put_batch_direct__(shard_index, entries)
    end
  end

  @spec write_delete_batch(non_neg_integer(), [binary()]) :: term()
  def write_delete_batch(shard_index, keys) when is_integer(shard_index) and is_list(keys) do
    case hot_batch_window_ms() do
      window_ms when window_ms > 0 ->
        call_or_commit_delete_batch_direct(shard_index, keys, window_ms)

      _disabled ->
        WARaftBackend.__commit_delete_batch_direct__(shard_index, keys)
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

    if slot.count >= state.max_batch_size do
      {:noreply, flush_slot(state, prefix)}
    else
      {:noreply, state}
    end
  end

  def handle_call({:write_put_batch, entries, window_ms}, from, state) do
    state = enqueue_hot_put_batch(state, from, entries, window_ms)

    if state.put_slot.count >= state.hot_max_batch_size do
      {:noreply, flush_hot_put_slot(state)}
    else
      {:noreply, state}
    end
  end

  def handle_call({:write_delete_batch, keys, window_ms}, from, state) do
    state = enqueue_hot_delete_batch(state, from, keys, window_ms)

    if state.delete_slot.count >= state.hot_max_batch_size do
      {:noreply, flush_hot_delete_slot(state)}
    else
      {:noreply, state}
    end
  end

  def handle_call(:stop, _from, state) do
    state = flush_all_slots(state)
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_info({:flush, prefix, token}, state) do
    case Map.get(state.slots, prefix) do
      %{timer_token: ^token} -> {:noreply, flush_slot(state, prefix)}
      _stale_or_missing -> {:noreply, state}
    end
  end

  def handle_info({:flush_hot_put_batch, token}, state) do
    case state.put_slot do
      %{timer_token: ^token} -> {:noreply, flush_hot_put_slot(state)}
      _stale_or_missing -> {:noreply, state}
    end
  end

  def handle_info({:flush_hot_delete_batch, token}, state) do
    case state.delete_slot do
      %{timer_token: ^token} -> {:noreply, flush_hot_delete_slot(state)}
      _stale_or_missing -> {:noreply, state}
    end
  end

  def handle_info({:flush, _prefix}, state), do: {:noreply, state}
  def handle_info(:flush_hot_put_batch, state), do: {:noreply, state}
  def handle_info(:flush_hot_delete_batch, state), do: {:noreply, state}

  defp call_or_commit_put_batch_direct(shard_index, entries, window_ms) do
    case Process.whereis(name(shard_index)) do
      nil ->
        WARaftBackend.__commit_put_batch_direct__(shard_index, entries)

      _pid ->
        GenServer.call(name(shard_index), {:write_put_batch, entries, window_ms}, @call_timeout)
    end
  catch
    :exit, {:noproc, _} -> WARaftBackend.__commit_put_batch_direct__(shard_index, entries)
    :exit, {:normal, _} -> WARaftBackend.__commit_put_batch_direct__(shard_index, entries)
  end

  defp call_or_commit_delete_batch_direct(shard_index, keys, window_ms) do
    case Process.whereis(name(shard_index)) do
      nil ->
        WARaftBackend.__commit_delete_batch_direct__(shard_index, keys)

      _pid ->
        GenServer.call(name(shard_index), {:write_delete_batch, keys, window_ms}, @call_timeout)
    end
  catch
    :exit, {:noproc, _} -> WARaftBackend.__commit_delete_batch_direct__(shard_index, keys)
    :exit, {:normal, _} -> WARaftBackend.__commit_delete_batch_direct__(shard_index, keys)
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

  defp flush_all_slots(state) do
    state =
      state.slots
      |> Map.keys()
      |> Enum.reduce(state, &flush_slot(&2, &1))

    state
    |> flush_hot_put_slot()
    |> flush_hot_delete_slot()
  end

  defp flush_slot(state, prefix) do
    case Map.pop(state.slots, prefix) do
      {nil, _slots} ->
        state

      {slot, slots} ->
        cancel_timer(slot.timer_ref)

        commands = Enum.reverse(slot.commands)
        froms = Enum.reverse(slot.froms)
        result = WARaftBackend.write_batch(state.shard_index, commands)
        replies = replies_for_batch(result, length(commands))

        Enum.zip(froms, replies)
        |> Enum.each(fn {from, reply} -> GenServer.reply(from, reply) end)

        emit_flush_telemetry(state, prefix, slot, result)
        %{state | slots: slots}
    end
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

    %{
      groups: [],
      count: 0,
      timer_ref: Process.send_after(self(), {flush_message, token}, window_ms),
      timer_token: token,
      window_ms: window_ms,
      created_mono: System.monotonic_time()
    }
  end

  defp flush_hot_put_slot(%{put_slot: nil} = state), do: state

  defp flush_hot_put_slot(%{put_slot: slot} = state) do
    cancel_timer(slot.timer_ref)
    groups = Enum.reverse(slot.groups)
    entries = hot_batch_items(groups)
    result = WARaftBackend.__commit_put_batch_direct__(state.shard_index, entries)
    reply_hot_batch_groups(groups, result)
    emit_hot_flush_telemetry(state, :put_batch, slot, result)
    %{state | put_slot: nil}
  end

  defp flush_hot_delete_slot(%{delete_slot: nil} = state), do: state

  defp flush_hot_delete_slot(%{delete_slot: slot} = state) do
    cancel_timer(slot.timer_ref)
    groups = Enum.reverse(slot.groups)
    keys = hot_batch_items(groups)
    result = WARaftBackend.__commit_delete_batch_direct__(state.shard_index, keys)
    reply_hot_batch_groups(groups, result)
    emit_hot_flush_telemetry(state, :delete_batch, slot, result)
    %{state | delete_slot: nil}
  end

  defp hot_batch_items(groups) do
    Enum.flat_map(groups, fn {_from, items} -> items end)
  end

  defp reply_hot_batch_groups(groups, {:ok, replies}) when is_list(replies) do
    expected = total_hot_batch_items(groups)

    if length(replies) == expected do
      {group_replies, []} =
        Enum.map_reduce(groups, replies, fn {from, items}, remaining ->
          {reply, rest} = Enum.split(remaining, length(items))
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

  defp total_hot_batch_items(groups) do
    Enum.reduce(groups, 0, fn {_from, items}, acc -> acc + length(items) end)
  end

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

  defp commit_single_direct(shard_index, command) do
    case WARaftBackend.write_batch(shard_index, [command]) do
      {:ok, [reply]} -> reply
      other -> other
    end
  end

  defp emit_flush_telemetry(state, prefix, slot, result) do
    :telemetry.execute(
      [:ferricstore, :waraft, :batcher, :slot_flush],
      %{
        batch_size: slot.count,
        queue_wait_us:
          System.convert_time_unit(
            System.monotonic_time() - slot.created_mono,
            :native,
            :microsecond
          )
      },
      %{
        shard_index: state.shard_index,
        prefix: prefix,
        window_ms: slot.window_ms,
        result: result_shape(result)
      }
    )
  end

  defp result_shape({:ok, _replies}), do: :ok
  defp result_shape({:error, reason}), do: {:error, reason}
  defp result_shape(_other), do: :other

  defp emit_hot_flush_telemetry(state, kind, slot, result) do
    :telemetry.execute(
      [:ferricstore, :waraft, :batcher, :hot_flush],
      %{
        batch_size: slot.count,
        group_count: length(slot.groups),
        queue_wait_us:
          System.convert_time_unit(
            System.monotonic_time() - slot.created_mono,
            :native,
            :microsecond
          )
      },
      %{
        shard_index: state.shard_index,
        kind: kind,
        window_ms: slot.window_ms,
        result: result_shape(result)
      }
    )
  end

  defp hot_batch_window_ms do
    Application.get_env(:ferricstore, :waraft_hot_batch_window_ms, @default_hot_batch_window_ms)
  end
end
