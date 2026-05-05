defmodule Ferricstore.Flow.LMDBWriter do
  @moduledoc false

  use GenServer

  @default_flush_interval_ms 10
  @default_max_ops 1_000

  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: name(shard_index))
  end

  def enqueue(shard_index, ops, after_flush \\ []) when is_list(ops) and is_list(after_flush) do
    try do
      if Ferricstore.Flow.LMDB.mirror?() and ops != [] do
        GenServer.cast(name(shard_index), {:enqueue, ops, after_flush})
      end

      :ok
    catch
      :exit, _ -> :ok
    end
  end

  def flush_all(shard_count, timeout \\ 30_000) do
    0..(shard_count - 1)
    |> Enum.reduce(:ok, fn shard_index, acc ->
      try do
        case GenServer.call(name(shard_index), :flush, timeout) do
          :ok -> acc
          {:error, _reason} = error -> error
        end
      catch
        :exit, _ -> acc
      end
    end)
  end

  def name(shard_index), do: :"Ferricstore.Flow.LMDBWriter.#{shard_index}"

  @impl true
  def init(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    data_dir = Keyword.fetch!(opts, :data_dir)

    state = %{
      shard_index: shard_index,
      path:
        data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Flow.LMDB.path(),
      pending: [],
      pending_after_flush: [],
      count: 0,
      timer_ref: nil,
      flush_interval_ms:
        Application.get_env(
          :ferricstore,
          :flow_lmdb_flush_interval_ms,
          @default_flush_interval_ms
        ),
      max_ops: Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops, @default_max_ops)
    }

    {:ok, state}
  end

  @impl true
  def handle_cast({:enqueue, ops, after_flush}, state) do
    state =
      %{
        state
        | pending: prepend_reverse(ops, state.pending),
          pending_after_flush: prepend_reverse(after_flush, state.pending_after_flush),
          count: state.count + length(ops)
      }
      |> ensure_timer()

    if state.count >= state.max_ops do
      {:noreply, flush_pending(state)}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_call(:flush, _from, state), do: {:reply, :ok, flush_pending(state)}

  @impl true
  def handle_info(:flush, state), do: {:noreply, flush_pending(%{state | timer_ref: nil})}

  defp ensure_timer(%{timer_ref: nil, flush_interval_ms: interval} = state) do
    %{state | timer_ref: Process.send_after(self(), :flush, interval)}
  end

  defp ensure_timer(state), do: state

  defp flush_pending(%{pending: []} = state), do: %{state | count: 0, pending_after_flush: []}

  defp flush_pending(state) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)

    ops = Enum.reverse(state.pending)
    after_flush = Enum.reverse(state.pending_after_flush)

    case Ferricstore.Flow.LMDB.write_batch(state.path, ops) do
      :ok -> Enum.each(after_flush, &apply_after_flush/1)
      {:error, _reason} -> :ok
    end

    %{state | pending: [], pending_after_flush: [], count: 0, timer_ref: nil}
  end

  defp prepend_reverse([], acc), do: acc
  defp prepend_reverse([head | rest], acc), do: prepend_reverse(rest, [head | acc])

  defp apply_after_flush(
         {:prune_terminal_flow, ets, zset_index, zset_lookup, state_key, state_index_key, id,
          version}
       ) do
    prune_terminal_state_key(ets, state_key, version)

    if zset_index != nil and zset_lookup != nil do
      Ferricstore.Store.Shard.ZSetIndex.delete_member(
        zset_index,
        zset_lookup,
        state_index_key,
        id
      )
    end

    :ok
  end

  defp apply_after_flush(_action), do: :ok

  defp prune_terminal_state_key(ets, state_key, version) do
    case :ets.lookup(ets, state_key) do
      [
        {^state_key, nil, _expire_at_ms, {:flow_state_version, ^version, _lfu}, _fid, _off,
         _vsize}
      ] ->
        :ets.delete(ets, state_key)

      _ ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end
end
