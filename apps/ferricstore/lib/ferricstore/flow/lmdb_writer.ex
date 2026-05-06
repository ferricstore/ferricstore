defmodule Ferricstore.Flow.LMDBWriter do
  @moduledoc false

  use GenServer

  alias Ferricstore.Flow.LMDBReplaySafeIndex

  require Logger

  @default_flush_interval_ms 10
  @default_max_ops 1_000

  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    instance_name = instance_name_from_opts(opts)
    GenServer.start_link(__MODULE__, opts, name: name(instance_name, shard_index))
  end

  def enqueue(shard_index, ops) when is_integer(shard_index) and is_list(ops) do
    enqueue(:default, shard_index, ops, [])
  end

  def enqueue(shard_index, ops, after_flush)
      when is_integer(shard_index) and is_list(ops) and is_list(after_flush) do
    enqueue(:default, shard_index, ops, after_flush)
  end

  def enqueue(instance_name, shard_index, ops)
      when is_atom(instance_name) and is_integer(shard_index) and is_list(ops) do
    enqueue(instance_name, shard_index, ops, [])
  end

  def enqueue(instance_name, shard_index, ops, after_flush)
      when is_atom(instance_name) and is_integer(shard_index) and is_list(ops) and
             is_list(after_flush) do
    try do
      if ops != [] do
        GenServer.cast(name(instance_name, shard_index), {:enqueue, ops, after_flush})
      end

      :ok
    catch
      :exit, _ -> :ok
    end
  end

  def durable?(instance_ctx, shard_index, shard_data_path, index) do
    durable_index(instance_ctx, shard_index, shard_data_path) >= index
  end

  def durable_index(
        %{flow_lmdb_replay_safe_index: replay_safe_index},
        shard_index,
        _shard_data_path
      )
      when is_reference(replay_safe_index) do
    if shard_index < :atomics.info(replay_safe_index).size do
      :atomics.get(replay_safe_index, shard_index + 1)
    else
      0
    end
  rescue
    _ -> 0
  end

  def durable_index(_instance_ctx, _shard_index, shard_data_path) do
    LMDBReplaySafeIndex.read(shard_data_path)
  end

  def request(instance_ctx, shard_index, shard_data_path, index)
      when is_integer(index) and index >= 0 do
    instance_name = instance_name_from_ctx(instance_ctx)
    publish_requested(instance_ctx, shard_index, index)

    cond do
      durable?(instance_ctx, shard_index, shard_data_path, index) ->
        :durable

      is_pid(writer_pid = Process.whereis(name(instance_name, shard_index))) ->
        GenServer.cast(writer_pid, {:persist_replay_safe, index})
        :requested

      Ferricstore.Flow.LMDB.write_through?() ->
        sync_persist(instance_ctx, shard_index, shard_data_path, index)

      true ->
        {:error, :writer_not_started}
    end
  catch
    :exit, reason -> {:error, reason}
  end

  def flush_all(shard_count) when is_integer(shard_count) and shard_count >= 0 do
    flush_all(:default, shard_count, 30_000)
  end

  def flush_all(shard_count, timeout)
      when is_integer(shard_count) and shard_count >= 0 and is_integer(timeout) do
    flush_all(:default, shard_count, timeout)
  end

  def flush_all(instance_name, shard_count)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 do
    flush_all(instance_name, shard_count, 30_000)
  end

  def flush_all(instance_name, shard_count, timeout)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 and
             is_integer(timeout) do
    shard_indexes =
      if shard_count == 0 do
        []
      else
        0..(shard_count - 1)
      end

    Enum.reduce(shard_indexes, :ok, fn shard_index, acc ->
      case flush(instance_name, shard_index, timeout) do
        :ok -> acc
        {:error, _reason} = error -> error
      end
    end)
  end

  def flush(shard_index) when is_integer(shard_index) and shard_index >= 0 do
    flush(:default, shard_index, 30_000)
  end

  def flush(shard_index, timeout)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(timeout) do
    flush(:default, shard_index, timeout)
  end

  def flush(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    flush(instance_name, shard_index, 30_000)
  end

  def flush(instance_name, shard_index, timeout)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(timeout) do
    try do
      case GenServer.call(name(instance_name, shard_index), :flush, timeout) do
        :ok -> :ok
        {:error, _reason} = error -> error
      end
    catch
      :exit, _ -> :ok
    end
  end

  def name(shard_index), do: :"Ferricstore.Flow.LMDBWriter.#{shard_index}"

  def name(:default, shard_index), do: name(shard_index)

  def name(instance_name, shard_index) do
    :"Ferricstore.Flow.LMDBWriter.#{instance_name}.#{shard_index}"
  end

  @impl true
  def init(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    data_dir = Keyword.fetch!(opts, :data_dir)

    state = %{
      instance_name: instance_name_from_opts(opts),
      shard_index: shard_index,
      shard_data_path: Ferricstore.DataDir.shard_data_path(data_dir, shard_index),
      path:
        data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Flow.LMDB.path(),
      instance_ctx: Keyword.get(opts, :instance_ctx),
      pending: [],
      pending_after_flush: [],
      count: 0,
      timer_ref: nil,
      durable_index: 0,
      requested_index: 0,
      flush_interval_ms:
        Application.get_env(
          :ferricstore,
          :flow_lmdb_flush_interval_ms,
          @default_flush_interval_ms
        ),
      max_ops: Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops, @default_max_ops)
    }

    durable_index = LMDBReplaySafeIndex.read(state.shard_data_path)
    publish_durable(state.instance_ctx, shard_index, durable_index)

    state = %{state | durable_index: durable_index, requested_index: durable_index}

    {:ok, state}
  end

  defp instance_name_from_opts(opts) do
    case {Keyword.get(opts, :instance_name), Keyword.get(opts, :instance_ctx)} do
      {name, _ctx} when is_atom(name) and not is_nil(name) -> name
      {_name, %{name: name}} when is_atom(name) and not is_nil(name) -> name
      _ -> :default
    end
  end

  defp instance_name_from_ctx(%{name: name}) when is_atom(name) and not is_nil(name), do: name
  defp instance_name_from_ctx(_ctx), do: :default

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

  def handle_cast({:persist_replay_safe, index}, state) when is_integer(index) and index >= 0 do
    requested_index = max(state.requested_index, index)
    publish_requested(state.instance_ctx, state.shard_index, requested_index)

    {:noreply, flush_pending(%{state | requested_index: requested_index})}
  end

  @impl true
  def handle_call(:flush, _from, state) do
    {state, reply} = flush_pending_with_reply(state)
    {:reply, reply, state}
  end

  @impl true
  def handle_info(:flush, state), do: {:noreply, flush_pending(%{state | timer_ref: nil})}

  defp ensure_timer(%{timer_ref: nil, flush_interval_ms: interval} = state) do
    %{state | timer_ref: Process.send_after(self(), :flush, interval)}
  end

  defp ensure_timer(state), do: state

  defp flush_pending(state) do
    {state, _reply} = flush_pending_with_reply(state)
    state
  end

  defp flush_pending_with_reply(
         %{pending: [], requested_index: requested, durable_index: durable} = state
       )
       when requested <= durable do
    {%{state | count: 0, pending_after_flush: [], timer_ref: nil}, :ok}
  end

  defp flush_pending_with_reply(state) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)

    ops = Enum.reverse(state.pending)
    after_flush = Enum.reverse(state.pending_after_flush)
    started_at = System.monotonic_time()

    case flush_ops_and_marker(state, ops, started_at) do
      {:ok, state} ->
        Enum.each(after_flush, &apply_after_flush/1)

        {%{state | pending: [], pending_after_flush: [], count: 0, timer_ref: nil}, :ok}

      {:error, reason, state} ->
        record_persist_failure(state.instance_ctx, state.shard_index)
        emit_persist({:error, reason}, state, state.requested_index, started_at)

        Logger.warning(
          "Flow LMDB writer shard #{state.shard_index} flush failed: #{inspect(reason)}"
        )

        {ensure_timer(%{state | timer_ref: nil}), {:error, reason}}
    end
  end

  defp flush_ops_and_marker(state, ops, started_at) do
    with {:ok, ops} <- expand_ops(state.path, ops),
         :ok <- write_ops(state.path, ops),
         {:ok, state} <- persist_requested(state, started_at) do
      {:ok, state}
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp write_ops(_path, []), do: :ok
  defp write_ops(path, ops), do: Ferricstore.Flow.LMDB.write_batch(path, ops)

  defp expand_ops(_path, []), do: {:ok, []}

  defp expand_ops(path, ops) do
    initial = %{ops: [], counts: %{}, terminal_values: %{}}

    Enum.reduce_while(ops, {:ok, initial}, fn op, {:ok, acc} ->
      case expand_op(path, op, acc) do
        {:ok, acc} -> {:cont, {:ok, acc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, %{ops: expanded}} -> {:ok, Enum.reverse(expanded)}
      {:error, _reason} = error -> error
    end
  end

  defp expand_op(path, {:terminal_put, terminal_key, value, state_key, count_key}, acc)
       when is_binary(terminal_key) and is_binary(value) and is_binary(state_key) and
              is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      existed? = is_binary(old_value)
      count = if existed?, do: count, else: count + 1
      reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

      ops =
        [
          {:put, terminal_key, value},
          {:put, reverse_key, terminal_key},
          {:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}
        ]
        |> maybe_put_expire_key(terminal_key, value, state_key, count_key)
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], value)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  defp expand_op(path, {:terminal_put, terminal_key, value, nil, count_key}, acc)
       when is_binary(terminal_key) and is_binary(value) and is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      existed? = is_binary(old_value)
      count = if existed?, do: count, else: count + 1

      ops =
        [
          {:put, terminal_key, value},
          {:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}
        ]
        |> maybe_put_expire_key(terminal_key, value, nil, count_key)
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], value)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  defp expand_op(_path, {:query_put, query_key, value}, acc)
       when is_binary(query_key) and is_binary(value) do
    {:ok, prepend_ops(acc, [{:put, query_key, value}])}
  end

  defp expand_op(_path, {:query_delete, query_key}, acc) when is_binary(query_key) do
    {:ok, prepend_ops(acc, [{:delete, query_key}])}
  end

  defp expand_op(path, {:terminal_delete, terminal_key, state_key, count_key}, acc)
       when is_binary(terminal_key) and is_binary(state_key) and is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

      {count, count_ops} =
        if is_binary(old_value) do
          count = max(count - 1, 0)
          {count, [{:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}]}
        else
          {count, []}
        end

      ops =
        [{:delete, terminal_key}, {:delete, reverse_key} | count_ops]
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], nil)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  defp expand_op(path, {:terminal_delete, terminal_key, nil, count_key}, acc)
       when is_binary(terminal_key) and is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      {count, count_ops} =
        if is_binary(old_value) do
          count = max(count - 1, 0)
          {count, [{:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}]}
        else
          {count, []}
        end

      ops =
        [{:delete, terminal_key} | count_ops]
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], nil)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  defp expand_op(_path, op, acc), do: {:ok, prepend_ops(acc, [op])}

  defp terminal_value(path, terminal_key, acc) do
    case Map.fetch(acc.terminal_values, terminal_key) do
      {:ok, value} ->
        {:ok, value, acc}

      :error ->
        case Ferricstore.Flow.LMDB.get(path, terminal_key) do
          {:ok, value} -> {:ok, value, put_in(acc, [:terminal_values, terminal_key], value)}
          :not_found -> {:ok, nil, put_in(acc, [:terminal_values, terminal_key], nil)}
          {:error, _reason} = error -> error
        end
    end
  end

  defp terminal_count(path, count_key, acc) do
    case Map.fetch(acc.counts, count_key) do
      {:ok, count} ->
        {:ok, count, acc}

      :error ->
        case Ferricstore.Flow.LMDB.get(path, count_key) do
          {:ok, value} ->
            case Ferricstore.Flow.LMDB.decode_count(value) do
              {:ok, count} -> {:ok, count, put_in(acc, [:counts, count_key], count)}
              :error -> {:ok, 0, put_in(acc, [:counts, count_key], 0)}
            end

          :not_found ->
            {:ok, 0, put_in(acc, [:counts, count_key], 0)}

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp prepend_ops(acc, ops), do: %{acc | ops: Enum.reverse(ops) ++ acc.ops}

  defp maybe_put_expire_key(ops, terminal_key, value, state_key, count_key) do
    case Ferricstore.Flow.LMDB.decode_terminal_index_value(value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, _state_key}} when expire_at_ms > 0 ->
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)

        expire_value =
          Ferricstore.Flow.LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)

        [{:put, expire_key, expire_value} | ops]

      _ ->
        ops
    end
  end

  defp maybe_delete_old_expire_key(ops, _terminal_key, nil), do: ops

  defp maybe_delete_old_expire_key(ops, terminal_key, old_value) do
    case Ferricstore.Flow.LMDB.decode_terminal_index_value(old_value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, _state_key}} when expire_at_ms > 0 ->
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)
        [{:delete, expire_key} | ops]

      _ ->
        ops
    end
  end

  defp persist_requested(
         %{requested_index: requested, durable_index: durable} = state,
         _started_at
       )
       when requested <= durable do
    {:ok, state}
  end

  defp persist_requested(state, started_at) do
    index = state.requested_index

    case LMDBReplaySafeIndex.persist(state.shard_data_path, index) do
      :ok ->
        publish_durable(state.instance_ctx, state.shard_index, index)
        emit_persist(:ok, state, index, started_at)
        poke_release_cursor(state, index)
        {:ok, %{state | durable_index: index}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp prepend_reverse([], acc), do: acc
  defp prepend_reverse([head | rest], acc), do: prepend_reverse(rest, [head | acc])

  defp apply_after_flush(
         {:prune_terminal_flow, ets, zset_index, zset_lookup, state_key, state_index_key, id,
          version}
       ) do
    prune_terminal_state_key(ets, state_key, version)

    safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id)

    :ok
  end

  defp apply_after_flush({:delete_flow_tombstone, ets, key}) do
    case :ets.lookup(ets, key) do
      [{^key, nil, 0, :flow_state_deleted, :deleted, 0, 0}] -> :ets.delete(ets, key)
      _ -> :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp apply_after_flush(_action), do: :ok

  defp safe_zset_delete_member(nil, _zset_lookup, _state_index_key, _id), do: :ok
  defp safe_zset_delete_member(_zset_index, nil, _state_index_key, _id), do: :ok

  defp safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id) do
    Ferricstore.Store.Shard.ZSetIndex.delete_member(
      zset_index,
      zset_lookup,
      state_index_key,
      id
    )
  rescue
    ArgumentError -> :ok
  end

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

  defp sync_persist(instance_ctx, shard_index, shard_data_path, index) do
    publish_requested(instance_ctx, shard_index, index)

    case LMDBReplaySafeIndex.persist(shard_data_path, index) do
      :ok ->
        publish_durable(instance_ctx, shard_index, index)
        :durable

      {:error, _reason} = error ->
        record_persist_failure(instance_ctx, shard_index)
        error
    end
  end

  defp poke_release_cursor(state, index) do
    Ferricstore.Raft.Batcher.origin_submit(state.shard_index, {:release_cursor_poke, index})
  catch
    :exit, _reason -> :ok
  end

  defp publish_durable(%{flow_lmdb_replay_safe_index: replay_safe_index}, shard_index, index)
       when is_reference(replay_safe_index) do
    if shard_index < :atomics.info(replay_safe_index).size do
      :atomics.put(replay_safe_index, shard_index + 1, index)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp publish_durable(_instance_ctx, _shard_index, _index), do: :ok

  defp publish_requested(
         %{flow_lmdb_replay_safe_requested_index: requested_index},
         shard_index,
         index
       )
       when is_reference(requested_index) do
    put_atomic_max(requested_index, shard_index, index)
  rescue
    _ -> :ok
  end

  defp publish_requested(_instance_ctx, _shard_index, _index), do: :ok

  defp record_persist_failure(%{flow_lmdb_replay_safe_persist_failures: failures}, shard_index)
       when is_reference(failures) do
    if shard_index < :atomics.info(failures).size do
      :atomics.add(failures, shard_index + 1, 1)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp record_persist_failure(_instance_ctx, _shard_index), do: :ok

  defp put_atomic_max(ref, shard_index, value) do
    if shard_index < :atomics.info(ref).size do
      position = shard_index + 1
      current = :atomics.get(ref, position)

      if value > current do
        :atomics.put(ref, position, value)
      end
    end

    :ok
  end

  defp emit_persist(status, state, index, started_at) do
    requested_index = max(state.requested_index, index)
    durable_index = if status == :ok, do: index, else: state.durable_index

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_replay_safe_index, :persist],
      %{
        duration_us: duration_us(started_at),
        index: index,
        requested_index: requested_index,
        durable_index: durable_index,
        lag: max(requested_index - durable_index, 0)
      },
      %{
        status: persist_status(status),
        shard_index: state.shard_index,
        reason: persist_reason(status)
      }
    )
  end

  defp persist_status(:ok), do: :ok
  defp persist_status({:error, _}), do: :error

  defp persist_reason(:ok), do: :none
  defp persist_reason({:error, reason}), do: reason

  defp duration_us(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :microsecond)
  end
end
