defmodule Ferricstore.Store.Shard.Reads do
  @moduledoc "Shard read-path handlers: ETS hot lookup, cold-key pread from Bitcask, exists check, and key enumeration."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.ExpiryContext
  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.Store.{BlobValue, ColdRead, ReadResult}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  @cold_read_timeout_ms 10_000
  @max_get_many_keys 512
  @max_key_size 65_535
  @max_get_many_key_bytes 1_048_576
  @default_get_many_max_concurrency 4
  @default_get_many_max_queued 64
  @default_get_many_deadline_ms 4_500
  @get_many_busy_error {:error, "BUSY shard batch read queue is full"}
  @get_many_failed_error {:error, "ERR shard batch read failed"}
  @invalid_keydir_failure ReadResult.failure(:invalid_keydir_entry)

  defguardp valid_waraft_segment_location(file_id, offset, value_size)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   (elem(file_id, 0) == :waraft_segment or
                      elem(file_id, 0) == :waraft_projection or
                      elem(file_id, 0) == :waraft_apply_projection) and
                   is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0 and
                   is_integer(offset) and offset >= 0 and
                   is_integer(value_size) and value_size >= 0

  # -------------------------------------------------------------------
  # Read-path handlers (return {:reply, result, state})
  # -------------------------------------------------------------------

  @spec handle_get(binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_get(key, state) do
    # Fast path: ETS hit — no need to wait for in-flight writes.
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} ->
        {:reply, value, state}

      :expired ->
        {:reply, nil, state}

      {:error, :invalid_keydir_entry} ->
        {:reply, @invalid_keydir_failure, state}

      {:error, {:storage_read_failed, _reason}} = failure ->
        {:reply, failure, state}

      {:cold, fid, off, vsize, exp} ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            reply_cold_value(state, key, value, exp, fid, off, vsize)

          failed_read ->
            {:reply, cold_read_failure(failed_read), state}
        end

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          state = ShardFlush.flush_pending_for_read(state)
          {:reply, do_get(state, key), state}
        else
          {:reply, nil, state}
        end
    end
  end

  @spec handle_get_many([binary()], GenServer.from(), map()) ::
          {:noreply, map()} | {:reply, {:error, binary()}, map()}
  @doc false
  def handle_get_many(keys, from, state) when is_list(keys) do
    handle_get_many(keys, from, new_get_many_deadline(state), state)
  end

  @spec handle_get_many([binary()], GenServer.from(), integer(), map()) ::
          {:noreply, map()} | {:reply, term(), map()}
  @doc false
  def handle_get_many(keys, from, deadline_ms, state)
      when is_list(keys) and is_integer(deadline_ms) do
    cond do
      not valid_get_many_request?(keys) ->
        {:reply, {:error, "ERR invalid shard batch read request"}, state}

      get_many_expired?(deadline_ms) ->
        {:reply, unavailable_get_many_values(keys), state}

      true ->
        state = flush_pending_get_many_keys(state, keys)
        {:noreply, admit_get_many(state, from, keys, deadline_ms)}
    end
  end

  @doc false
  def handle_get_many_complete(job_ref, result, state) do
    workers = Map.get(state, :get_many_workers, %{})

    case Map.pop(workers, job_ref) do
      {%{monitor_ref: monitor_ref, timer_ref: timer_ref, from: from}, remaining_workers} ->
        cancel_get_many_timer(timer_ref)
        Process.demonitor(monitor_ref, [:flush])
        reply_get_many(from, result)

        state
        |> Map.put(:get_many_workers, remaining_workers)
        |> drain_get_many_waiting()

      {nil, _workers} ->
        state
    end
  end

  @doc false
  def handle_get_many_timeout(job_ref, state) do
    workers = Map.get(state, :get_many_workers, %{})

    case Map.fetch(workers, job_ref) do
      {:ok, %{from: nil}} ->
        state

      {:ok, %{pid: pid, from: from, keys: keys} = worker} ->
        Process.exit(pid, :kill)
        GenServer.reply(from, unavailable_get_many_values(keys))

        timed_out_worker = %{
          worker
          | from: nil,
            timer_ref: nil,
            timed_out?: true
        }

        Map.put(state, :get_many_workers, Map.put(workers, job_ref, timed_out_worker))

      :error ->
        state
    end
  end

  @doc false
  def handle_get_many_down(monitor_ref, state) do
    workers = Map.get(state, :get_many_workers, %{})

    case Enum.find(workers, fn {_job_ref, worker} ->
           worker.monitor_ref == monitor_ref
         end) do
      {job_ref, %{timer_ref: timer_ref, from: from}} ->
        cancel_get_many_timer(timer_ref)
        reply_get_many(from, @get_many_failed_error)

        state =
          state
          |> Map.put(:get_many_workers, Map.delete(workers, job_ref))
          |> drain_get_many_waiting()

        {:handled, state}

      nil ->
        :unhandled
    end
  end

  defp valid_get_many_request?(keys) do
    keys
    |> Enum.reduce_while({0, 0}, fn
      key, {count, key_bytes} when is_binary(key) and byte_size(key) <= @max_key_size ->
        next_count = count + 1
        next_key_bytes = key_bytes + byte_size(key)

        if next_count <= @max_get_many_keys and next_key_bytes <= @max_get_many_key_bytes do
          {:cont, {next_count, next_key_bytes}}
        else
          {:halt, :invalid}
        end

      _key, _totals ->
        {:halt, :invalid}
    end)
    |> case do
      :invalid -> false
      {_count, _key_bytes} -> true
    end
  end

  defp admit_get_many(state, from, keys, deadline_ms) do
    workers = Map.get(state, :get_many_workers, %{})

    cond do
      map_size(workers) < get_many_max_concurrency(state) ->
        start_get_many_worker(state, from, keys, deadline_ms)

      Map.get(state, :get_many_waiting_count, 0) < get_many_max_queued(state) ->
        waiting = Map.get(state, :get_many_waiting, :queue.new())

        state
        |> Map.put(:get_many_waiting, :queue.in({from, keys, deadline_ms}, waiting))
        |> Map.update(:get_many_waiting_count, 1, &(&1 + 1))

      true ->
        GenServer.reply(from, @get_many_busy_error)
        state
    end
  end

  defp start_get_many_worker(state, from, keys, deadline_ms) do
    case get_many_remaining_ms(deadline_ms) do
      0 ->
        GenServer.reply(from, unavailable_get_many_values(keys))
        state

      timeout_ms ->
        parent = self()
        job_ref = make_ref()

        read_state =
          state
          |> get_many_read_state()
          |> Map.put(:expiry_context, ExpiryContext.capture())

        {pid, monitor_ref} =
          spawn_monitor(fn ->
            result = safe_get_many_values(keys, read_state, deadline_ms)
            send(parent, {:shard_get_many_complete, job_ref, result})
          end)

        timer_ref = Process.send_after(parent, {:shard_get_many_timeout, job_ref}, timeout_ms)
        workers = Map.get(state, :get_many_workers, %{})

        worker = %{
          pid: pid,
          monitor_ref: monitor_ref,
          timer_ref: timer_ref,
          from: from,
          keys: keys,
          timed_out?: false
        }

        Map.put(state, :get_many_workers, Map.put(workers, job_ref, worker))
    end
  end

  defp drain_get_many_waiting(state) do
    workers = Map.get(state, :get_many_workers, %{})
    waiting_count = Map.get(state, :get_many_waiting_count, 0)

    if waiting_count > 0 and map_size(workers) < get_many_max_concurrency(state) do
      waiting = Map.get(state, :get_many_waiting, :queue.new())

      case :queue.out(waiting) do
        {{:value, {from, keys, deadline_ms}}, remaining_waiting} ->
          state =
            state
            |> Map.put(:get_many_waiting, remaining_waiting)
            |> Map.put(:get_many_waiting_count, waiting_count - 1)

          cond do
            get_many_expired?(deadline_ms) ->
              GenServer.reply(from, unavailable_get_many_values(keys))
              drain_get_many_waiting(state)

            not get_many_caller_alive?(from) ->
              drain_get_many_waiting(state)

            true ->
              state
              |> start_get_many_worker(from, keys, deadline_ms)
              |> drain_get_many_waiting()
          end

        {:empty, _waiting} ->
          state
          |> Map.put(:get_many_waiting, :queue.new())
          |> Map.put(:get_many_waiting_count, 0)
      end
    else
      state
    end
  end

  defp safe_get_many_values(keys, state, deadline_ms) do
    if get_many_expired?(deadline_ms) do
      unavailable_get_many_values(keys)
    else
      result =
        try do
          get_many_values(keys, state, deadline_ms)
        rescue
          _read_error -> @get_many_failed_error
        catch
          _kind, _reason -> @get_many_failed_error
        end

      if get_many_expired?(deadline_ms),
        do: unavailable_get_many_values(keys),
        else: result
    end
  end

  defp get_many_read_state(state) do
    Map.take(state, [
      :keydir,
      :shard_data_path,
      :data_dir,
      :index,
      :shard_index,
      :instance_ctx,
      :get_many_pread_batch,
      :get_many_waraft_batch
    ])
  end

  defp get_many_max_concurrency(state) do
    case Map.get(state, :get_many_max_concurrency) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> @default_get_many_max_concurrency
    end
  end

  defp get_many_max_queued(state) do
    case Map.get(state, :get_many_max_queued) do
      value when is_integer(value) and value >= 0 -> value
      _invalid -> @default_get_many_max_queued
    end
  end

  defp new_get_many_deadline(state) do
    System.monotonic_time(:millisecond) + get_many_deadline_duration(state)
  end

  defp get_many_deadline_duration(state) do
    case Map.get(state, :get_many_deadline_ms) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> @default_get_many_deadline_ms
    end
  end

  defp get_many_expired?(deadline_ms),
    do: System.monotonic_time(:millisecond) >= deadline_ms

  defp get_many_remaining_ms(deadline_ms) do
    max(deadline_ms - System.monotonic_time(:millisecond), 0)
  end

  defp unavailable_get_many_values(keys), do: List.duplicate(:unavailable, length(keys))

  defp cancel_get_many_timer(nil), do: false
  defp cancel_get_many_timer(timer_ref), do: Process.cancel_timer(timer_ref)

  defp reply_get_many(nil, _result), do: :ok
  defp reply_get_many(from, result), do: GenServer.reply(from, result)

  defp get_many_caller_alive?({pid, _tag}) when is_pid(pid), do: Process.alive?(pid)
  defp get_many_caller_alive?(_from), do: true

  defp flush_pending_get_many_keys(state, keys) do
    if Enum.any?(keys, &ShardETS.pending_cold?(state, &1)) do
      ShardFlush.flush_pending_for_read(state)
    else
      state
    end
  end

  defp get_many_values([], _state, _deadline_ms), do: []

  defp get_many_values(keys, state, deadline_ms) do
    {results, file_reads, segment_reads} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({%{}, [], []}, fn {key, index}, {results, file_reads, segment_reads} ->
        case ShardETS.ets_lookup(state, key, state.expiry_context) do
          {:hit, value, _expire_at_ms} ->
            {Map.put(results, index, value), file_reads, segment_reads}

          :expired ->
            {Map.put(results, index, nil), file_reads, segment_reads}

          :miss ->
            {Map.put(results, index, nil), file_reads, segment_reads}

          {:error, :invalid_keydir_entry} ->
            {Map.put(results, index, @invalid_keydir_failure), file_reads, segment_reads}

          {:error, {:storage_read_failed, _reason}} = failure ->
            {Map.put(results, index, failure), file_reads, segment_reads}

          {:cold, fid, off, vsize, exp}
          when valid_waraft_segment_location(fid, off, vsize) ->
            read = {index, key, exp, fid, off, vsize}
            {results, file_reads, [read | segment_reads]}

          {:cold, fid, off, vsize, exp} ->
            path = ShardETS.file_path(state.shard_data_path, fid)
            read = {index, key, exp, fid, off, vsize, path}
            {results, [read | file_reads], segment_reads}
        end
      end)

    results = read_get_many_files(state, results, Enum.reverse(file_reads), deadline_ms)
    results = read_get_many_segments(state, results, Enum.reverse(segment_reads), deadline_ms)

    Enum.map(0..(length(keys) - 1), &Map.get(results, &1))
  end

  defp read_get_many_files(_state, results, [], _deadline_ms), do: results

  defp read_get_many_files(state, results, reads, deadline_ms) do
    locations =
      Enum.map(reads, fn {_index, key, _exp, _fid, off, _vsize, path} ->
        {path, off, key}
      end)

    values =
      case get_many_pread_batch(state, locations, deadline_ms) do
        {:ok, values} when is_list(values) and length(values) == length(reads) -> values
        _error -> List.duplicate(:unavailable, length(reads))
      end

    reads
    |> Enum.zip(values)
    |> Enum.reduce(results, fn
      {{index, key, exp, fid, off, vsize, _path}, value}, acc when is_binary(value) ->
        Map.put(
          acc,
          index,
          materialize_get_many_value(state, key, exp, fid, off, vsize, value, deadline_ms)
        )

      {{index, _key, _exp, _fid, _off, _vsize, _path}, _error}, acc ->
        Map.put(acc, index, :unavailable)
    end)
  end

  defp read_get_many_segments(_state, results, [], _deadline_ms), do: results

  defp read_get_many_segments(state, results, reads, deadline_ms) do
    reads
    |> Enum.group_by(fn {_index, _key, _exp, file_id, _off, _vsize} -> file_id end)
    |> Enum.reduce(results, fn {file_id, segment_reads}, acc ->
      values_by_key = read_get_many_segment_group(state, file_id, segment_reads, deadline_ms)

      Enum.reduce(segment_reads, acc, fn {index, key, exp, ^file_id, off, vsize}, group_acc ->
        value =
          case Map.fetch(values_by_key, key) do
            {:ok, encoded_value} when is_binary(encoded_value) ->
              materialize_get_many_value(
                state,
                key,
                exp,
                file_id,
                off,
                vsize,
                encoded_value,
                deadline_ms
              )

            _missing_or_invalid ->
              :unavailable
          end

        Map.put(group_acc, index, value)
      end)
    end)
  end

  defp read_get_many_segment_group(state, file_id, reads, deadline_ms) do
    case get_many_remaining_ms(deadline_ms) do
      0 ->
        %{}

      timeout_ms ->
        keys = Enum.map(reads, fn {_index, key, _exp, ^file_id, _off, _vsize} -> key end)

        case get_many_waraft_batch(state, file_id, keys, timeout_ms) do
          {:ok, values} when is_map(values) -> values
          _error -> %{}
        end
    end
  end

  defp get_many_waraft_batch(state, file_id, keys, timeout_ms) do
    ctx = Map.get(state, :instance_ctx)
    shard_index = shard_index(state)

    case Map.get(state, :get_many_waraft_batch) do
      fun when is_function(fun, 5) ->
        fun.(ctx, shard_index, file_id, keys, timeout_ms)

      _default ->
        Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
          ctx,
          shard_index,
          file_id,
          keys,
          timeout_ms
        )
    end
  end

  defp materialize_get_many_value(state, key, exp, fid, off, vsize, value, deadline_ms) do
    if get_many_expired?(deadline_ms) do
      :unavailable
    else
      case materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize) do
        {:ok, materialized} ->
          if(get_many_expired?(deadline_ms), do: :unavailable, else: materialized)

        {:error, _reason} ->
          :unavailable
      end
    end
  end

  defp get_many_pread_batch(state, locations, deadline_ms) do
    case get_many_remaining_ms(deadline_ms) do
      0 ->
        {:error, :deadline_exceeded}

      timeout_ms ->
        case Map.get(state, :get_many_pread_batch) do
          fun when is_function(fun, 2) -> fun.(locations, timeout_ms)
          _default -> ColdRead.pread_batch_keyed(locations, timeout_ms)
        end
    end
  end

  @spec handle_get(binary(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_get(key, from, state) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} ->
        {:reply, value, state}

      :expired ->
        {:reply, nil, state}

      {:error, :invalid_keydir_entry} ->
        {:reply, @invalid_keydir_failure, state}

      {:error, {:storage_read_failed, _reason}} = failure ->
        {:reply, failure, state}

      {:cold, fid, off, vsize, exp} when valid_waraft_segment_location(fid, off, vsize) ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            reply_cold_value(state, key, value, exp, fid, off, vsize)

          failed_read ->
            {:reply, cold_read_failure(failed_read), state}
        end

      {:cold, fid, off, vsize, exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)
        submit_cold_read(p, off, key, state, {from, key, exp, fid, off, vsize})

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          state = ShardFlush.flush_pending_for_read(state)
          {:reply, do_get(state, key), state}
        else
          {:reply, nil, state}
        end
    end
  end

  # Returns {file_path, value_offset, value_size} for sendfile optimization,
  # or nil if the key is not found / expired / only in ETS (hot cache).
  # The offset stored in ETS is the RECORD offset (start of header).
  # For sendfile, we need the VALUE offset = record_offset + 26 (header) + key_len.
  @spec handle_get_file_ref(binary(), map()) ::
          {:reply, {binary(), non_neg_integer(), non_neg_integer()} | nil | ReadResult.failure(),
           map()}
  @doc false
  def handle_get_file_ref(key, state) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, _value, _expire_at_ms} ->
        # Key is hot (in ETS). The value may not yet be flushed to disk,
        # so we cannot safely sendfile. Return nil to fall back to normal path.
        {:reply, nil, state}

      :expired ->
        {:reply, nil, state}

      {:error, :invalid_keydir_entry} ->
        {:reply, @invalid_keydir_failure, state}

      {:error, {:storage_read_failed, _reason}} = failure ->
        {:reply, failure, state}

      {:cold, fid, off, vsize, _exp} when valid_waraft_segment_location(fid, off, vsize) ->
        {:reply, nil, state}

      {:cold, fid, off, vsize, _exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)
        {:reply, validated_file_ref(p, off, key, vsize), state}

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          state = ShardFlush.flush_pending_for_read(state)
          {:reply, file_ref_from_lookup(state, key), state}
        else
          {:reply, nil, state}
        end
    end
  end

  @spec handle_get_meta(binary(), map()) ::
          {:reply, {term(), non_neg_integer()} | nil | ReadResult.failure(), map()}
  @doc false
  def handle_get_meta(key, state) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, expire_at_ms} ->
        {:reply, {value, expire_at_ms}, state}

      :expired ->
        {:reply, nil, state}

      {:error, :invalid_keydir_entry} ->
        {:reply, @invalid_keydir_failure, state}

      {:error, {:storage_read_failed, _reason}} = failure ->
        {:reply, failure, state}

      {:cold, fid, off, vsize, exp} ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            reply_cold_meta_value(state, key, value, exp, fid, off, vsize)

          failed_read ->
            {:reply, cold_read_failure(failed_read), state}
        end

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          state = ShardFlush.flush_pending_for_read(state)
          {:reply, do_get_meta(state, key), state}
        else
          {:reply, nil, state}
        end
    end
  end

  @spec handle_get_meta(binary(), GenServer.from(), map()) ::
          {:reply, {term(), non_neg_integer()} | nil | ReadResult.failure(), map()}
          | {:noreply, map()}
  @doc false
  def handle_get_meta(key, from, state) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, expire_at_ms} ->
        {:reply, {value, expire_at_ms}, state}

      :expired ->
        {:reply, nil, state}

      {:error, :invalid_keydir_entry} ->
        {:reply, @invalid_keydir_failure, state}

      {:error, {:storage_read_failed, _reason}} = failure ->
        {:reply, failure, state}

      {:cold, fid, off, vsize, exp} when valid_waraft_segment_location(fid, off, vsize) ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            reply_cold_meta_value(state, key, value, exp, fid, off, vsize)

          failed_read ->
            {:reply, cold_read_failure(failed_read), state}
        end

      {:cold, fid, off, vsize, exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)
        submit_cold_read(p, off, key, state, {from, key, :meta, exp, fid, off, vsize})

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          state = ShardFlush.flush_pending_for_read(state)
          {:reply, do_get_meta(state, key), state}
        else
          {:reply, nil, state}
        end
    end
  end

  @spec handle_exists(binary(), map()) ::
          {:reply, boolean() | ReadResult.failure(), map()}
  @doc false
  def handle_exists(key, state) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, _value, _expire_at_ms} ->
        {:reply, true, state}

      {:cold, _fid, _off, _vsize, _exp} ->
        # Cold key — value evicted from RAM but key exists on disk.
        {:reply, true, state}

      :expired ->
        {:reply, false, state}

      {:error, :invalid_keydir_entry} ->
        {:reply, true, state}

      {:error, {:storage_read_failed, _reason}} = failure ->
        {:reply, failure, state}

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          {:reply, true, state}
        else
          {:reply, false, state}
        end
    end
  end

  @spec handle_keys(map()) :: {:reply, [binary()] | ReadResult.failure(), map()}
  @doc false
  def handle_keys(state) do
    # ETS is the read model for live keys, including pending writes that have
    # not reached Bitcask yet. Keep KEYS off the synchronous disk-flush path.
    {:reply, live_keys(state), state}
  end

  # -------------------------------------------------------------------
  # Internal read helpers
  # -------------------------------------------------------------------

  @spec do_get(map(), binary()) :: term() | nil | ReadResult.failure()
  @doc false
  def do_get(state, key) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} ->
        value

      {:cold, fid, off, vsize, exp} ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            case materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize) do
              {:ok, materialized} -> materialized
              {:error, reason} -> ReadResult.failure({:cold_read_failed, reason})
            end

          failed_read ->
            cold_read_failure(failed_read)
        end

      :expired ->
        nil

      {:error, :invalid_keydir_entry} ->
        @invalid_keydir_failure

      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      :miss ->
        nil
    end
  end

  defp file_ref_from_lookup(state, key) do
    case ShardETS.ets_lookup(state, key) do
      {:cold, fid, off, vsize, _exp} when valid_waraft_segment_location(fid, off, vsize) ->
        nil

      {:cold, fid, off, vsize, _exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)
        validated_file_ref(p, off, key, vsize)

      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      _ ->
        nil
    end
  end

  defp validated_file_ref(path, record_offset, key, value_size) do
    case Ferricstore.Bitcask.NIF.v2_validate_value_ref(path, record_offset, key, value_size) do
      {:ok, {value_offset, ^value_size}} -> {path, value_offset, value_size}
      _ -> nil
    end
  end

  @spec do_get_meta(map(), binary()) ::
          {term(), non_neg_integer()} | nil | ReadResult.failure()
  @doc false
  def do_get_meta(state, key) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, expire_at_ms} ->
        {value, expire_at_ms}

      {:cold, fid, off, vsize, exp} ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            case materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize) do
              {:ok, materialized} -> {materialized, exp}
              {:error, reason} -> ReadResult.failure({:cold_read_failed, reason})
            end

          failed_read ->
            cold_read_failure(failed_read)
        end

      :expired ->
        nil

      {:error, :invalid_keydir_entry} ->
        @invalid_keydir_failure

      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      :miss ->
        nil
    end
  end

  # Local read for transaction closures. Failed live reads remain failures so
  # read-modify-write commands cannot reinterpret corruption as key absence.
  @spec v2_local_read(map(), binary()) :: {:ok, term()} | ReadResult.failure()
  @doc false
  def v2_local_read(state, key) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} ->
        {:ok, value}

      {:cold, fid, off, _vsize, _expire_at_ms} ->
        state
        |> read_cold_raw(fid, off, key)
        |> materialize_v2_local_read(state)

      :expired ->
        {:ok, nil}

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          ReadResult.failure(:pending_cold_write)
        else
          {:ok, nil}
        end

      {:error, :invalid_keydir_entry} ->
        ReadResult.failure(:invalid_keydir_entry)

      {:error, {:storage_read_failed, _reason}} = failure ->
        failure
    end
  end

  defp materialize_v2_local_read({:ok, value}, state) when value != nil do
    case materialize_blob_value(state, value) do
      {:ok, materialized} when materialized != nil -> {:ok, materialized}
      {:ok, nil} -> ReadResult.failure({:cold_read_failed, :missing_live_cold_entry})
      {:error, reason} -> ReadResult.failure({:cold_read_failed, reason})
    end
  end

  defp materialize_v2_local_read({:ok, nil}, _state),
    do: ReadResult.failure({:cold_read_failed, :missing_live_cold_entry})

  defp materialize_v2_local_read({:error, reason}, _state),
    do: ReadResult.failure({:cold_read_failed, reason})

  defp submit_cold_read(path, offset, expected_key, state, pending_entry) do
    corr_id = state.next_correlation_id + 1

    case NIF.v2_pread_at_key_async(self(), corr_id, path, offset, expected_key) do
      :ok ->
        timer_ref =
          Process.send_after(self(), {:cold_read_timeout, corr_id}, @cold_read_timeout_ms)

        {:noreply,
         %{
           state
           | next_correlation_id: corr_id,
             pending_reads:
               Map.put(state.pending_reads, corr_id, {:pending_read, pending_entry, timer_ref})
         }}

      {:error, reason} ->
        ColdRead.emit_pread_error(path, reason)
        {:reply, ReadResult.failure({:cold_read_failed, reason}), state}
    end
  end

  defp read_cold_async(path, offset, expected_key, timeout_ms) do
    Ferricstore.Store.ColdRead.pread_keyed(path, offset, expected_key, timeout_ms)
  end

  defp read_cold_raw(state, file_id, offset, expected_key) do
    read_cold_raw(state, file_id, offset, expected_key, @cold_read_timeout_ms)
  end

  defp read_cold_raw(state, file_id, _offset, expected_key, _timeout_ms)
       when valid_waraft_segment_location(file_id, 0, 0) do
    case shard_index(state) do
      idx when is_integer(idx) and idx >= 0 ->
        Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
          Map.get(state, :instance_ctx),
          idx,
          file_id,
          expected_key
        )

      _ ->
        {:error, :missing_shard_index}
    end
  end

  defp read_cold_raw(state, file_id, offset, expected_key, timeout_ms) do
    state.shard_data_path
    |> ShardETS.file_path(file_id)
    |> read_cold_async(offset, expected_key, timeout_ms)
  end

  defp shard_index(%{index: index}), do: index
  defp shard_index(%{shard_index: shard_index}), do: shard_index
  defp shard_index(_state), do: nil

  defp reply_cold_value(state, key, value, exp, fid, off, vsize) do
    case materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize) do
      {:ok, materialized} -> {:reply, materialized, state}
      {:error, reason} -> {:reply, ReadResult.failure({:cold_read_failed, reason}), state}
    end
  end

  defp reply_cold_meta_value(state, key, value, exp, fid, off, vsize) do
    case materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize) do
      {:ok, materialized} -> {:reply, {materialized, exp}, state}
      {:error, reason} -> {:reply, ReadResult.failure({:cold_read_failed, reason}), state}
    end
  end

  defp materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize) do
    case materialize_blob_value(state, value) do
      {:ok, materialized} when materialized != nil ->
        ShardETS.cold_read_warm_ets(state, key, materialized, exp, fid, off, vsize)
        {:ok, materialized}

      {:ok, nil} ->
        {:error, :missing_live_cold_entry}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp cold_read_failure({:error, reason}),
    do: ReadResult.failure({:cold_read_failed, reason})

  defp cold_read_failure({:ok, nil}),
    do: ReadResult.failure({:cold_read_failed, :missing_live_cold_entry})

  defp cold_read_failure(invalid),
    do: ReadResult.failure({:cold_read_failed, {:invalid_read_result, invalid}})

  defp materialize_blob_value(%{data_dir: data_dir, index: shard_index} = state, value) do
    BlobValue.maybe_materialize(data_dir, shard_index, blob_side_channel_threshold(state), value)
  end

  defp blob_side_channel_threshold(%{instance_ctx: ctx}), do: BlobValue.threshold(ctx)
  defp blob_side_channel_threshold(_state), do: 0

  @spec live_keys(map()) :: [binary()] | ReadResult.failure()
  @doc false
  def live_keys(state) do
    expiry_context = ExpiryContext.capture()
    now = ExpiryContext.now_ms(expiry_context)

    {live_keys, expired_entries, failure} =
      :ets.foldl(
        fn
          _entry, {live, expired, {:error, {:storage_read_failed, _reason}} = failure} ->
            {live, expired, failure}

          {_key, _value, exp, _lfu, _fid, _off, _vsize} = entry, {live, expired, nil}
          when is_integer(exp) and exp > 0 and exp <= now ->
            case ExpiryContext.classify(expiry_context, exp) do
              {:unsafe, :hlc_drift_exceeded} ->
                {live, expired, ReadResult.failure(:hlc_drift_exceeded)}

              :expired ->
                {live, [entry | expired], nil}
            end

          {key, _value, _exp, _lfu, _fid, _off, _vsize}, {live, expired, nil} ->
            {[key | live], expired, nil}
        end,
        {[], [], nil},
        state.keydir
      )

    case failure do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      nil ->
        Enum.each(expired_entries, &ShardETS.delete_exact_entry(state, &1))
        Enum.reject(live_keys, &InternalKey.internal?/1)
    end
  end
end
