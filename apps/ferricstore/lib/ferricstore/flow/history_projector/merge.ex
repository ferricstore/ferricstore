defmodule Ferricstore.Flow.HistoryProjector.Merge do
  @moduledoc false

  # Reclaims closed history files. A record of a closed file is live while
  # something still points at it: its event's LMDB history index row, its
  # value's LMDB locator, or a hot keydir row. A closed file whose live records
  # fill at most `max_live_ratio` of it is merged: its live records, and its
  # tombstones while an older file could hold what they delete, are copied, a
  # bounded chunk at a time, into new files with the next free ids of its
  # slot, which take its place in replay order (see Files). After each chunk
  # every reference is moved to its copy by compare-and-swap, so a reference
  # changed meanwhile keeps its own value.
  # The LMDB moves run in the shard's LMDB writer, after its read-modify-write
  # of the same rows. A closed file nothing points at any more is deleted,
  # after a grace period for readers that looked a location up before it
  # moved. Files from the recovery checkpoint's file on are never touched.

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.{HistoryRecoveryCheckpoint, Keys, LMDB, LMDBWriter}
  alias Ferricstore.Flow.HistoryProjector.{Files, KeyCodec, Log}
  alias Ferricstore.Flow.LMDB.ValueLocator
  alias Ferricstore.Store.Keydir
  alias Ferricstore.TermCodec

  @page 512
  @chunk_records 20_000
  @default_max_live_ratio 0.5
  @default_delete_grace_ms 60_000
  @record_overhead_bytes 32

  @type result :: %{
          merged: [{non_neg_integer(), non_neg_integer()}],
          deleted: [non_neg_integer()],
          pending_delete: [non_neg_integer()],
          skipped: [non_neg_integer()]
        }

  @doc """
  Merges and deletes the closed history files of a shard that qualify. A
  file found dead is deleted once `:delete_grace_ms` has passed since
  `:dead_since` recorded it (a map of file id to monotonic ms); it is
  returned in `pending_delete` until then.
  """
  @spec run(map(), non_neg_integer(), keyword()) :: {:ok, result()} | {:error, term()}
  def run(ctx, shard_index, opts \\ []) do
    shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index)

    state = %{
      ctx: ctx,
      shard_index: shard_index,
      shard_data_path: shard_data_path,
      lmdb_path: LMDB.path(shard_data_path),
      keydir: elem(ctx.keydir_refs, shard_index),
      max_live_ratio: Keyword.get(opts, :max_live_ratio, max_live_ratio()),
      delete_grace_ms: Keyword.get(opts, :delete_grace_ms, delete_grace_ms()),
      dead_since: Keyword.get(opts, :dead_since, %{}),
      now_ms: System.monotonic_time(:millisecond)
    }

    # One merge per shard at a time: two would pick the same output ids.
    Ferricstore.LocalLock.trans({__MODULE__, shard_data_path}, fn ->
      run_locked(state, Keyword.get(opts, :exclude, []))
    end)
  end

  defp run_locked(state, exclude) do
    with :ok <- remove_unfinished_outputs(state.shard_data_path),
         {:ok, closed} <- closed_files(state.shard_data_path) do
      Enum.reduce_while(
        closed -- exclude,
        {:ok, %{merged: [], deleted: [], pending_delete: [], skipped: [], stats: %{}}},
        fn file_id, {:ok, acc} ->
          case merge_file(state, file_id) do
            {:merged, output_id, :deleted} ->
              {:cont, {:ok, %{acc | merged: [{file_id, output_id} | acc.merged]}}}

            {:merged, output_id, :pending} ->
              {:cont,
               {:ok,
                %{
                  acc
                  | merged: [{file_id, output_id} | acc.merged],
                    pending_delete: [file_id | acc.pending_delete]
                }}}

            :deleted ->
              {:cont, {:ok, %{acc | deleted: [file_id | acc.deleted]}}}

            :pending_delete ->
              {:cont, {:ok, %{acc | pending_delete: [file_id | acc.pending_delete]}}}

            {:skip, stats} ->
              {:cont,
               {:ok,
                %{
                  acc
                  | skipped: [file_id | acc.skipped],
                    stats: Map.put(acc.stats, file_id, stats)
                }}}

            :skip ->
              {:cont, {:ok, %{acc | skipped: [file_id | acc.skipped]}}}

            {:error, reason} ->
              {:halt, {:error, {:history_merge_failed, file_id, reason}}}
          end
        end
      )
      |> case do
        {:ok, acc} ->
          {:ok,
           Map.new(acc, fn
             {:stats, stats} -> {:stats, stats}
             {key, ids} -> {key, Enum.reverse(ids)}
           end)}

        {:error, _reason} = error ->
          error
      end
    end
  end

  def max_live_ratio do
    case Application.get_env(:ferricstore, :flow_history_merge_max_live_ratio) do
      ratio when is_number(ratio) and ratio > 0 and ratio < 1 -> ratio
      _default -> @default_max_live_ratio
    end
  end

  def delete_grace_ms do
    case Application.get_env(:ferricstore, :flow_history_merge_delete_grace_ms) do
      ms when is_integer(ms) and ms >= 0 -> ms
      _default -> @default_delete_grace_ms
    end
  end

  # Closed files below both the active file and the recovery checkpoint's
  # file: recovery never needs to replay them.
  defp closed_files(shard_data_path) do
    with {:ok, file_ids} <- Files.list(shard_data_path),
         {:ok, checkpoint_file_id} <- checkpoint_file_id(shard_data_path) do
      active_id = List.last(file_ids, 0)
      limit = min(active_id, checkpoint_file_id || active_id)
      {:ok, Enum.filter(file_ids, &(&1 < limit))}
    end
  end

  defp checkpoint_file_id(shard_data_path) do
    case LMDB.get(LMDB.path(shard_data_path), HistoryRecoveryCheckpoint.key()) do
      {:ok, marker} ->
        case HistoryRecoveryCheckpoint.decode(marker) do
          {:ok, {file_id, _offset, _projected, _digest}} -> {:ok, file_id}
          {:error, _reason} -> {:ok, nil}
        end

      :not_found ->
        {:ok, nil}

      {:error, reason} ->
        {:error, {:history_checkpoint_read_failed, reason}}
    end
  end

  # A merge interrupted before its output was renamed into place left only a
  # temporary file, which nothing reads.
  defp remove_unfinished_outputs(shard_data_path) do
    case Ferricstore.FS.ls(Files.dir(shard_data_path)) do
      {:ok, names} ->
        names
        |> Enum.filter(&(Path.extname(&1) == ".merging"))
        |> Enum.reduce_while(:ok, fn name, :ok ->
          case Ferricstore.FS.rm(Path.join(Files.dir(shard_data_path), name)) do
            :ok -> {:cont, :ok}
            {:error, {:not_found, _message}} -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, {:merge_output_remove_failed, reason}}}
          end
        end)

      {:error, {:not_found, _message}} ->
        :ok

      {:error, reason} ->
        {:error, {:history_dir_list_failed, reason}}
    end
  end

  defp merge_file(state, file_id) do
    path = Files.path(state.shard_data_path, file_id)

    with {:ok, %{type: :regular, size: file_size}} <- File.lstat(path),
         {:ok, older?} <- older_file?(state.shard_data_path, file_id),
         {:ok, stats} <- live_stats(state, file_id, path, older?) do
      cond do
        stats.unknown? ->
          {:skip, Map.put(stats, :file_size, file_size)}

        stats.live_count == 0 and stats.tombstones == 0 ->
          delete_when_settled(state, file_id)

        file_size == 0 or stats.live_bytes > state.max_live_ratio * file_size ->
          {:skip, Map.put(stats, :file_size, file_size)}

        true ->
          copy_and_move(state, file_id, path, older?)
      end
    else
      {:ok, %{type: type}} -> {:error, {:invalid_history_file_type, type}}
      {:error, _reason} = error -> error
    end
  end

  # Streams a file's records in chunks of @chunk_records; `fun` gets each
  # chunk's live records (with what points at them) and kept tombstones.
  defp reduce_chunks(state, file_id, path, older?, acc, fun) do
    result =
      Log.reduce_metadata_pages(path, {:ok, [], acc}, fn
        _record, {:error, _reason} = error ->
          error

        record, {:ok, chunk, acc} ->
          chunk = [record | chunk]

          if length(chunk) >= @chunk_records do
            case classify_chunk(state, file_id, Enum.reverse(chunk), older?, acc, fun) do
              {:ok, acc} -> {:ok, [], acc}
              {:error, _reason} = error -> error
            end
          else
            {:ok, chunk, acc}
          end
      end)

    case result do
      {:ok, {:ok, chunk, acc}} ->
        classify_chunk(state, file_id, Enum.reverse(chunk), older?, acc, fun)

      {:ok, {:error, _reason} = error} ->
        error

      {:error, reason} ->
        {:error, {:history_scan_failed, reason}}
    end
  end

  defp classify_chunk(_state, _file_id, [], _older?, acc, _fun), do: {:ok, acc}

  defp classify_chunk(state, file_id, records, older?, acc, fun) do
    with {:ok, live, tombstones, unknown?} <- classify(state, file_id, records, older?) do
      fun.(live, tombstones, unknown?, acc)
    end
  end

  defp live_stats(state, file_id, path, older?) do
    reduce_chunks(
      state,
      file_id,
      path,
      older?,
      %{live_bytes: 0, live_count: 0, tombstones: 0, unknown?: false},
      fn live, tombstones, unknown?, stats ->
        live_bytes =
          Enum.reduce(live, stats.live_bytes, fn {{key, _offset, size, _expire, _del}, _refs},
                                                 sum ->
            sum + byte_size(key) + size + @record_overhead_bytes
          end)

        {:ok,
         %{
           stats
           | live_bytes: live_bytes,
             live_count: stats.live_count + length(live),
             tombstones: stats.tombstones + length(tombstones),
             unknown?: stats.unknown? or unknown?
         }}
      end
    )
  end

  defp older_file?(shard_data_path, file_id) do
    with {:ok, file_ids} <- Files.list(shard_data_path) do
      {:ok, Enum.any?(file_ids, &(&1 < file_id))}
    end
  end

  # {:ok, [{record, refs}], tombstone_records, unknown_key?}
  defp classify(state, file_id, records, older?) do
    records
    |> Enum.chunk_every(@page)
    |> Enum.reduce_while({:ok, [], [], false}, fn page, {:ok, live, tombstones, unknown?} ->
      {page_tombstones, page_records} = Enum.split_with(page, &elem(&1, 4))
      page_tombstones = if older?, do: page_tombstones, else: []

      case references(state, file_id, page_records) do
        {:ok, referenced, page_unknown?} ->
          {:cont,
           {:ok, Enum.reverse(referenced, live), Enum.reverse(page_tombstones, tombstones),
            unknown? or page_unknown?}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, live, tombstones, unknown?} ->
        {:ok, Enum.reverse(live), Enum.reverse(tombstones), unknown?}

      {:error, _reason} = error ->
        error
    end
  end

  # The records of `file_id` something points at, each with what points at it.
  defp references(_state, _file_id, []), do: {:ok, [], false}

  defp references(state, file_id, records) do
    lmdb_keys = Enum.map(records, &reference_key(elem(&1, 0)))
    known_keys = Enum.reject(lmdb_keys, &is_nil/1)

    case LMDB.get_many(state.lmdb_path, known_keys) do
      {:ok, results} when length(results) == length(known_keys) ->
        {referenced, []} =
          records
          |> Enum.zip(lmdb_keys)
          |> Enum.flat_map_reduce(results, fn
            {record, nil}, results ->
              {record_refs(state, file_id, record, nil, nil), results}

            {record, lmdb_key}, [result | results] ->
              {record_refs(state, file_id, record, lmdb_key, result), results}
          end)

        {:ok, referenced, length(known_keys) < length(lmdb_keys)}

      {:ok, results} ->
        {:error, {:lmdb_result_count_mismatch, length(known_keys), length(results)}}

      {:error, reason} ->
        {:error, {:history_reference_read_failed, reason}}
    end
  end

  defp record_refs(state, file_id, record, lmdb_key, result) do
    refs =
      keydir_refs(state.keydir, record, file_id) ++ lmdb_refs(lmdb_key, result, record, file_id)

    if refs == [], do: [], else: [{record, refs}]
  end

  defp reference_key(key) do
    case KeyCodec.parse_history_entry_key(key) do
      {:ok, history_key, event_id, event_ms} ->
        LMDB.history_index_key(history_key, event_id, event_ms)

      :error ->
        if Keys.value_key?(key), do: key, else: nil
    end
  end

  defp keydir_refs(keydir, {key, offset, _value_size, _expire, _deleted}, file_id) do
    case :ets.lookup(keydir, key) do
      [{^key, _value, _expire_at_ms, _lfu, {:flow_history, ^file_id}, ^offset, _size} = row] ->
        [{:keydir, row}]

      _other ->
        []
    end
  rescue
    ArgumentError -> []
  end

  defp lmdb_refs(nil, _result, _record, _file_id), do: []

  defp lmdb_refs(lmdb_key, {:ok, blob}, {_key, offset, _size, _expire, _deleted}, file_id)
       when is_binary(blob) do
    case located(lmdb_key, blob) do
      {:ok, {{:flow_history, ^file_id}, ^offset, _value_size}} -> [{:lmdb, lmdb_key, blob}]
      _elsewhere_or_invalid -> []
    end
  end

  defp lmdb_refs(_lmdb_key, _result, _record, _file_id), do: []

  # Where an LMDB row says its bytes are.
  defp located(lmdb_key, blob) do
    case TermCodec.decode(blob) do
      {:ok, {:flow_value_locator, 1, _expire_at_ms, source, offset, value_size}} ->
        {:ok, {source, offset, value_size}}

      _history_row ->
        case LMDB.decode_history_index_location_row(lmdb_key, blob) do
          {:ok, {_event_id, _event_ms, _expire_at_ms, _compound_key, source, offset, value_size}} ->
            {:ok, {source, offset, value_size}}

          _other ->
            :error
        end
    end
  rescue
    _error -> :error
  end

  defp relocated(lmdb_key, blob, output_id, offset, value_size) do
    case TermCodec.decode(blob) do
      {:ok, {:flow_value_locator, 1, expire_at_ms, {:flow_history, _file_id}, _offset, _size}} ->
        ValueLocator.encode(expire_at_ms, {:flow_history, output_id}, offset, value_size)

      _history_row ->
        {:ok, {event_id, event_ms, expire_at_ms, compound_key, _source, _offset, _size}} =
          LMDB.decode_history_index_location_row(lmdb_key, blob)

        LMDB.encode_history_index_row(
          lmdb_key,
          event_id,
          event_ms,
          compound_key,
          expire_at_ms,
          {{:flow_history, output_id}, offset, value_size}
        )
    end
  end

  # Each chunk with something to keep goes to its own output file, renamed
  # into place before its references move, so neither a crash nor the size of
  # the file bounds what is held in memory.
  defp copy_and_move(state, file_id, path, older?) do
    result =
      reduce_chunks(state, file_id, path, older?, [], fn
        [], [], _unknown?, outputs ->
          {:ok, outputs}

        live, tombstones, _unknown?, outputs ->
          with {:ok, output_id} <- output_id(state.shard_data_path, file_id),
               {:ok, locations} <-
                 copy(state.shard_data_path, path, output_id, live, tombstones),
               :ok <- move_references(state, output_id, live, locations) do
            {:ok, [output_id | outputs]}
          end
      end)

    with {:ok, outputs} <- result do
      output_id = List.first(outputs)

      case delete_when_settled(state, file_id) do
        :deleted -> {:merged, output_id, :deleted}
        :pending_delete -> {:merged, output_id, :pending}
        :skip -> {:merged, output_id, :pending}
        {:error, _reason} = error -> error
      end
    end
  end

  # The next free id in the file's slot, after every id already there.
  defp output_id(shard_data_path, file_id) do
    with {:ok, file_ids} <- Files.list(shard_data_path) do
      slot = div(file_id, Files.slot())

      output_id =
        file_ids
        |> Enum.filter(&(div(&1, Files.slot()) == slot))
        |> Enum.max(fn -> file_id end)
        |> Kernel.+(1)

      if div(output_id, Files.slot()) == slot,
        do: {:ok, output_id},
        else: {:error, :history_slot_full}
    end
  end

  # Written under a temporary name and renamed into place once synced, so a
  # crash never leaves a partial file in replay order.
  defp copy(shard_data_path, source, output_id, live, tombstones) do
    final = Files.path(shard_data_path, output_id)
    temporary = Path.rootname(final) <> ".merging"

    live_offsets =
      Enum.map(live, fn {{_key, offset, _size, _expire, _deleted}, _refs} -> offset end)

    tombstone_offsets = Enum.map(tombstones, &elem(&1, 1))

    with :ok <- remove_if_present(temporary),
         {:ok, locations} <-
           NIF.v2_copy_records_preserve_tombstones(
             source,
             temporary,
             live_offsets,
             tombstone_offsets
           ),
         true <- length(locations) == length(live),
         :ok <- NIF.v2_fsync(temporary),
         :ok <- Ferricstore.FS.rename(temporary, final),
         :ok <- NIF.v2_fsync_dir(Files.dir(shard_data_path)) do
      {:ok, locations}
    else
      false -> {:error, :history_merge_copy_count_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp remove_if_present(path) do
    case Ferricstore.FS.rm(path) do
      :ok -> :ok
      {:error, {:not_found, _message}} -> :ok
      {:error, reason} -> {:error, {:merge_output_remove_failed, reason}}
    end
  end

  defp move_references(state, output_id, live, locations) do
    {lmdb_moves, keydir_moves} =
      live
      |> Enum.zip(locations)
      |> Enum.reduce({[], []}, fn {{_record, refs}, {offset, value_size}}, acc ->
        Enum.reduce(refs, acc, fn
          {:lmdb, key, blob}, {lmdb, keydir} ->
            {[{key, blob, relocated(key, blob, output_id, offset, value_size)} | lmdb], keydir}

          {:keydir, row}, {lmdb, keydir} ->
            moved = row |> put_elem(4, {:flow_history, output_id}) |> put_elem(5, offset)
            {lmdb, [{row, put_elem(moved, 6, value_size)} | keydir]}
        end)
      end)

    Enum.each(keydir_moves, fn {row, moved} -> Keydir.replace_exact(state.keydir, row, moved) end)
    move_lmdb_references(state, Enum.reverse(lmdb_moves))
  end

  defp move_lmdb_references(_state, []), do: :ok

  defp move_lmdb_references(state, moves) do
    instance_name = Map.get(state.ctx, :name, :default)
    action = {:relocate_flow_history_refs, state.lmdb_path, moves}

    with :ok <- LMDBWriter.enqueue(instance_name, state.shard_index, [], [action]) do
      LMDBWriter.flush(instance_name, state.shard_index, 60_000)
    end
  end

  @doc """
  Moves each LMDB row from `old` to `new` unless it changed meanwhile. Runs in
  the LMDB writer, so it is ordered with the writer's own read-modify-write of
  history rows.
  """
  @spec relocate(binary(), [{binary(), binary(), binary()}]) :: :ok | {:error, term()}
  def relocate(lmdb_path, moves) do
    moves
    |> Enum.chunk_every(@page)
    |> Enum.reduce_while(:ok, fn chunk, :ok ->
      case relocate_chunk(lmdb_path, chunk) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp relocate_chunk(_lmdb_path, []), do: :ok

  defp relocate_chunk(lmdb_path, chunk) do
    ops = Enum.flat_map(chunk, fn {key, old, new} -> [{:compare, key, old}, {:put, key, new}] end)

    case LMDB.write_batch(lmdb_path, ops) do
      :ok ->
        :ok

      {:error, {:compare_failed, _key}} when length(chunk) == 1 ->
        :ok

      {:error, {:compare_failed, _key}} ->
        {left, right} = Enum.split(chunk, div(length(chunk), 2))

        with :ok <- relocate_chunk(lmdb_path, left) do
          relocate_chunk(lmdb_path, right)
        end

      {:error, _reason} = error ->
        error
    end
  end

  # Deletes a closed file nothing points at once the grace period has passed
  # since it was first found so. Its tombstones are not looked at: a merged
  # file's were copied, and an unmerged one is only deleted without any.
  defp delete_when_settled(state, file_id) do
    path = Files.path(state.shard_data_path, file_id)

    with {:ok, older?} <- older_file?(state.shard_data_path, file_id),
         {:ok, %{live_count: 0, unknown?: false}} <-
           live_stats(state, file_id, path, older?) do
      dead_since = Map.get(state.dead_since, file_id, state.now_ms)

      if state.now_ms - dead_since < state.delete_grace_ms,
        do: :pending_delete,
        else: delete(state.shard_data_path, path)
    else
      {:ok, _stats} -> :skip
      {:error, _reason} = error -> error
    end
  end

  defp delete(shard_data_path, path) do
    with :ok <- Ferricstore.FS.rm(path),
         :ok <- NIF.v2_fsync_dir(Files.dir(shard_data_path)) do
      :deleted
    end
  end
end
