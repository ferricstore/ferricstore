defmodule Ferricstore.Raft.WARaftStorage.Sections.ProjectionSnapshot do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.HLC
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.LMDB, as: FlowLMDB
      alias Ferricstore.Raft.StateMachine
      alias Ferricstore.Raft.WARaftSegmentReader
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.ColdRead
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Promotion
      alias Ferricstore.Store.Shard.ETS, as: ShardETS
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      alias Ferricstore.Store.Shard.ZSetIndex

      defp prune_apply_projection_cache_after_segment_projection(
             ctx,
             shard_index,
             trim_index,
             relocations
           ) do
        keydir = elem(ctx.keydir_refs, shard_index)

        relocated_refs = apply_projection_refs_from_relocations(relocations)

        before_trim_refs =
          Ferricstore.Raft.WARaftSegmentReader.apply_projection_refs_before(
            ctx.data_dir,
            shard_index,
            trim_index
          )

        refs =
          relocated_refs
          |> Enum.concat(before_trim_refs)
          |> Enum.uniq()
          |> Enum.reject(&apply_projection_ref_still_referenced?(keydir, &1))

        Ferricstore.Raft.WARaftSegmentReader.delete_apply_projection_entries(
          ctx.data_dir,
          shard_index,
          refs
        )

        :ok
      end

      defp apply_projection_refs_from_relocations(relocations) do
        Enum.flat_map(relocations, fn
          {projection_index, relocation} when is_integer(projection_index) ->
            apply_projection_refs_from_relocations([relocation])

          {{key, _value, _expire_at_ms},
           {row_key, _ets_value, _ets_expire_at_ms, _lfu, {:waraft_apply_projection, index},
            _offset, _value_size}}
          when key == row_key and is_integer(index) and index > 0 ->
            [{index, key}]

          _relocation ->
            []
        end)
      end

      defp apply_projection_ref_still_referenced?(keydir, {index, key})
           when is_integer(index) and index > 0 and is_binary(key) do
        case :ets.lookup(keydir, key) do
          [
            {^key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, ^index}, _offset,
             _value_size}
          ] ->
            true

          _not_current ->
            false
        end
      rescue
        ArgumentError -> false
      end

      defp apply_projection_ref_still_referenced?(_keydir, _ref), do: false

      defp maybe_run_segment_projection_before_relocate_hook(
             shard_index,
             projection_root,
             relocations
           ) do
        case Application.get_env(:ferricstore, :waraft_segment_projection_before_relocate_hook) do
          hook when is_function(hook, 3) ->
            case hook.(shard_index, projection_root, relocations) do
              :ok -> :ok
              other -> {:error, {:segment_projection_before_relocate_hook, other}}
            end

          _ ->
            :ok
        end
      end

      defp relocate_segment_projection_row(
             keydir,
             projection_root,
             projection_index,
             {{key, value, expire_at_ms}, original_row}
           ) do
        with {:ok, projection_offset} <-
               projection_record_location(projection_root, projection_index) do
          compare_and_relocate_segment_projection_row(
            keydir,
            key,
            value,
            expire_at_ms,
            original_row,
            projection_index,
            projection_offset
          )
        end
      end

      defp compare_and_relocate_segment_projection_row(
             keydir,
             key,
             projected_value,
             expire_at_ms,
             {key, original_value, expire_at_ms, _original_lfu, original_file_id, original_offset,
              original_value_size},
             projection_index,
             projection_offset
           ) do
        case :ets.lookup(keydir, key) do
          [
            {^key, current_value, ^expire_at_ms, current_lfu, ^original_file_id, ^original_offset,
             ^original_value_size}
          ] ->
            if original_value == nil or current_value == original_value do
              :ets.insert(
                keydir,
                {key, current_value, expire_at_ms, current_lfu,
                 {:waraft_projection, projection_index}, projection_offset,
                 segment_projected_value_size(projected_value)}
              )
            end

            :ok

          _changed_or_deleted ->
            :ok
        end
      end

      defp compare_and_relocate_segment_projection_row(
             _keydir,
             key,
             _projected_value,
             _expire_at_ms,
             original_row,
             _projection_index,
             _projection_offset
           ),
           do: {:error, {:bad_segment_projection_relocation_row, key, original_row}}

      defp segment_projection_entry_from_keydir_row(
             {key, value, expire_at_ms, _lfu, _file_id, _offset, _value_size},
             _ctx,
             _shard_index,
             now
           )
           when is_binary(key) and is_binary(value) do
        if live_expire_at?(expire_at_ms, now) do
          {:ok, {key, value, expire_at_ms}}
        else
          :skip
        end
      end

      defp segment_projection_entry_from_keydir_row(
             {key, nil, expire_at_ms, _lfu, file_id, offset, _value_size},
             ctx,
             shard_index,
             now
           )
           when is_binary(key) do
        if live_expire_at?(expire_at_ms, now) do
          case read_keydir_cold_value(ctx, shard_index, key, file_id, offset) do
            {:ok, value} when is_binary(value) ->
              {:ok, {key, value, expire_at_ms}}

            :not_found ->
              {:error, {:segment_projection_missing_live_value, key, file_id}}

            {:error, reason} ->
              {:error, {:segment_projection_read_failed, key, file_id, reason}}
          end
        else
          :skip
        end
      end

      defp segment_projection_entry_from_keydir_row(_row, _ctx, _shard_index, _now), do: :skip

      defp read_keydir_cold_value(ctx, shard_index, key, file_id, _offset)
           when valid_segment_backed_file_id(file_id) do
        Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
          ctx,
          shard_index,
          file_id,
          key
        )
      end

      defp read_keydir_cold_value(ctx, shard_index, key, file_id, offset)
           when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 do
        path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> ShardETS.file_path(file_id)

        ColdRead.pread_keyed(path, offset, key, @cold_read_timeout_ms)
      end

      defp read_keydir_cold_value(
             ctx,
             shard_index,
             key,
             {:flow_history, file_id} = location,
             offset
           )
           when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 do
        path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> ShardETS.file_path(location)

        ColdRead.pread_keyed(path, offset, key, @cold_read_timeout_ms)
      end

      defp read_keydir_cold_value(_ctx, _shard_index, _key, file_id, _offset),
        do: {:error, {:unsupported_segment_projection_location, file_id}}

      defp flow_lmdb_path(%{data_dir: data_dir}, shard_index) do
        data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> FlowLMDB.path()
      end

      defp segment_keydir_available?(%{sm_state: %{ets: keydir}}),
        do: :ets.info(keydir) != :undefined

      defp segment_keydir_available?(_handle), do: false

      defp reduce_keydir_rows_while(keydir, acc, fun) when is_function(fun, 2) do
        case :ets.info(keydir) do
          :undefined ->
            :unavailable

          _info ->
            {:ok,
             :ets.foldl(
               fn row, next_acc ->
                 case fun.(row, next_acc) do
                   {:cont, reduced} -> reduced
                   {:halt, result} -> throw({:keydir_reduce_halt, result})
                 end
               end,
               acc,
               keydir
             )}
        end
      rescue
        ArgumentError -> :unavailable
      catch
        {:keydir_reduce_halt, result} -> result
      end

      defp live_expire_at?(0, _now), do: true

      defp live_expire_at?(expire_at_ms, now) when is_integer(expire_at_ms),
        do: expire_at_ms > now

      defp live_expire_at?(_expire_at_ms, _now), do: false

      defp write_snapshot_metadata(snapshot_path, handle, segment_projection \\ nil) do
        with {:ok, empty_payload_dirs} <- empty_snapshot_payload_kinds(snapshot_path),
             {:ok, empty_storage_payload_dirs} <-
               empty_snapshot_storage_payload_kinds(snapshot_path) do
          metadata =
            %{
              version: @version,
              position: handle.position,
              label: handle.label,
              config: handle.config,
              payload_dirs: snapshot_payload_kinds(),
              empty_payload_dirs: empty_payload_dirs,
              storage_payload_dirs: snapshot_storage_payload_kinds(),
              empty_storage_payload_dirs: empty_storage_payload_dirs
            }
            |> maybe_put_segment_projection_metadata(segment_projection)

          atomic_write_snapshot_metadata(snapshot_path, metadata)
        end
      end

      defp maybe_put_segment_projection_metadata(metadata, nil), do: metadata

      defp maybe_put_segment_projection_metadata(metadata, segment_projection),
        do: Map.put(metadata, :segment_projection, segment_projection)

      defp atomic_write_snapshot_metadata(snapshot_path, metadata) do
        with {:ok, payload} <- encode_snapshot_metadata(metadata) do
          atomic_write_binary(Path.join(snapshot_path, @snapshot_metadata_file), payload)
        end
      end

      defp encode_snapshot_metadata(metadata) do
        payload = metadata |> encode_persisted_metadata_term() |> :erlang.term_to_binary()

        if byte_size(payload) <= @max_snapshot_metadata_bytes do
          {:ok, payload}
        else
          {:error,
           {:snapshot_metadata_term_too_large, byte_size(payload), @max_snapshot_metadata_bytes}}
        end
      end

      defp read_snapshot_metadata(snapshot_path) do
        path = Path.join(snapshot_path, @snapshot_metadata_file)

        case read_snapshot_metadata_file(path) do
          {:ok, binary} ->
            case persisted_binary_to_term(binary) do
              {:ok, %{version: @version, position: _position} = metadata} ->
                validate_snapshot_metadata(metadata)

              {:ok, other} ->
                {:error, {:bad_snapshot_metadata, other}}

              {:error, reason} ->
                {:error, {:decode_snapshot_metadata, reason}}
            end

          {:error, reason} ->
            {:error, {:read_snapshot_metadata, reason}}
        end
      end

      defp read_snapshot_metadata_file(path) do
        read_bounded_metadata_file(
          path,
          @max_snapshot_metadata_bytes,
          :snapshot_metadata_file_too_large
        )
      end

      defp read_snapshot_segment_projection(
             _snapshot_path,
             %{segment_projection: nil},
             _position
           ),
           do: {:ok, []}

      defp read_snapshot_segment_projection(
             snapshot_path,
             %{segment_projection: projection_metadata},
             position
           )
           when is_map(projection_metadata) do
        projection_root = Path.join(snapshot_path, Map.fetch!(projection_metadata, :dir))
        expected_count = Map.get(projection_metadata, :count)

        with {:ok, projection} <- read_segment_projection_log(projection_root),
             :ok <- verify_segment_projection_position(projection, position),
             {:ok, entries} <- validate_segment_projection_entries(projection),
             :ok <- verify_segment_projection_count(entries, expected_count) do
          {:ok, entries}
        else
          {:error, reason} -> {:error, {:read_segment_projection_snapshot, reason}}
        end
      end

      defp read_snapshot_segment_projection(_snapshot_path, _metadata, _position), do: {:ok, []}

      defp read_segment_projection_log(projection_root) do
        case :ferricstore_waraft_spike_segment_log.fold_disk(
               to_charlist(projection_root),
               &fold_segment_projection_record/3,
               %{header: nil, entries: [], invalid: []}
             ) do
          {:ok, %{invalid: [invalid | _]}} ->
            {:error, {:bad_segment_projection_record, invalid}}

          {:ok, %{header: nil, entries: []}} ->
            {:error, :enoent}

          {:ok, %{header: nil}} ->
            {:error, :missing_segment_projection_header}

          {:ok, %{header: {position, count}, entries: entries}} ->
            entries = Enum.reverse(entries)

            with :ok <- verify_segment_projection_count(entries, count) do
              {:ok, %{version: @version, position: position, entries: entries}}
            end

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp fold_segment_projection_record(
             0,
             {0, {:ferricstore_segment_projection_header, position, count}},
             acc
           )
           when is_integer(count) and count >= 0 do
        %{acc | header: {position, count}}
      end

      defp fold_segment_projection_record(
             index,
             {0, {:ferricstore_segment_projection_entry, key, value, expire_at_ms}},
             acc
           )
           when is_integer(index) and index > 0 do
        %{acc | entries: [{key, value, expire_at_ms} | acc.entries]}
      end

      defp fold_segment_projection_record(index, entry, acc) do
        %{acc | invalid: [{index, entry} | acc.invalid]}
      end

      defp verify_segment_projection_position(%{position: expected}, expected), do: :ok

      defp verify_segment_projection_position(%{position: actual}, expected),
        do: {:error, {:bad_segment_projection_position, actual, expected}}

      defp validate_segment_projection_entries(%{entries: entries}) when is_list(entries) do
        if Enum.all?(entries, &valid_segment_projection_entry?/1) do
          {:ok, entries}
        else
          {:error, {:bad_segment_projection_entries, entries}}
        end
      end

      defp validate_segment_projection_entries(projection),
        do: {:error, {:bad_segment_projection_entries, projection}}

      defp valid_segment_projection_entry?({key, value, expire_at_ms})
           when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
                  expire_at_ms >= 0,
           do: true

      defp valid_segment_projection_entry?(_entry), do: false

      defp verify_segment_projection_count(entries, count) when is_integer(count) do
        if length(entries) == count do
          :ok
        else
          {:error, {:bad_segment_projection_count, count, length(entries)}}
        end
      end

      defp verify_segment_projection_count(_entries, _count), do: :ok

      defp read_bounded_metadata_file(path, max_bytes, too_large_reason) do
        case File.lstat(path) do
          {:ok, %{type: :regular, size: size}} when size <= max_bytes ->
            File.read(path)

          {:ok, %{type: :regular, size: size}} ->
            {:error, {too_large_reason, size, max_bytes}}

          {:ok, %{type: type}} ->
            {:error, {:unsafe_metadata_path, path, type}}

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp validate_snapshot_metadata(%{position: position, config: config} = metadata)
           when is_map(metadata) do
        with :ok <- validate_raft_position(position),
             :ok <- validate_storage_config(config),
             :ok <- validate_snapshot_payload_metadata(metadata) do
          {:ok, metadata}
        else
          {:error, reason} -> {:error, {:bad_snapshot_metadata, reason}}
        end
      end

      defp validate_snapshot_metadata(%{position: position} = metadata) when is_map(metadata) do
        validation =
          with :ok <- validate_raft_position(position),
               :ok <- validate_snapshot_payload_metadata(metadata) do
            :ok
          end

        case validation do
          :ok ->
            {:ok, metadata}

          {:error, reason} ->
            {:error, {:bad_snapshot_metadata, reason}}
        end
      end

      defp validate_snapshot_payload_metadata(metadata) do
        with :ok <-
               validate_snapshot_payload_dir_list(
                 :payload_dirs,
                 Map.get(metadata, :payload_dirs, snapshot_payload_kinds())
               ),
             :ok <-
               validate_snapshot_payload_dir_list(
                 :empty_payload_dirs,
                 Map.get(metadata, :empty_payload_dirs, [])
               ),
             :ok <-
               validate_snapshot_storage_payload_dir_list(
                 :storage_payload_dirs,
                 Map.get(metadata, :storage_payload_dirs, [])
               ),
             :ok <-
               validate_snapshot_storage_payload_dir_list(
                 :empty_storage_payload_dirs,
                 Map.get(metadata, :empty_storage_payload_dirs, [])
               ),
             :ok <- validate_segment_projection_metadata(Map.get(metadata, :segment_projection)) do
          :ok
        end
      end

      defp validate_segment_projection_metadata(nil), do: :ok

      defp validate_segment_projection_metadata(%{
             dir: @segment_projection_dir,
             format: :segment_log,
             count: count
           })
           when is_integer(count) and count >= 0,
           do: :ok

      defp validate_segment_projection_metadata(other),
        do: {:error, {:bad_segment_projection, other}}

      defp validate_snapshot_payload_dir_list(field, dirs) when is_list(dirs) do
        allowed = snapshot_payload_kinds()

        if Enum.all?(dirs, &(&1 in allowed)) do
          :ok
        else
          {:error, {:bad_payload_dirs, field, dirs}}
        end
      end

      defp validate_snapshot_payload_dir_list(field, other),
        do: {:error, {:bad_payload_dirs, field, other}}

      defp validate_snapshot_storage_payload_dir_list(field, dirs) when is_list(dirs) do
        allowed = snapshot_storage_payload_kinds()

        if Enum.all?(dirs, &(&1 in allowed)) do
          :ok
        else
          {:error, {:bad_payload_dirs, field, dirs}}
        end
      end

      defp validate_snapshot_storage_payload_dir_list(field, other),
        do: {:error, {:bad_payload_dirs, field, other}}

      defp verify_snapshot_position(%{position: expected}, expected), do: :ok

      defp verify_snapshot_position(%{position: actual}, expected),
        do: {:error, {:bad_position, actual, expected}}

      defp verify_snapshot_payload_dirs(metadata, snapshot_path, handle) do
        payload_dirs = Map.get(metadata, :payload_dirs, snapshot_payload_kinds())

        with :ok <-
               verify_snapshot_dirs(
                 metadata,
                 snapshot_path,
                 handle,
                 shard_dir_specs(handle, payload_dirs),
                 :empty_payload_dirs
               ),
             :ok <-
               verify_snapshot_dirs(
                 metadata,
                 snapshot_path,
                 handle,
                 storage_payload_dir_specs(handle, Map.get(metadata, :storage_payload_dirs, [])),
                 :empty_storage_payload_dirs
               ) do
          :ok
        end
      end

      defp verify_snapshot_dirs(_metadata, _snapshot_path, _handle, [], _empty_field), do: :ok

      defp verify_snapshot_dirs(metadata, snapshot_path, handle, specs, empty_field) do
        empty_payload_dirs = Map.get(metadata, empty_field, [])

        missing =
          Enum.reject(specs, fn {kind, _dest} ->
            Ferricstore.FS.dir?(Path.join(snapshot_path, Atom.to_string(kind)))
          end)

        forbidden_missing =
          Enum.reject(missing, fn {kind, _dest} ->
            kind in empty_payload_dirs
          end)

        cond do
          forbidden_missing == [] ->
            :ok

          bootstrap_empty_snapshot?(missing, specs, handle) ->
            :ok

          true ->
            {kind, _dest} = hd(forbidden_missing)

            {:error,
             {:missing_snapshot_dir, kind, Path.join(snapshot_path, Atom.to_string(kind))}}
        end
      end

      defp empty_snapshot_payload_kinds(snapshot_path) do
        empty_snapshot_payload_kinds(snapshot_path, snapshot_payload_kinds())
      end

      defp empty_snapshot_storage_payload_kinds(snapshot_path) do
        empty_snapshot_payload_kinds(snapshot_path, snapshot_storage_payload_kinds())
      end

      defp empty_snapshot_payload_kinds(snapshot_path, kinds) do
        kinds
        |> Enum.reduce_while({:ok, []}, fn kind, {:ok, acc} ->
          path = Path.join(snapshot_path, Atom.to_string(kind))

          case dir_payload_empty(path) do
            {:ok, true} -> {:cont, {:ok, [kind | acc]}}
            {:ok, false} -> {:cont, {:ok, acc}}
            {:error, reason} -> {:halt, {:error, {:snapshot_payload_empty, kind, reason}}}
          end
        end)
        |> case do
          {:ok, kinds} -> {:ok, Enum.reverse(kinds)}
          {:error, _reason} = error -> error
        end
      end

      # WARaft can create a metadata-only witness snapshot while bootstrapping an empty
      # member. Any non-empty/opened storage position must include payload dirs, because
      # installing a metadata-only snapshot would reset Bitcask/blob/probability files.
      defp bootstrap_empty_snapshot?(missing, specs, %{position: @zero_pos}) do
        length(missing) == length(specs) and live_snapshot_payload_empty?(specs) == {:ok, true}
      end

      defp bootstrap_empty_snapshot?(_missing, _specs, _handle), do: false

      defp live_snapshot_payload_empty?(specs) do
        Enum.reduce_while(specs, {:ok, true}, fn {_kind, dest}, {:ok, true} ->
          case dir_payload_empty(dest) do
            {:ok, true} -> {:cont, {:ok, true}}
            {:ok, false} -> {:halt, {:ok, false}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp dir_payload_empty(path) do
        case File.lstat(path) do
          {:ok, %{type: :directory}} ->
            case Ferricstore.FS.ls(path) do
              {:ok, children} -> payload_children_empty(path, children)
              {:error, reason} -> {:error, {:list_dir, path, reason}}
            end

          {:ok, %{type: type}} ->
            {:error, {:not_directory, path, type}}

          {:error, :enoent} ->
            {:ok, true}

          {:error, reason} ->
            {:error, {:stat, path, reason}}
        end
      end

      defp live_payload_empty?(ctx, shard_index) do
        %{ctx: ctx, shard_index: shard_index}
        |> shard_dir_specs()
        |> live_snapshot_payload_empty?()
      end

      defp live_storage_payload_empty?(storage_root, ctx, shard_index) do
        with {:ok, true} <- live_payload_empty?(ctx, shard_index),
             {:ok, true} <- segment_log_payload_empty?(storage_root) do
          {:ok, true}
        else
          {:ok, false} -> {:ok, false}
          {:error, _reason} = error -> error
        end
      end

      defp segment_log_payload_empty?(storage_root) do
        storage_root
        |> Path.join("segment_log")
        |> dir_payload_empty()
      end

      defp payload_children_empty(_path, []), do: {:ok, true}

      defp payload_children_empty(path, [child | rest]) do
        child_path = Path.join(path, child)

        cond do
          ignorable_payload_dir?(child_path) ->
            payload_children_empty(path, rest)

          true ->
            payload_child_empty(path, child_path, rest)
        end
      end

      defp payload_child_empty(path, child_path, rest) do
        case File.lstat(child_path) do
          {:ok, %{type: :directory}} ->
            case dir_payload_empty(child_path) do
              {:ok, true} -> payload_children_empty(path, rest)
              {:ok, false} -> {:ok, false}
              {:error, _reason} = error -> error
            end

          {:ok, %{type: :regular, size: 0}} ->
            payload_children_empty(path, rest)

          {:ok, %{type: :regular}} ->
            if ignorable_payload_marker?(child_path) do
              payload_children_empty(path, rest)
            else
              {:ok, false}
            end

          {:ok, %{type: type}} ->
            {:error, {:unsafe_snapshot_payload_path, child_path, type}}

          {:error, :enoent} ->
            payload_children_empty(path, rest)

          {:error, reason} ->
            {:error, {:stat, child_path, reason}}
        end
      end

      defp ignorable_payload_dir?(path) do
        # LMDB is a lagged/cold Flow projection. It is rebuilt from durable Flow
        # records and must not make a fresh WARaft storage bootstrap look unsafe.
        Path.basename(path) == "flow_lmdb" and Ferricstore.Flow.LMDB.env_present?(path)
      end

      defp ignorable_payload_marker?(path) do
        Path.basename(path) == "flow_history_projected.index" and
          Ferricstore.Flow.HistoryProjectedIndex.read(Path.dirname(path)) == 0
      end

      defp atomic_write_term(path, term) do
        atomic_write_binary(path, :erlang.term_to_binary(term))
      end

      defp atomic_write_binary(path, payload) do
        tmp = "#{path}.tmp.#{System.unique_integer([:positive])}"
        previous = maybe_metadata_previous_path(path)

        with :ok <- Ferricstore.FS.mkdir_p(Path.dirname(path)),
             :ok <- File.write(tmp, payload),
             :ok <- fsync_metadata_file(tmp),
             :ok <- stage_previous_metadata(path, previous),
             :ok <- Ferricstore.FS.rename(tmp, path),
             :ok <- fsync_dir(Path.dirname(path)) do
          :ok
        else
          {:error, reason} = error ->
            _ = Ferricstore.FS.rm(tmp)
            _ = restore_previous_metadata(path, previous)
            {:error, reason || error}
        end
      end

      defp maybe_metadata_previous_path(path) do
        if Path.basename(path) == @metadata_file do
          metadata_previous_path(path)
        else
          nil
        end
      end

      defp stage_previous_metadata(_path, nil), do: :ok

      defp stage_previous_metadata(path, previous) do
        case Ferricstore.FS.rename(path, previous) do
          :ok -> :ok
          {:error, {:not_found, _}} -> :ok
          {:error, reason} -> {:error, {:stage_previous_metadata, path, previous, reason}}
        end
      end

      defp restore_previous_metadata(_path, nil), do: :ok

      defp restore_previous_metadata(path, previous) do
        cond do
          Ferricstore.FS.exists?(path) ->
            :ok

          Ferricstore.FS.exists?(previous) ->
            Ferricstore.FS.rename(previous, path)

          true ->
            :ok
        end
      end

      defp restore_previous_metadata_after_publish(path) do
        previous = maybe_metadata_previous_path(path)

        cond do
          is_nil(previous) ->
            :ok

          not Ferricstore.FS.exists?(previous) ->
            :ok

          true ->
            do_restore_previous_metadata_after_publish(path, previous)
        end
      end
    end
  end
end
