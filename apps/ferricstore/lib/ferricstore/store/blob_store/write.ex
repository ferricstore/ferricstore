defmodule Ferricstore.Store.BlobStore.Write do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore.TableOwner

      @doc """
      Stores `payload` in the shard append segment and returns the small ref.
      """
      @spec put(binary(), non_neg_integer(), binary()) :: {:ok, BlobRef.t()} | {:error, reason()}
      def put(data_dir, shard_index, payload)
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
                 is_binary(payload) do
        batch = prepare_single_payload_batch(payload)

        with_blob_lock(data_dir, shard_index, fn ->
          case do_put_many(data_dir, shard_index, batch) do
            {:ok, [ref]} -> {:ok, ref}
            {:ok, refs} when is_list(refs) -> {:error, {:unexpected_blob_ref_count, length(refs)}}
            {:error, _reason} = error -> error
          end
        end)
      end

      @doc false
      @spec put_protected(binary(), non_neg_integer(), binary()) ::
              {:ok, BlobRef.t(), protection_token()} | {:error, reason()}
      def put_protected(data_dir, shard_index, payload)
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
                 is_binary(payload) do
        batch = prepare_single_payload_batch(payload)

        with_blob_lock(data_dir, shard_index, fn ->
          case do_put_many(data_dir, shard_index, batch) do
            {:ok, [ref]} -> {:ok, ref, protect_refs(data_dir, shard_index, [ref])}
            {:ok, refs} when is_list(refs) -> {:error, {:unexpected_blob_ref_count, length(refs)}}
            {:error, _reason} = error -> error
          end
        end)
      end

      defp prepare_single_payload_batch(payload) do
        size = byte_size(payload)
        checksum = :crypto.hash(:sha256, payload)
        entry = %{payload: payload, checksum: checksum, size: size}

        %{
          unique_entries: [entry],
          value_indexes: [0],
          batch_bytes: @segment_header_bytes + size,
          error_ref: %BlobRef{checksum: checksum, size: size}
        }
      end

      @doc """
      Stores payloads in one append batch and fsyncs the segment once.
      """
      @spec put_many(binary(), non_neg_integer(), [binary()]) ::
              {:ok, [BlobRef.t()]} | {:error, reason()}
      def put_many(data_dir, shard_index, [payload])
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
                 is_binary(payload) do
        case put(data_dir, shard_index, payload) do
          {:ok, ref} -> {:ok, [ref]}
          {:error, _reason} = error -> error
        end
      end

      def put_many(data_dir, shard_index, [_payload])
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
        {:error, :invalid_blob_payload}
      end

      def put_many(data_dir, shard_index, payloads)
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
                 is_list(payloads) do
        case prepare_payload_batch(payloads) do
          {:ok, %{batch_bytes: 0}} ->
            {:ok, []}

          {:ok, batch} ->
            with_blob_lock(data_dir, shard_index, fn ->
              do_put_many(data_dir, shard_index, batch)
            end)

          {:error, :invalid_blob_payload} = error ->
            error
        end
      end

      @doc false
      @spec put_many_protected(binary(), non_neg_integer(), [binary()]) ::
              {:ok, [BlobRef.t()], protection_token()} | {:error, reason()}
      def put_many_protected(data_dir, shard_index, payloads)
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
                 is_list(payloads) do
        case prepare_payload_batch(payloads) do
          {:ok, %{batch_bytes: 0}} ->
            {:ok, [], nil}

          {:ok, batch} ->
            with_blob_lock(data_dir, shard_index, fn ->
              case do_put_many(data_dir, shard_index, batch) do
                {:ok, refs} -> {:ok, refs, protect_refs(data_dir, shard_index, refs)}
                {:error, _reason} = error -> error
              end
            end)

          {:error, :invalid_blob_payload} = error ->
            error
        end
      end

      defp do_put_many(data_dir, shard_index, batch) do
        fallback_path = segment_path(data_dir, shard_index, @segment_id)

        result =
          case do_put_many_once(data_dir, shard_index, batch) do
            {:error, :blob_segment_dir_missing} ->
              clear_active_segment_cache(data_dir, shard_index)
              clear_segment_dir_cache(data_dir, shard_index)
              do_put_many_once(data_dir, shard_index, batch)

            other ->
              other
          end

        case result do
          {:ok, refs} ->
            {:ok, refs}

          {:error, reason} = error ->
            emit_error(:put, shard_index, fallback_path, batch.error_ref, reason)
            recover_shard(data_dir, shard_index)
            error
        end
      end

      defp do_put_many_once(data_dir, shard_index, batch) do
        with {:ok, _stats} <- ensure_recovered(data_dir, shard_index),
             :ok <- ensure_segment_dir(data_dir, shard_index),
             {:ok, segment} <- writable_segment(data_dir, shard_index, batch.batch_bytes),
             :ok <- ensure_safe_segment_file_for_append(segment.path) do
          case File.open(segment.path, [:append, :raw, :binary]) do
            {:ok, io} ->
              try do
                case build_segment_records(batch, segment.id, segment.start_offset) do
                  {:ok, refs, iodata, next_offset} ->
                    case append_and_sync_segment(io, segment, shard_index, iodata) do
                      :ok ->
                        cache_active_segment(
                          data_dir,
                          shard_index,
                          segment.id,
                          segment.path,
                          next_offset
                        )

                        {:ok, refs}

                      {:error, _reason} = error ->
                        error
                    end

                  {:error, _reason} = error ->
                    error
                end
              after
                :file.close(io)
              end

            {:error, :enoent} ->
              {:error, :blob_segment_dir_missing}

            {:error, reason} ->
              {:error, reason}
          end
        end
      end

      defp append_and_sync_segment(io, segment, shard_index, iodata) do
        with :ok <- write_file(io, iodata),
             :ok <- fsync_file(segment.path),
             :ok <- maybe_fsync_new_segment_dir(Path.dirname(segment.path), segment.file_existed?) do
          :ok
        else
          {:error, _reason} = error ->
            rollback_segment_append(io, segment.path, shard_index, segment.start_offset)
            error
        end
      end

      defp rollback_segment_append(io, path, shard_index, start_offset) do
        with {:ok, _} <- :file.position(io, start_offset),
             :ok <- :file.truncate(io) do
          :ok
        else
          {:error, reason} ->
            emit_error(:rollback_append, shard_index, path, empty_blob_error_ref(), reason)
            {:error, reason}
        end
      end

      defp build_segment_records(batch, segment_id, start_offset) do
        Enum.reduce_while(batch.unique_entries, {:ok, [], [], start_offset}, fn entry,
                                                                                {:ok, refs,
                                                                                 records,
                                                                                 record_offset} ->
          payload_offset = record_offset + @segment_header_bytes
          ref = blob_ref_from_prehashed_segment(entry, segment_id, payload_offset)
          next_offset = payload_offset + entry.size
          record = [segment_header(ref), entry.payload]

          {:cont, {:ok, [ref | refs], [record | records], next_offset}}
        end)
        |> case do
          {:ok, unique_refs, records, next_offset} ->
            unique_refs = Enum.reverse(unique_refs)
            unique_refs_tuple = List.to_tuple(unique_refs)
            refs = Enum.map(batch.value_indexes, &elem(unique_refs_tuple, &1))
            {:ok, refs, Enum.reverse(records), next_offset}

          {:error, _reason} = error ->
            error
        end
      end

      defp blob_ref_from_prehashed_segment(entry, segment_id, offset) do
        %BlobRef{
          version: 2,
          checksum: entry.checksum,
          size: entry.size,
          segment_id: segment_id,
          offset: offset
        }
      end

      defp write_file(io, iodata) do
        with :ok <-
               (case blob_store_write_hook() do
                  fun when is_function(fun, 2) -> fun.(io, iodata)
                  _ -> :file.write(io, iodata)
                end) do
          Ferricstore.FaultInjection.maybe_pause(:after_blob_store_write, %{
            bytes: :erlang.iolist_size(iodata)
          })
        end
      end

      defp blob_store_write_hook do
        Process.get(:ferricstore_blob_store_write_hook) ||
          Application.get_env(:ferricstore, :blob_store_write_hook)
      end

      defp prepare_payload_batch(payloads) do
        Enum.reduce_while(payloads, {:ok, [], %{}, [], 0, nil, 0}, fn
          payload, {:ok, entries, seen, indexes, bytes, error_ref, unique_count}
          when is_binary(payload) ->
            size = byte_size(payload)
            checksum = :crypto.hash(:sha256, payload)
            key = {size, checksum}

            case find_seen_payload(Map.get(seen, key, []), payload) do
              {:ok, index} ->
                {:cont, {:ok, entries, seen, [index | indexes], bytes, error_ref, unique_count}}

              :error ->
                entry = %{payload: payload, checksum: checksum, size: size}

                seen =
                  Map.update(
                    seen,
                    key,
                    [{payload, unique_count}],
                    &[{payload, unique_count} | &1]
                  )

                error_ref = error_ref || %BlobRef{checksum: checksum, size: size}

                {:cont,
                 {:ok, [entry | entries], seen, [unique_count | indexes],
                  bytes + @segment_header_bytes + size, error_ref, unique_count + 1}}
            end

          _payload, {:ok, _entries, _seen, _indexes, _bytes, _error_ref, _unique_count} ->
            {:halt, {:error, :invalid_blob_payload}}
        end)
        |> case do
          {:ok, entries, _seen, indexes, bytes, error_ref, _unique_count} ->
            {:ok,
             %{
               unique_entries: Enum.reverse(entries),
               value_indexes: Enum.reverse(indexes),
               batch_bytes: bytes,
               error_ref: error_ref || empty_blob_error_ref()
             }}

          {:error, _reason} = error ->
            error
        end
      end

      defp find_seen_payload([], _payload), do: :error

      defp find_seen_payload([{seen_payload, index} | _rest], payload)
           when seen_payload == payload,
           do: {:ok, index}

      defp find_seen_payload([_other | rest], payload), do: find_seen_payload(rest, payload)

      defp segment_header(%BlobRef{version: 2, size: size, checksum: checksum})
           when is_binary(checksum) and byte_size(checksum) == 32 do
        <<@segment_header_magic::binary, size::unsigned-big-64, checksum::binary>>
      end

      defp maybe_fsync_new_segment_dir(_dir, true), do: :ok
      defp maybe_fsync_new_segment_dir(dir, false), do: fsync_dir(dir)

      defp writable_segment(data_dir, shard_index, batch_bytes) do
        case cached_active_segment(data_dir, shard_index, batch_bytes) do
          {:ok, segment} -> {:ok, segment}
          :miss -> scan_writable_segment(data_dir, shard_index, batch_bytes)
          {:error, _reason} = error -> error
        end
      end

      defp cached_active_segment(data_dir, shard_index, batch_bytes) do
        ensure_segment_table()
        key = {data_dir, shard_index}

        case :ets.lookup(@segment_table, key) do
          [{^key, id, path, cached_size}]
          when is_integer(id) and id >= 0 and is_binary(path) and is_integer(cached_size) and
                 cached_size >= 0 ->
            if rotate_segment?(cached_size, batch_bytes) do
              rotate_after_segment(data_dir, shard_index, id)
            else
              {:ok, %{id: id, path: path, start_offset: cached_size, file_existed?: true}}
            end

          [] ->
            :miss

          _other ->
            :miss
        end
      end

      defp scan_writable_segment(data_dir, shard_index, batch_bytes) do
        shard_path = Ferricstore.DataDir.blob_shard_path(data_dir, shard_index)

        with {:ok, latest} <- latest_segment(shard_path),
             {:ok, next_id} <- read_next_segment_id(Path.join(shard_path, "segments")) do
          case latest do
            nil ->
              id = next_id || @segment_id
              path = segment_path(data_dir, shard_index, id)

              {:ok,
               %{
                 id: id,
                 path: path,
                 start_offset: 0,
                 file_existed?: Ferricstore.FS.exists?(path)
               }}

            %{id: id, path: path, size: size} ->
              if rotate_segment?(size, batch_bytes) do
                new_id = max(next_id || 0, id + 1)
                new_path = segment_path(data_dir, shard_index, new_id)

                {:ok,
                 %{
                   id: new_id,
                   path: new_path,
                   start_offset: 0,
                   file_existed?: Ferricstore.FS.exists?(new_path)
                 }}
              else
                cache_active_segment(data_dir, shard_index, id, path, size)
                {:ok, %{id: id, path: path, start_offset: size, file_existed?: true}}
              end
          end
        end
      end

      defp rotate_after_segment(data_dir, shard_index, id) do
        shard_path = Ferricstore.DataDir.blob_shard_path(data_dir, shard_index)

        with {:ok, next_id} <- read_next_segment_id(Path.join(shard_path, "segments")) do
          new_id = max(next_id || 0, id + 1)
          path = segment_path(data_dir, shard_index, new_id)

          {:ok,
           %{
             id: new_id,
             path: path,
             start_offset: 0,
             file_existed?: Ferricstore.FS.exists?(path)
           }}
        end
      end

      defp rotate_segment?(current_size, batch_bytes) do
        max_bytes = segment_max_bytes()
        current_size > 0 and current_size + batch_bytes > max_bytes
      end

      defp segment_max_bytes do
        case Process.get(
               :ferricstore_blob_store_segment_max_bytes,
               Application.get_env(
                 :ferricstore,
                 :blob_segment_max_bytes,
                 @default_segment_max_bytes
               )
             ) do
          value when is_integer(value) and value > 0 -> value
          _other -> @default_segment_max_bytes
        end
      end

      defp latest_segment(shard_path) do
        with {:ok, paths} <- segment_files(shard_path) do
          Enum.reduce_while(paths, {:ok, nil}, fn path, {:ok, latest} ->
            with {:ok, id} <- segment_id_from_path(path),
                 {:ok, size} <- safe_segment_lstat_size(path) do
              latest =
                case latest do
                  nil -> %{id: id, path: path, size: size}
                  %{id: current_id} when id > current_id -> %{id: id, path: path, size: size}
                  _other -> latest
                end

              {:cont, {:ok, latest}}
            else
              {:error, reason} ->
                {:halt, {:error, reason}}
            end
          end)
        end
      end

      defp ensure_recovered(data_dir, shard_index) do
        ensure_recovery_table()
        key = {data_dir, shard_index}

        case :ets.lookup(@recovery_table, key) do
          [{^key, :recovered}] ->
            {:ok, %{segments: 0, truncated_segments: 0, truncated_bytes: 0}}

          [] ->
            recover_shard(data_dir, shard_index)
        end
      end

      defp mark_recovered(data_dir, shard_index) do
        ensure_recovery_table()
        :ets.insert(@recovery_table, {{data_dir, shard_index}, :recovered})
        :ok
      end

      defp ensure_recovery_table do
        case :ets.whereis(@recovery_table) do
          :undefined ->
            TableOwner.ensure_tables()

          tid ->
            tid
        end
      end

      defp ensure_segment_dir(data_dir, shard_index) do
        ensure_dir_table()
        key = {data_dir, shard_index}
        dir = Path.dirname(segment_path(data_dir, shard_index, @segment_id))

        case :ets.lookup(@dir_table, key) do
          [{^key, ^dir}] ->
            :ok

          _other ->
            create_segment_dir(key, dir)
        end
      end

      defp create_segment_dir(key, dir) do
        dir_existed? = Ferricstore.FS.dir?(dir)

        with :ok <- Ferricstore.FS.mkdir_p(dir),
             :ok <- fsync_parent_after_mkdir(dir, dir_existed?) do
          :ets.insert(@dir_table, {key, dir})
          :ok
        end
      end

      defp clear_segment_dir_cache(data_dir, shard_index) do
        ensure_dir_table()
        :ets.delete(@dir_table, {data_dir, shard_index})
        :ok
      end

      defp ensure_dir_table do
        case :ets.whereis(@dir_table) do
          :undefined ->
            TableOwner.ensure_tables()

          tid ->
            tid
        end
      end

      defp cache_active_segment(data_dir, shard_index, id, path, size) do
        ensure_segment_table()
        :ets.insert(@segment_table, {{data_dir, shard_index}, id, path, size})
        :ok
      end

      defp clear_active_segment_cache(data_dir, shard_index) do
        ensure_segment_table()
        :ets.delete(@segment_table, {data_dir, shard_index})
        :ok
      end

      defp empty_blob_error_ref, do: %BlobRef{checksum: :binary.copy(<<0>>, 32), size: 0}

      # Blob segments are shard-local files. A local ETS latch is enough to
      # serialize append offsets and GC deletes without paying for :global locks.
      defp with_blob_lock(data_dir, shard_index, fun) do
        key = {data_dir, shard_index}
        held = Process.get(@held_locks_key, %{})

        case Map.get(held, key) do
          nil ->
            :ok = acquire_blob_lock(key)
            Process.put(@held_locks_key, Map.put(held, key, 1))

            try do
              fun.()
            after
              release_blob_lock(key)
            end

          count when is_integer(count) and count > 0 ->
            Process.put(@held_locks_key, Map.put(held, key, count + 1))

            try do
              fun.()
            after
              release_blob_lock(key)
            end
        end
      end

      defp acquire_blob_lock(key) do
        ensure_lock_table()

        case :ets.insert_new(@lock_table, {key, self()}) do
          true ->
            :ok

          false ->
            wait_for_blob_lock(key)
        end
      end

      defp wait_for_blob_lock(key) do
        case :ets.lookup(@lock_table, key) do
          [{^key, holder}] when is_pid(holder) ->
            if Process.alive?(holder) do
              blob_lock_backoff()
            else
              :ets.select_delete(@lock_table, [{{key, holder}, [], [true]}])
            end

          _other ->
            :ok
        end

        acquire_blob_lock(key)
      end

      defp release_blob_lock(key) do
        held = Process.get(@held_locks_key, %{})

        case Map.get(held, key) do
          count when is_integer(count) and count > 1 ->
            Process.put(@held_locks_key, Map.put(held, key, count - 1))

          1 ->
            next = Map.delete(held, key)

            if map_size(next) == 0 do
              Process.delete(@held_locks_key)
            else
              Process.put(@held_locks_key, next)
            end

            ensure_lock_table()
            :ets.select_delete(@lock_table, [{{key, self()}, [], [true]}])

          _other ->
            :ok
        end
      end

      defp blob_lock_backoff do
        receive do
        after
          @lock_retry_ms -> :ok
        end
      end

      defp ensure_lock_table do
        case :ets.whereis(@lock_table) do
          :undefined ->
            TableOwner.ensure_tables()

          tid ->
            tid
        end
      end
    end
  end
end
