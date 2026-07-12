defmodule Ferricstore.Store.BlobStore.GC do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore.TableOwner

      @spec recover_shard(binary(), non_neg_integer()) ::
              {:ok,
               %{
                 segments: non_neg_integer(),
                 truncated_segments: non_neg_integer(),
                 truncated_bytes: non_neg_integer()
               }}
              | {:error, term()}
      def recover_shard(data_dir, shard_index)
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
        clear_active_segment_cache(data_dir, shard_index)
        clear_segment_dir_cache(data_dir, shard_index)
        shard_path = Ferricstore.DataDir.blob_shard_path(data_dir, shard_index)

        with {:ok, paths} <- segment_files(shard_path) do
          latest_path = List.last(paths)

          result =
            Enum.reduce_while(
              paths,
              {:ok, %{segments: 0, truncated_segments: 0, truncated_bytes: 0}},
              fn path, {:ok, acc} ->
                case recover_segment(path, path == latest_path) do
                  {:ok, bytes} ->
                    acc = %{
                      acc
                      | segments: acc.segments + 1,
                        truncated_segments:
                          acc.truncated_segments + if(bytes > 0, do: 1, else: 0),
                        truncated_bytes: acc.truncated_bytes + bytes
                    }

                    {:cont, {:ok, acc}}

                  {:error, _reason} = error ->
                    {:halt, error}
                end
              end
            )

          case result do
            {:ok, stats} ->
              mark_recovered(data_dir, shard_index)
              {:ok, stats}

            {:error, _reason} = error ->
              error
          end
        end
      end

      @doc """
      Deletes blob files that are not present in `live_refs`.

      The caller owns producing a complete live set. This function is deliberately
      conservative for append segments: a segment is kept while any live v2 ref
      points into it, prepared refs can register a short protection token until
      replicated apply finishes, and fresh dead segments are kept for a grace window
      as a final safety net. The shard must still establish a durable WARaft replay
      boundary before calling this because pending projection data can contain blob refs.
      """
      @spec sweep_unreferenced(binary(), non_neg_integer(), Enumerable.t()) ::
              {:ok,
               %{
                 deleted_files: non_neg_integer(),
                 deleted_bytes: non_neg_integer(),
                 kept_files: non_neg_integer()
               }}
              | {:error, term()}
      def sweep_unreferenced(data_dir, shard_index, live_refs)
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
        with_blob_lock(data_dir, shard_index, fn ->
          do_sweep_unreferenced(data_dir, shard_index, live_refs)
        end)
      end

      @doc false
      @spec sweep_unreferenced_with_live_refs(
              binary(),
              non_neg_integer(),
              (-> {:ok, Enumerable.t()} | {:error, term()})
            ) ::
              {:ok,
               %{
                 deleted_files: non_neg_integer(),
                 deleted_bytes: non_neg_integer(),
                 kept_files: non_neg_integer()
               }}
              | {:error, term()}
      def sweep_unreferenced_with_live_refs(data_dir, shard_index, live_refs_fun)
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
                 is_function(live_refs_fun, 0) do
        with_blob_lock(data_dir, shard_index, fn ->
          with {:ok, live_refs} <- live_refs_fun.() do
            do_sweep_unreferenced(data_dir, shard_index, live_refs)
          end
        end)
      end

      @doc false
      @spec sweep_unreferenced_releasing_hardened(
              binary(),
              non_neg_integer(),
              [reference()],
              Enumerable.t()
            ) :: {:ok, map()} | {:error, term()}
      def sweep_unreferenced_releasing_hardened(data_dir, shard_index, hardened_ids, live_refs)
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
                 is_list(hardened_ids) do
        with_blob_lock(data_dir, shard_index, fn ->
          released = release_hardened_protections(data_dir, shard_index, hardened_ids)

          case do_sweep_unreferenced(data_dir, shard_index, live_refs) do
            {:ok, stats} -> {:ok, Map.put(stats, :hardened_protections_released, released)}
            {:error, _reason} = error -> error
          end
        end)
      end

      @doc false
      @spec sweep_unreferenced_releasing_hardened_with_live_refs(
              binary(),
              non_neg_integer(),
              [reference()],
              (-> {:ok, Enumerable.t()} | {:error, term()})
            ) :: {:ok, map()} | {:error, term()}
      def sweep_unreferenced_releasing_hardened_with_live_refs(
            data_dir,
            shard_index,
            hardened_ids,
            live_refs_fun
          )
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
                 is_list(hardened_ids) and is_function(live_refs_fun, 0) do
        with_blob_lock(data_dir, shard_index, fn ->
          released = release_hardened_protections(data_dir, shard_index, hardened_ids)

          with {:ok, live_refs} <- live_refs_fun.(),
               {:ok, stats} <- do_sweep_unreferenced(data_dir, shard_index, live_refs) do
            {:ok, Map.put(stats, :hardened_protections_released, released)}
          end
        end)
      end

      @spec storage_stats(binary()) ::
              {:ok,
               %{
                 files: non_neg_integer(),
                 bytes: non_neg_integer(),
                 legacy_files: non_neg_integer(),
                 legacy_bytes: non_neg_integer(),
                 segment_files: non_neg_integer(),
                 segment_bytes: non_neg_integer(),
                 tmp_files: non_neg_integer(),
                 tmp_bytes: non_neg_integer()
               }}
              | {:error, term()}
      def storage_stats(data_dir) when is_binary(data_dir) do
        blob_glob = Path.join([data_dir, "blob", "shard_*", "**", "*.blob"])
        segment_glob = Path.join([data_dir, "blob", "shard_*", "segments", "*.bloblog"])
        tmp_glob = Path.join([data_dir, "blob", "shard_*", "**", "*.tmp"])

        with {:ok, blob_stats} <-
               storage_stats_for_paths({Path.wildcard(blob_glob), Path.wildcard(segment_glob)}),
             {:ok, tmp_stats} <- storage_stats_for_paths(Path.wildcard(tmp_glob, match_dot: true)) do
          {:ok,
           %{
             files: blob_stats.files,
             bytes: blob_stats.bytes,
             legacy_files: blob_stats.legacy_files,
             legacy_bytes: blob_stats.legacy_bytes,
             segment_files: blob_stats.segment_files,
             segment_bytes: blob_stats.segment_bytes,
             tmp_files: tmp_stats.files,
             tmp_bytes: tmp_stats.bytes
           }}
        end
      rescue
        error -> {:error, {:blob_storage_stats_failed, error}}
      end

      defp storage_stats_for_paths({legacy_paths, segment_paths}) do
        with {:ok, legacy_stats} <- storage_stats_for_paths(legacy_paths),
             {:ok, segment_stats} <- storage_stats_for_paths(segment_paths) do
          {:ok,
           %{
             files: legacy_stats.files + segment_stats.files,
             bytes: legacy_stats.bytes + segment_stats.bytes,
             legacy_files: legacy_stats.files,
             legacy_bytes: legacy_stats.bytes,
             segment_files: segment_stats.files,
             segment_bytes: segment_stats.bytes
           }}
        end
      end

      defp storage_stats_for_paths(paths) when is_list(paths) do
        Enum.reduce_while(paths, {:ok, %{files: 0, bytes: 0}}, fn path, {:ok, acc} ->
          case File.stat(path) do
            {:ok, %{type: :regular, size: size}} ->
              {:cont, {:ok, %{files: acc.files + 1, bytes: acc.bytes + size}}}

            {:ok, %{type: type}} ->
              {:halt, {:error, {:blob_storage_stats_invalid_file, path, type}}}

            {:error, reason} ->
              {:halt, {:error, {:blob_storage_stats_stat_failed, path, reason}}}
          end
        end)
      end

      defp do_sweep_unreferenced(data_dir, shard_index, live_refs) do
        shard_path = Ferricstore.DataDir.blob_shard_path(data_dir, shard_index)
        live_paths = live_relative_paths(live_refs)

        case blob_files(shard_path) do
          {:ok, paths} ->
            pending_paths = protected_relative_paths(data_dir, shard_index)

            with {:ok, protected_paths} <-
                   protected_dead_segment_paths(shard_path, paths, live_paths),
                 protected_paths <- MapSet.union(protected_paths, pending_paths),
                 :ok <-
                   ensure_next_segment_id_for_dead_segments(
                     shard_path,
                     paths,
                     live_paths,
                     protected_paths
                   ),
                 {:ok, stats} <- sweep_blob_paths(shard_path, paths, live_paths, protected_paths) do
              if stats.deleted_files > 0 do
                clear_active_segment_cache(data_dir, shard_index)
              end

              {:ok, stats}
            end

          {:error, _reason} = error ->
            error
        end
      end

      defp live_relative_paths(live_refs) do
        Enum.reduce(live_refs, MapSet.new(), fn
          %BlobRef{} = ref, acc ->
            if BlobRef.valid?(ref), do: MapSet.put(acc, BlobRef.relative_path(ref)), else: acc

          _other, acc ->
            acc
        end)
      end

      defp protected_dead_segment_paths(shard_path, paths, live_paths) do
        Enum.reduce_while(paths, {:ok, MapSet.new()}, fn path, {:ok, protected} ->
          relative = Path.relative_to(path, shard_path)

          cond do
            MapSet.member?(live_paths, relative) ->
              {:cont, {:ok, protected}}

            fresh_blob_segment?(path) ->
              {:cont, {:ok, MapSet.put(protected, relative)}}

            true ->
              {:cont, {:ok, protected}}
          end
        end)
      end

      defp fresh_blob_segment?(path) do
        case segment_id_from_path(path) do
          {:ok, _id} ->
            fresh_segment_file?(path)

          :not_segment ->
            false

          {:error, _reason} ->
            false
        end
      end

      defp fresh_segment_file?(path) do
        grace_ms = segment_gc_grace_ms()

        grace_ms > 0 and
          case File.stat(path, time: :posix) do
            {:ok, %{type: :regular, mtime: mtime}} when is_integer(mtime) ->
              age_ms = max(System.system_time(:second) - mtime, 0) * 1_000
              age_ms < grace_ms

            _other ->
              false
          end
      end

      defp segment_gc_grace_ms do
        case Process.get(
               :ferricstore_blob_store_segment_gc_grace_ms,
               Application.get_env(
                 :ferricstore,
                 :blob_segment_gc_grace_ms,
                 @default_segment_gc_grace_ms
               )
             ) do
          value when is_integer(value) and value >= 0 -> value
          _other -> @default_segment_gc_grace_ms
        end
      end

      defp ensure_next_segment_id_for_dead_segments(
             shard_path,
             paths,
             live_paths,
             protected_paths
           ) do
        Enum.reduce_while(paths, {:ok, nil}, fn path, {:ok, max_dead_id} ->
          relative = Path.relative_to(path, shard_path)

          case segment_id_from_path(path) do
            {:ok, id} ->
              if MapSet.member?(live_paths, relative) or MapSet.member?(protected_paths, relative) do
                {:cont, {:ok, max_dead_id}}
              else
                {:cont, {:ok, max(id, max_dead_id || id)}}
              end

            :not_segment ->
              {:cont, {:ok, max_dead_id}}

            {:error, _reason} = error ->
              {:halt, error}
          end
        end)
        |> case do
          {:ok, nil} -> :ok
          {:ok, max_dead_id} -> ensure_next_segment_id_at_least(shard_path, max_dead_id + 1)
          {:error, _reason} = error -> error
        end
      end

      defp ensure_next_segment_id_at_least(shard_path, min_next_id) do
        segment_dir = Path.join(shard_path, "segments")

        with {:ok, current_next_id} <- read_next_segment_id(segment_dir) do
          if (current_next_id || 0) >= min_next_id do
            :ok
          else
            persist_next_segment_id(segment_dir, min_next_id)
          end
        end
      end

      defp read_next_segment_id(segment_dir) do
        path = Path.join(segment_dir, @segment_next_id_filename)

        case File.read(path) do
          {:ok, data} ->
            parse_next_segment_id(data, path)

          {:error, :enoent} ->
            {:ok, nil}

          {:error, reason} ->
            {:error, {:blob_segment_next_id_read_failed, path, reason}}
        end
      end

      defp parse_next_segment_id(data, path) when is_binary(data) do
        case Integer.parse(String.trim(data)) do
          {id, ""} when id >= 0 -> {:ok, id}
          _other -> {:error, {:blob_segment_next_id_invalid, path}}
        end
      end

      defp persist_next_segment_id(segment_dir, next_id)
           when is_integer(next_id) and next_id >= 0 do
        path = Path.join(segment_dir, @segment_next_id_filename)
        tmp_path = path <> ".tmp"

        result =
          with :ok <- File.write(tmp_path, Integer.to_string(next_id) <> "\n", [:binary]),
               :ok <- fsync_file(tmp_path),
               :ok <- Ferricstore.FS.rename(tmp_path, path),
               :ok <- fsync_dir(segment_dir) do
            :ok
          end

        case result do
          :ok ->
            :ok

          {:error, reason} ->
            _ = Ferricstore.FS.rm(tmp_path)
            {:error, {:blob_segment_next_id_persist_failed, path, reason}}
        end
      end

      defp segment_id_from_path(path) do
        if Path.extname(path) == ".bloblog" do
          path
          |> Path.basename(".bloblog")
          |> Integer.parse()
          |> case do
            {id, ""} when id >= 0 -> {:ok, id}
            _other -> {:error, {:invalid_blob_segment_name, path}}
          end
        else
          :not_segment
        end
      end

      defp blob_files(shard_path) do
        with {:ok, legacy_paths} <- legacy_blob_files(shard_path),
             {:ok, segment_paths} <- segment_files(shard_path) do
          {:ok, legacy_paths ++ segment_paths}
        end
      end

      defp legacy_blob_files(shard_path) do
        if Ferricstore.FS.dir?(shard_path) do
          {:ok, Path.wildcard(Path.join(shard_path, "**/*.blob"))}
        else
          {:ok, []}
        end
      rescue
        error -> {:error, {:blob_list_failed, error}}
      end

      defp segment_files(shard_path) do
        segment_path = Path.join(shard_path, "segments")

        if Ferricstore.FS.dir?(segment_path) do
          {:ok, Path.wildcard(Path.join(segment_path, "*.bloblog"))}
        else
          {:ok, []}
        end
      rescue
        error -> {:error, {:blob_segment_list_failed, error}}
      end

      defp blob_tmp_files(shard_path) do
        if Ferricstore.FS.dir?(shard_path) do
          {:ok, Path.wildcard(Path.join(shard_path, "**/*.tmp"), match_dot: true)}
        else
          {:ok, []}
        end
      rescue
        error -> {:error, {:blob_tmp_list_failed, error}}
      end

      defp sweep_blob_paths(shard_path, paths, live_paths, protected_paths) do
        result =
          Enum.reduce_while(
            paths,
            {:ok, %{deleted_files: 0, deleted_bytes: 0, kept_files: 0}, MapSet.new()},
            fn path, {:ok, stats, dirs} ->
              relative = Path.relative_to(path, shard_path)

              if MapSet.member?(live_paths, relative) or MapSet.member?(protected_paths, relative) do
                {:cont, {:ok, %{stats | kept_files: stats.kept_files + 1}, dirs}}
              else
                case delete_blob_file(path) do
                  {:ok, size} ->
                    stats = %{
                      stats
                      | deleted_files: stats.deleted_files + 1,
                        deleted_bytes: stats.deleted_bytes + size
                    }

                    {:cont, {:ok, stats, MapSet.put(dirs, Path.dirname(path))}}

                  {:error, _reason} = error ->
                    {:halt, error}
                end
              end
            end
          )

        case result do
          {:ok, stats, dirs} ->
            with {:ok, tmp_stats, tmp_dirs} <- sweep_tmp_paths(shard_path),
                 :ok <- fsync_deleted_dirs(MapSet.union(dirs, tmp_dirs)) do
              {:ok, Map.merge(stats, tmp_stats)}
            end

          {:error, _reason} = error ->
            error
        end
      end

      defp sweep_tmp_paths(shard_path) do
        case blob_tmp_files(shard_path) do
          {:ok, paths} ->
            Enum.reduce_while(
              paths,
              {:ok, %{deleted_tmp_files: 0, deleted_tmp_bytes: 0}, MapSet.new()},
              fn path, {:ok, stats, dirs} ->
                if stale_tmp_file?(path) do
                  case delete_blob_file(path) do
                    {:ok, size} ->
                      stats = %{
                        stats
                        | deleted_tmp_files: stats.deleted_tmp_files + 1,
                          deleted_tmp_bytes: stats.deleted_tmp_bytes + size
                      }

                      {:cont, {:ok, stats, MapSet.put(dirs, Path.dirname(path))}}

                    {:error, _reason} = error ->
                      {:halt, error}
                  end
                else
                  {:cont, {:ok, stats, dirs}}
                end
              end
            )

          {:error, _reason} = error ->
            error
        end
      end

      defp stale_tmp_file?(path) do
        case File.stat(path, time: :posix) do
          {:ok, %{type: :regular, mtime: mtime}} when is_integer(mtime) ->
            System.system_time(:second) - mtime >= @tmp_stale_after_seconds

          _ ->
            false
        end
      end

      defp delete_blob_file(path) do
        size =
          case File.stat(path) do
            {:ok, %{type: :regular, size: size}} -> size
            _ -> 0
          end

        case Ferricstore.FS.rm(path) do
          :ok -> {:ok, size}
          {:error, {:not_found, _message}} -> {:ok, 0}
          {:error, reason} -> {:error, {:blob_delete_failed, path, reason}}
        end
      end

      defp fsync_deleted_dirs(dirs) do
        Enum.reduce_while(dirs, :ok, fn dir, :ok ->
          case fsync_dir(dir) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, {:blob_delete_fsync_failed, dir, reason}}}
          end
        end)
      end
    end
  end
end
