defmodule Ferricstore.Raft.WARaftStorage.Sections.ZSetTreeMaintenance do
  @moduledoc false

  # Tree-encoded sorted sets' files: removing the ones no set names any more,
  # and compacting the ones writes have grown.
  #
  # Both wait for the replay floor, the segment projection checkpoint: recovery
  # replays only the entries after it, so a file no entry after the floor reads
  # can go, and frames at or below it can be folded together.
  #
  # The sweep runs in the storage process between applies, where the keydir is
  # exactly the state at the applied position. Each pass looks at a few of the
  # shard's tree directories. A generation the set's marker no longer names
  # (deleted, expired and swept, or replaced by a later conversion) gets a
  # removal record at the applied position; one removed at or below the floor
  # is deleted. Compaction runs in a task, beside applies (the engine locks
  # the file only to copy the frames written meanwhile), and is drained before
  # a snapshot copies or replaces the files.

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Store.ZSetTree

      @zset_tree_dir_prefix "zset_tree:"
      @default_zset_tree_maintenance_interval_ms 1_000
      @default_zset_tree_dirs_per_pass 64
      @default_zset_tree_compact_min_bytes 1_048_576
      @default_zset_tree_compact_growth 2

      defp maybe_start_zset_tree_maintenance(handle) do
        floor = position_index(Map.get(handle, :segment_projection_position))

        # Removals wait for the floor to move; compaction for trees written
        # since the last pass.
        cond do
          Map.has_key?(handle, :zset_tree_maintenance) ->
            handle

          not zset_tree_maintenance_due?(handle) ->
            handle

          floor > Map.get(handle, :zset_tree_maintenance_floor, 0) or
              MapSet.size(Process.get(:sm_zset_tree_written, MapSet.new())) > 0 ->
            run_zset_tree_maintenance(handle, floor)

          true ->
            handle
        end
      end

      defp zset_tree_maintenance_due?(handle) do
        interval = zset_tree_env(:zset_tree_maintenance_interval_ms)

        case Map.get(handle, :zset_tree_maintenance_at_ms) do
          nil -> true
          last -> System.monotonic_time(:millisecond) - last >= interval
        end
      end

      defp run_zset_tree_maintenance(handle, floor) do
        applied = position_index(Map.get(handle, :position))
        root = Path.join([handle.ctx.data_dir, "dedicated", "shard_#{handle.shard_index}"])

        {dirs, cursor} =
          zset_tree_dirs_batch(root, Map.get(handle, :zset_tree_maintenance_cursor))

        live =
          Enum.flat_map(dirs, fn dir ->
            sweep_zset_tree_dir(handle, Path.join(root, dir), floor, applied)
          end)

        written = Process.get(:sm_zset_tree_written, MapSet.new())
        Process.delete(:sm_zset_tree_written)

        handle =
          handle
          |> Map.put(:zset_tree_maintenance_floor, floor)
          |> Map.put(:zset_tree_maintenance_cursor, cursor)
          |> Map.put(:zset_tree_maintenance_at_ms, System.monotonic_time(:millisecond))

        case Enum.filter(Enum.uniq(live ++ MapSet.to_list(written)), &zset_tree_grown?/1) do
          [] -> handle
          paths -> start_zset_tree_compaction(handle, paths, floor)
        end
      end

      # The next few tree directories after `cursor`, in name order, and the
      # cursor for the pass after; it wraps to the start at the end.
      defp zset_tree_dirs_batch(root, cursor) do
        case Ferricstore.FS.ls(root) do
          {:ok, names} ->
            dirs =
              names |> Enum.filter(&String.starts_with?(&1, @zset_tree_dir_prefix)) |> Enum.sort()

            after_cursor = if cursor, do: Enum.drop_while(dirs, &(&1 <= cursor)), else: dirs
            batch = Enum.take(after_cursor, zset_tree_env(:zset_tree_dirs_per_pass))

            next =
              if length(batch) < length(after_cursor), do: List.last(batch), else: nil

            {batch, next}

          {:error, _reason} ->
            {[], nil}
        end
      end

      # Returns the paths of the generations still in use, for compaction.
      defp sweep_zset_tree_dir(handle, dir, floor, applied) do
        with {:ok, names} <- Ferricstore.FS.ls(dir),
             {:ok, redis_key} <- ZSetTree.read_key(dir),
             {:ok, named} <- zset_tree_named_generation(handle, redis_key) do
          generations =
            names
            |> Enum.flat_map(fn name ->
              case Integer.parse(Path.rootname(name)) do
                {generation, ""} when generation > 0 -> [generation]
                _other -> []
              end
            end)
            |> Enum.uniq()

          live =
            Enum.flat_map(generations, fn generation ->
              sweep_zset_tree_generation(dir, generation, named, floor, applied)
            end)

          _ = Enum.each(names, &remove_stale_zset_tree_compaction(dir, &1))
          if live == [], do: remove_empty_zset_tree_dir(dir)
          live
        else
          _unknown -> []
        end
      end

      # The generation the set's marker names, nil when it names none, whatever
      # the marker's TTL: apply judges expiry by each entry's own time, so a
      # marker that looks expired here may still be live to the next entry.
      # :unknown when its value cannot be read (the directory waits).
      defp zset_tree_named_generation(handle, redis_key) do
        marker_key = Ferricstore.Store.Promotion.marker_key(redis_key)

        case :ets.lookup(Map.fetch!(handle.sm_state, :ets), marker_key) do
          [{^marker_key, value, _exp, _lfu, _fid, _off, _size}] when is_binary(value) ->
            {:ok, ZSetTree.marker_generation(value)}

          [{^marker_key, nil, _exp, _lfu, file_id, offset, _size}] ->
            case read_keydir_cold_value(
                   handle.ctx,
                   handle.shard_index,
                   marker_key,
                   file_id,
                   offset
                 ) do
              {:ok, value} when is_binary(value) -> {:ok, ZSetTree.marker_generation(value)}
              _unreadable -> :unknown
            end

          [_other] ->
            :unknown

          [] ->
            {:ok, nil}
        end
      end

      defp sweep_zset_tree_generation(dir, generation, named, floor, applied) do
        tree = Path.join(dir, "#{generation}.tree")
        removed = ZSetTree.removed_path(tree)

        case read_zset_tree_removed(removed) do
          {:ok, index} when index <= floor ->
            with :ok <- rm_zset_tree_file(tree), do: rm_zset_tree_file(removed)
            []

          {:ok, _index} ->
            []

          :none when generation == named ->
            if Ferricstore.FS.exists?(tree), do: [tree], else: []

          :none ->
            # Nothing names it from here on; the entries up to `applied` may
            # still have read it.
            _ =
              Ferricstore.FS.atomic_replace_nofollow(removed, Integer.to_string(applied), 64)

            []
        end
      end

      defp read_zset_tree_removed(path) do
        with true <- Ferricstore.FS.exists?(path),
             {:ok, contents} <- Ferricstore.FS.read_nofollow(path, 64),
             {index, ""} <- Integer.parse(contents) do
          {:ok, index}
        else
          false -> :none
          # Unreadable: keep the file until a later pass.
          _invalid -> {:ok, :infinity}
        end
      end

      defp rm_zset_tree_file(path) do
        case Ferricstore.FS.rm(path) do
          :ok -> :ok
          {:error, {:not_found, _message}} -> :ok
          {:error, _reason} = error -> error
        end
      end

      # A compaction or a rewind stopped before it put its file in place (no
      # compaction runs while the sweep does, and rewinds run in this process).
      defp remove_stale_zset_tree_compaction(dir, name) do
        if String.ends_with?(name, [".compacting", ".rewinding", ".rewound"]),
          do: rm_zset_tree_file(Path.join(dir, name))
      end

      defp remove_empty_zset_tree_dir(dir) do
        with {:ok, names} <- Ferricstore.FS.ls(dir),
             true <- Enum.all?(names, &(&1 == Path.basename(ZSetTree.key_path(dir)))) do
          _ = rm_zset_tree_file(ZSetTree.key_path(dir))
          _ = Ferricstore.FS.rm_rf(dir)
        end
      end

      # Worth compacting: past the minimum size and grown to some multiple of
      # the tree its first frame built.
      defp zset_tree_grown?(path) do
        case ZSetTree.file_stats(path) do
          {:ok, {length, first_frame}} ->
            length >= zset_tree_env(:zset_tree_compact_min_bytes) and
              length > first_frame * zset_tree_env(:zset_tree_compact_growth)

          _missing_or_failed ->
            false
        end
      end

      defp start_zset_tree_compaction(handle, paths, floor) do
        ref = make_ref()
        storage_name = Map.fetch!(handle.options, :storage_name)

        {:ok, pid} =
          Task.start(fn ->
            results = Enum.map(paths, &{&1, ZSetTree.compact(&1, floor)})

            send_storage_info(
              storage_name,
              {:ferricstore_zset_tree_compaction_done, ref, results}
            )
          end)

        Map.put(handle, :zset_tree_maintenance, %{
          ref: ref,
          pid: pid,
          monitor: Process.monitor(pid)
        })
      end

      defp finish_zset_tree_compaction(
             ref,
             results,
             %{zset_tree_maintenance: %{ref: ref}} = handle
           ) do
        Enum.each(results, fn
          {_path, {:ok, _done_or_skipped}} ->
            :ok

          {path, {:error, reason}} ->
            Logger.warning("Sorted set tree #{path} was not compacted: #{inspect(reason)}")
        end)

        {:ok, clear_zset_tree_maintenance(handle)}
      end

      defp finish_zset_tree_compaction(_ref, _results, handle), do: {:ok, handle}

      defp finish_zset_tree_compaction_down(
             monitor,
             reason,
             %{zset_tree_maintenance: %{monitor: monitor}} = handle
           ) do
        Logger.warning("Sorted set tree compaction stopped: #{inspect(reason)}")
        {:ok, clear_zset_tree_maintenance(handle)}
      end

      defp finish_zset_tree_compaction_down(_monitor, _reason, handle), do: {:ok, handle}

      defp clear_zset_tree_maintenance(handle) do
        case Map.get(handle, :zset_tree_maintenance) do
          %{monitor: monitor} -> Process.demonitor(monitor, [:flush])
          nil -> :ok
        end

        Map.delete(handle, :zset_tree_maintenance)
      end

      # A snapshot copies or replaces the tree files, so a running compaction
      # finishes first.
      defp drain_zset_tree_maintenance(handle) do
        case Map.get(handle, :zset_tree_maintenance) do
          %{pid: pid} when is_pid(pid) ->
            monitor = Process.monitor(pid)

            receive do
              {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
            end

          _none ->
            :ok
        end
      end

      defp zset_tree_env(:zset_tree_maintenance_interval_ms),
        do:
          zset_tree_env(
            :zset_tree_maintenance_interval_ms,
            @default_zset_tree_maintenance_interval_ms
          )

      defp zset_tree_env(:zset_tree_dirs_per_pass),
        do: zset_tree_env(:zset_tree_dirs_per_pass, @default_zset_tree_dirs_per_pass)

      defp zset_tree_env(:zset_tree_compact_min_bytes),
        do: zset_tree_env(:zset_tree_compact_min_bytes, @default_zset_tree_compact_min_bytes)

      defp zset_tree_env(:zset_tree_compact_growth),
        do: zset_tree_env(:zset_tree_compact_growth, @default_zset_tree_compact_growth)

      defp zset_tree_env(key, default) do
        case Application.get_env(:ferricstore, key, default) do
          value when is_integer(value) and value >= 0 -> value
          _invalid -> default
        end
      end
    end
  end
end
