defmodule Ferricstore.Store.Shard.Flush do
  @moduledoc "Async and sync Bitcask batch flush, file rotation, hint-file writing, and per-file dead-byte fragmentation tracking."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.ExpiryContext
  alias Ferricstore.Store.AppendResult
  alias Ferricstore.Store.BlobValue
  alias Ferricstore.Store.Promotion
  alias Ferricstore.Store.SegmentFilename
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  require Logger

  # Default timeout for synchronous flush (used when instance_ctx is not available).
  @default_sync_flush_timeout_ms 5_000

  # Record header size for dead byte accounting (same as @bitcask_header_size).
  @record_header_size 26

  # -------------------------------------------------------------------
  # Flush pending writes
  # -------------------------------------------------------------------

  # Async flush — used by the timer and by put (first-write-in-window).
  # Writes to page cache only (no fsync) — durability comes from the
  # periodic fsync on the flush timer. This reduces per-write latency
  # from ~50-200us (NVMe fsync) to ~1-10us (memcpy to page cache).
  # If a flush is already in-flight or pending is empty, this is a no-op.
  @spec flush_pending(map()) :: map()
  @doc false
  def flush_pending(%{pending: []} = state), do: state
  def flush_pending(%{flush_in_flight: op_id} = state) when op_id != nil, do: state

  def flush_pending(%{pending: pending} = state) do
    raw_batch = Enum.reverse(pending)

    with {:ok, batch} <- build_flush_batch(state, raw_batch),
         state <- maybe_rotate_file(state),
         {:ok, locations} <-
           NIF.v2_append_batch_nosync(state.active_file_path, append_batch(batch)),
         :ok <- AppendResult.validate_locations(locations, length(batch)) do
      Ferricstore.Store.DiskPressure.clear(state.instance_ctx, state.index)
      # Raise dirty flag so BitcaskCheckpointer picks this up on the
      # next tick. This is the ONLY fsync trigger for the nosync path.
      if state.instance_ctx do
        :atomics.put(state.instance_ctx.checkpoint_flags, state.index + 1, 1)
      end

      written = total_record_bytes(batch)
      state = update_ets_locations(state, batch, locations)
      state = track_flush_bytes(state, written)

      state =
        %{
          state
          | pending: [],
            pending_count: 0,
            active_file_size: state.active_file_size + written
        }
        |> Map.put(:last_flush_error, nil)

      maybe_notify_fragmentation(state)
    else
      {:error, reason} ->
        Ferricstore.Store.DiskPressure.set(state.instance_ctx, state.index)

        Logger.error(
          "Shard #{state.index}: flush_pending (nosync) failed: #{inspect(reason)} — retaining #{length(raw_batch)} pending entries"
        )

        Map.put(state, :last_flush_error, reason)
    end
  end

  # Synchronous flush — used by delete, :flush, and :keys calls that need
  # durability guarantees. Uses v2_append_batch (write + fsync in one call).
  # Also ensures any previously-nosync'd data is fsynced.
  @spec flush_pending_sync(map()) :: map()
  @doc false
  def flush_pending_sync(%{pending: []} = state) do
    # Even with empty pending, we may need to fsync previously-nosync'd
    # data. Consult the shared checkpoint_flags atomic: if any writer
    # has raised it since the last fsync, fsync now and clear the flag.
    idx = state.index

    if state.instance_ctx &&
         :atomics.get(state.instance_ctx.checkpoint_flags, idx + 1) == 1 do
      case NIF.v2_fsync(state.active_file_path) do
        :ok ->
          :atomics.put(state.instance_ctx.checkpoint_flags, idx + 1, 0)
          Ferricstore.Store.DiskPressure.clear(state.instance_ctx, state.index)
          Map.put(state, :last_flush_error, nil)

        {:error, reason} ->
          Ferricstore.Store.DiskPressure.set(state.instance_ctx, state.index)

          Logger.error(
            "Shard #{state.index}: flush_pending_sync fsync failed: #{inspect(reason)} — keeping checkpoint dirty"
          )

          Map.put(state, :last_flush_error, reason)
      end
    else
      state
    end
  end

  def flush_pending_sync(%{pending: pending} = state) do
    raw_batch = Enum.reverse(pending)

    with {:ok, batch} <- build_flush_batch(state, raw_batch),
         state <- maybe_rotate_file(state),
         {:ok, locations} <- NIF.v2_append_batch(state.active_file_path, append_batch(batch)),
         :ok <- AppendResult.validate_locations(locations, length(batch)) do
      Ferricstore.Store.DiskPressure.clear(state.instance_ctx, state.index)
      # v2_append_batch fsyncs inside the NIF — we just wrote & fsynced
      # in one call, so clear the checkpoint flag too.
      if state.instance_ctx do
        :atomics.put(state.instance_ctx.checkpoint_flags, state.index + 1, 0)
      end

      written = total_record_bytes(batch)
      state = update_ets_locations(state, batch, locations)
      state = track_flush_bytes(state, written)

      state =
        %{
          state
          | pending: [],
            pending_count: 0,
            active_file_size: state.active_file_size + written
        }
        |> Map.put(:last_flush_error, nil)

      maybe_notify_fragmentation(state)
    else
      {:error, reason} ->
        Ferricstore.Store.DiskPressure.set(state.instance_ctx, state.index)

        Logger.error(
          "Shard #{state.index}: flush_pending_sync failed: #{inspect(reason)} — retaining #{length(raw_batch)} pending entries"
        )

        Map.put(state, :last_flush_error, reason)
    end
  end

  @spec flush_pending_for_read(map()) :: map()
  @doc false
  def flush_pending_for_read(state) do
    state
    |> await_in_flight()
    |> flush_pending()
  end

  # -------------------------------------------------------------------
  # Await in-flight async flush
  # -------------------------------------------------------------------

  # Wait for any in-flight async fsync to complete before proceeding.
  # This blocks the GenServer until the Tokio fsync result arrives.
  # Used before durability-critical operations (delete, keys, explicit flush).
  @spec await_in_flight(map()) :: map()
  @doc false
  def await_in_flight(%{flush_in_flight: nil} = state), do: state

  def await_in_flight(%{flush_in_flight: corr_id} = state) do
    timeout = sync_flush_timeout(state)

    receive do
      {:tokio_complete, ^corr_id, :ok, :ok} ->
        %{state | flush_in_flight: nil}

      {:tokio_complete, ^corr_id, :error, _reason} ->
        # Fsync failed — log at caller site if needed. Clear in-flight.
        %{state | flush_in_flight: nil}
    after
      timeout ->
        # Timeout — clear in-flight to avoid permanent blocking.
        Logger.error("Shard #{state.index}: await_in_flight timed out for corr_id #{corr_id}")
        %{state | flush_in_flight: nil}
    end
  end

  defp sync_flush_timeout(%{instance_ctx: ctx}) when ctx != nil do
    Map.get(ctx, :sync_flush_timeout_ms, @default_sync_flush_timeout_ms)
  end

  defp sync_flush_timeout(_state), do: @default_sync_flush_timeout_ms

  # -------------------------------------------------------------------
  # ETS location updates after flush
  # -------------------------------------------------------------------

  defp build_flush_batch(state, raw_batch) do
    threshold = blob_threshold(state)
    hot_cache_threshold = ShardETS.hot_cache_threshold(state)

    {prepared_reversed, disk_values_reversed} =
      Enum.reduce(raw_batch, {[], []}, fn {key, value, exp}, {prepared_acc, disk_acc} ->
        disk_value = ShardETS.to_disk_binary(value)
        staged_value = ShardETS.value_for_ets(disk_value, hot_cache_threshold)

        {
          [{key, disk_value, exp, staged_value} | prepared_acc],
          [disk_value | disk_acc]
        }
      end)

    with {:ok, persisted_values} <-
           externalize_flush_values(state, threshold, Enum.reverse(disk_values_reversed)),
         {:ok, batch} <-
           attach_persisted_flush_values(Enum.reverse(prepared_reversed), persisted_values) do
      {:ok, batch}
    else
      {:error, reason} -> {:error, {:blob_externalize_failed, reason}}
    end
  end

  defp attach_persisted_flush_values(prepared, persisted_values),
    do: attach_persisted_flush_values(prepared, persisted_values, [])

  defp attach_persisted_flush_values(
         [{key, _disk_value, exp, staged_value} | prepared],
         [persisted_value | persisted_values],
         acc
       ) do
    attach_persisted_flush_values(prepared, persisted_values, [
      {key, persisted_value, exp, staged_value} | acc
    ])
  end

  defp attach_persisted_flush_values([], [], acc), do: {:ok, Enum.reverse(acc)}

  defp attach_persisted_flush_values(_prepared, _persisted_values, _acc),
    do: {:error, :blob_externalize_result_mismatch}

  defp blob_threshold(%{instance_ctx: ctx}), do: BlobValue.threshold(ctx)
  defp blob_threshold(_state), do: 0

  defp externalize_flush_values(_state, threshold, disk_values) when threshold <= 0,
    do: BlobValue.maybe_externalize_many(nil, 0, 0, disk_values)

  defp externalize_flush_values(
         %{data_dir: data_dir, index: shard_index},
         threshold,
         disk_values
       ),
       do: BlobValue.maybe_externalize_many(data_dir, shard_index, threshold, disk_values)

  defp externalize_flush_values(_state, _threshold, _disk_values),
    do: {:error, :missing_blob_data_dir}

  defp append_batch(batch) do
    Enum.map(batch, fn {key, persisted_value, exp, _staged_value} ->
      {key, persisted_value, exp}
    end)
  end

  @spec update_ets_locations(map(), [{binary(), binary(), non_neg_integer(), binary() | nil}], [
          {non_neg_integer(), non_neg_integer()}
        ]) :: map()
  @doc false
  def update_ets_locations(state, batch, locations) do
    fid = state.active_file_id
    batch = normalize_flush_location_batch(batch)

    last_index_by_key =
      batch
      |> Enum.with_index()
      |> Map.new(fn {{key, _persisted_value, _exp, _staged_value}, index} -> {key, index} end)

    new_file_stats =
      Enum.zip(batch, locations)
      |> Enum.with_index()
      |> Enum.reduce(state.file_stats, fn
        {{{key, persisted_value, exp, staged_value}, {offset, _record_size}}, index}, fs ->
          if last_index_by_key[key] == index do
            update_single_ets_location(
              state,
              key,
              persisted_value,
              exp,
              staged_value,
              fid,
              offset,
              fs
            )
          else
            track_overwrite_dead_bytes(fs, key, fid, persisted_value_size(persisted_value))
          end
      end)

    %{state | file_stats: new_file_stats}
  end

  defp normalize_flush_location_batch(batch) do
    Enum.map(batch, fn
      {key, persisted_value, exp, staged_value} -> {key, persisted_value, exp, staged_value}
      {key, value, exp} -> {key, value, exp, value}
    end)
  end

  defp update_single_ets_location(
         state,
         key,
         persisted_value,
         exp,
         expected_value,
         fid,
         offset,
         fs
       ) do
    keydir = state.keydir

    case :ets.lookup(keydir, key) do
      [{^key, ^expected_value, ^exp, _lfu, :pending, old_fid, old_vsize}] ->
        vsize = persisted_value_size(persisted_value)

        replaced =
          :ets.select_replace(keydir, [
            {
              {key, expected_value, exp, :"$1", :pending, old_fid, old_vsize},
              [],
              [{{key, expected_value, exp, :"$1", fid, offset, vsize}}]
            }
          ])

        if replaced == 1 do
          {dead_fid, dead_vsize} = overwrite_dead_ref(:pending, old_fid, old_vsize)
          track_overwrite_dead_bytes(fs, key, dead_fid, dead_vsize)
        else
          fs
        end

      [] ->
        fs

      [{^key, ^expected_value, ^exp, _lfu, old_fid, old_off, old_vsize}]
      when old_fid != :pending and is_integer(old_fid) and old_fid >= 0 and
             is_integer(old_off) and old_off >= 0 and
             is_integer(old_vsize) and old_vsize >= 0 ->
        vsize = persisted_value_size(persisted_value)

        replaced =
          :ets.select_replace(keydir, [
            {
              {key, expected_value, exp, :"$1", old_fid, old_off, old_vsize},
              [],
              [{{key, expected_value, exp, :"$1", fid, offset, vsize}}]
            }
          ])

        if replaced == 1 do
          track_overwrite_dead_bytes(fs, key, old_fid, old_vsize)
        else
          fs
        end

      _ ->
        fs
    end
  end

  defp persisted_value_size(value) when is_binary(value), do: byte_size(value)
  defp persisted_value_size(value) when is_integer(value), do: byte_size(Integer.to_string(value))
  defp persisted_value_size(value) when is_float(value), do: byte_size(Float.to_string(value))

  defp overwrite_dead_ref(:pending, old_fid, old_vsize)
       when is_integer(old_fid) and old_fid >= 0 and is_integer(old_vsize) and old_vsize >= 0 do
    {old_fid, old_vsize}
  end

  defp overwrite_dead_ref(old_fid, _old_off, old_vsize), do: {old_fid, old_vsize}

  defp track_overwrite_dead_bytes(fs, key, old_fid, old_vsize)
       when is_integer(old_fid) and old_fid >= 0 and is_integer(old_vsize) and old_vsize >= 0 do
    dead_increment = old_vsize + @record_header_size + byte_size(key)
    {old_total, old_dead} = Map.get(fs, old_fid, {0, 0})
    Map.put(fs, old_fid, {old_total, old_dead + dead_increment})
  end

  defp track_overwrite_dead_bytes(fs, _key, _old_fid, _old_vsize), do: fs

  # -------------------------------------------------------------------
  # Byte tracking / fragmentation
  # -------------------------------------------------------------------

  @spec total_record_bytes([{binary(), binary(), non_neg_integer(), binary() | nil}]) ::
          non_neg_integer()
  @doc false
  def total_record_bytes(batch) do
    Enum.reduce(batch, 0, fn {key, persisted_value, _expire_at_ms, _staged_value}, acc ->
      acc + @record_header_size + byte_size(key) + byte_size(persisted_value)
    end)
  end

  # Increment total_bytes for the active file after a flush.
  @spec track_flush_bytes(map(), non_neg_integer()) :: map()
  @doc false
  def track_flush_bytes(state, written_bytes) do
    fid = state.active_file_id
    {total, dead} = Map.get(state.file_stats, fid, {0, 0})
    %{state | file_stats: Map.put(state.file_stats, fid, {total + written_bytes, dead})}
  end

  # Track dead bytes when a key is deleted via tombstone (direct path only).
  # Reads the old ETS entry to determine which file contains the now-dead record.
  @spec track_delete_dead_bytes(map(), binary()) :: map()
  @doc false
  def track_delete_dead_bytes(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, _v, _exp, _lfu, old_fid, _off, old_vsize}]
      when is_integer(old_fid) and old_fid >= 0 and is_integer(old_vsize) and old_vsize >= 0 ->
        dead_increment = old_vsize + @record_header_size + byte_size(key)
        {old_total, old_dead} = Map.get(state.file_stats, old_fid, {0, 0})

        %{
          state
          | file_stats: Map.put(state.file_stats, old_fid, {old_total, old_dead + dead_increment})
        }

      _ ->
        state
    end
  end

  @doc false
  def track_delete_dead_bytes_entry(
        state,
        {key, _value, _expire_at_ms, _lfu, old_fid, _offset, old_vsize}
      )
      when is_integer(old_fid) and old_fid >= 0 and is_integer(old_vsize) and old_vsize >= 0 do
    dead_increment = old_vsize + @record_header_size + byte_size(key)
    {old_total, old_dead} = Map.get(state.file_stats, old_fid, {0, 0})

    %{
      state
      | file_stats: Map.put(state.file_stats, old_fid, {old_total, old_dead + dead_increment})
    }
  end

  def track_delete_dead_bytes_entry(state, _entry), do: state

  # Check if any non-active file exceeds fragmentation thresholds and notify
  # the merge scheduler. Cheap: iterates a small map (typically <20 files).
  @spec maybe_notify_fragmentation(map()) :: map()
  @doc false
  def maybe_notify_fragmentation(state) do
    frag_threshold = state.merge_config.fragmentation_threshold
    dead_bytes_min = state.merge_config.dead_bytes_threshold

    candidates =
      state.file_stats
      |> Enum.filter(fn {fid, {total, dead}} ->
        fid != state.active_file_id and
          total > 0 and
          dead / total >= frag_threshold and
          dead >= dead_bytes_min
      end)
      |> Enum.map(fn {fid, _} -> fid end)

    if candidates != [] do
      file_count = map_size(state.file_stats)
      # Direct GenServer.cast avoids the compile-time cycle
      # Merge.Scheduler → Store.Router → Store.ListOps → Store.Ops →
      # Store.Shard.Writes → Store.Shard.Reads → Store.Shard.Flush →
      # Merge.Scheduler. The name must be instance-scoped; otherwise embedded
      # instances would publish compaction work to the default scheduler.
      # Fire-and-forget; unknown-name catches are handled by `try/catch :exit`.
      try do
        GenServer.cast(
          merge_scheduler_name(Map.get(state, :instance_ctx), state.index),
          {:fragmentation, candidates, file_count}
        )
      catch
        :exit, _ -> :ok
      end
    end

    state
  end

  # -------------------------------------------------------------------
  # File stats / rotation / hints
  # -------------------------------------------------------------------

  # Compute per-file dead bytes stats from disk file sizes + ETS live data.
  # Called once during init after recover_keydir. O(file_count + key_count).
  @spec compute_file_stats(binary(), :ets.tid()) :: %{
          non_neg_integer() => {non_neg_integer(), non_neg_integer()}
        }
  @doc false
  def compute_file_stats(shard_path, keydir) do
    case Ferricstore.FS.ls(shard_path) do
      {:ok, files} ->
        # 1. Get total bytes per file from disk
        file_totals =
          files
          |> Enum.reduce(%{}, fn name, acc ->
            case regular_log_file(shard_path, name) do
              :skip ->
                acc

              {:ok, fid, size} ->
                Map.put(acc, fid, size)
            end
          end)

        # 2. Sum non-expired live bytes per file from ETS
        # (record_header + key + value per entry). Expired ETS rows are
        # already logically dead, even when the periodic sweep has not removed
        # them yet.
        expiry_cutoff_ms =
          ExpiryContext.capture()
          |> ExpiryContext.safe_expiry_cutoff_ms()

        live_per_file =
          :ets.foldl(
            fn {key, _value, expire_at_ms, _lfu, fid, _off, vsize}, acc ->
              if live_expiry?(expire_at_ms, expiry_cutoff_ms) do
                accumulate_live_bytes(acc, key, fid, vsize)
              else
                acc
              end
            end,
            %{},
            keydir
          )

        # 3. dead_bytes = total_bytes - live_bytes per file
        Map.new(file_totals, fn {fid, total} ->
          live = Map.get(live_per_file, fid, 0)
          dead = max(total - live, 0)
          {fid, {total, dead}}
        end)

      _ ->
        %{}
    end
  end

  defp accumulate_live_bytes(acc, key, fid, vsize)
       when is_integer(fid) and fid >= 0 and is_integer(vsize) and vsize >= 0 do
    record_bytes = @record_header_size + byte_size(key) + vsize
    Map.update(acc, fid, record_bytes, &(&1 + record_bytes))
  end

  defp accumulate_live_bytes(acc, _key, _fid, _vsize), do: acc

  defp regular_log_file(shard_path, name) do
    with {:ok, fid} <- SegmentFilename.parse(name),
         {:ok, %File.Stat{type: :regular, size: size}} <-
           File.lstat(Path.join(shard_path, name)) do
      {:ok, fid, size}
    else
      _ -> :skip
    end
  end

  defp live_expiry?(expire_at_ms, _now_ms) when expire_at_ms in [0, nil], do: true
  defp live_expiry?(expire_at_ms, now_ms) when is_integer(expire_at_ms), do: expire_at_ms > now_ms
  defp live_expiry?(_expire_at_ms, _now_ms), do: true

  defp merge_scheduler_name(%{name: :default}, index), do: :"Ferricstore.Merge.Scheduler.#{index}"
  defp merge_scheduler_name(%{name: name}, index), do: :"#{name}.Merge.Scheduler.#{index}"
  defp merge_scheduler_name(_instance_ctx, index), do: :"Ferricstore.Merge.Scheduler.#{index}"

  @spec maybe_rotate_file(map()) :: map()
  @doc false
  def maybe_rotate_file(%{compound_promotion_worker: worker} = state)
      when not is_nil(worker),
      do: state

  def maybe_rotate_file(state) do
    if state.active_file_size >= state.max_active_file_size do
      case Promotion.try_acquire_shared_log_latch(state) do
        :busy ->
          state

        :none ->
          do_maybe_rotate_file(state)

        {:ok, latch_token} ->
          try do
            do_maybe_rotate_file(state)
          after
            Promotion.release_compaction_latch(latch_token)
          end
      end
    else
      state
    end
  end

  defp do_maybe_rotate_file(state) do
    if state.active_file_size >= state.max_active_file_size do
      # Rotation durability handoff
      # (the active-file rotation design):
      #
      # 1. Synchronously fsync the outgoing active file so any bytes
      #    written since the last checkpoint land on disk BEFORE we
      #    publish the new active file. Otherwise the checkpointer's
      #    next tick would target the NEW file and the OLD file's
      #    tail could be lost on kernel panic.
      case Ferricstore.Bitcask.NIF.v2_fsync(state.active_file_path) do
        :ok ->
          :telemetry.execute(
            [:ferricstore, :bitcask, :rotation_fsync],
            %{},
            %{shard_index: state.index, kind: :old_file, path: state.active_file_path}
          )

          # Hint files are a recovery accelerator, not part of the commit
          # boundary. Writing them here folds the full keydir during Ra apply
          # and creates avoidable p99 spikes on hot shards. Shutdown and
          # explicit sync paths still write hints; recovery scans unhinted logs.
          new_id = state.active_file_id + 1
          sp = state.shard_data_path
          new_path = ShardETS.file_path(sp, new_id)
          Ferricstore.FS.touch!(new_path)

          # 2. Fsync the shard directory so the new filename entry
          #    (`new_path`) is durable. Without this, a kernel panic
          #    between touch! and the first append can leave the file
          #    absent on reboot — the next append would create a fresh
          #    one but we'd lose any bytes already buffered in page cache.
          case fsync_rotation_dir(sp) do
            :ok ->
              :telemetry.execute(
                [:ferricstore, :bitcask, :rotation_fsync],
                %{},
                %{shard_index: state.index, kind: :new_dir, path: sp}
              )

              if ctx = Map.get(state, :instance_ctx) do
                Ferricstore.Store.ActiveFile.publish(ctx, state.index, new_id, new_path, sp)
              end

              Ferricstore.Store.HintBuilder.enqueue(
                Map.get(state, :instance_ctx),
                state.index,
                state.active_file_id,
                state.active_file_path,
                sp
              )

              # Initialize file_stats for the new file
              new_file_stats = Map.put(state.file_stats, new_id, {0, 0})

              # Notify the merge scheduler that a rotation happened. File ids can
              # have gaps after compaction deletes old logs, so use tracked live
              # file count instead of deriving count from the newest id.
              # Direct cast avoids the Merge.Scheduler → ... → Shard.Flush cycle.
              try do
                GenServer.cast(
                  merge_scheduler_name(Map.get(state, :instance_ctx), state.index),
                  {:file_rotated, map_size(new_file_stats)}
                )
              catch
                :exit, _ -> :ok
              end

              state
              |> Map.merge(%{
                active_file_id: new_id,
                active_file_path: new_path,
                active_file_size: 0,
                file_stats: new_file_stats
              })
              |> Map.delete(:last_rotation_error)

            {:error, reason} ->
              if ctx = Map.get(state, :instance_ctx) do
                Ferricstore.Store.DiskPressure.set(ctx, state.index)
              end

              cleanup_rotation_candidate(new_path)

              Logger.warning(
                "Shard #{state.index}: rotation fsync_dir failed: #{inspect(reason)}; keeping active file"
              )

              Map.put(state, :last_rotation_error, {:directory_fsync_failed, reason})
          end

        {:error, reason} ->
          if ctx = Map.get(state, :instance_ctx) do
            Ferricstore.Store.DiskPressure.set(ctx, state.index)
          end

          Logger.warning(
            "Shard #{state.index}: rotation fsync of old active file failed: #{inspect(reason)}; keeping active file"
          )

          state
      end
    else
      state
    end
  end

  defp fsync_rotation_dir(path) do
    case Process.get(:ferricstore_shard_rotation_fsync_dir_hook) do
      hook when is_function(hook, 1) -> hook.(path)
      _ -> Ferricstore.Bitcask.NIF.v2_fsync_dir(path)
    end
  end

  defp cleanup_rotation_candidate(path) do
    case Ferricstore.FS.rm(path) do
      :ok ->
        :ok

      {:error, {:not_found, _message}} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to remove rotation candidate #{path}: #{inspect(reason)}")
    end
  end

  @spec write_hint_for_file(map(), non_neg_integer()) :: :ok | {:error, term()}
  @doc false
  def write_hint_for_file(state, target_fid) do
    sp = state.shard_data_path
    hint_path = Path.join(sp, "#{String.pad_leading(Integer.to_string(target_fid), 5, "0")}.hint")

    Ferricstore.Store.HintFile.write_from_keydir(hint_path, state.keydir, target_fid)
  end

  # -------------------------------------------------------------------
  # Schedule flush timer
  # -------------------------------------------------------------------

  @doc """
  Schedules the periodic `:drain_pending` timer tick. The tick drains
  `state.pending` to the active file via `v2_append_batch_nosync` —
  i.e. BEAM memory → kernel page cache. It does NOT fsync. Data-file
  durability is owned by `Ferricstore.Store.BitcaskCheckpointer`, which
  runs on its own (much longer) interval.
  """
  @spec schedule_drain_pending(non_neg_integer()) :: reference()
  def schedule_drain_pending(ms) do
    Process.send_after(self(), :drain_pending, ms)
  end
end
