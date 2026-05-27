defmodule Ferricstore.Store.Shard.Lifecycle do
  @moduledoc "Shard startup (log/hint recovery, keydir rebuild), expiry sweep, probabilistic-file migration, Raft init, and graceful shutdown."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.HLC
  alias Ferricstore.Store.{CompoundKey, ExpiryTracker, LFU}
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  require Logger

  # Expiry sweep runs an ETS select over the entire keydir looking for
  # expired TTLs. On a keydir with 280K+ keys, each sweep burns millions
  # of reductions even when nothing has expired. A 5s interval gives up
  # some freshness (TTL hit-but-still-in-keydir window grows to 5s) for
  # a 5x reduction in idle scheduler pressure. Application.get_env can
  # override via :expiry_sweep_interval_ms.
  @default_sweep_interval_ms 5_000
  @default_max_keys_per_sweep 100
  @default_frag_check_interval_ms 60_000
  @default_recovery_scan_page_size 8_192
  @log_header_size 26

  # Number of consecutive ceiling-hit sweeps before emitting the
  # :expiry_struggling telemetry event.
  @struggling_threshold 3

  # -------------------------------------------------------------------
  # Discovery / Recovery
  # -------------------------------------------------------------------

  # Scans the shard data directory for .log files and returns
  # {highest_file_id, file_size_of_highest}. Starts at 0 if no files exist.
  # Uses a single Enum.reduce pass instead of filter + map + max to avoid
  # creating intermediate lists (perf audit L5).
  @spec discover_active_file(binary()) :: {non_neg_integer(), non_neg_integer()}
  @doc false
  def discover_active_file(shard_path) do
    case Ferricstore.FS.ls(shard_path) do
      {:ok, files} ->
        # Clean up leftover compaction temp files from a previous crash.
        # These are always incomplete — if compaction had finished, the
        # rename would have replaced the original and the temp is gone.
        cleanup_compact_temps(shard_path, files)

        max_id =
          files
          |> Enum.flat_map(&regular_log_file_id/1)
          |> Enum.reduce(-1, fn id, best -> max(id, best) end)

        if max_id < 0 do
          {0, 0}
        else
          size = File.stat!(ShardETS.file_path(shard_path, max_id)).size
          {max_id, size}
        end

      {:error, reason} ->
        Logger.error("discover_active_file failed to list #{shard_path}: #{inspect(reason)}")

        raise "discover_active_file failed to list #{shard_path}: #{inspect(reason)}"
    end
  end

  # Recovers the ETS keydir from hint files or by scanning log files.
  # Uses last-writer-wins semantics (higher file_id + higher offset wins).
  @spec recover_keydir(binary(), :ets.tid(), non_neg_integer(), term()) :: :ok
  @doc false
  def recover_keydir(shard_path, keydir, shard_index, instance_ctx \\ nil) do
    case Ferricstore.FS.ls(shard_path) do
      {:ok, files} ->
        log_files =
          files
          |> Enum.filter(&regular_log_file?/1)
          |> Enum.sort_by(&log_file_id/1)

        Logger.debug(
          "Shard #{shard_index}: recover_keydir scanning #{length(log_files)} log file(s) at #{shard_path}"
        )

        # Try hint files first for faster recovery
        hint_files =
          files
          |> Enum.filter(&regular_hint_file?/1)
          |> Enum.sort_by(&hint_file_id/1)

        recover_from_hints_or_logs(
          shard_path,
          keydir,
          shard_index,
          log_files,
          hint_files,
          instance_ctx
        )

      {:error, reason} ->
        Logger.error(
          "Shard #{shard_index}: recover_keydir failed to list #{shard_path}: #{inspect(reason)}"
        )

        raise "recover_keydir failed to list #{shard_path}: #{inspect(reason)}"
    end

    ets_size = :ets.info(keydir, :size)
    sample_keys = sample_keydir_entries(keydir, 10)

    Logger.debug(
      "Shard #{shard_index}: recover_keydir done, ETS size: #{ets_size}, keys: #{inspect(sample_keys)}"
    )
  end

  defp sample_keydir_entries(keydir, limit) do
    match_spec = [
      {{:"$1", :_, :_, :_, :"$2", :"$3", :"$4"}, [], [{{:"$1", :"$2", :"$3", :"$4"}}]}
    ]

    case :ets.select(keydir, match_spec, limit) do
      {entries, _continuation} -> format_keydir_sample(entries)
      :"$end_of_table" -> []
    end
  end

  defp format_keydir_sample(entries) do
    Enum.map(entries, fn {k, fid, off, vs} ->
      "#{k}(fid=#{inspect(fid)},off=#{off},vs=#{vs})"
    end)
  end

  @spec recover_from_log(binary(), binary(), :ets.tid(), non_neg_integer(), term()) :: :ok
  @doc false
  def recover_from_log(shard_path, log_name, keydir, shard_index, instance_ctx \\ nil) do
    log_path = Path.join(shard_path, log_name)
    fid = log_name |> String.trim_trailing(".log") |> String.to_integer()

    recover_from_log_pages(
      log_path,
      0,
      keydir,
      shard_index,
      fid,
      instance_ctx,
      :recover_from_log
    )
  end

  defp recover_from_log_from_offset(
         shard_path,
         log_name,
         keydir,
         shard_index,
         offset,
         instance_ctx
       ) do
    log_path = Path.join(shard_path, log_name)
    fid = log_file_id(log_name)

    recover_from_log_pages(
      log_path,
      offset,
      keydir,
      shard_index,
      fid,
      instance_ctx,
      :recover_from_log_from_offset
    )
  end

  defp recover_from_log_pages(
         log_path,
         offset,
         keydir,
         shard_index,
         fid,
         instance_ctx,
         operation
       ) do
    page_size = recovery_scan_page_size()

    recover_from_log_pages(
      log_path,
      offset,
      page_size,
      keydir,
      shard_index,
      fid,
      instance_ctx,
      operation
    )
  end

  defp recover_from_log_pages(
         log_path,
         offset,
         page_size,
         keydir,
         shard_index,
         fid,
         instance_ctx,
         operation
       ) do
    case NIF.v2_scan_file_page(log_path, offset, page_size) do
      {:ok, records, next_offset, done?} ->
        Enum.each(records, fn record ->
          recover_record(keydir, shard_index, fid, record, instance_ctx)
        end)

        cond do
          done? ->
            :ok

          is_integer(next_offset) and next_offset > offset ->
            recover_from_log_pages(
              log_path,
              next_offset,
              page_size,
              keydir,
              shard_index,
              fid,
              instance_ctx,
              operation
            )

          true ->
            fail_recovery_scan!(operation, log_path, shard_index, {:non_advancing_scan, offset})
        end

      {:error, reason} ->
        fail_recovery_scan!(operation, log_path, shard_index, reason)

      other ->
        fail_recovery_scan!(operation, log_path, shard_index, {:unexpected, other})
    end
  end

  defp recovery_scan_page_size do
    case Application.get_env(
           :ferricstore,
           :recovery_scan_page_size,
           @default_recovery_scan_page_size
         ) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_recovery_scan_page_size
    end
  end

  # -------------------------------------------------------------------
  # Expiry sweep
  # -------------------------------------------------------------------

  # Performs a single expiry sweep pass: scans ETS for up to `max_keys`
  # expired entries, deletes them from ETS, and purges expired entries
  # from the Bitcask store. Tracks consecutive ceiling-hit sweeps and
  # emits telemetry when the sweep is struggling or recovers.
  @spec do_expiry_sweep(map(), keyword()) :: map()
  @doc false
  def do_expiry_sweep(state, opts \\ []) do
    force? = Keyword.get(opts, :force, false)
    now = HLC.now_ms()

    cond do
      force? ->
        do_expiry_sweep_scan(state, now)

      ExpiryTracker.count_for_state(state) == 0 ->
        %{state | sweep_at_ceiling_count: 0, sweep_struggling: false}

      not ExpiryTracker.due_for_state?(state, now) and not memory_pressure?(state) ->
        %{state | sweep_at_ceiling_count: 0, sweep_struggling: false}

      true ->
        do_expiry_sweep_scan(state, now)
    end
  end

  defp do_expiry_sweep_scan(state, now) do
    max_keys =
      Application.get_env(:ferricstore, :expiry_max_keys_per_sweep, @default_max_keys_per_sweep)

    expired_keys = scan_expired(state.keydir, now, max_keys)

    count = length(expired_keys)

    state =
      if count > 0 do
        state = expire_keys(state, expired_keys)

        incr_expired_stats(state, count)
        Logger.debug("Shard #{state.index}: expiry sweep removed #{count} key(s)")
        state
      else
        defer_next_due(state, now)
        state
      end

    hit_ceiling = count >= max_keys and count > 0
    {new_ceiling_count, new_struggling} = update_sweep_ceiling(state, hit_ceiling, max_keys)

    %{state | sweep_at_ceiling_count: new_ceiling_count, sweep_struggling: new_struggling}
  end

  defp defer_next_due(state, now) do
    interval =
      Application.get_env(:ferricstore, :expiry_sweep_interval_ms, @default_sweep_interval_ms)

    ExpiryTracker.defer_due_for_state(state, now + interval)
  end

  defp memory_pressure?(%{instance_ctx: %{pressure_flags: ref}}) when ref != nil do
    :atomics.get(ref, 3) == 1 or :atomics.get(ref, 1) == 1
  end

  defp memory_pressure?(_state), do: false

  @spec scan_expired(:ets.tid(), integer(), non_neg_integer()) :: [binary()]
  @doc false
  def scan_expired(keydir, now, limit) do
    # 7-tuple format: {key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}
    # Match entries where expire_at_ms > 0 and expire_at_ms <= now
    match_spec = [
      {{:"$1", :_, :"$2", :_, :_, :_, :_}, [{:andalso, {:>, :"$2", 0}, {:"=<", :"$2", now}}],
       [:"$1"]}
    ]

    case :ets.select(keydir, match_spec, limit) do
      {keys, _continuation} -> keys
      :"$end_of_table" -> []
    end
  end

  @spec schedule_expiry_sweep() :: reference()
  @doc false
  def schedule_expiry_sweep do
    interval =
      Application.get_env(:ferricstore, :expiry_sweep_interval_ms, @default_sweep_interval_ms)

    Process.send_after(self(), :expiry_sweep, interval)
  end

  @spec schedule_frag_check() :: reference()
  @doc false
  def schedule_frag_check do
    interval =
      Application.get_env(:ferricstore, :frag_check_interval_ms, @default_frag_check_interval_ms)

    Process.send_after(self(), :frag_check, interval)
  end

  # -------------------------------------------------------------------
  # Prob file migration
  # -------------------------------------------------------------------

  @spec migrate_prob_files(binary(), :ets.tid(), non_neg_integer(), term()) :: :ok
  @doc false
  def migrate_prob_files(shard_data_path, keydir, index, instance_ctx \\ nil) do
    prob_dir = Path.join(shard_data_path, "prob")

    case Ferricstore.FS.ls(prob_dir) do
      {:ok, files} ->
        migrated =
          Enum.reduce(files, 0, fn filename, count ->
            migrate_prob_file(prob_dir, filename, keydir, index, count, instance_ctx)
          end)

        if migrated > 0 do
          Logger.info("Shard: migrated #{migrated} existing prob file(s) to Raft metadata")
        end

      {:error, {:not_found, _}} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Shard #{index}: migrate_prob_files failed to list #{prob_dir}: #{inspect(reason)}"
        )

        raise "migrate_prob_files failed to list #{prob_dir}: #{inspect(reason)}"
    end
  end

  @spec migrate_prob_file(
          binary(),
          binary(),
          :ets.tid(),
          non_neg_integer(),
          non_neg_integer(),
          term()
        ) ::
          non_neg_integer()
  @doc false
  def migrate_prob_file(prob_dir, filename, keydir, shard_index, count, instance_ctx \\ nil) do
    path = Path.join(prob_dir, filename)

    cond do
      String.ends_with?(filename, ".bloom") ->
        key = filename |> String.trim_trailing(".bloom")
        migrate_if_missing(keydir, shard_index, key, path, :bloom_meta, count, instance_ctx)

      String.ends_with?(filename, ".cms") ->
        key = filename |> String.trim_trailing(".cms")
        migrate_if_missing(keydir, shard_index, key, path, :cms_meta, count, instance_ctx)

      String.ends_with?(filename, ".cuckoo") ->
        key = filename |> String.trim_trailing(".cuckoo")
        migrate_if_missing(keydir, shard_index, key, path, :cuckoo_meta, count, instance_ctx)

      String.ends_with?(filename, ".topk") ->
        key = filename |> String.trim_trailing(".topk")
        migrate_if_missing(keydir, shard_index, key, path, :topk_meta, count, instance_ctx)

      true ->
        count
    end
  end

  # Writes a metadata marker into ETS if the key doesn't already have one.
  # The key in the filename may be Base64-encoded (new) or sanitized (old).
  # We try to decode as Base64 first; if that fails, treat the filename
  # stem as the literal key.
  @spec migrate_if_missing(
          :ets.tid(),
          non_neg_integer(),
          binary(),
          binary(),
          atom(),
          non_neg_integer()
        ) :: non_neg_integer()
  @doc false
  def migrate_if_missing(
        keydir,
        shard_index,
        filename_key,
        path,
        type,
        count,
        instance_ctx \\ nil
      ) do
    key =
      case Base.url_decode64(filename_key, padding: false) do
        {:ok, decoded} -> decoded
        :error -> filename_key
      end

    case :ets.lookup(keydir, key) do
      [{^key, _val, _exp, _lfu, _fid, _off, _vsize}] ->
        # Already has an ETS entry — no migration needed
        count

      [] ->
        # No ETS entry — write a metadata marker
        meta = build_prob_meta(type, path, key)
        meta_bin = :erlang.term_to_binary(meta)
        track_binary_add(shard_index, key, meta_bin, instance_ctx)
        :ets.insert(keydir, {key, meta_bin, 0, 0, 0, 0, byte_size(meta_bin)})
        count + 1
    end
  rescue
    ArgumentError -> count
  end

  @spec build_prob_meta(atom(), binary(), binary()) :: {atom(), map()}
  @doc false
  def build_prob_meta(:bloom_meta, path, _key) do
    # Try to read bloom header for capacity/error_rate derivation
    case NIF.bloom_file_info(path) do
      {:ok, {num_bits, _count, num_hashes}} ->
        capacity =
          if num_hashes > 0,
            do: max(1, round(num_bits * :math.log(2) / num_hashes)),
            else: 100

        error_rate =
          if capacity > 0,
            do: :math.exp(-num_bits * :math.pow(:math.log(2), 2) / capacity),
            else: 0.01

        {:bloom_meta,
         %{
           path: path,
           num_bits: num_bits,
           num_hashes: num_hashes,
           capacity: capacity,
           error_rate: error_rate
         }}

      _ ->
        {:bloom_meta, %{path: path}}
    end
  end

  def build_prob_meta(:cms_meta, path, _key) do
    case NIF.cms_file_info(path) do
      {:ok, {width, depth, _count}} ->
        {:cms_meta, %{width: width, depth: depth}}

      _ ->
        {:cms_meta, %{path: path}}
    end
  end

  def build_prob_meta(:cuckoo_meta, path, _key) do
    case NIF.cuckoo_file_info(path) do
      {:ok, {num_buckets, _bs, _fp, _ni, _nd, _ts, _mk}} ->
        {:cuckoo_meta, %{capacity: num_buckets}}

      _ ->
        {:cuckoo_meta, %{path: path}}
    end
  end

  def build_prob_meta(:topk_meta, path, _key) do
    case NIF.topk_file_info_v2(path) do
      {k, width, depth, decay} ->
        {:topk_meta, %{path: path, k: k, width: width, depth: depth, decay: decay}}

      _ ->
        {:topk_meta, %{path: path}}
    end
  end

  # -------------------------------------------------------------------
  # Raft startup
  # -------------------------------------------------------------------

  # WARaft owns default-instance replication outside the Shard GenServer. This
  # helper remains for old lifecycle call sites and reports whether the
  # production WARaft batcher for this shard exists.
  @spec start_raft_if_available(
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          binary(),
          :ets.tid(),
          atom(),
          keyword()
        ) :: boolean()
  @doc false
  def start_raft_if_available(
        index,
        shard_data_path,
        active_file_id,
        active_file_path,
        ets,
        instance_name \\ :default,
        opts \\ []
      ) do
    _ = {shard_data_path, active_file_id, active_file_path, ets, instance_name, opts}
    Process.whereis(Ferricstore.Raft.Batcher.batcher_name(index)) != nil
  end

  # -------------------------------------------------------------------
  # Terminate
  # -------------------------------------------------------------------

  @spec do_terminate(term(), map()) :: :ok
  @doc false
  def do_terminate(_reason, state) do
    t0 = System.monotonic_time(:microsecond)

    # Step 1: drain any in-flight async flush and flush remaining pending
    # writes synchronously to guarantee all data hits disk before exit.
    state = ShardFlush.await_in_flight(state)
    state = ShardFlush.flush_pending_sync(state)

    t_flush = System.monotonic_time(:microsecond)

    # Step 2: write v2 hint file for the active file so the next startup
    # can rebuild the keydir from hints instead of replaying the full log.
    hint_result = ShardFlush.write_hint_for_file(state, state.active_file_id)

    hint_dir_fsync_result =
      if hint_result == :ok do
        NIF.v2_fsync_dir(state.shard_data_path)
      end

    fsync_result = NIF.v2_fsync(state.active_file_path)
    shutdown_errors = shutdown_errors(hint_result, hint_dir_fsync_result, fsync_result)
    shutdown_status = if shutdown_errors == [], do: :ok, else: :warning

    t_hint = System.monotonic_time(:microsecond)

    # Step 3: emit shutdown telemetry for operator visibility.
    :telemetry.execute(
      [:ferricstore, :shard, :shutdown],
      %{
        flush_duration_us: t_flush - t0,
        hint_duration_us: t_hint - t_flush,
        total_duration_us: t_hint - t0
      },
      %{shard_index: state.index, status: shutdown_status, errors: shutdown_errors}
    )

    log_shutdown_result(state.index, t_flush - t0, t_hint - t_flush, shutdown_errors)

    :ok
  end

  defp shutdown_errors(hint_result, hint_dir_fsync_result, fsync_result) do
    []
    |> maybe_shutdown_error(:hint_write, hint_result)
    |> maybe_shutdown_error(:hint_dir_fsync, hint_dir_fsync_result)
    |> maybe_shutdown_error(:active_fsync, fsync_result)
    |> Enum.reverse()
  end

  defp maybe_shutdown_error(errors, _operation, result) when result in [:ok, nil], do: errors

  defp maybe_shutdown_error(errors, operation, {:error, reason}),
    do: [{operation, reason} | errors]

  defp maybe_shutdown_error(errors, operation, other),
    do: [{operation, {:unexpected_result, other}} | errors]

  defp log_shutdown_result(index, flush_us, hint_us, []) do
    Logger.info(
      "Shard #{index}: shutdown complete " <>
        "(flush=#{flush_us}us, hint=#{hint_us}us)"
    )
  end

  defp log_shutdown_result(index, flush_us, hint_us, errors) do
    Logger.warning(
      "Shard #{index}: shutdown complete with warnings " <>
        "(flush=#{flush_us}us, hint=#{hint_us}us, errors=#{inspect(errors)})"
    )
  end

  defp cleanup_compact_temps(shard_path, files) do
    Enum.each(files, fn name ->
      if String.starts_with?(name, "compact_") and String.ends_with?(name, ".log") do
        path = Path.join(shard_path, name)

        case Ferricstore.FS.rm(path) do
          :ok ->
            Logger.warning("Shard: removed leftover compaction temp file #{name}")

          {:error, reason} ->
            :telemetry.execute(
              [:ferricstore, :shard, :compact_temp_cleanup_failed],
              %{count: 1},
              %{path: path, name: name, reason: reason}
            )

            Logger.error(
              "Shard: failed to remove leftover compaction temp file #{name}: #{inspect(reason)}"
            )
        end
      end
    end)
  end

  defp regular_log_file?(name) do
    match?([_fid], regular_log_file_id(name))
  end

  defp regular_hint_file?(name), do: numeric_file_id(name, ".hint") != nil

  defp regular_log_file_id(name) do
    case numeric_file_id(name, ".log") do
      nil -> []
      fid -> [fid]
    end
  end

  defp numeric_file_id(name, suffix) do
    with true <- String.ends_with?(name, suffix),
         false <- String.starts_with?(name, "compact_"),
         stem <- String.trim_trailing(name, suffix),
         {fid, ""} <- Integer.parse(stem),
         true <- fid >= 0 do
      fid
    else
      _ -> nil
    end
  end

  defp recover_from_hints_or_logs(shard_path, keydir, shard_index, log_files, [], instance_ctx) do
    Enum.each(log_files, fn log_name ->
      recover_from_log(shard_path, log_name, keydir, shard_index, instance_ctx)
    end)
  end

  defp recover_from_hints_or_logs(
         shard_path,
         keydir,
         shard_index,
         log_files,
         hint_files,
         instance_ctx
       ) do
    hint_by_fid =
      Map.new(hint_files, fn hint_name ->
        fid = hint_name |> String.trim_trailing(".hint") |> String.to_integer()
        {fid, hint_name}
      end)

    hint_offsets =
      Enum.reduce(log_files, %{}, fn log_name, acc ->
        fid = log_file_id(log_name)

        case Map.fetch(hint_by_fid, fid) do
          {:ok, hint_name} ->
            case recover_from_hint(shard_path, hint_name, keydir, shard_index, instance_ctx) do
              {:ok, ^fid, end_offset} ->
                # Hint files contain live entries only. Scan tombstone metadata
                # from the paired log so deletes still override older hints
                # without reloading full values during startup.
                recover_tombstones_from_log(
                  shard_path,
                  log_name,
                  keydir,
                  shard_index,
                  instance_ctx
                )

                Map.put(acc, fid, end_offset)

              {:error, _fid} ->
                recover_from_log(shard_path, log_name, keydir, shard_index, instance_ctx)
                acc
            end

          :error ->
            recover_from_log(shard_path, log_name, keydir, shard_index, instance_ctx)
            acc
        end
      end)

    replay_hinted_active_tail(
      shard_path,
      keydir,
      shard_index,
      log_files,
      hint_offsets,
      instance_ctx
    )
  end

  defp recover_from_hint(shard_path, hint_name, keydir, shard_index, instance_ctx) do
    hint_path = Path.join(shard_path, hint_name)
    fid = hint_name |> String.trim_trailing(".hint") |> String.to_integer()

    case NIF.v2_read_hint_file(hint_path) do
      {:ok, entries} ->
        Enum.each(entries, fn {key, _file_id, offset, value_size, expire_at_ms} ->
          # Cold insert (value=nil): only key bytes matter for off-heap tracking
          track_binary_add(shard_index, key, nil, instance_ctx)
          ExpiryTracker.adjust(instance_ctx, shard_index, 0, expire_at_ms)
          :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), fid, offset, value_size})
        end)

        {:ok, fid, hint_end_offset(entries)}

      _ ->
        {:error, fid}
    end
  end

  defp replay_hinted_active_tail(
         _shard_path,
         _keydir,
         _shard_index,
         [],
         _hint_offsets,
         _instance_ctx
       ),
       do: :ok

  defp replay_hinted_active_tail(
         shard_path,
         keydir,
         shard_index,
         log_files,
         hint_offsets,
         instance_ctx
       ) do
    active_log_name = List.last(log_files)
    active_fid = log_file_id(active_log_name)

    case hint_offsets do
      %{^active_fid => end_offset} ->
        # The active file can receive appends after its hint was written. Replay
        # only the tail so old hinted files stay fast and startup preserves
        # last-write-wins correctness for the still-mutable active log.
        recover_from_log_from_offset(
          shard_path,
          active_log_name,
          keydir,
          shard_index,
          end_offset,
          instance_ctx
        )

      _ ->
        :ok
    end
  end

  defp hint_end_offset(entries) do
    Enum.reduce(entries, 0, fn {key, _file_id, offset, value_size, _expire_at_ms}, acc ->
      max(acc, offset + @log_header_size + byte_size(key) + value_size)
    end)
  end

  defp log_file_id(name), do: name |> String.trim_trailing(".log") |> String.to_integer()
  defp hint_file_id(name), do: name |> String.trim_trailing(".hint") |> String.to_integer()

  defp recover_tombstones_from_log(shard_path, log_name, keydir, shard_index, instance_ctx) do
    log_path = Path.join(shard_path, log_name)
    fid = log_file_id(log_name)

    case NIF.v2_scan_tombstones(log_path) do
      {:ok, records} ->
        Enum.each(records, fn record ->
          recover_hint_tombstone(keydir, shard_index, fid, record, instance_ctx)
        end)

      {:error, reason} ->
        Logger.warning(
          "Shard #{shard_index}: tombstone scan failed for #{log_path}: #{inspect(reason)}; falling back to full metadata scan"
        )

        recover_tombstones_from_full_scan(log_path, keydir, shard_index, fid, instance_ctx)
    end
  end

  defp recover_tombstones_from_full_scan(log_path, keydir, shard_index, fid, instance_ctx) do
    recover_tombstones_from_full_scan_pages(
      log_path,
      0,
      recovery_scan_page_size(),
      keydir,
      shard_index,
      fid,
      instance_ctx
    )
  end

  defp recover_tombstones_from_full_scan_pages(
         log_path,
         offset,
         page_size,
         keydir,
         shard_index,
         fid,
         instance_ctx
       ) do
    case NIF.v2_scan_file_page(log_path, offset, page_size) do
      {:ok, records, next_offset, done?} ->
        Enum.each(records, fn
          {key, record_offset, value_size, expire_at_ms, true} ->
            record_size = @log_header_size + byte_size(key) + value_size

            recover_hint_tombstone(
              keydir,
              shard_index,
              fid,
              {key, record_offset, record_size, expire_at_ms},
              instance_ctx
            )

          _record ->
            :ok
        end)

        cond do
          done? ->
            :ok

          is_integer(next_offset) and next_offset > offset ->
            recover_tombstones_from_full_scan_pages(
              log_path,
              next_offset,
              page_size,
              keydir,
              shard_index,
              fid,
              instance_ctx
            )

          true ->
            fail_recovery_scan!(
              :recover_tombstones_from_full_scan,
              log_path,
              shard_index,
              {:non_advancing_scan, offset}
            )
        end

      {:error, reason} ->
        fail_recovery_scan!(:recover_tombstones_from_full_scan, log_path, shard_index, reason)

      other ->
        fail_recovery_scan!(
          :recover_tombstones_from_full_scan,
          log_path,
          shard_index,
          {:unexpected, other}
        )
    end
  end

  defp fail_recovery_scan!(operation, log_path, shard_index, reason) do
    :telemetry.execute(
      [:ferricstore, :bitcask, :recovery_scan_failed],
      %{count: 1},
      %{operation: operation, path: log_path, shard_index: shard_index, reason: inspect(reason)}
    )

    Logger.error(
      "Shard #{shard_index}: #{operation} failed to scan #{log_path}: #{inspect(reason)}"
    )

    raise "#{operation} failed to scan #{log_path}: #{inspect(reason)}"
  end

  defp recover_hint_tombstone(
         keydir,
         shard_index,
         fid,
         {key, offset, _record_size, _expire_at_ms},
         instance_ctx
       ) do
    case :ets.lookup(keydir, key) do
      [{^key, _value, expire_at_ms, _lfu, entry_fid, entry_off, _vsize}]
      when is_integer(entry_fid) and
             (entry_fid < fid or
                (entry_fid == fid and is_integer(entry_off) and entry_off < offset)) ->
        track_binary_remove(keydir, shard_index, key, instance_ctx)
        ExpiryTracker.adjust(instance_ctx, shard_index, expire_at_ms, 0)
        :ets.delete(keydir, key)

      _ ->
        :ok
    end
  end

  defp recover_hint_tombstone(_keydir, _shard_index, _fid, _record, _instance_ctx), do: :ok

  defp recover_record(
         keydir,
         shard_index,
         _fid,
         {key, _offset, _value_size, _expire_at_ms, true},
         instance_ctx
       ) do
    ExpiryTracker.adjust(instance_ctx, shard_index, expire_at_ms_for_key(keydir, key), 0)
    track_binary_remove(keydir, shard_index, key, instance_ctx)
    :ets.delete(keydir, key)
  end

  defp recover_record(
         keydir,
         shard_index,
         fid,
         {key, offset, value_size, expire_at_ms, false},
         instance_ctx
       ) do
    # Cold insert (value=nil): only key bytes matter for off-heap tracking
    track_binary_add(shard_index, key, nil, instance_ctx)

    ExpiryTracker.adjust(
      instance_ctx,
      shard_index,
      expire_at_ms_for_key(keydir, key),
      expire_at_ms
    )

    :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), fid, offset, value_size})
  end

  defp expire_at_ms_for_key(keydir, key) do
    case :ets.lookup(keydir, key) do
      [{^key, _value, expire_at_ms, _lfu, _fid, _offset, _value_size}] -> expire_at_ms
      _ -> 0
    end
  end

  defp expire_keys(state, expired_keys) do
    {shared_keys, promoted_keys} =
      Enum.split_with(expired_keys, fn key -> promoted_expiry_target(state, key) == nil end)

    state
    |> expire_shared_keys(shared_keys)
    |> expire_promoted_keys(promoted_keys)
  end

  defp expire_shared_keys(state, []), do: state

  defp expire_shared_keys(state, keys) do
    tracked_state =
      Enum.reduce(keys, state, fn key, acc_state ->
        ShardFlush.track_delete_dead_bytes(acc_state, key)
      end)

    case append_tombstone_batch_sync(state.active_file_path, keys) do
      {:ok, _locations} ->
        Enum.each(keys, fn key -> ShardETS.ets_delete_key(tracked_state, key) end)
        tracked_state

      {:error, reason} ->
        Logger.warning(
          "Shard #{state.index}: tombstone batch failed during expiry sweep for #{length(keys)} keys: #{inspect(reason)}"
        )

        state
    end
  end

  defp expire_promoted_keys(state, []), do: state

  defp expire_promoted_keys(state, promoted_keys) do
    promoted_keys
    |> Enum.group_by(&promoted_expiry_target(state, &1))
    |> Enum.reduce(state, fn
      {{redis_key, dedicated_path}, keys}, acc_state ->
        expire_promoted_key_group(acc_state, redis_key, dedicated_path, keys)

      {nil, keys}, acc_state ->
        expire_shared_keys(acc_state, keys)
    end)
  end

  defp expire_promoted_key_group(state, redis_key, dedicated_path, keys) do
    tracked_state =
      Enum.reduce(keys, state, fn key, acc_state ->
        ShardCompound.track_promoted_delete_bytes(acc_state, redis_key, key)
      end)

    case ShardCompound.promoted_tombstone_batch(dedicated_path, keys) do
      {:ok, _locations} ->
        Enum.each(keys, fn key -> ShardETS.ets_delete_key(tracked_state, key) end)

        Enum.reduce(keys, tracked_state, fn _key, acc_state ->
          ShardCompound.bump_promoted_writes(acc_state, redis_key)
        end)

      {:error, reason} ->
        Logger.warning(
          "Shard #{state.index}: promoted tombstone batch failed during expiry sweep for #{length(keys)} keys: #{inspect(reason)}"
        )

        state
    end
  end

  defp append_tombstone_batch_sync(path, keys) do
    ops = Enum.map(keys, &{:delete, &1})

    case NIF.v2_append_ops_batch_nosync(path, ops) do
      {:ok, locations} ->
        with :ok <- validate_tombstone_locations(locations, length(keys)),
             :ok <- NIF.v2_fsync(path) do
          {:ok, locations}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_tombstone_locations(locations, expected_count)
       when length(locations) == expected_count do
    if Enum.all?(locations, &valid_tombstone_location?/1) do
      :ok
    else
      {:error, {:tombstone_batch_result_mismatch, expected_count, locations}}
    end
  end

  defp validate_tombstone_locations(locations, expected_count),
    do: {:error, {:tombstone_batch_result_mismatch, expected_count, locations}}

  defp valid_tombstone_location?({:delete, offset, record_size})
       when is_integer(offset) and offset >= 0 and is_integer(record_size) and record_size >= 0,
       do: true

  defp valid_tombstone_location?(_location), do: false

  defp promoted_expiry_target(%{promoted_instances: promoted}, key) when is_binary(key) do
    if promoted_member_key?(key) do
      redis_key = CompoundKey.extract_redis_key(key)

      case Map.get(promoted, redis_key) do
        %{path: path} when is_binary(path) -> {redis_key, path}
        _ -> nil
      end
    end
  end

  defp promoted_expiry_target(_state, _key), do: nil

  defp promoted_member_key?(<<"H:", _rest::binary>>), do: true
  defp promoted_member_key?(<<"S:", _rest::binary>>), do: true
  defp promoted_member_key?(<<"Z:", _rest::binary>>), do: true
  defp promoted_member_key?(_key), do: false

  defp incr_expired_stats(%{instance_ctx: nil}, count) do
    Ferricstore.Stats.incr_expired_keys(count)
  end

  defp incr_expired_stats(%{instance_ctx: ctx}, count) do
    Ferricstore.Stats.incr_expired_keys(ctx, count)
  end

  defp update_sweep_ceiling(state, true = _hit_ceiling, max_keys) do
    new_count = state.sweep_at_ceiling_count + 1

    if new_count >= @struggling_threshold and not state.sweep_struggling do
      :telemetry.execute(
        [:ferricstore, :expiry, :struggling],
        %{
          shard_index: state.index,
          consecutive_ceiling_sweeps: new_count,
          max_keys_per_sweep: max_keys
        },
        %{}
      )

      {new_count, true}
    else
      {new_count, state.sweep_struggling}
    end
  end

  defp update_sweep_ceiling(state, false, _max_keys) do
    if state.sweep_struggling do
      :telemetry.execute(
        [:ferricstore, :expiry, :recovered],
        %{shard_index: state.index, previous_ceiling_sweeps: state.sweep_at_ceiling_count},
        %{}
      )
    end

    {0, false}
  end

  # -- Off-heap binary byte tracking --

  defp keydir_binary_ref(%{keydir_binary_bytes: ref, shard_count: count}, shard_index)
       when ref != nil do
    if shard_index < count, do: ref, else: nil
  end

  defp keydir_binary_ref(name, shard_index) when is_atom(name) do
    keydir_binary_ref_for_instance(name, shard_index)
  end

  defp keydir_binary_ref(_instance_ctx, shard_index) do
    keydir_binary_ref_for_instance(:default, shard_index)
  end

  defp keydir_binary_ref_for_instance(name, shard_index) do
    try do
      %{keydir_binary_bytes: ref, shard_count: count} = FerricStore.Instance.get(name)
      if ref != nil and shard_index < count, do: ref, else: nil
    rescue
      _ -> nil
    catch
      :exit, _ -> nil
    end
  end

  # Tracks bytes added for a fresh insert (no existing entry expected, or replaces).
  defp track_binary_add(shard_index, key, value, instance_ctx) do
    ref = keydir_binary_ref(instance_ctx, shard_index)

    if ref do
      bytes = offheap_size(key) + offheap_size(value)
      if bytes > 0, do: :atomics.add(ref, shard_index + 1, bytes)
    end
  end

  # Tracks bytes removed for a delete (lookup existing entry first).
  defp track_binary_remove(keydir, shard_index, key, instance_ctx) do
    ref = keydir_binary_ref(instance_ctx, shard_index)

    if ref do
      bytes =
        case :ets.lookup(keydir, key) do
          [{^key, val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(val)
          _ -> 0
        end

      if bytes > 0, do: :atomics.sub(ref, shard_index + 1, bytes)
    end
  end

  defp offheap_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
  defp offheap_size(_), do: 0
end
