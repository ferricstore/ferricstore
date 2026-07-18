defmodule Ferricstore.Store.Shard.Lifecycle do
  @moduledoc "Shard startup, recovery, expiry sweeping, and graceful shutdown."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.ExpiryContext

  alias Ferricstore.Store.{
    CompactionTombstoneCatalog,
    CompoundKey,
    ExpiryTracker,
    HintMetadata,
    LFU,
    Router,
    SegmentFilename
  }

  alias Ferricstore.Store.CompactionJournal
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Lifecycle.BinaryAccounting
  alias Ferricstore.Store.Shard.Lifecycle.ProbFiles
  alias Ferricstore.Store.Shard.Lifecycle.Shutdown

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
  @recovery_hint_page_items 4_096
  @recovery_hint_page_bytes 4 * 1024 * 1024
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
    case CompactionJournal.recover_all(shard_path) do
      :ok ->
        discover_active_file_after_compaction_recovery(shard_path)

      {:error, reason} ->
        Logger.error(
          "discover_active_file failed to recover compaction journal under #{shard_path}: #{inspect(reason)}"
        )

        raise "discover_active_file failed to recover compaction journal under #{shard_path}: #{inspect(reason)}"
    end
  end

  defp discover_active_file_after_compaction_recovery(shard_path) do
    case Ferricstore.FS.ls(shard_path) do
      {:ok, files} ->
        validate_canonical_numeric_file_names!(files)

        # Clean up leftover compaction temp files from a previous crash.
        # These are always incomplete — if compaction had finished, the
        # rename would have replaced the original and the temp is gone.
        cleanup_compact_temps(shard_path, files)

        Enum.reduce(files, {0, 0}, fn name, {best_id, _best_size} = best ->
          case regular_numeric_file(shard_path, name, ".log") do
            {:ok, file_id, size} when file_id > best_id -> {file_id, size}
            {:ok, 0, size} when best == {0, 0} -> {0, size}
            _ -> best
          end
        end)

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
        validate_canonical_numeric_file_names!(files)

        log_files =
          files
          |> Enum.filter(&regular_log_file?(shard_path, &1))
          |> Enum.sort_by(&log_file_id/1)

        Logger.debug(
          "Shard #{shard_index}: recover_keydir scanning #{length(log_files)} log file(s) at #{shard_path}"
        )

        # Try hint files first for faster recovery
        hint_files =
          files
          |> Enum.filter(&regular_hint_file?(shard_path, &1))
          |> Enum.sort_by(&hint_file_id/1)

        recover_from_hints_or_logs(
          shard_path,
          keydir,
          shard_index,
          log_files,
          hint_files,
          instance_ctx
        )

        BinaryAccounting.rebuild(keydir, shard_index, instance_ctx)

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
    recover_log_records(shard_path, log_name, keydir, shard_index, instance_ctx)
    BinaryAccounting.rebuild(keydir, shard_index, instance_ctx)
  end

  defp recover_log_records(shard_path, log_name, keydir, shard_index, instance_ctx) do
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
        case recovery_type_values(log_path, records) do
          {:ok, type_values} ->
            Enum.each(records, fn record ->
              recover_record(keydir, shard_index, fid, record, instance_ctx, type_values)
            end)

          {:error, reason} ->
            fail_recovery_scan!(operation, log_path, shard_index, reason)
        end

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
  # expired entries and conditionally removes the exact rows observed by the
  # scan. Replicated contexts record conditional deletes through Raft; direct
  # contexts rely on the record's absolute expiry during recovery. Tracks
  # consecutive ceiling-hit sweeps and emits telemetry when the sweep is
  # struggling or recovers.
  @spec do_expiry_sweep(map(), keyword()) :: map()
  @doc false
  def do_expiry_sweep(state, opts \\ []) do
    force? = Keyword.get(opts, :force, false)

    expiry_cutoff_ms =
      ExpiryContext.capture()
      |> ExpiryContext.safe_expiry_cutoff_ms()

    cond do
      force? ->
        do_expiry_sweep_scan(state, expiry_cutoff_ms)

      ExpiryTracker.count_for_state(state) == 0 ->
        recover_sweep_if_needed(state)

      not ExpiryTracker.due_for_state?(state, expiry_cutoff_ms) and not memory_pressure?(state) ->
        recover_sweep_if_needed(state)

      true ->
        do_expiry_sweep_scan(state, expiry_cutoff_ms)
    end
  end

  defp recover_sweep_if_needed(state) do
    {new_ceiling_count, new_struggling} = update_sweep_ceiling(state, false, 0)
    %{state | sweep_at_ceiling_count: new_ceiling_count, sweep_struggling: new_struggling}
  end

  defp do_expiry_sweep_scan(state, now) do
    max_keys =
      Application.get_env(:ferricstore, :expiry_max_keys_per_sweep, @default_max_keys_per_sweep)

    expired_entries = scan_expired(state.keydir, now, max_keys)
    scanned_count = length(expired_entries)

    {state, _expired_count} =
      if scanned_count > 0 do
        {state, expired_count} = expire_entries(state, expired_entries)

        if expired_count > 0 do
          incr_expired_stats(state, expired_count)
          Logger.debug("Shard #{state.index}: expiry sweep removed #{expired_count} key(s)")
        end

        {state, expired_count}
      else
        defer_next_due(state, now)
        {state, 0}
      end

    hit_ceiling = scanned_count >= max_keys and scanned_count > 0
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

  @spec scan_expired(:ets.tid(), integer(), non_neg_integer()) :: [tuple()]
  @doc false
  def scan_expired(keydir, now, limit) do
    # 7-tuple format: {key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}
    # Match entries where expire_at_ms > 0 and expire_at_ms <= now
    match_spec = [
      {{:_, :_, :"$1", :_, :_, :_, :_}, [{:andalso, {:>, :"$1", 0}, {:"=<", :"$1", now}}],
       [:"$_"]}
    ]

    case :ets.select(keydir, match_spec, limit) do
      {entries, _continuation} -> entries
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
  # Probabilistic sidecars
  # -------------------------------------------------------------------

  @spec validate_prob_files(binary(), non_neg_integer(), :ets.tid() | atom() | nil) :: :ok
  @doc false
  def validate_prob_files(shard_data_path, index, keydir \\ nil) do
    case ProbFiles.validate(shard_data_path, index, keydir) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Shard #{index}: validate_prob_files failed under #{shard_data_path}: #{inspect(reason)}"
        )

        raise "validate_prob_files failed under #{shard_data_path}: #{inspect(reason)}"
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
  def do_terminate(reason, state), do: Shutdown.do_terminate(reason, state)

  defp cleanup_compact_temps(shard_path, files) do
    Enum.each(files, fn name ->
      if compaction_temp_name?(name) do
        path = Path.join(shard_path, name)

        case remove_compaction_temp(path, name) do
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

  defp compaction_temp_name?(name) do
    (String.starts_with?(name, "compact_") and String.ends_with?(name, ".log")) or
      (String.starts_with?(name, "compaction_plan_") and
         (String.ends_with?(name, ".txn") or String.ends_with?(name, ".txn.tmp"))) or
      String.starts_with?(name, "compaction_tombstones_")
  end

  defp remove_compaction_temp(path, "compaction_tombstones_" <> _suffix),
    do: CompactionTombstoneCatalog.remove_path(path)

  defp remove_compaction_temp(path, _name), do: Ferricstore.FS.rm(path)

  defp regular_log_file?(shard_path, name),
    do: match?({:ok, _file_id, _size}, regular_numeric_file(shard_path, name, ".log"))

  defp regular_hint_file?(shard_path, name),
    do: match?({:ok, _file_id, _size}, regular_numeric_file(shard_path, name, ".hint"))

  defp regular_numeric_file(shard_path, name, suffix) do
    with {:ok, file_id} <- SegmentFilename.parse(name, suffix),
         {:ok, %File.Stat{type: :regular, size: size}} <-
           File.lstat(Path.join(shard_path, name)) do
      {:ok, file_id, size}
    else
      _ -> :skip
    end
  end

  defp validate_canonical_numeric_file_names!(files) do
    Enum.each(files, fn name ->
      validate_canonical_numeric_file_name!(name, ".log")
      validate_canonical_numeric_file_name!(name, ".hint")
    end)
  end

  defp validate_canonical_numeric_file_name!(name, suffix) do
    case SegmentFilename.parse(name, suffix) do
      {:error, {kind, ^name, canonical}} ->
        raise "#{kind}: #{inspect(name)} aliases #{inspect(canonical)}"

      _valid_or_unrelated ->
        :ok
    end
  end

  defp recover_from_hints_or_logs(shard_path, keydir, shard_index, log_files, [], instance_ctx) do
    Enum.each(log_files, fn log_name ->
      recover_log_records(shard_path, log_name, keydir, shard_index, instance_ctx)
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
            case recover_from_hint(
                   shard_path,
                   log_name,
                   hint_name,
                   keydir,
                   shard_index,
                   instance_ctx
                 ) do
              {:ok, ^fid, covered_offset} ->
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

                Map.put(acc, fid, covered_offset)

              {:error, _fid} ->
                recover_log_records(shard_path, log_name, keydir, shard_index, instance_ctx)
                acc
            end

          :error ->
            recover_log_records(shard_path, log_name, keydir, shard_index, instance_ctx)
            acc
        end
      end)

    replay_hinted_tails(
      shard_path,
      keydir,
      shard_index,
      log_files,
      hint_offsets,
      instance_ctx
    )
  end

  defp recover_from_hint(
         shard_path,
         log_name,
         hint_name,
         keydir,
         _shard_index,
         _instance_ctx
       ) do
    hint_path = Path.join(shard_path, hint_name)
    log_path = Path.join(shard_path, log_name)
    fid = hint_name |> String.trim_trailing(".hint") |> String.to_integer()

    with {:ok, covered_offset} <- HintMetadata.covered_source_size(log_path, hint_path, fid),
         {:ok, _last_live_end_offset} <- recover_hint_pages(hint_path, keydir, fid, 0, 0) do
      {:ok, fid, covered_offset}
    else
      {:error, _reason} -> {:error, fid}
    end
  end

  defp recover_hint_pages(hint_path, keydir, fid, offset, end_offset) do
    case NIF.v2_read_hint_file_page(
           hint_path,
           offset,
           @recovery_hint_page_items,
           @recovery_hint_page_bytes
         ) do
      {:ok, entries, next_offset, done?} ->
        next_end_offset = recover_hint_entries(entries, keydir, fid, end_offset)

        cond do
          done? ->
            {:ok, next_end_offset}

          is_integer(next_offset) and next_offset > offset ->
            recover_hint_pages(hint_path, keydir, fid, next_offset, next_end_offset)

          true ->
            {:error, {:non_advancing_hint_page, offset}}
        end

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:unexpected_hint_page, other}}
    end
  end

  defp recover_hint_entries(entries, keydir, fid, end_offset) do
    Enum.reduce(entries, end_offset, fn
      {key, _file_id, offset, value_size, expire_at_ms}, acc
      when is_binary(key) and is_integer(offset) and offset >= 0 and is_integer(value_size) and
             value_size >= 0 ->
        :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), fid, offset, value_size})
        max(acc, offset + @log_header_size + byte_size(key) + value_size)

      _invalid, acc ->
        acc
    end)
  end

  defp replay_hinted_tails(
         _shard_path,
         _keydir,
         _shard_index,
         [],
         _hint_offsets,
         _instance_ctx
       ),
       do: :ok

  defp replay_hinted_tails(
         shard_path,
         keydir,
         shard_index,
         log_files,
         hint_offsets,
         instance_ctx
       ) do
    Enum.each(log_files, fn log_name ->
      fid = log_file_id(log_name)

      case Map.fetch(hint_offsets, fid) do
        {:ok, covered_offset} ->
          replay_hinted_tail(
            shard_path,
            log_name,
            keydir,
            shard_index,
            covered_offset,
            instance_ctx
          )

        :error ->
          :ok
      end
    end)

    :ok
  end

  defp replay_hinted_tail(
         shard_path,
         log_name,
         keydir,
         shard_index,
         covered_offset,
         instance_ctx
       ) do
    log_path = Path.join(shard_path, log_name)

    case File.lstat(log_path) do
      {:ok, %File.Stat{type: :regular, size: size}} when size > covered_offset ->
        # A direct writer can already hold the old active path when rotation
        # publishes the next segment. Replay every hinted tail, not just the
        # newest segment, so a delayed append cannot be hidden by a valid hint.
        recover_from_log_from_offset(
          shard_path,
          log_name,
          keydir,
          shard_index,
          covered_offset,
          instance_ctx
        )

      {:ok, %File.Stat{type: :regular, size: ^covered_offset}} ->
        :ok

      {:ok, %File.Stat{type: :regular, size: size}} ->
        raise "hint covered offset #{covered_offset} exceeds #{log_path} size #{size}"

      {:ok, %File.Stat{type: type}} ->
        raise "hint source #{log_path} is not a regular file: #{inspect(type)}"

      {:error, reason} ->
        raise "hint source #{log_path} cannot be inspected: #{inspect(reason)}"
    end
  end

  defp log_file_id(name), do: name |> String.trim_trailing(".log") |> String.to_integer()
  defp hint_file_id(name), do: name |> String.trim_trailing(".hint") |> String.to_integer()

  defp recover_tombstones_from_log(shard_path, log_name, keydir, shard_index, instance_ctx) do
    log_path = Path.join(shard_path, log_name)
    fid = log_file_id(log_name)

    case recover_tombstone_pages(
           log_path,
           0,
           min(recovery_scan_page_size(), 65_536),
           keydir,
           shard_index,
           fid,
           instance_ctx
         ) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "Shard #{shard_index}: tombstone scan failed for #{log_path}: #{inspect(reason)}; falling back to full metadata scan"
        )

        recover_tombstones_from_full_scan(log_path, keydir, shard_index, fid, instance_ctx)
    end
  end

  defp recover_tombstone_pages(
         log_path,
         offset,
         page_size,
         keydir,
         shard_index,
         fid,
         instance_ctx
       ) do
    case NIF.v2_scan_tombstones_page(log_path, offset, page_size) do
      {:ok, records, next_offset, done?} when is_list(records) ->
        Enum.each(records, fn record ->
          recover_hint_tombstone(keydir, shard_index, fid, record, instance_ctx)
        end)

        cond do
          done? == true and is_integer(next_offset) and next_offset >= offset ->
            :ok

          done? == false and is_integer(next_offset) and next_offset > offset ->
            recover_tombstone_pages(
              log_path,
              next_offset,
              page_size,
              keydir,
              shard_index,
              fid,
              instance_ctx
            )

          true ->
            {:error, {:invalid_tombstone_page_cursor, offset, next_offset, done?}}
        end

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:unexpected_tombstone_page, other}}
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
         _shard_index,
         fid,
         {key, offset, _record_size, _expire_at_ms},
         _instance_ctx
       ) do
    case :ets.lookup(keydir, key) do
      [{^key, _value, _expire_at_ms, _lfu, entry_fid, entry_off, _vsize}]
      when is_integer(entry_fid) and
             (entry_fid < fid or
                (entry_fid == fid and is_integer(entry_off) and entry_off < offset)) ->
        :ets.delete(keydir, key)

      _ ->
        :ok
    end
  end

  defp recover_hint_tombstone(_keydir, _shard_index, _fid, _record, _instance_ctx), do: :ok

  defp recover_record(
         keydir,
         _shard_index,
         _fid,
         {key, _offset, _value_size, _expire_at_ms, true},
         _instance_ctx,
         _type_values
       ) do
    :ets.delete(keydir, key)
  end

  defp recover_record(
         keydir,
         _shard_index,
         fid,
         {key, offset, value_size, expire_at_ms, false},
         _instance_ctx,
         type_values
       ) do
    value = Map.get(type_values, offset)
    :ets.insert(keydir, {key, value, expire_at_ms, LFU.initial(), fid, offset, value_size})
  end

  defp recovery_type_values(log_path, records) do
    type_records =
      Enum.filter(records, fn
        {<<"T:", _rest::binary>>, _offset, _value_size, _expire_at_ms, false} -> true
        _record -> false
      end)

    offsets = Enum.map(type_records, &elem(&1, 1))

    case offsets do
      [] ->
        {:ok, %{}}

      offsets ->
        case NIF.v2_pread_batch(log_path, offsets) do
          {:ok, values} when length(values) == length(offsets) ->
            if Enum.all?(values, &is_binary/1) do
              {:ok, Map.new(Enum.zip(offsets, values))}
            else
              {:error, :invalid_type_metadata_values}
            end

          {:ok, _wrong_length} ->
            {:error, :invalid_type_metadata_batch_length}

          {:error, reason} ->
            {:error, {:type_metadata_read_failed, reason}}

          other ->
            {:error, {:unexpected_type_metadata_read, other}}
        end
    end
  end

  defp expire_entries(%{instance_ctx: ctx} = state, entries) when not is_nil(ctx) do
    if Router.durable_context?(ctx) and context_keydir?(state, ctx) do
      expire_replicated_entries(state, entries)
    else
      expire_direct_entries(state, entries)
    end
  end

  defp expire_entries(state, entries), do: expire_direct_entries(state, entries)

  defp context_keydir?(%{index: index, keydir: keydir}, %{keydir_refs: refs, shard_count: count})
       when is_integer(index) and index >= 0 and index < count do
    elem(refs, index) == keydir
  end

  defp context_keydir?(_state, _ctx), do: false

  defp expire_replicated_entries(state, entries) do
    expected =
      Enum.map(entries, fn {key, _value, expire_at_ms, _lfu, _fid, _off, _size} ->
        {key, expire_at_ms}
      end)

    case Router.expire_if_batch(state.instance_ctx, state.index, expected) do
      results when is_list(results) ->
        {state, Enum.count(results, &(&1 == true))}

      {:error, reason} ->
        Logger.warning("Shard #{state.index}: replicated expiry batch failed: #{inspect(reason)}")

        {state, 0}

      other ->
        Logger.warning(
          "Shard #{state.index}: invalid replicated expiry result: #{inspect(other)}"
        )

        {state, 0}
    end
  end

  defp expire_direct_entries(state, entries) do
    Enum.reduce(entries, {state, 0}, fn entry, {acc_state, count} ->
      if ShardETS.delete_exact_entry(acc_state, entry) do
        key = elem(entry, 0)

        next_state =
          case promoted_expiry_target(acc_state, key) do
            {redis_key, _dedicated_path} ->
              ShardCompound.track_promoted_delete_bytes_entry(acc_state, redis_key, entry)

            nil ->
              ShardFlush.track_delete_dead_bytes_entry(acc_state, entry)
          end

        {next_state, count + 1}
      else
        {acc_state, count}
      end
    end)
  end

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

  def track_binary_add(shard_index, key, value, instance_ctx) do
    BinaryAccounting.track_add(shard_index, key, value, instance_ctx)
  end
end
