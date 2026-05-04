defmodule Ferricstore.Store.Promotion do
  @moduledoc """
  Collection promotion: migrates large compound-key collections from the
  shared shard Bitcask into a dedicated per-key Bitcask instance.

  ## Background (spec section 2B.4b)

  Small collections (hashes, sets, sorted sets) are stored as compound keys
  in the shared shard Bitcask (`H:key\\0field`, `S:key\\0member`,
  `Z:key\\0member`). When any compound-key collection exceeds the
  configurable promotion threshold (default: 100 entries), it is promoted
  to a dedicated Bitcask instance stored under:

      dedicated/shard_N/{type}:{sha256_of_key}/

  where `{type}` is `hash`, `set`, or `zset`.

  Promotion is **one-way** -- once promoted, a collection stays in its
  dedicated instance even if entries are later deleted below the threshold.
  The dedicated instance is only removed when the entire key is deleted
  via `DEL` / `UNLINK`.

  ## Lists are not promoted

  Lists store all elements as a single serialized Erlang term in one
  Bitcask entry (via `ListOps`). Since there is no compound key fan-out,
  a list with 1000 elements is still a single Bitcask entry and does not
  benefit from promotion. List promotion is intentionally skipped.

  ## Promotion marker

  When a key is promoted, a marker entry `PM:redis_key` is written to the
  shared Bitcask with the type as its value (`"hash"`, `"set"`, or
  `"zset"`). This allows the shard to rediscover promoted keys on restart
  by scanning for `PM:` prefixed keys during initialization.

  ## Configuration

      config :ferricstore, :promotion_threshold, 100

  Set to `0` to disable automatic promotion entirely (no collections will
  ever be promoted).
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.HLC
  alias Ferricstore.Store.{CompoundKey, LFU}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  require Logger

  @promotion_marker_prefix "PM:"
  @cold_read_timeout_ms 10_000
  @compaction_latch_sleep_ms 1
  @default_compaction_latch_timeout_ms 30_000

  @spec threshold() :: non_neg_integer()
  def threshold do
    :persistent_term.get(:ferricstore_promotion_threshold, 100)
  rescue
    ArgumentError -> Application.get_env(:ferricstore, :promotion_threshold, 100)
  end

  @doc "Returns the promotion threshold from instance ctx."
  @spec threshold(FerricStore.Instance.t()) :: non_neg_integer()
  def threshold(_ctx) do
    # The promotion threshold is not yet stored on the Instance struct,
    # so delegate to the global version. This ctx variant exists for
    # API consistency and can be updated when the field is added.
    threshold()
  end

  @spec dedicated_path(binary(), non_neg_integer(), atom(), binary()) :: binary()
  def dedicated_path(data_dir, shard_index, type, redis_key) do
    hash = :crypto.hash(:sha256, redis_key) |> Base.encode16(case: :lower)
    type_str = Atom.to_string(type)
    Path.join([data_dir, "dedicated", "shard_#{shard_index}", "#{type_str}:#{hash}"])
  end

  @spec marker_key(binary()) :: binary()
  def marker_key(redis_key), do: @promotion_marker_prefix <> redis_key

  @doc """
  Runs `fun` while holding the per-promoted-key compaction latch.

  Promoted dedicated compaction rewrites member records into a newer log file.
  Raft-applied promoted writes must not append to that same per-key log while
  the rewrite snapshot is in progress, or disk replay order can resurrect stale
  compacted records. The latch is only consulted on promoted dedicated paths.
  """
  @spec with_compaction_latch(map(), binary(), (-> term())) :: term()
  def with_compaction_latch(owner, redis_key, fun) when is_function(fun, 0) do
    case compaction_latch(owner, redis_key) do
      nil ->
        fun.()

      {tab, latch_key, shard_index} ->
        acquire_compaction_latch(tab, latch_key, shard_index)

        try do
          fun.()
        after
          :ets.take(tab, latch_key)
        end
    end
  end

  @doc """
  Waits until no promoted compaction latch is held for `redis_key`.

  Called by Raft apply before appending promoted dedicated writes/tombstones.
  """
  @spec await_compaction_latch(map(), binary()) :: :ok
  def await_compaction_latch(owner, redis_key) do
    case compaction_latch(owner, redis_key) do
      nil -> :ok
      {tab, latch_key, shard_index} -> wait_compaction_latch_clear!(tab, latch_key, shard_index)
    end
  end

  @spec open_dedicated(binary(), non_neg_integer(), atom(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def open_dedicated(data_dir, shard_index, type, redis_key) do
    path = dedicated_path(data_dir, shard_index, type, redis_key)
    created_dir? = not Ferricstore.FS.dir?(path)
    Ferricstore.FS.mkdir_p!(path)

    with :ok <- maybe_fsync_dir(created_dir?, Path.dirname(path), :create_dedicated_dir) do
      active_file = Path.join(path, "00000.log")

      # credo:disable-for-next-line Credo.Check.Refactor.UnlessWithElse
      created_file? =
        unless Ferricstore.FS.exists?(active_file) do
          Ferricstore.FS.touch!(active_file)
          true
        else
          false
        end

      with :ok <- maybe_fsync_dir(created_dir? or created_file?, path, :create_active_file) do
        {:ok, path}
      end
    end
  end

  defp maybe_fsync_dir(false, _path, _phase), do: :ok
  defp maybe_fsync_dir(true, path, phase), do: fsync_dir(path, phase)

  defp fsync_dir(path, phase) do
    result =
      case Process.get(:ferricstore_promotion_fsync_dir_hook) do
        fun when is_function(fun, 1) -> fun.(path)
        _ -> NIF.v2_fsync_dir(path)
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Promotion: directory fsync failed during #{phase} for #{path}: #{inspect(reason)}"
        )

        {:error, {:fsync_dir_failed, phase, reason}}
    end
  end

  @spec promote_hash!(binary(), reference(), atom(), binary(), non_neg_integer(), term()) ::
          {:ok, reference()} | {:error, term()}
  def promote_hash!(redis_key, shared_store, keydir, data_dir, shard_index, instance_ctx \\ nil) do
    promote_collection!(
      :hash,
      redis_key,
      shared_store,
      keydir,
      data_dir,
      shard_index,
      instance_ctx
    )
  end

  @spec promote_collection!(
          atom(),
          binary(),
          binary(),
          atom(),
          binary(),
          non_neg_integer(),
          term()
        ) ::
          {:ok, reference()} | {:error, term()}
  def promote_collection!(
        type,
        redis_key,
        shard_data_path,
        keydir,
        data_dir,
        shard_index,
        instance_ctx \\ nil
      ) do
    prefix = compound_prefix_for(type, redis_key)
    type_str = CompoundKey.encode_type(type)
    type_label = type_label(type)
    now = HLC.now_ms()

    entries =
      :ets.foldl(
        fn {key, value, exp, _lfu, fid, off, _vsize}, acc ->
          if is_binary(key) and String.starts_with?(key, prefix) and (exp == 0 or exp > now) do
            case promotion_entry_value(shard_data_path, key, value, fid, off) do
              nil -> acc
              live_value -> [{key, live_value, exp} | acc]
            end
          else
            acc
          end
        end,
        [],
        keydir
      )

    active_path = find_active(shard_data_path)
    mk = marker_key(redis_key)

    # --------------------------------------------------------------------
    # Crash-safe promotion order (docs/bitcask-background-fsync.md §D2):
    #
    #   1. Write the marker FIRST. If we crash here, recover_promoted
    #      sees the marker and falls back to compound keys still in the
    #      shared log (not yet tombstoned) — no data loss.
    #   2. Open/create dedicated dir + write the batch. Dedicated data is
    #      fsynced by v2_append_batch. If we crash here, recover_promoted
    #      sees the marker, opens the dedicated dir, finds either data
    #      (good) or empty (falls back to shared log compound keys).
    #   3. Tombstone compound keys in shared log LAST. If we crash
    #      partway through, the un-tombstoned keys are still in the
    #      shared log — recover_promoted reads them via the fallback.
    #
    # Old order (dedicated-first → tombstones → marker) lost data when
    # crashed between tombstones and marker: compound keys gone, no
    # marker to locate dedicated dir, collection silently vanished.
    # --------------------------------------------------------------------

    # Step 1: marker
    case NIF.v2_append_record(active_path, mk, type_str, 0) do
      {:ok, {moffset, mvsize}} ->
        marker_fid = file_id_from_path(active_path)

        track_binary_insert(keydir, shard_index, mk, type_str, instance_ctx)
        :ets.insert(keydir, {mk, type_str, 0, LFU.initial(), marker_fid, moffset, mvsize})

      {:error, reason} ->
        Logger.error(
          "Promotion: marker write failed for #{inspect(redis_key)}: #{inspect(reason)}"
        )

        raise "promotion marker write failed: #{inspect(reason)}"
    end

    # Step 2: open dedicated + write batch
    dedicated_path =
      case open_dedicated(data_dir, shard_index, type, redis_key) do
        {:ok, path} ->
          path

        {:error, reason} ->
          Logger.error(
            "Promotion: open dedicated failed for #{inspect(redis_key)}: #{inspect(reason)}"
          )

          raise "promotion open dedicated failed: #{inspect(reason)}"
      end

    if entries != [] do
      batch = Enum.map(entries, fn {k, v, exp} -> {k, v, exp} end)
      dedicated_active = find_active(dedicated_path)

      dedicated_fid = file_id_from_path(dedicated_active)

      case NIF.v2_append_batch(dedicated_active, batch) do
        {:ok, locations} ->
          Enum.zip(entries, locations)
          |> Enum.each(fn {{k, _v, _exp}, {offset, value_size}} ->
            :ets.update_element(keydir, k, [{5, dedicated_fid}, {6, offset}, {7, value_size}])
          end)

        {:error, reason} ->
          Logger.error(
            "Promotion: v2_append_batch failed for #{inspect(redis_key)}: #{inspect(reason)}"
          )

          raise "promotion dedicated write failed: #{inspect(reason)}"
      end
    end

    # Step 3: tombstone compound keys in shared log (LAST step).
    # Batch tombstones and fsync once. If this crashes before the fsync,
    # recovery is still safe because marker + dedicated data are already
    # durable, and any un-tombstoned shared keys remain valid fallback copies.
    # If the write/fsync returns an error, fail closed so the caller does not
    # observe a successful promotion whose shared-log cleanup may be missing.
    tombstone_ops = Enum.map(entries, fn {key, _value, _exp} -> {:delete, key} end)

    if tombstone_ops != [] do
      run_before_shared_tombstones_hook()

      case NIF.v2_append_ops_batch_nosync(active_path, tombstone_ops) do
        {:ok, _locations} ->
          case NIF.v2_fsync(active_path) do
            :ok ->
              :ok

            {:error, reason} ->
              Logger.error(
                "Promotion: tombstone batch fsync failed for #{inspect(redis_key)}: #{inspect(reason)}"
              )

              raise "promotion shared tombstone fsync failed: #{inspect(reason)}"
          end

        {:error, reason} ->
          Logger.error(
            "Promotion: tombstone batch write failed for #{inspect(redis_key)}: #{inspect(reason)}"
          )

          raise "promotion shared tombstone write failed: #{inspect(reason)}"
      end
    end

    Logger.info(
      "Promoted #{type_label} #{inspect(redis_key)} to dedicated Bitcask " <>
        "(#{length(entries)} entries, shard #{shard_index})"
    )

    {:ok, dedicated_path}
  end

  defp run_before_shared_tombstones_hook do
    case Process.get(:ferricstore_promotion_before_shared_tombstones_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  @spec recover_promoted(binary(), atom(), binary(), non_neg_integer(), term()) :: map()
  def recover_promoted(shard_data_path, keydir, data_dir, shard_index, instance_ctx \\ nil) do
    # v2: promotion markers are recovered from ETS (populated by recover_keydir).
    # Use :ets.select with a match spec bound to the "PM:" prefix instead of
    # scanning every key in the keydir via :ets.foldl (memory audit L6).
    pm_prefix = "PM:"
    pm_len = byte_size(pm_prefix)

    # Match PM: keys with either a binary value (hot) or nil (cold, needs pread).
    # After recover_keydir, PM: entries may be cold (value=nil, offset>0).
    match_spec = [
      {{:"$1", :"$2", :_, :_, :"$3", :"$4", :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, pm_len},
           {:==, {:binary_part, :"$1", 0, pm_len}, pm_prefix}}}
       ], [{{:"$1", :"$2", :"$3", :"$4"}}]}
    ]

    all_markers =
      :ets.select(keydir, match_spec)
      |> Enum.flat_map(fn {full_key, value, fid, offset} ->
        <<"PM:", redis_key::binary>> = full_key

        # If value is nil (cold entry), read the type string from disk
        type_str =
          if is_binary(value) do
            value
          else
            file_path =
              Path.join(
                shard_data_path,
                "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log"
              )

            case read_cold_async(file_path, offset, full_key) do
              {:ok, v} when is_binary(v) -> v
              _ -> nil
            end
          end

        if value == nil and is_binary(type_str) do
          track_binary_insert(keydir, shard_index, full_key, type_str, instance_ctx)
          :ets.update_element(keydir, full_key, {2, type_str})
        end

        case decode_promoted_type(type_str) do
          {:ok, type} ->
            [{redis_key, type}]

          :error ->
            Logger.warning(
              "Promotion recovery: ignoring invalid marker #{inspect(full_key)} with type #{inspect(type_str)}"
            )

            track_binary_delete(keydir, shard_index, full_key, instance_ctx)
            :ets.delete(keydir, full_key)
            []
        end
      end)
      |> Enum.uniq_by(fn {redis_key, _} -> redis_key end)

    marker_types = Map.new(all_markers)

    shared_live_compound_keys = shared_live_compound_keys_by_marker(keydir, marker_types)

    Enum.reduce(all_markers, %{}, fn {redis_key, type}, acc ->
      {:ok, dedicated_path} = open_dedicated(data_dir, shard_index, type, redis_key)

      log_files = list_log_files(dedicated_path)

      final_state =
        Enum.reduce(log_files, %{}, fn {fid, file_path}, acc ->
          case NIF.v2_scan_file(file_path) do
            {:ok, records} ->
              Enum.reduce(records, acc, fn {key, offset, value_size, expire_at_ms, is_tombstone},
                                           inner_acc ->
                if is_tombstone do
                  Map.put(inner_acc, key, :tombstone)
                else
                  Map.put(
                    inner_acc,
                    key,
                    {:live, fid, file_path, offset, value_size, expire_at_ms}
                  )
                end
              end)

            _ ->
              acc
          end
        end)

      # Crash-safety fallback (docs/bitcask-background-fsync.md §D2):
      #
      # If the marker exists but the dedicated dir has no records at all,
      # we crashed between step 1 (marker write) and step 2 (dedicated
      # batch) of a marker-first promotion. In that case the compound keys
      # in the SHARED log are still authoritative (step 3 tombstones hadn't
      # run yet), and returning a promoted instance would route public reads
      # to the empty dedicated dir.
      #
      # recover_keydir (called before us) has already re-mapped those
      # compound keys back into the keydir from the shared log. We
      # simply leave them in place — by NOT overwriting them with
      # "missing from dedicated" we preserve the data.
      dedicated_keys = final_state |> Map.keys() |> MapSet.new()

      partial_dedicated? =
        shared_uncovered_live_compound?(
          Map.get(shared_live_compound_keys, redis_key, MapSet.new()),
          dedicated_keys
        )

      dedicated_empty? = map_size(final_state) == 0

      if not dedicated_empty? and not partial_dedicated? do
        # Normal recovery path: dedicated has a complete final state. Apply it
        # even when that state is tombstone-only so stale shared rows cannot
        # survive restart.
        hot_cache_threshold = recover_hot_cache_threshold(instance_ctx)

        Enum.each(final_state, fn
          {key, :tombstone} ->
            track_binary_delete(keydir, shard_index, key, instance_ctx)
            :ets.delete(keydir, key)

          {key, {:live, fid, file_path, offset, value_size, expire_at_ms}} ->
            disk_value =
              case read_cold_async(file_path, offset, key) do
                {:ok, v} when v != nil -> v
                _ -> nil
              end

            value = ShardETS.value_for_ets(disk_value, hot_cache_threshold)
            track_binary_insert(keydir, shard_index, key, value, instance_ctx)

            :ets.insert(
              keydir,
              {key, value, expire_at_ms, LFU.initial(), fid, offset, value_size}
            )
        end)
      end

      if dedicated_empty? or partial_dedicated? do
        # Fallback path: marker exists, dedicated has no records. Compound
        # keys in shared log (already in keydir via recover_keydir) are the
        # source of truth, so do not include this key in promoted_instances.
        Logger.info(
          "Promotion recovery: marker for #{inspect(redis_key)} exists but dedicated " <>
            "dir is incomplete; falling back to compound keys in shared log."
        )

        acc
      else
        total_bytes = dir_total_size(dedicated_path)

        live_bytes =
          Enum.reduce(final_state, 0, fn {key, entry}, acc ->
            case entry do
              :tombstone ->
                acc

              {:live, _fid, _path, _off, _vs, _exp} ->
                case :ets.lookup(keydir, key) do
                  [{^key, _v, _exp, _lfu, _f, _o, vsize}] when vsize > 0 ->
                    acc + 26 + byte_size(key) + vsize

                  _ ->
                    acc
                end
            end
          end)

        dead_bytes = max(total_bytes - live_bytes, 0)

        Map.put(acc, redis_key, %{
          path: dedicated_path,
          writes: 0,
          total_bytes: total_bytes,
          dead_bytes: dead_bytes,
          last_compacted_at: nil
        })
      end
    end)
  end

  defp recover_hot_cache_threshold(%{hot_cache_max_value_size: threshold})
       when is_integer(threshold) and threshold >= 0,
       do: threshold

  defp recover_hot_cache_threshold(_instance_ctx), do: 65_536

  defp shared_live_compound_keys_by_marker(keydir, marker_types) do
    now = HLC.now_ms()

    :ets.foldl(
      fn
        {key, _value, exp, _lfu, _fid, _off, _vsize}, acc when is_binary(key) ->
          if live_hash_set_or_zset_compound?(key, exp, now) do
            redis_key = CompoundKey.extract_redis_key(key)

            case Map.get(marker_types, redis_key) do
              nil ->
                acc

              marker_type ->
                if compound_key_type?(key, marker_type) do
                  Map.update(acc, redis_key, MapSet.new([key]), &MapSet.put(&1, key))
                else
                  acc
                end
            end
          else
            acc
          end

        _entry, acc ->
          acc
      end,
      %{},
      keydir
    )
  end

  defp live_hash_set_or_zset_compound?(key, exp, now) do
    (exp == 0 or exp > now) and
      (match?(<<"H:", _::binary>>, key) or match?(<<"S:", _::binary>>, key) or
         match?(<<"Z:", _::binary>>, key))
  end

  defp compound_key_type?(<<"H:", _::binary>>, :hash), do: true
  defp compound_key_type?(<<"S:", _::binary>>, :set), do: true
  defp compound_key_type?(<<"Z:", _::binary>>, :zset), do: true
  defp compound_key_type?(_key, _type), do: false

  defp decode_promoted_type("hash"), do: {:ok, :hash}
  defp decode_promoted_type("set"), do: {:ok, :set}
  defp decode_promoted_type("zset"), do: {:ok, :zset}
  defp decode_promoted_type(_type_str), do: :error

  defp shared_uncovered_live_compound?(shared_keys, dedicated_keys) do
    Enum.any?(shared_keys, fn key -> not MapSet.member?(dedicated_keys, key) end)
  end

  @spec cleanup_promoted!(binary(), binary(), atom(), binary(), non_neg_integer(), term()) :: :ok
  def cleanup_promoted!(
        redis_key,
        shard_data_path,
        keydir,
        data_dir,
        shard_index,
        instance_ctx \\ nil
      ) do
    mk = marker_key(redis_key)

    type =
      case :ets.lookup(keydir, mk) do
        [{^mk, type_str, _exp, _lfu, fid, off, _vsize}] ->
          case promotion_entry_value(shard_data_path, mk, type_str, fid, off) do
            type_str when is_binary(type_str) -> CompoundKey.decode_type(type_str)
            _ -> :hash
          end

        _ ->
          :hash
      end

    type_label = type_label(type)

    # v2: write tombstone for the marker key
    active_path = find_active(shard_data_path)

    case NIF.v2_append_tombstone(active_path, mk) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Promotion cleanup: tombstone write failed for marker #{inspect(mk)}: #{inspect(reason)}"
        )

        raise "promotion cleanup marker tombstone failed: #{inspect(reason)}"
    end

    track_binary_delete(keydir, shard_index, mk, instance_ctx)
    :ets.delete(keydir, mk)

    path = dedicated_path(data_dir, shard_index, type, redis_key)

    if Ferricstore.FS.dir?(path) do
      Ferricstore.FS.rm_rf!(path)
      parent = Path.dirname(path)

      case fsync_dir(parent, :remove_dedicated_dir) do
        :ok ->
          :ok

        {:error, reason} ->
          raise "promotion cleanup directory fsync failed: #{inspect(reason)}"
      end
    end

    Logger.debug("Cleaned up promoted #{type_label} #{inspect(redis_key)} (shard #{shard_index})")

    :ok
  end

  @spec compound_prefix_for(atom(), binary()) :: binary()
  defp compound_prefix_for(:hash, redis_key), do: CompoundKey.hash_prefix(redis_key)
  defp compound_prefix_for(:set, redis_key), do: CompoundKey.set_prefix(redis_key)
  defp compound_prefix_for(:zset, redis_key), do: CompoundKey.zset_prefix(redis_key)

  defp promotion_entry_value(_shard_data_path, _key, value, _fid, _off) when value != nil,
    do: value

  defp promotion_entry_value(shard_data_path, key, nil, fid, off)
       when is_integer(fid) and fid >= 0 and is_integer(off) and off >= 0 do
    file_path =
      Path.join(shard_data_path, "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log")

    case read_cold_async(file_path, off, key) do
      {:ok, value} when value != nil -> value
      _ -> nil
    end
  end

  defp promotion_entry_value(_shard_data_path, _key, _value, _fid, _off), do: nil

  defp read_cold_async(path, offset, key) do
    Ferricstore.Store.ColdRead.pread_at(path, offset, key, @cold_read_timeout_ms)
  end

  @spec type_label(atom()) :: binary()
  defp type_label(:hash), do: "hash"
  defp type_label(:set), do: "set"
  defp type_label(:zset), do: "zset"

  # Finds the active (highest numbered) .log file in a shard data directory.
  @doc "Returns the active (highest file_id) log file path in a dedicated directory."
  @spec find_active(binary()) :: binary()
  def find_active(path) do
    case list_log_files(path) do
      [] -> Path.join(path, "00000.log")
      files -> files |> List.last() |> elem(1)
    end
  end

  defp compaction_latch(owner, redis_key) do
    with %FerricStore.Instance{} = ctx <- latch_context(owner),
         idx when is_integer(idx) and idx >= 0 <- latch_index(owner),
         true <- idx < tuple_size(ctx.latch_refs) do
      {elem(ctx.latch_refs, idx), {:promoted_compaction, redis_key}, idx}
    else
      _ -> nil
    end
  end

  defp latch_context(%FerricStore.Instance{} = ctx), do: ctx
  defp latch_context(%{instance_ctx: %FerricStore.Instance{} = ctx}), do: ctx

  defp latch_context(%{instance_name: name}) when is_atom(name) do
    FerricStore.Instance.get(name)
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  defp latch_context(_owner), do: nil

  defp latch_index(%{index: index}), do: index
  defp latch_index(%{shard_index: index}), do: index
  defp latch_index(_owner), do: nil

  defp acquire_compaction_latch(tab, latch_key, shard_index) do
    case :ets.insert_new(tab, {latch_key, self()}) do
      true ->
        :ok

      false ->
        wait_compaction_latch_clear!(tab, latch_key, shard_index)
        acquire_compaction_latch(tab, latch_key, shard_index)
    end
  end

  defp wait_compaction_latch_clear!(tab, latch_key, shard_index) do
    emit_compaction_latch_event(:blocked, shard_index, latch_key, 0)

    started_ms = System.monotonic_time(:millisecond)
    timeout_ms = compaction_latch_timeout_ms()

    case wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms) do
      :ok ->
        :ok

      {:error, {:timeout, wait_ms}} ->
        Logger.error(
          "Promoted compaction latch timeout after #{wait_ms}ms for #{inspect(latch_key)} " <>
            "on shard #{inspect(shard_index)}"
        )

        raise "compaction latch timeout after #{wait_ms}ms for #{inspect(latch_key)}"
    end
  end

  defp wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms) do
    wait_ms = max(System.monotonic_time(:millisecond) - started_ms, 0)

    if wait_ms >= timeout_ms do
      emit_compaction_latch_event(:timeout, shard_index, latch_key, wait_ms)
      {:error, {:timeout, wait_ms}}
    else
      do_wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms)
    end
  end

  defp do_wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms) do
    case :ets.lookup(tab, latch_key) do
      [] ->
        :ok

      [{^latch_key, owner}] ->
        wait_for_latch_owner(tab, latch_key, owner, shard_index, started_ms, timeout_ms)
    end
  end

  defp wait_for_latch_owner(tab, latch_key, owner, shard_index, started_ms, timeout_ms)
       when is_pid(owner) do
    if Process.alive?(owner) do
      Process.sleep(@compaction_latch_sleep_ms)
    else
      :ets.take(tab, latch_key)
    end

    wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms)
  end

  defp wait_for_latch_owner(tab, latch_key, _owner, shard_index, started_ms, timeout_ms) do
    Process.sleep(@compaction_latch_sleep_ms)
    wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms)
  end

  defp compaction_latch_timeout_ms do
    Application.get_env(
      :ferricstore,
      :promotion_compaction_latch_timeout_ms,
      @default_compaction_latch_timeout_ms
    )
  end

  defp emit_compaction_latch_event(status, shard_index, latch_key, wait_ms) do
    :telemetry.execute(
      [:ferricstore, :promotion, :compaction_latch],
      %{count: 1, wait_ms: wait_ms},
      %{
        status: status,
        shard_index: shard_index,
        redis_key_hash: compaction_latch_key_hash(latch_key)
      }
    )
  end

  defp compaction_latch_key_hash({:promoted_compaction, redis_key}) when is_binary(redis_key),
    do: :erlang.phash2(redis_key)

  defp compaction_latch_key_hash(_latch_key), do: :unknown

  defp file_id_from_path(path) do
    path
    |> Path.basename()
    |> String.trim_trailing(".log")
    |> String.to_integer()
  end

  # Returns total size of all .log files in a directory.
  defp dir_total_size(dir) do
    case Ferricstore.FS.ls(dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".log"))
        |> Enum.reject(&String.starts_with?(&1, "compact_"))
        |> Enum.reduce(0, fn name, acc ->
          case File.stat(Path.join(dir, name)) do
            {:ok, %{size: s}} -> acc + s
            _ -> acc
          end
        end)

      _ ->
        0
    end
  end

  # Returns all .log files in a directory as [{file_id, full_path}], sorted by file_id.
  # Cleans up leftover compact_*.log temp files from crashed compaction.
  defp list_log_files(dir) do
    case Ferricstore.FS.ls(dir) do
      {:ok, files} ->
        cleanup_leftover_compaction_temp_files(dir, files)

        files
        |> Enum.filter(fn name ->
          String.ends_with?(name, ".log") and not String.starts_with?(name, "compact_")
        end)
        |> Enum.map(fn name ->
          fid = name |> String.trim_trailing(".log") |> String.to_integer()
          {fid, Path.join(dir, name)}
        end)
        |> Enum.sort_by(fn {fid, _} -> fid end)

      _ ->
        []
    end
  end

  defp cleanup_leftover_compaction_temp_files(dir, files) do
    Enum.each(files, fn name ->
      if String.starts_with?(name, "compact_") and String.ends_with?(name, ".log") do
        path = Path.join(dir, name)

        case Ferricstore.FS.rm(path) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.warning(
              "Promotion recovery: failed to remove leftover compact temp file #{name} " <>
                "at #{path}: #{inspect(reason)}"
            )
        end
      end
    end)
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

  defp track_binary_insert(keydir, shard_index, key, new_val, instance_ctx) do
    ref = keydir_binary_ref(instance_ctx, shard_index)

    if ref do
      new_bytes = offheap_size(key) + offheap_size(new_val)

      old_bytes =
        case :ets.lookup(keydir, key) do
          [{^key, old_val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(old_val)
          _ -> 0
        end

      delta = new_bytes - old_bytes
      if delta != 0, do: :atomics.add(ref, shard_index + 1, delta)
    end
  end

  defp track_binary_delete(keydir, shard_index, key, instance_ctx) do
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
