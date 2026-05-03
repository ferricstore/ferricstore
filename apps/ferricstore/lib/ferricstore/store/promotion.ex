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

  require Logger

  @promotion_marker_prefix "PM:"
  @cold_read_timeout_ms 10_000

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

  @spec open_dedicated(binary(), non_neg_integer(), atom(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def open_dedicated(data_dir, shard_index, type, redis_key) do
    path = dedicated_path(data_dir, shard_index, type, redis_key)
    created_dir? = not Ferricstore.FS.dir?(path)
    Ferricstore.FS.mkdir_p!(path)

    if created_dir? do
      parent = Path.dirname(path)
      _ = Ferricstore.Bitcask.NIF.v2_fsync_dir(parent)
    end

    active_file = Path.join(path, "00000.log")

    # credo:disable-for-next-line Credo.Check.Refactor.UnlessWithElse
    created_file? =
      unless Ferricstore.FS.exists?(active_file) do
        Ferricstore.FS.touch!(active_file)
        true
      else
        false
      end

    if created_dir? or created_file? do
      _ = Ferricstore.Bitcask.NIF.v2_fsync_dir(path)
    end

    {:ok, path}
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
    {:ok, dedicated_path} = open_dedicated(data_dir, shard_index, type, redis_key)

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

    # Step 3: tombstone compound keys in shared log (LAST step)
    Enum.each(entries, fn {key, _value, _exp} ->
      case NIF.v2_append_tombstone(active_path, key) do
        {:ok, _} ->
          :ok

        {:error, reason} ->
          Logger.warning(
            "Promotion: tombstone write failed for #{inspect(key)}: #{inspect(reason)}"
          )
      end
    end)

    Logger.info(
      "Promoted #{type_label} #{inspect(redis_key)} to dedicated Bitcask " <>
        "(#{length(entries)} entries, shard #{shard_index})"
    )

    {:ok, dedicated_path}
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
      |> Enum.map(fn {full_key, value, fid, offset} ->
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

        {redis_key, type_str}
      end)
      |> Enum.filter(fn {_, type_str} -> is_binary(type_str) end)
      |> Enum.uniq_by(fn {redis_key, _} -> redis_key end)

    Enum.reduce(all_markers, %{}, fn {redis_key, type_str}, acc ->
      type = CompoundKey.decode_type(type_str)

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
      live_count =
        Enum.count(final_state, fn {_, entry} ->
          case entry do
            {:live, _, _, _, _, _} -> true
            _ -> false
          end
        end)

      dedicated_empty? = map_size(final_state) == 0

      if live_count > 0 do
        # Normal recovery path: dedicated has data, apply it.
        Enum.each(final_state, fn
          {key, :tombstone} ->
            track_binary_delete(keydir, shard_index, key, instance_ctx)
            :ets.delete(keydir, key)

          {key, {:live, fid, file_path, offset, value_size, expire_at_ms}} ->
            value =
              case read_cold_async(file_path, offset, key) do
                {:ok, v} when v != nil -> v
                _ -> nil
              end

            track_binary_insert(keydir, shard_index, key, value, instance_ctx)

            :ets.insert(
              keydir,
              {key, value, expire_at_ms, LFU.initial(), fid, offset, value_size}
            )
        end)
      end

      if dedicated_empty? do
        # Fallback path: marker exists, dedicated has no records. Compound
        # keys in shared log (already in keydir via recover_keydir) are the
        # source of truth, so do not include this key in promoted_instances.
        Logger.info(
          "Promotion recovery: marker for #{inspect(redis_key)} exists but dedicated " <>
            "dir has no live records; falling back to compound keys in shared log."
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
      _ = Ferricstore.Bitcask.NIF.v2_fsync_dir(parent)
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
        # Delete leftover compaction temp files (same as shared shard C2 fix)
        Enum.each(files, fn name ->
          if String.starts_with?(name, "compact_") and String.ends_with?(name, ".log") do
            _ = Ferricstore.FS.rm(Path.join(dir, name))
          end
        end)

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
