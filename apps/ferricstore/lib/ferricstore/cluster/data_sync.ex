defmodule Ferricstore.Cluster.DataSync do
  @moduledoc """
  Shard-by-shard data directory copy for new node sync.

  Provides WARaft segment-log gap detection to avoid unnecessary full copies,
  per-shard sync status tracking, leader-aware copy source resolution, and
  automatic retry with partial cleanup on failure.
  """

  require Logger

  alias Ferricstore.Raft.Cluster, as: RaftCluster

  @default_max_retries 3
  @copy_chunk_bytes 1_048_576

  # ---------------------------------------------------------------------------
  # Segment-log gap detection
  # ---------------------------------------------------------------------------

  @doc """
  Reads the persisted replay-safe index for a copied shard.

  Returns 0 when the marker is absent or unreadable.
  """
  @spec read_last_applied_from_disk(binary(), non_neg_integer()) :: non_neg_integer()
  def read_last_applied_from_disk(data_dir, shard_index) do
    data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> Ferricstore.Raft.ReplaySafeIndex.read()
  end

  @doc """
  Pure segment-log gap check: given the target's replay-safe index and the
  leader's first available segment index, determines if log replay can bridge
  the gap.
  """
  @spec wal_bridgeable?(non_neg_integer(), non_neg_integer()) :: :wal_bridgeable | :needs_resync
  def wal_bridgeable?(target_index, leader_first_index) do
    if target_index >= leader_first_index do
      :wal_bridgeable
    else
      :needs_resync
    end
  end

  @doc false
  def __maybe_require_blob_resync_for_test__(wal_result, leader_has_blob_files?),
    do: maybe_require_blob_resync(wal_result, leader_has_blob_files?)

  @doc false
  def __pause_batcher_for_test__(node, shard_index), do: pause_batcher(node, shard_index)

  @doc false
  def __pause_shard_for_test__(node, shard_name), do: pause_shard(node, shard_name)

  @spec needs_resync?(non_neg_integer(), node(), node()) :: :wal_bridgeable | :needs_resync
  def needs_resync?(shard_index, target_node, leader_node),
    do: do_needs_resync?(shard_index, target_node, leader_node)

  defp do_needs_resync?(shard_index, target_node, leader_node) do
    # Check if the TARGET node has data files for this shard.
    # A node with an empty/missing data dir always needs a full resync.
    target_has_data =
      try do
        target_ctx = :erpc.call(target_node, FerricStore.Instance, :get, [:default])
        target_shard_path = Ferricstore.DataDir.shard_data_path(target_ctx.data_dir, shard_index)

        case :erpc.call(target_node, File, :ls, [target_shard_path]) do
          {:ok, files} ->
            # Check if any .log file has actual data (not just the empty 00000.log
            # created by DataDir.ensure_layout!)
            log_files = Enum.filter(files, &String.ends_with?(&1, ".log"))

            Enum.any?(log_files, fn f ->
              path = Path.join(target_shard_path, f)

              case :erpc.call(target_node, File, :stat, [path]) do
                {:ok, %{size: size}} -> size > 0
                _ -> false
              end
            end)

          _ ->
            false
        end
      catch
        _, _ -> false
      end

    if target_has_data do
      # Target has data -- check whether its WARaft segment log can bridge to
      # the leader without a full file copy.
      target_index =
        case waraft_storage_position(target_node, shard_index) do
          {:ok, {:raft_log_pos, index, _term}} when is_integer(index) -> index
          _other -> 0
        end

      case waraft_log_first_index(leader_node, shard_index) do
        first_index when is_integer(first_index) ->
          if target_index >= first_index do
            blob_status = leader_blob_files?(shard_index, leader_node)
            result = maybe_require_blob_resync(:wal_bridgeable, blob_status)

            case {result, blob_status} do
              {:wal_bridgeable, _} ->
                Logger.info(
                  "Shard #{shard_index}: segment log bridgeable (target=#{target_index} >= first=#{first_index})"
                )

              {:needs_resync, {:ok, true}} ->
                Logger.info(
                  "Shard #{shard_index}: blob side-channel data present on leader, full resync required despite segment-log bridgeability"
                )

              {:needs_resync, {:error, reason}} ->
                Logger.warning(
                  "Shard #{shard_index}: could not inspect leader blob side-channel data (#{inspect(reason)}), full resync required despite segment-log bridgeability"
                )

              {:needs_resync, _} ->
                Logger.info(
                  "Shard #{shard_index}: full resync required despite segment-log bridgeability"
                )
            end

            result
          else
            Logger.info(
              "Shard #{shard_index}: segment-log gap (target=#{target_index} < first=#{first_index}), needs resync"
            )

            :needs_resync
          end

        _other ->
          :needs_resync
      end
    else
      Logger.info("Shard #{shard_index}: target #{target_node} has no data, needs full resync")
      :needs_resync
    end
  end

  defp maybe_require_blob_resync(:wal_bridgeable, {:ok, true}), do: :needs_resync
  defp maybe_require_blob_resync(:wal_bridgeable, {:ok, false}), do: :wal_bridgeable
  defp maybe_require_blob_resync(:wal_bridgeable, {:error, _reason}), do: :needs_resync
  defp maybe_require_blob_resync(result, _leader_has_blob_files?), do: result

  defp leader_blob_files?(shard_index, leader_node) do
    ctx =
      if leader_node == node() do
        FerricStore.Instance.get(:default)
      else
        :erpc.call(leader_node, FerricStore.Instance, :get, [:default])
      end

    blob_path = blob_shard_path(ctx.data_dir, shard_index)
    remote_tree_regular_file_status(leader_node, blob_path)
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # ---------------------------------------------------------------------------
  # Single-shard sync (leader-aware)
  # ---------------------------------------------------------------------------

  @doc """
  Syncs a single shard's data to a target node.

  Resolves the current leader for the shard and copies data FROM the leader
  (not from the local node). Before copying, checks whether the target can
  catch up via WARaft segment replay alone -- if so, the expensive data copy is
  skipped.

  1. Find leader for the shard
  2. Check segment-log bridgeability
  3. If resync needed: pause writes, copy data, resume writes
  4. Return `{:ok, detail}` with `:wal_bridgeable` or the Raft index at copy time

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `target_node` -- the node to sync data to
    * `ctx` -- the FerricStore instance context
  """
  @spec sync_shard(non_neg_integer(), node(), FerricStore.Instance.t()) ::
          {:ok, :wal_bridgeable | non_neg_integer()} | {:error, term()}
  def sync_shard(shard_index, target_node, ctx) do
    with {:ok, leader_node} <- find_leader_for(shard_index) do
      case needs_resync?(shard_index, target_node, leader_node) do
        :wal_bridgeable ->
          {:ok, :wal_bridgeable}

        :needs_resync ->
          do_sync_shard(shard_index, target_node, leader_node, ctx)
      end
    end
  end

  @doc """
  Retries `sync_shard/3` up to `max_retries` times, cleaning up partial data
  on the target node between attempts.

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `target_node` -- the node to sync data to
    * `ctx` -- the FerricStore instance context
    * `max_retries` -- maximum number of attempts (default: #{@default_max_retries})
  """
  @spec retry_sync_shard(non_neg_integer(), node(), FerricStore.Instance.t(), non_neg_integer()) ::
          {:ok, :wal_bridgeable | non_neg_integer()} | {:error, term()}
  def retry_sync_shard(shard_index, target_node, ctx, max_retries \\ @default_max_retries)

  def retry_sync_shard(_shard_index, _target_node, _ctx, 0), do: {:error, :not_attempted}

  def retry_sync_shard(shard_index, target_node, ctx, max_retries) when max_retries > 0 do
    Enum.reduce_while(1..max_retries, {:error, :not_attempted}, fn attempt, _acc ->
      case sync_shard(shard_index, target_node, ctx) do
        {:ok, _} = ok ->
          {:halt, ok}

        {:error, reason} ->
          Logger.warning(
            "Shard #{shard_index} sync attempt #{attempt}/#{max_retries} failed: #{inspect(reason)}"
          )

          cleanup_partial_sync(shard_index, target_node, ctx)

          if attempt < max_retries do
            {:cont, {:error, reason}}
          else
            {:halt, {:error, reason}}
          end
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # All-shards sync with per-shard status
  # ---------------------------------------------------------------------------

  @doc """
  Copies all shards sequentially, tracking per-shard sync status.

  Returns `{:ok, results}` when every shard succeeds, where `results` is a
  map of `shard_index => {:synced, detail}`. On partial failure returns
  `{:error, {:partial_sync, results}}` with per-shard success/failure info.

  ## Parameters

    * `target_node` -- the node to sync data to
    * `ctx` -- the FerricStore instance context
  """
  @spec sync_all_shards(node(), FerricStore.Instance.t()) :: {:ok, map()} | {:error, term()}
  def sync_all_shards(target_node, ctx), do: do_sync_all_shards(target_node, ctx)

  defp do_sync_all_shards(target_node, ctx) do
    Logger.info("DataSync: starting sync of #{ctx.shard_count} shards to #{target_node}")

    results =
      for shard_idx <- 0..(ctx.shard_count - 1), into: %{} do
        case sync_shard(shard_idx, target_node, ctx) do
          {:ok, detail} -> {shard_idx, {:synced, detail}}
          {:error, reason} -> {shard_idx, {:failed, reason}}
        end
      end

    failed = Enum.filter(results, fn {_, {status, _}} -> status == :failed end)

    if failed == [] do
      # After copying all shard data, tell the target to rebuild keydirs
      # from the newly copied files. The target's shards started with empty
      # keydirs — now the Bitcask files are in place, so re-recovery populates ETS.
      case rebuild_keydirs_on_target(target_node, ctx.shard_count) do
        :ok -> {:ok, results}
        {:error, reason} -> {:error, {:keydir_rebuild_failed, results, reason}}
      end
    else
      {:error, {:partial_sync, results}}
    end
  end

  @doc false
  def rebuild_keydirs_on_target(_target_node, 0), do: :ok

  def rebuild_keydirs_on_target(target_node, shard_count) do
    Logger.info("DataSync: rebuilding keydirs on #{target_node}")

    failures =
      Enum.reduce(0..(shard_count - 1), %{}, fn shard_idx, failures ->
        case rebuild_keydir_on_target(target_node, shard_idx) do
          :ok -> failures
          {:error, reason} -> Map.put(failures, shard_idx, reason)
        end
      end)

    if map_size(failures) == 0 do
      :ok
    else
      {:error, {:target_keydir_rebuild_failed, target_node, failures}}
    end
  end

  defp rebuild_keydir_on_target(target_node, shard_idx) do
    shard_name = :"Ferricstore.Store.Shard.#{shard_idx}"

    shard_state = :erpc.call(target_node, :sys, :get_state, [shard_name])
    keydir = shard_state.keydir
    shard_data_path = shard_state.shard_data_path

    :erpc.call(target_node, :ets, :delete_all_objects, [keydir])

    :erpc.call(target_node, Ferricstore.Store.Shard.Lifecycle, :recover_keydir, [
      shard_data_path,
      keydir,
      shard_idx
    ])

    ets_size = :erpc.call(target_node, :ets, :info, [keydir, :size])

    Logger.info(
      "DataSync: shard #{shard_idx} keydir rebuilt on #{target_node} (#{ets_size} keys)"
    )

    :ok
  catch
    kind, reason ->
      failure = {kind, reason}

      Logger.error(
        "DataSync: failed to rebuild keydir for shard #{shard_idx} on #{target_node}: #{inspect(failure)}"
      )

      {:error, failure}
  end

  # ---------------------------------------------------------------------------
  # Private: leader resolution
  # ---------------------------------------------------------------------------

  @spec find_leader_for(non_neg_integer(), (non_neg_integer(), timeout() -> term())) ::
          {:ok, node()} | {:error, term()}
  @doc false
  def find_leader_for(
        shard_index,
        members_fun \\ fn index, timeout -> RaftCluster.members(index, timeout) end
      ) do
    case members_fun.(shard_index, 5_000) do
      {:ok, _members, {_name, leader_node}} when is_atom(leader_node) ->
        {:ok, leader_node}

      {:error, reason} ->
        {:error, {:leader_unavailable, reason}}

      other ->
        {:error, {:leader_unavailable, {:unexpected_members_result, other}}}
    end
  catch
    kind, reason -> {:error, {:leader_unavailable, {kind, reason}}}
  end

  # ---------------------------------------------------------------------------
  # Private: single-shard data copy (runs on the leader)
  # ---------------------------------------------------------------------------

  @spec do_sync_shard(non_neg_integer(), node(), node(), FerricStore.Instance.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  defp do_sync_shard(shard_index, target_node, leader_node, ctx) do
    shard_name = :"Ferricstore.Store.Shard.#{shard_index}"
    leader_data_dir = get_leader_data_dir(leader_node, ctx)
    target_data_dir = get_target_data_dir(target_node)
    leader_shard_data = Ferricstore.DataDir.shard_data_path(leader_data_dir, shard_index)
    target_shard_data = Ferricstore.DataDir.shard_data_path(target_data_dir, shard_index)

    Logger.info(
      "Shard #{shard_index}: syncing #{leader_node}:#{leader_shard_data} → #{target_node}:#{target_shard_data}"
    )

    # 1. Pause writes on the LEADER (not local). Pause the Batcher first
    # because optimized pipeline SET/DEL can bypass the Shard GenServer.
    case pause_batcher(leader_node, shard_index) do
      :ok ->
        try do
          case pause_shard(leader_node, shard_name) do
            :ok ->
              try do
                # 2. Get current Raft index (last_applied, not commit_index) from the
                #    leader. last_applied tracks what the state machine has actually
                #    flushed to Bitcask. commit_index can be ahead — entries committed
                #    but not yet applied would be skipped on the joiner if we used
                #    commit_index, losing those writes.
                leader_server_id = RaftCluster.shard_server_id_on(shard_index, leader_node)

                {raft_index, overview_info} =
                  get_raft_index_with_detail(leader_node, leader_server_id)

                # 3. Copy shard storage from leader to target. This includes the shared
                # Bitcask shard directory, promoted dedicated collection files, and
                # blob side-channel files.
                copy_shard_storage_from(
                  leader_node,
                  leader_data_dir,
                  target_node,
                  target_data_dir,
                  shard_index
                )

                Logger.info(
                  "Shard #{shard_index}: sync complete at raft last_applied=#{raft_index} #{overview_info}"
                )

                {:ok, raft_index}
              after
                resume_shard(leader_node, shard_name)
              end

            {:error, _reason} = error ->
              error

            other ->
              {:error, {:pause_shard_failed, other}}
          end
        rescue
          e -> {:error, Exception.message(e)}
        after
          resume_batcher(leader_node, shard_index)
        end

      {:error, _reason} = error ->
        error

      other ->
        {:error, {:pause_batcher_failed, other}}
    end
  end

  defp pause_batcher(node, shard_index) do
    # WARaft writes normally bypass the Shard GenServer, so the replication
    # write facade is the synchronization point for any direct file copy path.
    if node == node() do
      Ferricstore.Raft.Batcher.pause_writes_for_sync(shard_index, 30_000)
    else
      :erpc.call(node, Ferricstore.Raft.Batcher, :pause_writes_for_sync, [shard_index, 30_000])
    end
  catch
    kind, reason -> {:error, {:pause_batcher_failed, {kind, reason}}}
  end

  defp resume_batcher(node, shard_index) do
    if node == node() do
      Ferricstore.Raft.Batcher.resume_writes_for_sync(shard_index, 5_000)
    else
      :erpc.call(node, Ferricstore.Raft.Batcher, :resume_writes_for_sync, [shard_index, 5_000])
    end
  catch
    _kind, _reason -> :ok
  end

  defp pause_shard(node, shard_name) do
    if node == node() do
      GenServer.call(shard_name, {:pause_writes}, 30_000)
    else
      :erpc.call(node, GenServer, :call, [shard_name, {:pause_writes}, 30_000])
    end
  catch
    kind, reason -> {:error, {:pause_shard_failed, {kind, reason}}}
  end

  defp resume_shard(node, shard_name) do
    if node == node() do
      GenServer.call(shard_name, {:resume_writes}, 5_000)
    else
      try do
        :erpc.call(node, GenServer, :call, [shard_name, {:resume_writes}, 5_000])
      catch
        _, _ -> :ok
      end
    end
  end

  defp get_target_data_dir(target_node) do
    ctx = :erpc.call(target_node, FerricStore.Instance, :get, [:default])
    ctx.data_dir
  end

  defp get_leader_data_dir(leader_node, ctx) do
    if leader_node == node() do
      ctx.data_dir
    else
      remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default])
      remote_ctx.data_dir
    end
  end

  defp get_raft_index_with_detail(leader_node, server_id) do
    shard_index = shard_index_from_server_id(server_id)

    case waraft_storage_position(leader_node, shard_index) do
      {:ok, {:raft_log_pos, index, term}} -> {index, "(waraft_term=#{term})"}
      _ -> {0, "(no overview)"}
    end
  end

  defp waraft_log_first_index(leader_node, shard_index) do
    status =
      if leader_node == node() do
        Ferricstore.Raft.WARaftBackend.segment_log_memory_status(shard_index)
      else
        :erpc.call(
          leader_node,
          Ferricstore.Raft.WARaftBackend,
          :segment_log_memory_status,
          [shard_index]
        )
      end

    case status do
      %{disk_first_index: index} when is_integer(index) -> index
      _other -> 0
    end
  catch
    _, _ -> 0
  end

  defp waraft_storage_position(leader_node, shard_index) do
    if leader_node == node() do
      Ferricstore.Raft.WARaftBackend.storage_position(shard_index)
    else
      try do
        :erpc.call(leader_node, Ferricstore.Raft.WARaftBackend, :storage_position, [shard_index])
      catch
        _, _ -> :error
      end
    end
  end

  defp shard_index_from_server_id({name, _node}) when is_atom(name) do
    name = Atom.to_string(name)

    cond do
      String.starts_with?(name, "raft_server_ferricstore_waraft_backend_") ->
        name
        |> String.trim_leading("raft_server_ferricstore_waraft_backend_")
        |> String.to_integer()
        |> Kernel.-(1)

      true ->
        0
    end
  rescue
    _ -> 0
  end

  # ---------------------------------------------------------------------------
  # Private: partial cleanup
  # ---------------------------------------------------------------------------

  @doc false
  @spec cleanup_partial_sync(non_neg_integer(), node(), FerricStore.Instance.t()) :: :ok
  def cleanup_partial_sync(shard_index, target_node, ctx) do
    target_data_dir = get_target_data_dir(target_node)
    cleanup_partial_sync(shard_index, target_node, ctx, target_data_dir)
  end

  @doc false
  @spec cleanup_partial_sync(non_neg_integer(), node(), FerricStore.Instance.t(), binary()) :: :ok
  def cleanup_partial_sync(shard_index, target_node, _ctx, target_data_dir) do
    shard_paths = [
      Ferricstore.DataDir.shard_data_path(target_data_dir, shard_index),
      dedicated_shard_path(target_data_dir, shard_index),
      blob_shard_path(target_data_dir, shard_index)
    ]

    Enum.each(shard_paths, fn path ->
      try do
        :erpc.call(target_node, File, :rm_rf!, [path])
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  @doc false
  @spec copy_shard_storage_from(node(), binary(), node(), binary(), non_neg_integer()) :: :ok
  def copy_shard_storage_from(
        source_node,
        source_data_dir,
        target_node,
        target_data_dir,
        shard_index
      ) do
    if source_node == target_node and
         Path.expand(source_data_dir) == Path.expand(target_data_dir) do
      raise ArgumentError, "source and target shard storage must be distinct"
    end

    source_shard_data = Ferricstore.DataDir.shard_data_path(source_data_dir, shard_index)
    target_shard_data = Ferricstore.DataDir.shard_data_path(target_data_dir, shard_index)
    source_dedicated = dedicated_shard_path(source_data_dir, shard_index)
    target_dedicated = dedicated_shard_path(target_data_dir, shard_index)
    source_blob = blob_shard_path(source_data_dir, shard_index)
    target_blob = blob_shard_path(target_data_dir, shard_index)

    Enum.each([target_shard_data, target_dedicated, target_blob], fn path ->
      replace_target_tree!(target_node, path)
    end)

    :ok = copy_directory_from(source_node, source_shard_data, target_node, target_shard_data)

    if remote_dir?(source_node, source_dedicated),
      do: copy_directory_from(source_node, source_dedicated, target_node, target_dedicated)

    if remote_dir?(source_node, source_blob),
      do: copy_directory_from(source_node, source_blob, target_node, target_blob)

    :ok
  end

  defp replace_target_tree!(target_node, path) do
    case call_on(target_node, Ferricstore.FS, :rm_rf, [path]) do
      :ok ->
        parent = Path.dirname(path)
        :erpc.call(target_node, File, :mkdir_p!, [parent])
        sync_target_dir!(target_node, parent, :replace_target_tree, path)

      {:error, reason} ->
        raise "DataSync: failed to remove stale target tree #{path}: #{inspect(reason)}"

      other ->
        raise "DataSync: stale target tree removal returned #{inspect(other)} for #{path}"
    end
  end

  defp dedicated_shard_path(data_dir, shard_index) do
    Path.join([data_dir, "dedicated", "shard_#{shard_index}"])
  end

  defp blob_shard_path(data_dir, shard_index) do
    Ferricstore.DataDir.blob_shard_path(data_dir, shard_index)
  end

  defp remote_dir?(node, path) do
    if node == node() do
      Ferricstore.FS.dir?(path)
    else
      try do
        :erpc.call(node, File, :dir?, [path])
      catch
        _, _ -> false
      end
    end
  end

  defp remote_tree_regular_file_status(node, path) do
    case remote_dir_status(node, path) do
      {:ok, false} ->
        {:ok, false}

      {:ok, true} ->
        case call_on(node, File, :ls, [path]) do
          {:ok, entries} -> entries_have_regular_files?(node, path, entries)
          {:error, reason} -> {:error, {:ls_failed, path, reason}}
        end

      {:error, _reason} = error ->
        error
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp entries_have_regular_files?(node, parent, entries) do
    Enum.reduce_while(entries, {:ok, false}, fn entry, {:ok, false} ->
      child = Path.join(parent, entry)

      case remote_dir_status(node, child) do
        {:ok, true} ->
          case remote_tree_regular_file_status(node, child) do
            {:ok, true} -> {:halt, {:ok, true}}
            {:ok, false} -> {:cont, {:ok, false}}
            {:error, _reason} = error -> {:halt, error}
          end

        {:ok, false} ->
          case call_on(node, File, :stat, [child]) do
            {:ok, %{type: :regular}} -> {:halt, {:ok, true}}
            {:ok, _other} -> {:cont, {:ok, false}}
            {:error, reason} -> {:halt, {:error, {:stat_failed, child, reason}}}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp remote_dir_status(node, path) do
    if node == node() do
      {:ok, Ferricstore.FS.dir?(path)}
    else
      {:ok, :erpc.call(node, File, :dir?, [path])}
    end
  catch
    kind, reason -> {:error, {:dir_check_failed, path, kind, reason}}
  end

  # ---------------------------------------------------------------------------
  # Private: directory copy (source_node -> target_node)
  # ---------------------------------------------------------------------------

  # Reads files from `source_node` and writes them to `target_node`.
  # When source_node == node(), reads are local.
  @doc false
  @spec copy_directory_from(node(), binary(), node(), binary()) :: :ok
  def copy_directory_from(source_node, source_path, target_node, target_path) do
    Logger.info("DataSync: copying #{source_node}:#{source_path} → #{target_node}:#{target_path}")
    :erpc.call(target_node, File, :mkdir_p!, [target_path])
    sync_target_dir!(target_node, Path.dirname(target_path), :create_target_dir, target_path)

    files =
      if source_node == node() do
        {:ok, f} = Ferricstore.FS.ls(source_path)
        f
      else
        {:ok, f} = :erpc.call(source_node, File, :ls, [source_path])
        f
      end

    Enum.each(files, fn file ->
      src = Path.join(source_path, file)
      dest = Path.join(target_path, file)

      is_dir =
        if source_node == node() do
          Ferricstore.FS.dir?(src)
        else
          :erpc.call(source_node, File, :dir?, [src])
        end

      if is_dir do
        copy_directory_from(source_node, src, target_node, dest)
      else
        copy_file_from(source_node, src, target_node, dest)
      end
    end)

    sync_target_dir!(target_node, target_path, :copy_directory, target_path)
  end

  @doc false
  def __prepare_target_file_for_copy__(path) when is_binary(path) do
    case File.open(path, [:write, :raw, :binary]) do
      {:ok, io} ->
        File.close(io)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  def __read_file_chunk_for_copy__(path, offset, bytes)
      when is_binary(path) and is_integer(offset) and offset >= 0 and is_integer(bytes) and
             bytes > 0 do
    with {:ok, io} <- File.open(path, [:read, :raw, :binary]) do
      try do
        :file.pread(io, offset, bytes)
      after
        File.close(io)
      end
    end
  end

  @doc false
  def __write_file_chunk_for_copy__(path, offset, chunk)
      when is_binary(path) and is_integer(offset) and offset >= 0 and is_binary(chunk) do
    with {:ok, io} <- File.open(path, [:read, :write, :raw, :binary]) do
      try do
        :file.pwrite(io, offset, chunk)
      after
        File.close(io)
      end
    end
  end

  @doc false
  def __sync_file_for_copy__(path) when is_binary(path) do
    with {:ok, io} <- File.open(path, [:read, :write, :raw, :binary]) do
      try do
        :file.sync(io)
      after
        File.close(io)
      end
    end
  end

  defp copy_file_from(source_node, source_path, target_node, target_path) do
    :ok =
      call_on(target_node, __MODULE__, :__prepare_target_file_for_copy__, [target_path])

    copy_file_chunks(source_node, source_path, target_node, target_path, 0)
    sync_target_file!(target_node, target_path)
  end

  defp copy_file_chunks(source_node, source_path, target_node, target_path, offset) do
    case call_on(source_node, __MODULE__, :__read_file_chunk_for_copy__, [
           source_path,
           offset,
           @copy_chunk_bytes
         ]) do
      :eof ->
        :ok

      {:error, reason} ->
        raise File.Error, reason: reason, action: "read", path: source_path

      {:ok, chunk} when is_binary(chunk) ->
        write_copy_chunk(source_node, source_path, target_node, target_path, offset, chunk)

      chunk when is_binary(chunk) ->
        write_copy_chunk(source_node, source_path, target_node, target_path, offset, chunk)
    end
  end

  defp write_copy_chunk(source_node, source_path, target_node, target_path, offset, chunk) do
    case call_on(target_node, __MODULE__, :__write_file_chunk_for_copy__, [
           target_path,
           offset,
           chunk
         ]) do
      :ok ->
        observe_copy_chunk(source_path, target_path, byte_size(chunk))

        copy_file_chunks(
          source_node,
          source_path,
          target_node,
          target_path,
          offset + byte_size(chunk)
        )

      {:error, reason} ->
        raise File.Error, reason: reason, action: "write", path: target_path
    end
  end

  defp sync_target_file!(target_node, target_path) do
    case target_file_sync(target_node, target_path) do
      :ok ->
        :ok

      {:error, reason} ->
        raise "DataSync: file fsync failed for #{target_path}: #{inspect(reason)}"

      other ->
        raise "DataSync: file fsync returned unexpected result for #{target_path}: #{inspect(other)}"
    end
  end

  defp target_file_sync(target_node, target_path) do
    case Process.get(:ferricstore_data_sync_file_sync_hook) do
      hook when is_function(hook, 1) and target_node == node() -> hook.(target_path)
      _ -> call_on(target_node, __MODULE__, :__sync_file_for_copy__, [target_path])
    end
  end

  defp sync_target_dir!(target_node, dir_path, phase, copied_path) do
    case target_dir_sync(target_node, dir_path) do
      :ok ->
        :ok

      {:error, reason} ->
        raise "DataSync: directory fsync failed during #{phase} for #{dir_path} " <>
                "while copying #{copied_path}: #{inspect(reason)}"

      other ->
        raise "DataSync: directory fsync returned unexpected result during #{phase} " <>
                "for #{dir_path} while copying #{copied_path}: #{inspect(other)}"
    end
  end

  defp target_dir_sync(target_node, dir_path) do
    case Process.get(:ferricstore_data_sync_fsync_dir_hook) do
      hook when is_function(hook, 1) and target_node == node() -> hook.(dir_path)
      _ -> call_on(target_node, Ferricstore.Bitcask.NIF, :v2_fsync_dir, [dir_path])
    end
  end

  defp call_on(target_node, module, function, args) do
    if target_node == node() do
      apply(module, function, args)
    else
      :erpc.call(target_node, module, function, args)
    end
  end

  defp observe_copy_chunk(source_path, target_path, bytes) do
    case Process.get(:ferricstore_data_sync_copy_chunk_hook) do
      hook when is_function(hook, 3) -> hook.(source_path, target_path, bytes)
      _ -> :ok
    end
  end
end
