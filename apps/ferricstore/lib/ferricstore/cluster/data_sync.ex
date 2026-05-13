defmodule Ferricstore.Cluster.DataSync do
  @moduledoc """
  Shard-by-shard data directory copy for new node sync.

  Provides WAL gap detection to avoid unnecessary full copies, per-shard
  sync status tracking, leader-aware copy source resolution, and automatic
  retry with partial cleanup on failure.
  """

  require Logger

  alias Ferricstore.Raft.Cluster, as: RaftCluster

  @default_max_retries 3

  # ---------------------------------------------------------------------------
  # WAL gap detection
  # ---------------------------------------------------------------------------

  @doc """
  Reads the last applied Raft index from a ra meta.dets file on disk.

  Used when a node boots from a disk clone (EBS snapshot) — we need to know
  the Raft index the cloned data is consistent up to, BEFORE starting ra
  (since the WAL has the wrong node IDs and must be deleted).

  Returns the last_applied index, or 0 if the file doesn't exist or can't be read.
  """
  @spec read_last_applied_from_disk(binary(), non_neg_integer()) :: non_neg_integer()
  def read_last_applied_from_disk(data_dir, shard_index) do
    meta_path = Path.join([data_dir, "ra", "meta.dets"])

    try do
      # Open the DETS file read-only
      dets_name = :"ferricstore_meta_read_#{shard_index}_#{System.unique_integer([:positive])}"

      case :dets.open_file(dets_name, file: String.to_charlist(meta_path), access: :read) do
        {:ok, ref} ->
          uid = "ferricstore_shard_#{shard_index}"

          result =
            case :dets.lookup(ref, uid) do
              [{^uid, _term, _voted_for, last_applied}] when is_integer(last_applied) ->
                last_applied

              _ ->
                0
            end

          :dets.close(ref)
          result

        {:error, _} ->
          0
      end
    catch
      _, _ -> 0
    end
  end

  @doc """
  Pure WAL gap check: given the target's last applied index and the leader's
  first available WAL index, determines if WAL replay can bridge the gap.
  """
  @spec wal_bridgeable?(non_neg_integer(), non_neg_integer()) :: :wal_bridgeable | :needs_resync
  def wal_bridgeable?(target_index, leader_first_index) do
    if target_index >= leader_first_index do
      :wal_bridgeable
    else
      :needs_resync
    end
  end

  @spec needs_resync?(non_neg_integer(), node(), node()) :: :wal_bridgeable | :needs_resync
  def needs_resync?(shard_index, target_node, leader_node) do
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
      # Target has data — check if its WAL can bridge to the leader
      target_server_id = RaftCluster.shard_server_id_on(shard_index, target_node)

      target_index =
        try do
          case :erpc.call(target_node, :ra, :member_overview, [target_server_id]) do
            {:ok, overview, _} -> Map.get(overview, :commit_index, 0)
            _ -> 0
          end
        catch
          _, _ -> 0
        end

      leader_server_id = RaftCluster.shard_server_id_on(shard_index, leader_node)

      case :erpc.call(leader_node, :ra, :member_overview, [leader_server_id]) do
        {:ok, overview, _} ->
          first_index = Map.get(overview, :first_index, 0)

          if target_index >= first_index do
            Logger.info(
              "Shard #{shard_index}: WAL bridgeable (target=#{target_index} >= first=#{first_index})"
            )

            :wal_bridgeable
          else
            Logger.info(
              "Shard #{shard_index}: WAL gap (target=#{target_index} < first=#{first_index}), needs resync"
            )

            :needs_resync
          end

        _ ->
          :needs_resync
      end
    else
      Logger.info("Shard #{shard_index}: target #{target_node} has no data, needs full resync")
      :needs_resync
    end
  end

  # ---------------------------------------------------------------------------
  # Single-shard sync (leader-aware)
  # ---------------------------------------------------------------------------

  @doc """
  Syncs a single shard's data to a target node.

  Resolves the current leader for the shard and copies data FROM the leader
  (not from the local node). Before copying, checks whether the target can
  catch up via WAL replay alone -- if so, the expensive data copy is skipped.

  1. Find leader for the shard
  2. Check WAL bridgeability
  3. If resync needed: pause writes, copy data + ra dir, resume writes
  4. Return `{:ok, detail}` with `:wal_bridgeable` or the Raft index at copy time

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `target_node` -- the node to sync data to
    * `ctx` -- the FerricStore instance context
  """
  @spec sync_shard(non_neg_integer(), node(), FerricStore.Instance.t()) ::
          {:ok, :wal_bridgeable | non_neg_integer()} | {:error, term()}
  def sync_shard(shard_index, target_node, ctx) do
    leader_node = find_leader_for(shard_index)

    case needs_resync?(shard_index, target_node, leader_node) do
      :wal_bridgeable ->
        {:ok, :wal_bridgeable}

      :needs_resync ->
        do_sync_shard(shard_index, target_node, leader_node, ctx)
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
  def retry_sync_shard(shard_index, target_node, ctx, max_retries \\ @default_max_retries) do
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
  def sync_all_shards(target_node, ctx) do
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
      rebuild_keydirs_on_target(target_node, ctx.shard_count)
      {:ok, results}
    else
      {:error, {:partial_sync, results}}
    end
  end

  @doc false
  def rebuild_keydirs_on_target(target_node, shard_count) do
    Logger.info("DataSync: rebuilding keydirs on #{target_node}")

    for shard_idx <- 0..(shard_count - 1) do
      try do
        # Use the shard's GenServer to get its actual keydir ref and data path.
        # This avoids ETS naming mismatches between Instance refs and actual tables.
        shard_name = :"Ferricstore.Store.Shard.#{shard_idx}"

        # Get the shard state via :sys.get_state to find the real keydir ref
        shard_state = :erpc.call(target_node, :sys, :get_state, [shard_name])
        keydir = shard_state.keydir
        shard_data_path = shard_state.shard_data_path

        # Clear existing entries and re-recover from copied Bitcask files
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
      catch
        kind, reason ->
          Logger.error(
            "DataSync: failed to rebuild keydir for shard #{shard_idx} on #{target_node}: #{inspect({kind, reason})}"
          )
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Private: leader resolution
  # ---------------------------------------------------------------------------

  @spec find_leader_for(non_neg_integer()) :: node()
  @doc false
  def find_leader_for(shard_index) do
    case RaftCluster.members(shard_index) do
      {:ok, _members, {_name, leader_node}} -> leader_node
      _ -> node()
    end
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

    # 1. Pause writes on the LEADER's shard (not local)
    pause_shard(leader_node, shard_name)

    try do
      # 2. Get current Raft index (last_applied, not commit_index) from the
      #    leader. last_applied tracks what the state machine has actually
      #    flushed to Bitcask. commit_index can be ahead — entries committed
      #    but not yet applied would be skipped on the joiner if we used
      #    commit_index, losing those writes.
      leader_server_id = RaftCluster.shard_server_id_on(shard_index, leader_node)
      {raft_index, overview_info} = get_raft_index_with_detail(leader_node, leader_server_id)

      # 3. Copy shard storage from leader to target. This includes the shared
      # Bitcask shard directory and promoted dedicated collection files.
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
    rescue
      e -> {:error, Exception.message(e)}
    after
      # 5. Always resume writes on the leader
      resume_shard(leader_node, shard_name)
    end
  end

  defp pause_shard(node, shard_name) do
    if node == node() do
      GenServer.call(shard_name, {:pause_writes}, 30_000)
    else
      :erpc.call(node, GenServer, :call, [shard_name, {:pause_writes}, 30_000])
    end
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
    overview_result =
      if leader_node == node() do
        :ra.member_overview(server_id)
      else
        try do
          :erpc.call(leader_node, :ra, :member_overview, [server_id])
        catch
          _, _ -> :error
        end
      end

    case overview_result do
      {:ok, overview, _} ->
        la = Map.get(overview, :last_applied, 0)
        ci = Map.get(overview, :commit_index, 0)
        {la, "(commit_index=#{ci})"}

      _ ->
        {0, "(no overview)"}
    end
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
    source_shard_data = Ferricstore.DataDir.shard_data_path(source_data_dir, shard_index)
    target_shard_data = Ferricstore.DataDir.shard_data_path(target_data_dir, shard_index)

    :ok = copy_directory_from(source_node, source_shard_data, target_node, target_shard_data)

    source_dedicated = dedicated_shard_path(source_data_dir, shard_index)
    target_dedicated = dedicated_shard_path(target_data_dir, shard_index)

    if remote_dir?(source_node, source_dedicated) do
      copy_directory_from(source_node, source_dedicated, target_node, target_dedicated)
    else
      :ok
    end

    source_blob = blob_shard_path(source_data_dir, shard_index)
    target_blob = blob_shard_path(target_data_dir, shard_index)

    if remote_dir?(source_node, source_blob) do
      copy_directory_from(source_node, source_blob, target_node, target_blob)
    else
      :ok
    end

    :ok
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
        content =
          if source_node == node() do
            File.read!(src)
          else
            :erpc.call(source_node, File, :read!, [src])
          end

        :erpc.call(target_node, File, :write!, [dest, content])
      end
    end)
  end
end
