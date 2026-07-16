defmodule Ferricstore.Store.FlushPromotedRestartTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.{ActiveFile, CompoundKey, Ops.Flush, Promotion, Router}
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    apply_context_snapshot =
      ShardHelpers.replace_default_apply_context(promotion_threshold: 1)

    on_exit(fn ->
      _ = WARaftBackend.stop()
      :ok = WARaftBackend.start(FerricStore.Instance.get(:default))
      ShardHelpers.wait_default_pipeline_ready()
      ShardHelpers.restore_default_apply_context(apply_context_snapshot)
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  test "FLUSHDB tombstones promotion markers before removing dedicated storage" do
    %{ctx: ctx, redis_key: redis_key, shard_index: shard_index, dedicated_path: dedicated_path} =
      promote_hash!("restart")

    marker_key = Promotion.marker_key(redis_key)
    assert File.dir?(dedicated_path)

    assert :ok = Flush.flush(ctx)
    refute File.exists?(dedicated_path)
    assert [] = :ets.lookup(elem(ctx.keydir_refs, shard_index), marker_key)

    assert :ok = WARaftBackend.stop()
    assert :ok = WARaftBackend.start(ctx)
    assert :ok = ShardHelpers.wait_default_pipeline_ready()

    assert [] = :ets.lookup(elem(ctx.keydir_refs, shard_index), marker_key)
    refute File.exists?(dedicated_path)
  end

  test "FLUSHDB retains dedicated storage when marker tombstone durability fails" do
    %{ctx: ctx, shard_index: shard_index, dedicated_path: dedicated_path} =
      promote_hash!("failure")

    {_file_id, active_path, _shard_path} = ActiveFile.get(ctx, shard_index)
    backup_path = active_path <> ".flush-marker-backup"

    File.rename!(active_path, backup_path)
    File.mkdir!(active_path)

    try do
      assert {:error,
              {:flush_shard_failed, ^shard_index,
               {:bitcask_append_failed,
                {:flush_promoted_cleanup_failed,
                 {:append_promotion_marker_tombstones_failed, _reason}}}}} = Flush.flush(ctx)

      assert File.dir?(dedicated_path)
    after
      File.rm_rf!(active_path)
      File.rename!(backup_path, active_path)
      :ok = Ferricstore.Bitcask.NIF.v2_fsync_dir(Path.dirname(active_path))
    end

    assert :ok = WARaftBackend.stop()
    assert :ok = WARaftBackend.start(ctx)
    assert :ok = ShardHelpers.wait_default_pipeline_ready()
    refute File.exists?(dedicated_path)
  end

  test "derived cleanup failure blocks storage position until restart replay" do
    ctx = FerricStore.Instance.get(:default)
    key = "flush-derived-restart-#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, key)

    assert :ok = Router.put(ctx, key, "value", 0)
    assert {:ok, position_before} = WARaftBackend.storage_position(shard_index)

    target_lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    Application.put_env(
      :ferricstore,
      :flush_derived_lmdb_clear_hook,
      fn path ->
        if path == target_lmdb_path,
          do: {:error, :forced_lmdb_clear_failure},
          else: Ferricstore.Flow.LMDB.clear(path)
      end
    )

    try do
      assert {:error,
              {:flush_shard_failed, ^shard_index,
               {:flush_shard_apply_failed,
                {:flush_derived_state_cleanup_failed,
                 {:lmdb_clear_failed, :forced_lmdb_clear_failure}}}}} = Flush.flush(ctx)

      assert {:ok, status} = WARaftBackend.storage_status(shard_index)

      assert {:ok, {:raft_log_pos, blocked_index, _term}} =
               WARaftBackend.storage_position(shard_index)

      assert blocked_index < Keyword.fetch!(status, :last_applied)

      assert {:flush_shard_apply_failed,
              {:flush_derived_state_cleanup_failed,
               {:lmdb_clear_failed, :forced_lmdb_clear_failure}}} ==
               Keyword.fetch!(status, :blocked_error)
    after
      Application.delete_env(:ferricstore, :flush_derived_lmdb_clear_hook)
    end

    assert :ok = WARaftBackend.stop()
    assert :ok = WARaftBackend.start(ctx)
    assert :ok = ShardHelpers.wait_default_pipeline_ready()

    assert nil == Router.get(ctx, key)
    assert {:ok, position_after} = WARaftBackend.storage_position(shard_index)
    assert position_after != position_before
  end

  defp promote_hash!(suffix) do
    ctx = FerricStore.Instance.get(:default)
    redis_key = "flush-promoted-#{suffix}-#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, redis_key)

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.type_key(redis_key),
               "hash",
               0
             )

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "one"),
               "1",
               0
             )

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "two"),
               "2",
               0
             )

    ShardHelpers.eventually(
      fn ->
        ctx
        |> Router.shard_name(shard_index)
        |> GenServer.call({:promoted?, redis_key})
      end,
      "expected hash to be promoted before FLUSHDB"
    )

    dedicated_path = Promotion.dedicated_path(ctx.data_dir, shard_index, :hash, redis_key)
    assert File.dir?(dedicated_path)

    %{
      ctx: ctx,
      redis_key: redis_key,
      shard_index: shard_index,
      dedicated_path: dedicated_path
    }
  end
end
