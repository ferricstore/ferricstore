defmodule Ferricstore.Store.BlobSideChannelTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey}
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 65_536)
    original_threshold = Application.get_env(:ferricstore, :promotion_threshold)

    original_persistent_threshold =
      try do
        :persistent_term.get(:ferricstore_promotion_threshold)
      rescue
        ArgumentError -> :not_set
      end

    Application.put_env(:ferricstore, :promotion_threshold, 1)
    :persistent_term.put(:ferricstore_promotion_threshold, 1)

    on_exit(fn ->
      case original_threshold do
        nil -> Application.delete_env(:ferricstore, :promotion_threshold)
        value -> Application.put_env(:ferricstore, :promotion_threshold, value)
      end

      case original_persistent_threshold do
        :not_set -> :persistent_term.erase(:ferricstore_promotion_threshold)
        value -> :persistent_term.put(:ferricstore_promotion_threshold, value)
      end

      IsolatedInstance.checkin(ctx)
    end)

    %{ctx: ctx, shard: elem(ctx.shard_names, 0), keydir: elem(ctx.keydir_refs, 0)}
  end

  test "shared Bitcask compaction rewrites only blob refs and leaves blob bytes untouched", %{
    ctx: ctx,
    shard: shard
  } do
    payload = :binary.copy("A", 1024)
    assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
    blob_path = BlobRef.path(ctx.data_dir, 0, ref)
    blob_ref = BlobRef.encode!(ref)

    assert :ok = GenServer.call(shard, {:put, "blob:shared", blob_ref, 0})
    assert :ok = GenServer.call(shard, {:put, "blob:dead", "dead", 0})
    assert :ok = GenServer.call(shard, :flush)
    assert :ok = GenServer.call(shard, {:delete, "blob:dead"})

    force_rotate_active_file(shard)

    assert {:ok, {_written, _dropped, _reclaimed}} =
             GenServer.call(shard, {:run_compaction, [0]})

    assert blob_ref == GenServer.call(shard, {:get, "blob:shared"})
    assert File.read!(blob_path) == payload
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
  end

  test "promoted dedicated compaction rewrites blob refs without owning blob bytes", %{
    ctx: ctx,
    shard: shard
  } do
    first_payload = :binary.copy("B", 2048)
    second_payload = :binary.copy("C", 4096)

    assert {:ok, first_blob_ref} = BlobStore.put(ctx.data_dir, 0, first_payload)
    assert {:ok, second_blob_ref} = BlobStore.put(ctx.data_dir, 0, second_payload)

    blob_path = BlobRef.path(ctx.data_dir, 0, first_blob_ref)
    first_ref = BlobRef.encode!(first_blob_ref)
    second_ref = BlobRef.encode!(second_blob_ref)

    redis_key = "blob_hash"
    field_a = CompoundKey.hash_field(redis_key, "a")
    field_b = CompoundKey.hash_field(redis_key, "b")

    assert :ok = GenServer.call(shard, {:compound_put, redis_key, field_a, first_ref, 0})
    assert :ok = GenServer.call(shard, {:compound_put, redis_key, field_b, second_ref, 0})

    state = :sys.get_state(shard)
    dedicated_path = state.promoted_instances[redis_key].path

    refute String.starts_with?(
             dedicated_path,
             Ferricstore.DataDir.blob_shard_path(ctx.data_dir, 0)
           )

    :sys.replace_state(shard, fn state ->
      ShardCompound.compact_dedicated(state, redis_key, dedicated_path)
    end)

    assert first_ref == GenServer.call(shard, {:compound_get, redis_key, field_a})
    assert second_ref == GenServer.call(shard, {:compound_get, redis_key, field_b})
    assert File.read!(blob_path) == first_payload
    assert {:ok, ^first_payload} = BlobStore.get(ctx.data_dir, 0, first_blob_ref)
    assert {:ok, ^second_payload} = BlobStore.get(ctx.data_dir, 0, second_blob_ref)
  end

  defp force_rotate_active_file(shard) do
    :sys.replace_state(shard, fn state ->
      new_id = state.active_file_id + 1
      shard_path = state.shard_data_path
      new_path = Ferricstore.Store.Shard.ETS.file_path(shard_path, new_id)

      Ferricstore.FS.touch!(new_path)

      Ferricstore.Store.ActiveFile.publish(
        state.instance_ctx,
        state.index,
        new_id,
        new_path,
        shard_path
      )

      %{
        state
        | active_file_id: new_id,
          active_file_path: new_path,
          active_file_size: 0,
          file_stats: Map.put(state.file_stats, new_id, {0, 0})
      }
    end)
  end
end
