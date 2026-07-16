defmodule Ferricstore.Store.PromotionCopyFailureTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{CompoundKey, LFU, Promotion}
  alias Ferricstore.Store.Shard.CompoundMemberIndex

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "promotion_copy_failure_#{System.unique_integer([:positive])}"
      )

    data_dir = Path.join(root, "data")
    shard_path = Path.join(data_dir, "shard_0")
    shared_log = Path.join(shard_path, "00000.log")
    File.mkdir_p!(shard_path)
    File.touch!(shared_log)

    keydir = :ets.new(:promotion_copy_failure, [:set, :public])

    on_exit(fn ->
      File.rm_rf!(root)
    end)

    %{data_dir: data_dir, keydir: keydir, shard_path: shard_path, shared_log: shared_log}
  end

  test "a cold member read failure aborts before promotion publication", ctx do
    redis_key = "hash:copy-read-failure"
    type_key = CompoundKey.type_key(redis_key)
    member_key = CompoundKey.hash_field(redis_key, "wanted")
    other_key = CompoundKey.hash_field(redis_key, "other")

    {:ok, locations} =
      NIF.v2_append_batch(ctx.shared_log, [
        {type_key, "hash", 0},
        {other_key, "other-value", 0},
        {member_key, "wanted-value", 0}
      ])

    [{type_offset, type_size}, {other_offset, _other_size}, {_member_offset, member_size}] =
      locations

    :ets.insert(ctx.keydir, {
      type_key,
      "hash",
      0,
      LFU.initial(),
      0,
      type_offset,
      type_size
    })

    # The live member is cold, but its locator points at another key. A keyed
    # read must reject it; silently omitting the row would publish a partial copy.
    :ets.insert(ctx.keydir, {
      member_key,
      nil,
      0,
      LFU.initial(),
      0,
      other_offset,
      member_size
    })

    member_index = :ets.new(:promotion_copy_failure_members, [:ordered_set, :public])
    :ok = CompoundMemberIndex.rebuild(member_index, ctx.keydir)

    assert_raise RuntimeError, ~r/promotion copy read failed/, fn ->
      Promotion.promote_collection!(
        :hash,
        redis_key,
        ctx.shard_path,
        ctx.keydir,
        ctx.data_dir,
        0,
        nil,
        member_index
      )
    end

    assert [] = :ets.lookup(ctx.keydir, Promotion.marker_key(redis_key))

    refute File.exists?(Promotion.dedicated_path(ctx.data_dir, 0, :hash, redis_key))

    assert {:ok, records} = NIF.v2_scan_file(ctx.shared_log)

    assert Enum.any?(records, fn
             {^member_key, _offset, _size, 0, false} -> true
             _record -> false
           end)
  end

  test "indexed promotion never folds unrelated keydir rows", ctx do
    redis_key = "hash:indexed-copy"
    type_key = CompoundKey.type_key(redis_key)
    member_key = CompoundKey.hash_field(redis_key, "field")

    {:ok, [{type_offset, type_size}, {member_offset, member_size}]} =
      NIF.v2_append_batch(ctx.shared_log, [
        {type_key, "hash", 0},
        {member_key, "value", 0}
      ])

    :ets.insert(ctx.keydir, {
      type_key,
      "hash",
      0,
      LFU.initial(),
      0,
      type_offset,
      type_size
    })

    :ets.insert(ctx.keydir, {
      member_key,
      "value",
      0,
      LFU.initial(),
      0,
      member_offset,
      member_size
    })

    # A full keydir fold would invoke its seven-tuple row matcher on this
    # unrelated row and crash. The collection index must isolate the migration.
    :ets.insert(ctx.keydir, {:unrelated_malformed_row, :ignored})

    member_index = :ets.new(:promotion_copy_members, [:ordered_set, :public])
    CompoundMemberIndex.reset(member_index)
    CompoundMemberIndex.put(member_index, member_key)

    assert {:ok, dedicated_path} =
             Promotion.promote_collection!(
               :hash,
               redis_key,
               ctx.shard_path,
               ctx.keydir,
               ctx.data_dir,
               0,
               nil,
               member_index
             )

    assert {:ok, records} = dedicated_path |> Promotion.find_active() |> NIF.v2_scan_file()

    assert Enum.any?(records, fn
             {^member_key, _offset, _size, 0, false} -> true
             _record -> false
           end)
  end

  test "promotion rejects mismatched type metadata before publishing a marker", ctx do
    redis_key = "hash:mismatched-type"
    type_key = CompoundKey.type_key(redis_key)
    member_key = CompoundKey.hash_field(redis_key, "field")

    {:ok, [{type_offset, type_size}, {member_offset, member_size}]} =
      NIF.v2_append_batch(ctx.shared_log, [
        {type_key, "set", 0},
        {member_key, "value", 0}
      ])

    :ets.insert(ctx.keydir, {
      type_key,
      "set",
      0,
      LFU.initial(),
      0,
      type_offset,
      type_size
    })

    :ets.insert(ctx.keydir, {
      member_key,
      "value",
      0,
      LFU.initial(),
      0,
      member_offset,
      member_size
    })

    member_index = :ets.new(:promotion_mismatched_type_members, [:ordered_set, :public])
    CompoundMemberIndex.reset(member_index)
    CompoundMemberIndex.put(member_index, member_key)

    assert_raise RuntimeError, ~r/type metadata mismatch/, fn ->
      Promotion.promote_collection!(
        :hash,
        redis_key,
        ctx.shard_path,
        ctx.keydir,
        ctx.data_dir,
        0,
        nil,
        member_index
      )
    end

    assert [] = :ets.lookup(ctx.keydir, Promotion.marker_key(redis_key))
  end
end
