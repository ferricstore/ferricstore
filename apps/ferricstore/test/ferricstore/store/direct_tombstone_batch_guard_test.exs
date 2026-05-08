defmodule Ferricstore.Store.DirectTombstoneBatchGuardTest do
  use ExUnit.Case, async: true

  @compound_path Path.expand("../../../lib/ferricstore/store/shard/compound.ex", __DIR__)
  @lifecycle_path Path.expand("../../../lib/ferricstore/store/shard/lifecycle.ex", __DIR__)
  @native_ops_path Path.expand("../../../lib/ferricstore/store/shard/native_ops.ex", __DIR__)
  @writes_path Path.expand("../../../lib/ferricstore/store/shard/writes.ex", __DIR__)

  test "direct compound batch delete uses batched tombstone append" do
    source = File.read!(@compound_path)
    body = function_body(source, "handle_compound_batch_delete_direct")

    assert body =~ "delete_compound_key_group_direct"
    refute body =~ "handle_compound_delete_direct(redis_key, compound_key"
  end

  test "direct compound batch put uses batched append paths" do
    source = File.read!(@compound_path)
    body = function_body(source, "handle_compound_batch_put_direct")

    assert body =~ "put_compound_key_group_direct"
    refute body =~ "handle_compound_put_direct(redis_key, compound_key"
  end

  test "direct promoted compound batch put writes values in one append batch" do
    source = File.read!(@compound_path)
    body = function_body(source, "promoted_write_batch")

    assert body =~ "NIF.v2_append_batch"
    refute body =~ "NIF.v2_append_record"
  end

  test "direct compound prefix delete writes tombstones in one ops batch" do
    source = File.read!(@compound_path)
    body = function_body(source, "tombstone_and_delete_keys")

    assert body =~ "append_tombstone_batch_sync"
    refute body =~ "NIF.v2_append_tombstone"
  end

  test "direct list compound batch delete writes tombstones in one ops batch" do
    source = File.read!(@native_ops_path)

    [_before, direct_store] =
      String.split(source, "def build_list_compound_store_direct", parts: 2)

    [_before_delete, section] = String.split(direct_store, "compound_batch_delete: fn", parts: 2)
    [body, _after] = String.split(section, "\n      end,", parts: 2)

    assert body =~ "append_tombstone_batch_sync"
    refute body =~ "NIF.v2_append_tombstone"
  end

  test "direct delete_prefix writes tombstones in one ops batch" do
    source = File.read!(@writes_path)
    body = function_body(source, "tombstone_and_delete_keys")

    assert body =~ "append_tombstone_batch_sync"
    refute body =~ "NIF.v2_append_tombstone"
  end

  test "expiry sweep batches shared tombstones" do
    source = File.read!(@lifecycle_path)

    assert source =~ "defp expire_shared_keys"

    body = function_body(source, "expire_shared_keys")

    assert body =~ "append_tombstone_batch_sync"
    refute body =~ "NIF.v2_append_tombstone"
  end

  test "expiry sweep batches promoted tombstones by dedicated path" do
    source = File.read!(@lifecycle_path)
    body = function_body(source, "expire_promoted_key_group")

    assert body =~ "promoted_tombstone_batch"
    refute body =~ "NIF.v2_append_tombstone"
  end

  defp function_body(source, function) do
    [_before, rest] = String.split(source, "defp #{function}", parts: 2)
    [body, _after] = String.split(rest, "\n  end\n", parts: 2)
    body
  end
end
