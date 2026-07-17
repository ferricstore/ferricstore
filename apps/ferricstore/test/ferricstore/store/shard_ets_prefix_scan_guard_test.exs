defmodule Ferricstore.Store.ShardETSPrefixScanGuardTest do
  use ExUnit.Case, async: true

  alias Ferricstore.CommandTime
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ETS
  alias Ferricstore.Store.Shard.ZSetIndex

  @prefix_scan_path Path.expand(
                      "../../../lib/ferricstore/store/shard/ets/prefix_scan.ex",
                      __DIR__
                    )

  test "prefix scans batch cold disk reads" do
    source = File.read!(@prefix_scan_path)

    # HGETALL and related compound scans can touch many cold large values.
    # Keep the scan path from regressing to one blocking pread per cold entry.
    assert source =~ "ColdRead.pread_batch_keyed",
           "expected Shard.ETS prefix scan cold path to use keyed batched cold reads"

    assert source =~ "ColdRead.emit_pread_error",
           "expected Shard.ETS prefix scan cold path to report corrupt/missing cold records"

    refute Regex.match?(~r/(?<!_)v2_pread_at\(/, source),
           "expected Shard.ETS prefix scan cold path to avoid blocking v2_pread_at/2"
  end

  test "prefix scans use the stamped command time for expiry" do
    keydir = :ets.new(:prefix_scan_stamped_time, [:set, :public])
    prefix = CompoundKey.hash_prefix("stamped-time")
    field_key = CompoundKey.hash_field("stamped-time", "field")
    local_now = Ferricstore.HLC.now_ms()
    expire_at_ms = local_now - 1
    :ets.insert(keydir, {field_key, "value", expire_at_ms, 0, 0, 0, 5})

    assert [{"field", "value"}] =
             CommandTime.with_now_ms(local_now - 10_000, fn ->
               ETS.prefix_scan_entries(keydir, prefix, nil)
             end)
  end

  @tag :exact_cold_prefix_scan
  test "exact compound-index scans hydrate cold rows without abandoning the catalog" do
    root = Path.join(System.tmp_dir!(), "exact_cold_prefix_#{System.unique_integer()}")
    log_path = Path.join(root, "00000.log")
    File.mkdir_p!(root)
    File.touch!(log_path)

    keydir = :ets.new(:exact_cold_prefix_keydir, [:set, :public])
    index = :ets.new(:exact_cold_prefix_index, [:ordered_set, :public])
    CompoundMemberIndex.reset(index)

    redis_key = "cold-hash"
    prefix = CompoundKey.hash_prefix(redis_key)
    field_key = CompoundKey.hash_field(redis_key, "field")
    {:ok, [{offset, value_size}]} = NIF.v2_append_batch(log_path, [{field_key, "cold", 0}])

    :ets.insert(keydir, {field_key, nil, 0, 0, 0, offset, value_size})
    CompoundMemberIndex.put(index, field_key)

    state = %{
      keydir: keydir,
      compound_member_index: index,
      data_dir: root,
      index: 0
    }

    try do
      assert [{"field", "cold"}] = ETS.prefix_scan_entries(state, prefix, root)
    after
      :ets.delete(index)
      :ets.delete(keydir)
      File.rm_rf!(root)
    end
  end

  @tag :exact_cold_prefix_scan
  test "compound-index scan branch consumes exact rows instead of hot-only entries" do
    source = File.read!(@prefix_scan_path)
    [_before, body] = String.split(source, "defp maybe_compound_index_scan_entries", parts: 2)
    [body | _after] = String.split(body, "defp compound_member_index_ref", parts: 2)

    assert body =~ "CompoundMemberIndex.scan_rows"
    refute body =~ "CompoundMemberIndex.scan_entries"
  end

  test "prefix scans batch-materialize blob refs" do
    source = File.read!(@prefix_scan_path)
    [_before, section] = String.split(source, "def prefix_read_cold_batch_async", parts: 2)

    [read_body, helper_section] =
      String.split(section, "def prefix_materialize_blob_values", parts: 2)

    assert read_body =~ "prefix_materialize_blob_values",
           "expected prefix scans to materialize duplicate blob refs once per batch"

    assert helper_section =~ "BlobValue.maybe_materialize_many",
           "expected prefix scans to use the BlobValue batch materializer"

    refute read_body =~ "materialize_blob_value(state, value)",
           "prefix scans should not materialize blob refs one entry at a time"
  end

  test "prefix key collection can stop at a caller-provided bound" do
    keydir = :ets.new(:bounded_prefix_keys, [:set, :public])

    for suffix <- 1..3 do
      true = :ets.insert(keydir, {"watch:#{suffix}", "value", 0, 0, 0, 0, 0})
    end

    true = :ets.insert(keydir, {"other:1", "value", 0, 0, 0, 0, 0})

    keys = Ferricstore.Store.Shard.ETS.prefix_collect_keys(keydir, "watch:", 2)

    assert length(keys) == 2
    assert Enum.all?(keys, &String.starts_with?(&1, "watch:"))
  end

  test "full prefix traversals use bounded ETS continuation pages" do
    source = File.read!(@prefix_scan_path)

    [_before, field_scan] = String.split(source, "def do_prefix_scan_fields", parts: 2)
    [field_scan | _after] = String.split(field_scan, "def flow_history_cold_location?", parts: 2)

    assert field_scan =~ ":ets.select(keydir, ms, @bounded_select_chunk_size)"
    refute field_scan =~ "keys = :ets.select(keydir, ms)"

    [_before, expired_cleanup] =
      String.split(source, "def delete_expired_prefix_entries", parts: 2)

    [expired_cleanup | _after] =
      String.split(expired_cleanup, "def maybe_delete_expired_prefix_entry", parts: 2)

    assert expired_cleanup =~ ":ets.select(keydir, expired_ms, @bounded_select_chunk_size)"
    refute expired_cleanup =~ "|> :ets.select(expired_ms)"

    [_before, key_collection] =
      String.split(source, "def prefix_collect_keys(keydir, prefix)", parts: 2)

    [key_collection | _after] =
      String.split(key_collection, "def prefix_collect_keys(_keydir", parts: 2)

    assert key_collection =~ "collect_prefix_keys"
    refute key_collection =~ ":ets.select(keydir, prefix_key_match_spec(prefix))"
  end

  test "chunked field traversal returns every live field and removes expired rows" do
    keydir = :ets.new(:chunked_prefix_fields, [:set, :public])
    prefix = "H:chunked" <> <<0>>

    live_fields = Enum.map(1..300, &"live-#{&1}")
    expired_fields = Enum.map(1..150, &"expired-#{&1}")

    try do
      Enum.each(live_fields, fn field ->
        true = :ets.insert(keydir, {prefix <> field, "value", 0, 0, 0, 0, 5})
      end)

      Enum.each(expired_fields, fn field ->
        true = :ets.insert(keydir, {prefix <> field, "value", 1, 0, 0, 0, 5})
      end)

      assert MapSet.new(live_fields) ==
               keydir
               |> then(&ETS.prefix_scan_fields(%{keydir: &1}, prefix))
               |> MapSet.new()

      assert Enum.all?(expired_fields, fn field -> :ets.lookup(keydir, prefix <> field) == [] end)
      assert :ets.info(keydir, :size) == length(live_fields)
    after
      :ets.delete(keydir)
    end
  end

  test "prefix expiry cleanup evicts a ready zset score-index member" do
    keydir = :ets.new(:prefix_scan_zset_expiry_keydir, [:set, :public])
    index = :ets.new(:prefix_scan_zset_expiry_index, [:ordered_set, :public])
    lookup = :ets.new(:prefix_scan_zset_expiry_lookup, [:set, :public])
    redis_key = "expired-zset"
    member = "member"
    prefix = CompoundKey.zset_prefix(redis_key)
    compound_key = CompoundKey.zset_member(redis_key, member)
    expired = {compound_key, "1", 1, 0, 0, 0, 1}

    state = %{
      keydir: keydir,
      zset_score_index: index,
      zset_score_lookup: lookup
    }

    try do
      true = :ets.insert(keydir, expired)
      :ok = ZSetIndex.mark_ready_empty(index, lookup, redis_key)
      :ok = ZSetIndex.put_member(index, lookup, redis_key, member, "1")

      assert [] =
               CommandTime.with_now_ms(10, fn ->
                 ETS.prefix_scan_entries(state, prefix, nil)
               end)

      assert [] == :ets.lookup(keydir, compound_key)
      assert [] == ZSetIndex.rank_range(index, redis_key, 0, 10, false)
      assert 0 == ZSetIndex.count(index, lookup, redis_key, :neg_inf, :inf)
    after
      :ets.delete(lookup)
      :ets.delete(index)
      :ets.delete(keydir)
    end
  end

  test "bounded prefix iteration visits every key while the callback deletes rows" do
    keydir = :ets.new(:chunked_prefix_each, [:set, :public])
    prefix = "delete:"

    try do
      Enum.each(1..350, fn suffix ->
        true = :ets.insert(keydir, {prefix <> Integer.to_string(suffix), "value", 0, 0, 0, 0, 5})
      end)

      true = :ets.insert(keydir, {"keep:1", "value", 0, 0, 0, 0, 5})
      Process.put(:prefix_each_count, 0)

      assert :ok =
               ETS.prefix_each_key(keydir, prefix, fn key ->
                 Process.put(:prefix_each_count, Process.get(:prefix_each_count, 0) + 1)
                 :ets.delete(keydir, key)
               end)

      assert Process.get(:prefix_each_count) == 350
      assert :ets.info(keydir, :size) == 1
      assert [{"keep:1", "value", 0, 0, 0, 0, 5}] = :ets.lookup(keydir, "keep:1")
    after
      Process.delete(:prefix_each_count)
      :ets.delete(keydir)
    end
  end

  test "raft shard prefix deletion streams keys instead of materializing the prefix" do
    source =
      __DIR__
      |> Path.join("../../../lib/ferricstore/store/shard/writes.ex")
      |> Path.expand()
      |> File.read!()

    [_before, delete_prefix] = String.split(source, "def handle_delete_prefix", parts: 2)
    [delete_prefix | _after] = String.split(delete_prefix, "# Helpers", parts: 2)
    [raft_branch | _direct_branch] = String.split(delete_prefix, "else", parts: 2)

    assert raft_branch =~ "ShardETS.prefix_each_key"
    refute raft_branch =~ "ShardETS.prefix_collect_keys"
  end

  test "prefix cleanup cannot delete a replacement committed after its scan" do
    keydir = :ets.new(:prefix_cleanup_exact_delete, [:set, :public])
    key = "watch:renewed"
    observed = {key, "old", 10, 1, 2, 3, 4}
    replacement = {key, "new", 0, 5, 6, 7, 8}

    try do
      true = :ets.insert(keydir, replacement)

      refute Ferricstore.Store.Shard.ETS.PrefixScan.delete_prefix_entry(%{}, keydir, observed)
      assert [^replacement] = :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "prefix scans fail instead of omitting an unreadable cold row" do
    keydir = :ets.new(:prefix_scan_cold_failure, [:set, :public])
    data_dir = Path.join(System.tmp_dir!(), "missing_prefix_scan_#{System.unique_integer()}")
    prefix = "H:key" <> <<0>>
    compound_key = prefix <> "field"
    state = %{keydir: keydir, data_dir: data_dir, index: 0}

    try do
      true = :ets.insert(keydir, {compound_key, nil, 0, 0, 17, 0, 5})

      result = ETS.prefix_scan_entries(state, prefix, data_dir)

      assert ReadResult.failure?(result)
    after
      :ets.delete(keydir)
    end
  end
end
