defmodule Ferricstore.Store.Shard.CompoundMemberIndexTest do
  use ExUnit.Case, async: true

  alias Ferricstore.CommandTime
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.Compound.Ops, as: CompoundOps
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  setup do
    keydir = :ets.new(:compound_member_index_keydir, [:set, :public])
    index = :ets.new(:compound_member_index, [:ordered_set, :public])
    CompoundMemberIndex.reset(index)
    %{keydir: keydir, index: index}
  end

  test "put ignores non-compound keys without creating a named index table" do
    table = :"compound_member_index_missing_#{System.unique_integer([:positive])}"

    assert :ets.whereis(table) == :undefined
    assert CompoundMemberIndex.put(table, "flow/state/0/id") == :ok
    assert :ets.whereis(table) == :undefined
  end

  test "scan_entries returns indexed hash fields without scanning unrelated keys", %{
    keydir: keydir,
    index: index
  } do
    Enum.each(1..1_000, fn i ->
      :ets.insert(keydir, {"plain:#{i}", "value", 0, 0, 0, 0, 5})
    end)

    key = "user:1"
    field_key = CompoundKey.hash_field(key, "name")
    :ets.insert(keydir, {field_key, "yoav", 0, 0, 0, 0, 4})
    CompoundMemberIndex.put(index, field_key)

    state = %{keydir: keydir, shard_data_path: nil, compound_member_index: index}

    assert CompoundMemberIndex.scan_entries(index, state, CompoundKey.hash_prefix(key)) ==
             {:ok, [{"name", "yoav"}]}
  end

  test "scan page fails closed instead of materializing an unready catalog", %{keydir: keydir} do
    key = "fallback-hash"
    prefix = CompoundKey.hash_prefix(key)

    Enum.each(["a", "b", "c"], fn field ->
      compound_key = CompoundKey.hash_field(key, field)
      :ets.insert(keydir, {compound_key, field, 0, 0, 0, 0, 1})
    end)

    state = %{
      ets: keydir,
      keydir: keydir,
      compound_member_index: nil,
      promoted_instances: %{},
      shard_data_path: nil
    }

    assert {:reply, {:error, {:storage_read_failed, :compound_member_index_unavailable}}, state} ==
             CompoundOps.handle_compound_scan_page(
               key,
               prefix,
               0,
               2,
               nil,
               false,
               state
             )
  end

  test "delete removes compound members from the index", %{keydir: keydir, index: index} do
    key = "tags"
    member_key = CompoundKey.set_member(key, "hot")
    :ets.insert(keydir, {member_key, "1", 0, 0, 0, 0, 1})
    CompoundMemberIndex.put(index, member_key)
    CompoundMemberIndex.delete(index, member_key)

    state = %{keydir: keydir, shard_data_path: nil, compound_member_index: index}

    assert CompoundMemberIndex.scan_entries(index, state, CompoundKey.set_prefix(key)) ==
             {:ok, []}
  end

  test "keydir mutation helpers keep the compound catalog in sync", %{
    keydir: keydir,
    index: index
  } do
    prefix = CompoundKey.hash_prefix("transaction")
    compound_key = CompoundKey.hash_field("transaction", "field")
    state = %{keydir: keydir, compound_member_index: index}

    assert true = ShardETS.ets_insert(state, compound_key, "value", 0)

    assert {:ok, {0, ["field"]}} =
             CompoundMemberIndex.scan_page(index, state, prefix, 0, 10, nil)

    assert true = ShardETS.ets_delete_key(state, compound_key)
    assert {:ok, {0, []}} = CompoundMemberIndex.scan_page(index, state, prefix, 0, 10, nil)
  end

  test "any_live? checks only the requested compound prefix", %{keydir: keydir, index: index} do
    Enum.each(1..1_000, fn i ->
      other_key = CompoundKey.set_member("other:#{i}", "member")
      :ets.insert(keydir, {other_key, "1", 0, 0, 0, 0, 1})
      CompoundMemberIndex.put(index, other_key)
    end)

    member_key = CompoundKey.set_member("tags", "hot")
    :ets.insert(keydir, {member_key, "1", 0, 0, 0, 0, 1})
    CompoundMemberIndex.put(index, member_key)

    state = %{keydir: keydir, shard_data_path: nil, compound_member_index: index}

    assert CompoundMemberIndex.any_live?(index, state, CompoundKey.set_prefix("tags"))
    refute CompoundMemberIndex.any_live?(index, state, CompoundKey.set_prefix("missing"))
  end

  test "any_live? uses the stamped command time for member expiry", %{
    keydir: keydir,
    index: index
  } do
    member_key = CompoundKey.set_member("stamped-tags", "member")
    prefix = CompoundKey.set_prefix("stamped-tags")
    local_now = Ferricstore.HLC.now_ms()
    expire_at_ms = local_now - 1
    :ets.insert(keydir, {member_key, "1", expire_at_ms, 0, 0, 0, 1})
    CompoundMemberIndex.put(index, member_key)
    state = %{keydir: keydir, shard_data_path: nil, compound_member_index: index}

    assert CommandTime.with_now_ms(local_now - 10_000, fn ->
             CompoundMemberIndex.any_live?(index, state, prefix)
           end)

    assert [{^member_key, "1", ^expire_at_ms, _lfu, 0, 0, 1}] =
             :ets.lookup(keydir, member_key)
  end

  test "any_live? drops stale index entries", %{keydir: keydir, index: index} do
    member_key = CompoundKey.set_member("tags", "stale")
    CompoundMemberIndex.put(index, member_key)

    state = %{keydir: keydir, shard_data_path: nil, compound_member_index: index}

    refute CompoundMemberIndex.any_live?(index, state, CompoundKey.set_prefix("tags"))
    assert :ets.lookup(index, {CompoundKey.set_prefix("tags"), "stale"}) == []
  end

  test "count_live walks only the requested collection and drops stale index rows", %{
    keydir: keydir,
    index: index
  } do
    Enum.each(1..1_000, fn i ->
      member_key = CompoundKey.set_member("other:#{i}", "member")
      :ets.insert(keydir, {member_key, "1", 0, 0, 0, 0, 1})
      CompoundMemberIndex.put(index, member_key)
    end)

    live_key = CompoundKey.set_member("tags", "live")
    cold_key = CompoundKey.set_member("tags", "cold")
    stale_key = CompoundKey.set_member("tags", "stale")
    prefix = CompoundKey.set_prefix("tags")

    :ets.insert(keydir, {live_key, "1", 0, 0, 0, 0, 1})
    :ets.insert(keydir, {cold_key, nil, 0, 0, 7, 99, 1})
    CompoundMemberIndex.put(index, live_key)
    CompoundMemberIndex.put(index, cold_key)
    CompoundMemberIndex.put(index, stale_key)

    assert {:ok, 2} = CompoundMemberIndex.count_live(index, %{keydir: keydir}, prefix)
    assert :ets.lookup(index, {prefix, "stale"}) == []
  end

  test "count_live reports an unavailable index" do
    assert :unavailable = CompoundMemberIndex.count_live(nil, %{}, "S:tags\0")
  end

  test "prefix count falls back to the keydir while an index is still empty", %{
    keydir: keydir
  } do
    index = :ets.new(:unready_compound_member_index, [:ordered_set, :public])
    member_key = CompoundKey.set_member("tags", "live")
    :ets.insert(keydir, {member_key, "1", 0, 0, 0, 0, 1})

    state = %{keydir: keydir, compound_member_index: index}

    assert 1 = ShardETS.prefix_count_entries(state, CompoundKey.set_prefix("tags"))
  end

  test "a rebuilt empty index is authoritative for count and scan", %{
    keydir: keydir,
    index: index
  } do
    prefix = CompoundKey.set_prefix("tags")
    CompoundMemberIndex.rebuild(index, keydir)

    # A ready catalog must not fall back to an unrelated full-keydir scan when
    # its exact prefix range is empty.
    member_key = CompoundKey.set_member("tags", "not-indexed")
    :ets.insert(keydir, {member_key, "1", 0, 0, 0, 0, 1})
    state = %{keydir: keydir, shard_data_path: nil, compound_member_index: index}

    assert 0 = ShardETS.prefix_count_entries(state, prefix)
    assert [] = ShardETS.prefix_scan_entries(state, prefix, nil)
  end

  test "any_live? ignores pending-deleted compound keys", %{keydir: keydir, index: index} do
    member_key = CompoundKey.set_member("tags", "hot")
    :ets.insert(keydir, {member_key, "1", 0, 0, 0, 0, 1})
    CompoundMemberIndex.put(index, member_key)

    state = %{keydir: keydir, shard_data_path: nil, compound_member_index: index}

    assert CompoundMemberIndex.any_live?(index, state, CompoundKey.set_prefix("tags"))

    refute CompoundMemberIndex.any_live?(
             index,
             state,
             CompoundKey.set_prefix("tags"),
             %{member_key => :deleted}
           )
  end

  test "scan_entries drops stale index entries", %{keydir: keydir, index: index} do
    key = "tags"
    member_key = CompoundKey.set_member(key, "stale")
    CompoundMemberIndex.put(index, member_key)

    state = %{keydir: keydir, shard_data_path: nil, compound_member_index: index}

    assert CompoundMemberIndex.scan_entries(index, state, CompoundKey.set_prefix(key)) ==
             {:ok, []}

    assert :ets.lookup(index, {CompoundKey.set_prefix(key), "stale"}) == []
  end

  test "scan_entries preserves malformed live rows and their index entries", %{
    keydir: keydir,
    index: index
  } do
    key = "tags"
    prefix = CompoundKey.set_prefix(key)
    member_key = CompoundKey.set_member(key, "broken")
    index_key = {prefix, "broken"}

    :ets.insert(keydir, {member_key, nil, 0, 0, 0, :invalid_offset, 1})
    CompoundMemberIndex.put(index, member_key)

    assert :unavailable == CompoundMemberIndex.scan_entries(index, %{keydir: keydir}, prefix)
    assert [{^member_key, nil, 0, 0, 0, :invalid_offset, 1}] = :ets.lookup(keydir, member_key)
    assert [{^index_key, ^member_key}] = :ets.lookup(index, index_key)
  end

  test "reduce_rows_while stops catalog traversal immediately", %{
    keydir: keydir,
    index: index
  } do
    prefix = CompoundKey.hash_prefix("bounded")

    Enum.each(["a", "b", "c"], fn field ->
      compound_key = CompoundKey.hash_field("bounded", field)
      :ets.insert(keydir, {compound_key, field, 0, 0, 0, 0, 1})
      CompoundMemberIndex.put(index, compound_key)
    end)

    reducer = fn {compound_key, _value, _exp, _lfu, _fid, _off, _vsize}, visited ->
      field =
        binary_part(compound_key, byte_size(prefix), byte_size(compound_key) - byte_size(prefix))

      send(self(), {:visited, field})

      if field == "b", do: {:halt, :limit_reached}, else: {:cont, [field | visited]}
    end

    assert {:halt, :limit_reached} =
             CompoundMemberIndex.reduce_rows_while(index, %{keydir: keydir}, prefix, [], reducer)

    assert_received {:visited, "a"}
    assert_received {:visited, "b"}
    refute_received {:visited, "c"}
  end

  @tag :transaction_watch_catalog
  test "bounded key lookup stops before materializing an oversized prefix", %{index: index} do
    prefix = CompoundKey.hash_prefix("watch-budget")

    keys =
      Enum.map(["a", "b", "c"], fn field ->
        compound_key = CompoundKey.hash_field("watch-budget", field)
        CompoundMemberIndex.put(index, compound_key)
        compound_key
      end)

    assert {:error, :limit_exceeded} = CompoundMemberIndex.keys_for_prefix(index, prefix, 2)
    assert {:ok, ^keys} = CompoundMemberIndex.keys_for_prefix(index, prefix, 3)
    assert {:ok, []} = CompoundMemberIndex.keys_for_prefix(index, "H:other\0", 0)
  end

  test "bounded prefix scan enforces limits through catalog and keydir continuations", %{
    keydir: keydir,
    index: index
  } do
    prefix = CompoundKey.hash_prefix("bounded-scan")

    Enum.each(["a", "b", "c"], fn field ->
      compound_key = CompoundKey.hash_field("bounded-scan", field)
      :ets.insert(keydir, {compound_key, field, 0, 0, 0, 0, 1})
      CompoundMemberIndex.put(index, compound_key)
    end)

    limits = %{
      max_entries: 1,
      max_bytes: :unlimited,
      entry_overhead: 0,
      include_values: true,
      fields_only: false
    }

    CompoundMemberIndex.reset(index)

    Enum.each(["a", "b", "c"], fn field ->
      CompoundMemberIndex.put(index, CompoundKey.hash_field("bounded-scan", field))
    end)

    indexed_state = %{keydir: keydir, compound_member_index: index}

    assert {:error, :collection_response_limit} =
             ShardETS.prefix_scan_entries_bounded(indexed_state, prefix, nil, limits)

    assert {:error, :collection_response_limit} =
             ShardETS.prefix_scan_entries_bounded(%{keydir: keydir}, prefix, nil, limits)

    unbounded = %{limits | max_entries: :unlimited}

    assert entries = ShardETS.prefix_scan_entries_bounded(indexed_state, prefix, nil, unbounded)
    assert Enum.sort(entries) == [{"a", "a"}, {"b", "b"}, {"c", "c"}]
  end

  test "rebuild indexes live compound rows and skips expired rows", %{
    keydir: keydir,
    index: index
  } do
    live_key = CompoundKey.hash_field("hash", "live")
    expired_key = CompoundKey.hash_field("hash", "expired")

    :ets.insert(keydir, {live_key, "1", 0, 0, 0, 0, 1})
    :ets.insert(keydir, {expired_key, "2", 1, 0, 0, 0, 1})

    CompoundMemberIndex.rebuild(index, keydir)

    state = %{keydir: keydir, shard_data_path: nil, compound_member_index: index}

    assert CompoundMemberIndex.scan_entries(index, state, CompoundKey.hash_prefix("hash")) ==
             {:ok, [{"live", "1"}]}
  end

  test "member_slice returns a bounded deterministic window and wraps", %{
    keydir: keydir,
    index: index
  } do
    prefix = CompoundKey.set_prefix("tags")

    Enum.each(["a", "b", "c", "d"], fn member ->
      member_key = CompoundKey.set_member("tags", member)
      :ets.insert(keydir, {member_key, "1", 0, 0, 0, 0, 1})
      CompoundMemberIndex.put(index, member_key)
    end)

    state = %{keydir: keydir}

    assert {:ok, ["c", "d", "a"]} =
             CompoundMemberIndex.member_slice(index, state, prefix, "c", 3, 10, %{})
  end

  test "member_slice skips pending deletes and accepts valid cold locations", %{
    keydir: keydir,
    index: index
  } do
    prefix = CompoundKey.set_prefix("tags")
    deleted_key = CompoundKey.set_member("tags", "a")
    cold_key = CompoundKey.set_member("tags", "b")

    :ets.insert(keydir, {deleted_key, "1", 0, 0, 0, 0, 1})
    :ets.insert(keydir, {cold_key, nil, 0, 0, 7, 99, 1})
    CompoundMemberIndex.put(index, deleted_key)
    CompoundMemberIndex.put(index, cold_key)

    assert {:ok, ["b"]} =
             CompoundMemberIndex.member_slice(
               index,
               %{keydir: keydir},
               prefix,
               "a",
               2,
               10,
               %{deleted_key => :deleted}
             )
  end

  test "member_slice reports malformed live keydir rows", %{keydir: keydir, index: index} do
    prefix = CompoundKey.set_prefix("tags")
    member_key = CompoundKey.set_member("tags", "broken")

    :ets.insert(keydir, {member_key, nil, 0, 0, :invalid, -1, -1})
    CompoundMemberIndex.put(index, member_key)

    assert {:error, {:invalid_cold_location, ^member_key, {:invalid, -1, -1}}} =
             CompoundMemberIndex.member_slice(
               index,
               %{keydir: keydir},
               prefix,
               "",
               1,
               10,
               %{}
             )
  end

  test "row_slice walks from the nearest catalog boundary", %{keydir: keydir, index: index} do
    prefix = CompoundKey.list_prefix("ranked")

    rows =
      Enum.map(0..9, fn rank ->
        compound_key = CompoundKey.list_element("ranked", rank * 1_000_000_000)
        row = {compound_key, "v#{rank}", 0, 0, 0, 0, byte_size("v#{rank}")}
        :ets.insert(keydir, row)
        CompoundMemberIndex.put(index, compound_key)
        {rank, compound_key, row}
      end)

    Enum.each(Enum.take(rows, 8), fn {_rank, compound_key, _row} ->
      :ets.delete(keydir, compound_key)
    end)

    assert {:ok, [{_compound_key, "v8", 0, 0, 0, 0, 2}]} =
             CompoundMemberIndex.row_slice(index, %{keydir: keydir}, prefix, 8, 1, 10)

    Enum.each(Enum.take(rows, 8), fn {_rank, compound_key, _row} ->
      member =
        binary_part(compound_key, byte_size(prefix), byte_size(compound_key) - byte_size(prefix))

      assert [_index_entry] =
               :ets.lookup(index, {prefix, member})
    end)
  end

  test "scan_page returns bounded ordered pages and applies MATCH while walking the index", %{
    keydir: keydir,
    index: index
  } do
    prefix = CompoundKey.hash_prefix("hash")

    Enum.each(["a", "b-one", "b-two", "c"], fn field ->
      compound_key = CompoundKey.hash_field("hash", field)
      :ets.insert(keydir, {compound_key, "value-#{field}", 0, 0, 0, 0, 7 + byte_size(field)})
      CompoundMemberIndex.put(index, compound_key)
    end)

    state = %{keydir: keydir}

    assert {:ok, {cursor, ["a", "b-one"]}} =
             CompoundMemberIndex.scan_page(index, state, prefix, 0, 2, nil)

    assert {:ok, {0, ["b-two", "c"]}} =
             CompoundMemberIndex.scan_page(index, state, prefix, cursor, 2, nil)

    assert {:ok, {after_a, []}} =
             CompoundMemberIndex.scan_page(index, state, prefix, 0, 1, "b*")

    assert {:ok, {after_b_one, ["b-one"]}} =
             CompoundMemberIndex.scan_page(index, state, prefix, after_a, 1, "b*")

    assert {:ok, {after_b_two, ["b-two"]}} =
             CompoundMemberIndex.scan_page(index, state, prefix, after_b_one, 1, "b*")

    assert {:ok, {0, []}} =
             CompoundMemberIndex.scan_page(index, state, prefix, after_b_two, 1, "b*")
  end

  test "scan_page cursor seeks after the previous member without revisiting it", %{
    keydir: keydir,
    index: index
  } do
    prefix = CompoundKey.set_prefix("seek")

    keys =
      Enum.map(["a", "b", "c", "d"], fn member ->
        compound_key = CompoundKey.set_member("seek", member)
        :ets.insert(keydir, {compound_key, "1", 0, 0, 0, 0, 1})
        CompoundMemberIndex.put(index, compound_key)
        {member, compound_key}
      end)

    assert {:ok, {cursor, ["a", "b"]}} =
             CompoundMemberIndex.scan_page(index, %{keydir: keydir}, prefix, 0, 2, nil)

    refute cursor == 0

    Enum.each(Enum.take(keys, 2), fn {_member, compound_key} ->
      :ets.delete(keydir, compound_key)
    end)

    assert {:ok, {0, ["c", "d"]}} =
             CompoundMemberIndex.scan_page(
               index,
               %{keydir: keydir},
               prefix,
               cursor,
               2,
               nil
             )

    Enum.each(Enum.take(keys, 2), fn {member, compound_key} ->
      assert [{{^prefix, ^member}, ^compound_key}] = :ets.lookup(index, {prefix, member})
    end)
  end

  test "zero member_slice does not require index or keydir tables" do
    assert {:ok, []} = CompoundMemberIndex.member_slice(nil, %{}, "S:tags\0", "", 0, 10, %{})
  end
end
