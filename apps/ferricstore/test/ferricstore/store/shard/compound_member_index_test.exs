defmodule Ferricstore.Store.Shard.CompoundMemberIndexTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.CompoundMemberIndex

  setup do
    keydir = :ets.new(:compound_member_index_keydir, [:set, :public])
    index = :ets.new(:compound_member_index, [:ordered_set, :public])
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

  test "any_live? drops stale index entries", %{keydir: keydir, index: index} do
    member_key = CompoundKey.set_member("tags", "stale")
    CompoundMemberIndex.put(index, member_key)

    state = %{keydir: keydir, shard_data_path: nil, compound_member_index: index}

    refute CompoundMemberIndex.any_live?(index, state, CompoundKey.set_prefix("tags"))
    assert :ets.lookup(index, {CompoundKey.set_prefix("tags"), "stale"}) == []
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
end
