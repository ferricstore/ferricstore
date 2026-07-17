defmodule Ferricstore.Store.Shard.ZSetIndexTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.ZSetIndex

  setup do
    index = :ets.new(:zset_index_test, [:ordered_set])
    lookup = :ets.new(:zset_lookup_test, [:set])

    on_exit(fn ->
      delete_table(index)
      delete_table(lookup)
    end)

    {:ok, index: index, lookup: lookup}
  end

  test "reverse score range slice includes inclusive max-score ties", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [{"a", "2"}, {"b", "2"}, {"c", "2"}, {"d", "1"}])

    assert [{"c", 2.0}, {"b", 2.0}] ==
             ZSetIndex.range_slice(
               index,
               "zs",
               {:inclusive, 2.0},
               {:inclusive, 2.0},
               true,
               0,
               2
             )
  end

  test "forward score range slice starts at inclusive lower bound", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [
      {"a", "2"},
      {"b", "2"},
      {"c", "2"},
      {"d", "3"},
      {"before", "1"}
    ])

    assert [{"b", 2.0}, {"c", 2.0}] ==
             ZSetIndex.range_slice(
               index,
               "zs",
               {:inclusive, 2.0},
               {:inclusive, 3.0},
               false,
               1,
               2
             )
  end

  test "score range honors exclusive bounds through the ETS index", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [{"a", "1"}, {"b", "2"}, {"c", "2"}, {"d", "3"}])

    assert [{"d", 3.0}] ==
             ZSetIndex.range(index, "zs", {:exclusive, 2.0}, {:inclusive, 3.0}, false)

    assert [{"a", 1.0}] ==
             ZSetIndex.range(index, "zs", {:inclusive, 1.0}, {:exclusive, 2.0}, true)

    assert 2 == ZSetIndex.count(index, lookup, "zs", {:inclusive, 2.0}, {:inclusive, 2.0})
    assert 4 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
  end

  test "rank cursors stay inside the redis key namespace", %{index: index, lookup: lookup} do
    insert_members(index, lookup, "other", [{"z", "99"}])
    insert_members(index, lookup, "zs", [{"a", "1"}, {"b", "2"}, {"c", "3"}])

    assert [{"c", 3.0}, {"b", 2.0}] == ZSetIndex.rank_range(index, "zs", 0, 1, true)
    assert 1 == ZSetIndex.member_rank(index, lookup, "zs", "b", true)
  end

  test "bulk put updates members and keeps count exact", %{index: index, lookup: lookup} do
    insert_members(index, lookup, "zs", [{"a", "1"}, {"b", "2"}])

    assert :ok == ZSetIndex.put_members(index, lookup, "zs", [{"b", 4}, {"c", 3.0}])

    assert 3 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
    assert [{"a", 1.0}, {"c", 3.0}, {"b", 4.0}] == ZSetIndex.rank_range(index, "zs", 0, 2, false)
    assert 2 == ZSetIndex.member_rank(index, lookup, "zs", "b", false)
  end

  test "bulk put invalidates the whole key when any score is invalid", %{
    index: index,
    lookup: lookup
  } do
    :ok = ZSetIndex.mark_ready_empty(index, lookup, "zs")
    insert_members(index, lookup, "zs", [{"old", "1"}])

    assert :ok =
             ZSetIndex.put_members(index, lookup, "zs", [
               {"before-invalid", "2"},
               {"invalid", "not-a-score"},
               {"after-invalid", "3"}
             ])

    refute ZSetIndex.ready?(lookup, "zs")
    assert [] == ZSetIndex.rank_range(index, "zs", 0, 10, false)
    assert 0 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
  end

  test "bulk new put skips lookup and keeps count exact", %{index: index, lookup: lookup} do
    assert :ok == ZSetIndex.put_new_members(index, lookup, "zs", [{"a", 1}, {"b", 2.0}])

    assert 2 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
    assert [{"a", 1.0}, {"b", 2.0}] == ZSetIndex.rank_range(index, "zs", 0, 2, false)
  end

  test "new-member helpers invalidate readiness for an invalid score", %{
    index: index,
    lookup: lookup
  } do
    :ok = ZSetIndex.mark_ready_empty(index, lookup, "zs")
    assert :ok = ZSetIndex.put_new_member(index, lookup, "zs", "invalid", "not-a-score")
    refute ZSetIndex.ready?(lookup, "zs")

    :ok = ZSetIndex.mark_ready_empty(index, lookup, "zs")

    assert :ok =
             ZSetIndex.put_new_members(index, lookup, "zs", [
               {"valid", "1"},
               {"invalid", "not-a-score"}
             ])

    refute ZSetIndex.ready?(lookup, "zs")
    assert [] == ZSetIndex.rank_range(index, "zs", 0, 10, false)
  end

  test "ready empty clears stale entries before accepting tracked puts", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [{"old", "99"}])

    assert :ok == ZSetIndex.mark_ready_empty(index, lookup, "zs")
    assert ZSetIndex.ready?(lookup, "zs")
    assert 0 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
    assert [] == ZSetIndex.rank_range(index, "zs", 0, 10, false)

    assert :ok == ZSetIndex.apply_put_to_tables(index, lookup, "zs", <<"Z:zs", 0, "new">>, "1")
    assert [{"new", 1.0}] == ZSetIndex.rank_range(index, "zs", 0, 10, false)
  end

  test "an invalid tracked score update invalidates the index instead of retaining an old score",
       %{
         index: index,
         lookup: lookup
       } do
    compound_key = CompoundKey.zset_member("zs", "member")
    :ok = ZSetIndex.mark_ready_empty(index, lookup, "zs")
    :ok = ZSetIndex.put_member(index, lookup, "zs", "member", "1")

    assert :ok =
             ZSetIndex.apply_put_to_tables(index, lookup, "zs", compound_key, "not-a-score")

    refute ZSetIndex.ready?(lookup, "zs")
    assert [] == ZSetIndex.rank_range(index, "zs", 0, 10, false)
    assert 0 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
  end

  test "bulk delete removes only present members and keeps count exact", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [{"a", "1"}, {"b", "2"}, {"c", "3"}])

    assert :ok == ZSetIndex.delete_members(index, lookup, "zs", ["a", "missing", "c"])

    assert 1 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
    assert [{"b", 2.0}] == ZSetIndex.rank_range(index, "zs", 0, 10, false)
    assert nil == ZSetIndex.member_rank(index, lookup, "zs", "a", false)
  end

  test "bulk delete handles duplicate delete requests once", %{index: index, lookup: lookup} do
    insert_members(index, lookup, "zs", [{"a", "1"}, {"b", "2"}])

    assert :ok == ZSetIndex.delete_members(index, lookup, "zs", ["a", "a", "missing"])

    assert 1 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
    assert [{"b", 2.0}] == ZSetIndex.rank_range(index, "zs", 0, 10, false)
  end

  test "rebuild_key atomically replaces one key and marks it ready", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [{"stale", "99"}])
    insert_members(index, lookup, "other", [{"keep", "1"}])

    assert :ok == ZSetIndex.rebuild_key(index, lookup, "zs", [{"a", "2"}, {"b", "3"}])

    assert ZSetIndex.ready?(lookup, "zs")
    assert [{"a", 2.0}, {"b", 3.0}] == ZSetIndex.rank_range(index, "zs", 0, 10, false)
    assert [{"keep", 1.0}] == ZSetIndex.rank_range(index, "other", 0, 10, false)
  end

  test "ensure does not publish a partial index after a storage read failure", %{
    index: index,
    lookup: lookup
  } do
    keydir = :ets.new(:zset_index_cold_failure, [:set, :public])
    data_path = Path.join(System.tmp_dir!(), "missing_zset_index_#{System.unique_integer()}")
    redis_key = "zs"
    prefix = "Z:zs" <> <<0>>

    state = %{
      keydir: keydir,
      data_dir: data_path,
      index: 0,
      zset_score_index: index,
      zset_score_lookup: lookup
    }

    try do
      true = :ets.insert(keydir, {prefix <> "member", nil, 0, 0, 17, 0, 5})

      result = ZSetIndex.ensure(state, redis_key, prefix, data_path)

      assert ReadResult.failure?(result)
      refute ZSetIndex.ready?(lookup, redis_key)
      assert [] == ZSetIndex.rank_range(index, redis_key, 0, 10, false)
    after
      :ets.delete(keydir)
    end
  end

  test "ensure fails closed instead of publishing a partial index for an invalid score", %{
    index: index,
    lookup: lookup
  } do
    keydir = :ets.new(:zset_index_invalid_score, [:set, :public])
    redis_key = "zs"
    prefix = "Z:zs" <> <<0>>

    state = %{
      keydir: keydir,
      zset_score_index: index,
      zset_score_lookup: lookup
    }

    try do
      true = :ets.insert(keydir, {prefix <> "valid", "1", 0, 0, 0, 0, 1})
      true = :ets.insert(keydir, {prefix <> "invalid", "not-a-score", 0, 0, 0, 0, 11})

      assert {:error, {:storage_read_failed, {:invalid_score, "invalid", "not-a-score"}}} =
               ZSetIndex.ensure(state, redis_key, prefix, nil)

      refute ZSetIndex.ready?(lookup, redis_key)
      assert [] == ZSetIndex.rank_range(index, redis_key, 0, 10, false)
      assert 0 == ZSetIndex.count(index, lookup, redis_key, :neg_inf, :inf)
    after
      :ets.delete(keydir)
    end
  end

  test "ensure stores readiness only in ETS and does not require per-state key retention", %{
    index: index,
    lookup: lookup
  } do
    keydir = :ets.new(:zset_index_ets_readiness, [:set, :public])
    redis_key = "zs"
    prefix = CompoundKey.zset_prefix(redis_key)

    state = %{
      keydir: keydir,
      zset_score_index: index,
      zset_score_lookup: lookup
    }

    try do
      true =
        :ets.insert(keydir, {CompoundKey.zset_member(redis_key, "member"), "1", 0, 0, 0, 0, 1})

      assert {:ok, ^state} = ZSetIndex.ensure(state, redis_key, prefix, nil)
      assert ZSetIndex.ready?(lookup, redis_key)
      assert [{"member", 1.0}] == ZSetIndex.rank_range(index, redis_key, 0, 10, false)
    after
      :ets.delete(keydir)
    end
  end

  test "exact keydir deletion evicts an expired member from a ready score index", %{
    index: index,
    lookup: lookup
  } do
    keydir = :ets.new(:zset_index_exact_delete, [:set, :public])
    redis_key = "zs"
    member = "expired"
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

      assert ShardETS.delete_exact_entry(state, expired)
      assert [] == ZSetIndex.rank_range(index, redis_key, 0, 10, false)
      assert 0 == ZSetIndex.count(index, lookup, redis_key, :neg_inf, :inf)
    after
      :ets.delete(keydir)
    end
  end

  test "exact keydir deletion preserves a concurrently renewed score index member", %{
    index: index,
    lookup: lookup
  } do
    keydir = :ets.new(:zset_index_exact_delete_renewal, [:set, :public])
    redis_key = "zs"
    member = "renewed"
    compound_key = CompoundKey.zset_member(redis_key, member)
    expired = {compound_key, "1", 1, 0, 0, 0, 1}
    replacement = {compound_key, "2", 0, 0, 1, 10, 1}

    state = %{
      keydir: keydir,
      zset_score_index: index,
      zset_score_lookup: lookup
    }

    try do
      true = :ets.insert(keydir, expired)
      :ok = ZSetIndex.mark_ready_empty(index, lookup, redis_key)
      :ok = ZSetIndex.put_member(index, lookup, redis_key, member, "1")

      Process.put(:ferricstore_after_exact_keydir_delete_hook, fn _state, ^expired ->
        true = :ets.insert(keydir, replacement)
        :ok = ZSetIndex.put_member(index, lookup, redis_key, member, "2")
      end)

      assert ShardETS.delete_exact_entry(state, expired)
      assert [^replacement] = :ets.lookup(keydir, compound_key)
      assert [{member, 2.0}] == ZSetIndex.rank_range(index, redis_key, 0, 10, false)
      assert 1 == ZSetIndex.count(index, lookup, redis_key, :neg_inf, :inf)
    after
      Process.delete(:ferricstore_after_exact_keydir_delete_hook)
      :ets.delete(keydir)
    end
  end

  test "exact zset type-marker deletion invalidates all indexed members", %{
    index: index,
    lookup: lookup
  } do
    keydir = :ets.new(:zset_index_type_expiry, [:set, :public])
    redis_key = "zs"
    type_key = CompoundKey.type_key(redis_key)
    expired_type = {type_key, "zset", 1, 0, 0, 0, 4}

    state = %{
      keydir: keydir,
      zset_score_index: index,
      zset_score_lookup: lookup
    }

    try do
      true = :ets.insert(keydir, expired_type)
      :ok = ZSetIndex.mark_ready_empty(index, lookup, redis_key)
      :ok = ZSetIndex.put_member(index, lookup, redis_key, "old-a", "1")
      :ok = ZSetIndex.put_member(index, lookup, redis_key, "old-b", "2")

      assert ShardETS.delete_exact_entry(state, expired_type)
      refute ZSetIndex.ready?(lookup, redis_key)
      assert [] == ZSetIndex.rank_range(index, redis_key, 0, 10, false)
      assert 0 == ZSetIndex.count(index, lookup, redis_key, :neg_inf, :inf)
    after
      :ets.delete(keydir)
    end
  end

  defp insert_members(index, lookup, redis_key, members) do
    Enum.each(members, fn {member, score} ->
      ZSetIndex.put_member(index, lookup, redis_key, member, score)
    end)
  end

  defp delete_table(table) do
    if :ets.info(table) != :undefined do
      :ets.delete(table)
    end
  end
end
