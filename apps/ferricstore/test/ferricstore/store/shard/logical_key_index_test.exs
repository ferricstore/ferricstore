defmodule Ferricstore.Store.Shard.LogicalKeyIndexTest do
  use ExUnit.Case, async: true

  alias Ferricstore.CommandTime
  alias Ferricstore.HLC
  alias Ferricstore.Store.{CompoundKey, LFU}
  alias Ferricstore.Store.Shard.LogicalKeyIndex

  setup do
    suffix = System.unique_integer([:positive])
    ordered = :"logical_key_index_test_#{suffix}"
    slots = :"logical_key_slots_test_#{suffix}"
    LogicalKeyIndex.ensure_tables!(ordered, slots)
    keydir = :ets.new(:logical_key_index_keydir, [:set, :public])

    on_exit(fn ->
      if :ets.whereis(ordered) != :undefined, do: :ets.delete(ordered)
      if :ets.whereis(slots) != :undefined, do: :ets.delete(slots)
    end)

    %{keydir: keydir, ordered: ordered, slots: slots}
  end

  test "rebuild collapses compound rows and pages logical keys with stored types", ctx do
    now = HLC.now_ms()
    hash_key = "users:1"
    type_key = CompoundKey.type_key(hash_key)
    field_key = CompoundKey.hash_field(hash_key, "name")

    :ets.insert(ctx.keydir, [
      {"plain", "value", 0, LFU.initial(), 0, 0, 5},
      {type_key, "hash", 0, LFU.initial(), 0, 5, 4},
      {field_key, "alice", 0, LFU.initial(), 0, 9, 5},
      {"expired", "gone", now - 1, LFU.initial(), 0, 14, 4},
      {"f:{f}:internal", "hidden", 0, LFU.initial(), 0, 18, 6}
    ])

    assert :ok = LogicalKeyIndex.rebuild(ctx.ordered, ctx.slots, ctx.keydir)

    assert {:ok, {{:after, "plain"}, ["plain"]}} =
             LogicalKeyIndex.scan_page(
               ctx.ordered,
               ctx.keydir,
               0,
               1,
               nil,
               nil,
               now
             )

    assert {:ok, {0, ["users:1"]}} =
             LogicalKeyIndex.scan_page(
               ctx.ordered,
               ctx.keydir,
               {:after, "plain"},
               10,
               "users:*",
               "hash",
               now
             )

    assert {:ok, {0, []}} =
             LogicalKeyIndex.scan_page(
               ctx.ordered,
               ctx.keydir,
               0,
               10,
               nil,
               "set",
               now
             )

    assert {:ok, ["plain", "users:1"]} =
             LogicalKeyIndex.all_live(ctx.ordered, ctx.keydir, now, 1)
  end

  @tag :prob_type_catalog
  test "rebuild lets exact probabilistic markers override metadata value rows", ctx do
    key = "catalog-prob"
    metadata = Ferricstore.TermCodec.encode({:cms_meta, %{width: 32, depth: 4}})

    :ets.insert(ctx.keydir, [
      {key, metadata, 0, LFU.initial(), 0, 0, byte_size(metadata)},
      {CompoundKey.type_key(key), "cms", 0, LFU.initial(), 0, byte_size(metadata), 3}
    ])

    assert :ok = LogicalKeyIndex.rebuild(ctx.ordered, ctx.slots, ctx.keydir)

    assert [{^key, "cms", 0, _storage_key, _slot}] = :ets.lookup(ctx.ordered, key)

    assert {:ok, {0, [^key]}} =
             LogicalKeyIndex.scan_page(ctx.ordered, ctx.keydir, 0, 10, nil, "cms", HLC.now_ms())

    assert {:ok, {0, []}} =
             LogicalKeyIndex.scan_page(
               ctx.ordered,
               ctx.keydir,
               0,
               10,
               nil,
               "string",
               HLC.now_ms()
             )
  end

  @tag :logical_rebuild_single_pass
  test "rebuild visits each keydir row once while preserving type precedence", ctx do
    key = "single-pass-catalog"
    metadata = Ferricstore.TermCodec.encode({:cms_meta, %{width: 32, depth: 4}})
    keydir = :ets.new(:logical_key_index_ordered_keydir, [:ordered_set, :public])

    :ets.insert(keydir, [
      {key, metadata, 0, LFU.initial(), 0, 0, byte_size(metadata)},
      {"plain-single-pass", "value", 0, LFU.initial(), 0, byte_size(metadata), 5},
      {CompoundKey.type_key(key), CompoundKey.encode_prob_type(:cms, 11), 0, LFU.initial(), 0,
       byte_size(metadata) + 5, 12}
    ])

    counter = :counters.new(1, [:atomics])

    Process.put(:ferricstore_logical_key_rebuild_visit_hook, fn _storage_key ->
      :counters.add(counter, 1, 1)
    end)

    try do
      assert :ok = LogicalKeyIndex.rebuild(ctx.ordered, ctx.slots, keydir)
    after
      Process.delete(:ferricstore_logical_key_rebuild_visit_hook)
    end

    assert :counters.get(counter, 1) == 3
    assert [{^key, "cms", 0, _storage_key, _slot}] = :ets.lookup(ctx.ordered, key)
  end

  @tag :hlc_drift_guard
  test "rebuild keeps wall-live keys under an unsafe expiry context", ctx do
    key = "wall-live-logical-key"
    :ets.insert(ctx.keydir, {key, "value", 31_000, LFU.initial(), 0, 0, 5})

    assert :ok =
             CommandTime.with_expiry_context(61_000, 1_000, fn ->
               LogicalKeyIndex.rebuild(ctx.ordered, ctx.slots, ctx.keydir)
             end)

    assert [{^key, _type, 31_000, ^key, _slot}] = :ets.lookup(ctx.ordered, key)
  end

  test "put and delete keep paging and random sampling exact", ctx do
    assert :ok = LogicalKeyIndex.reset(ctx.ordered, ctx.slots)

    :ets.insert(ctx.keydir, [
      {"alpha", "value", 0, LFU.initial(), 0, 0, 5},
      {CompoundKey.type_key("hash"), "hash", 0, LFU.initial(), 0, 5, 4}
    ])

    assert :ok =
             LogicalKeyIndex.put(
               ctx.ordered,
               ctx.slots,
               "alpha",
               "value",
               0
             )

    assert :ok =
             LogicalKeyIndex.put(
               ctx.ordered,
               ctx.slots,
               CompoundKey.type_key("hash"),
               "hash",
               0
             )

    assert {:ok, random_key} = LogicalKeyIndex.random_key(ctx.ordered, ctx.slots, ctx.keydir)
    assert random_key in ["alpha", "hash"]

    assert :ok = LogicalKeyIndex.delete(ctx.ordered, ctx.slots, "alpha")

    assert {:ok, {0, ["hash"]}} =
             LogicalKeyIndex.scan_page(
               ctx.ordered,
               ctx.keydir,
               0,
               10,
               nil,
               nil,
               HLC.now_ms()
             )

    assert {:ok, "hash"} = LogicalKeyIndex.random_key(ctx.ordered, ctx.slots, ctx.keydir)
  end

  test "delete keeps random slots dense under churn", ctx do
    assert :ok = LogicalKeyIndex.reset(ctx.ordered, ctx.slots)

    Enum.each(["alpha", "beta", "gamma"], fn key ->
      :ets.insert(ctx.keydir, {key, "value", 0, LFU.initial(), 0, 0, 5})
      assert :ok = LogicalKeyIndex.put(ctx.ordered, ctx.slots, key, "value", 0)
    end)

    assert :ok = LogicalKeyIndex.delete(ctx.ordered, ctx.slots, "beta")
    :ets.delete(ctx.keydir, "beta")

    numeric_slots =
      ctx.slots
      |> :ets.tab2list()
      |> Enum.filter(fn {slot, _key} -> is_integer(slot) end)
      |> Enum.sort()

    assert numeric_slots == [{1, "alpha"}, {2, "gamma"}]

    Enum.each(1..20, fn index ->
      key = "churn-#{index}"
      :ets.insert(ctx.keydir, {key, "value", 0, LFU.initial(), 0, 0, 5})
      assert :ok = LogicalKeyIndex.put(ctx.ordered, ctx.slots, key, "value", 0)
      assert :ok = LogicalKeyIndex.delete(ctx.ordered, ctx.slots, key)
      :ets.delete(ctx.keydir, key)
    end)

    assert 2 = LogicalKeyIndex.slot_count(ctx.ordered, ctx.slots)
    assert {:ok, random_key} = LogicalKeyIndex.random_key(ctx.ordered, ctx.slots, ctx.keydir)
    assert random_key in ["alpha", "gamma"]
  end

  test "a dead writer cannot permanently wedge the logical key catalog", ctx do
    assert :ok = LogicalKeyIndex.reset(ctx.ordered, ctx.slots)
    dead_owner = spawn(fn -> :ok end)
    monitor = Process.monitor(dead_owner)
    assert_receive {:DOWN, ^monitor, :process, ^dead_owner, _reason}

    lock_key = :"$ferricstore_logical_key_index_write_lock"
    true = :ets.insert(ctx.slots, {lock_key, dead_owner})
    :ets.insert(ctx.keydir, {"after-crash", "value", 0, LFU.initial(), 0, 0, 5})

    assert :ok =
             LogicalKeyIndex.put(
               ctx.ordered,
               ctx.slots,
               "after-crash",
               "value",
               0
             )

    assert 1 = LogicalKeyIndex.slot_count(ctx.ordered, ctx.slots)
    assert [] = :ets.lookup(ctx.slots, lock_key)
  end

  test "count_live counts logical keys once and removes expired projections", ctx do
    now = HLC.now_ms()
    hash_key = "counted-hash"
    type_key = CompoundKey.type_key(hash_key)

    :ets.insert(ctx.keydir, [
      {"plain", "value", 0, LFU.initial(), 0, 0, 5},
      {type_key, "hash", 0, LFU.initial(), 0, 5, 4},
      {CompoundKey.hash_field(hash_key, "one"), "1", 0, LFU.initial(), 0, 9, 1},
      {CompoundKey.hash_field(hash_key, "two"), "2", 0, LFU.initial(), 0, 10, 1},
      {"expired", "gone", now + 10, LFU.initial(), 0, 11, 4}
    ])

    assert :ok = LogicalKeyIndex.rebuild(ctx.ordered, ctx.slots, ctx.keydir)
    assert {:ok, 3} = LogicalKeyIndex.count_live(ctx.ordered, ctx.slots, ctx.keydir, now)

    assert {:ok, 2} =
             LogicalKeyIndex.count_live(ctx.ordered, ctx.slots, ctx.keydir, now + 10)

    assert {:ok, {0, ["counted-hash", "plain"]}} =
             LogicalKeyIndex.scan_page(
               ctx.ordered,
               ctx.keydir,
               0,
               10,
               nil,
               nil,
               now + 10
             )
  end

  test "expiry cleanup preserves a key renewed after the expired row was observed", ctx do
    assert :ok = LogicalKeyIndex.reset(ctx.ordered, ctx.slots)
    now = HLC.now_ms()
    key = "renewed-during-logical-cleanup"
    expired = {key, "old", now - 1, LFU.initial(), 0, 10, 3}
    renewed = {key, "new", now + 60_000, LFU.initial(), 0, 20, 3}

    true = :ets.insert(ctx.keydir, expired)
    assert :ok = LogicalKeyIndex.put(ctx.ordered, ctx.slots, key, "old", now - 1)

    assert {:ok, 1} =
             LogicalKeyIndex.count_live(
               ctx.ordered,
               ctx.slots,
               ctx.keydir,
               now,
               fn ^expired ->
                 true = :ets.insert(ctx.keydir, renewed)
                 false
               end
             )

    assert {:ok, {0, [^key]}} =
             LogicalKeyIndex.scan_page(
               ctx.ordered,
               ctx.keydir,
               0,
               10,
               nil,
               nil,
               now
             )
  end

  test "count_live remains exact while bounding synchronous expiry cleanup", ctx do
    assert :ok = LogicalKeyIndex.reset(ctx.ordered, ctx.slots)
    now = HLC.now_ms()

    Enum.each(1..1_000, fn index ->
      key = "expired-count:#{index}"
      :ets.insert(ctx.keydir, {key, "gone", now - 1, LFU.initial(), 0, index, 4})
      assert :ok = LogicalKeyIndex.put(ctx.ordered, ctx.slots, key, "gone", now - 1)
    end)

    :ets.insert(ctx.keydir, {"live", "value", 0, LFU.initial(), 0, 0, 5})
    assert :ok = LogicalKeyIndex.put(ctx.ordered, ctx.slots, "live", "value", 0)
    cleaned = :atomics.new(1, signed: false)

    assert {:ok, 1} =
             LogicalKeyIndex.count_live(
               ctx.ordered,
               ctx.slots,
               ctx.keydir,
               now,
               fn _entry ->
                 :atomics.add_get(cleaned, 1, 1)
                 :ok
               end
             )

    assert cleanup_count = :atomics.get(cleaned, 1)
    assert cleanup_count > 0
    assert cleanup_count <= 256
  end

  test "count_live fails closed on malformed catalog rows", ctx do
    assert :ok = LogicalKeyIndex.reset(ctx.ordered, ctx.slots)
    :ets.insert(ctx.ordered, {"broken", :invalid})

    assert {:error, {:invalid_logical_key_entry, "broken", [{"broken", :invalid}]}} =
             LogicalKeyIndex.count_live(
               ctx.ordered,
               ctx.slots,
               ctx.keydir,
               HLC.now_ms()
             )
  end

  test "random sampling purges expired slots without biasing toward the first live key", ctx do
    assert :ok = LogicalKeyIndex.reset(ctx.ordered, ctx.slots)
    now = HLC.now_ms()

    Enum.each(1..990, fn index ->
      key = "expired:#{index}"
      :ets.insert(ctx.keydir, {key, "gone", now - 1, LFU.initial(), 0, index, 4})
      assert :ok = LogicalKeyIndex.put(ctx.ordered, ctx.slots, key, "gone", now - 1)
    end)

    Enum.each(["live:a", "live:b"], fn key ->
      :ets.insert(ctx.keydir, {key, "value", 0, LFU.initial(), 0, 0, 5})
      assert :ok = LogicalKeyIndex.put(ctx.ordered, ctx.slots, key, "value", 0)
    end)

    assert :ok = drain_expired_random_slots(ctx, 10)

    samples =
      Enum.frequencies_by(1..200, fn _sample ->
        assert {:ok, key} = LogicalKeyIndex.random_key(ctx.ordered, ctx.slots, ctx.keydir)
        key
      end)

    assert Map.get(samples, "live:a", 0) > 50
    assert Map.get(samples, "live:b", 0) > 50
    assert 2 = LogicalKeyIndex.slot_count(ctx.ordered, ctx.slots)
  end

  test "random sampling bounds synchronous cleanup while making expiry progress", ctx do
    assert :ok = LogicalKeyIndex.reset(ctx.ordered, ctx.slots)
    now = HLC.now_ms()

    Enum.each(1..1_000, fn index ->
      key = "expired-random:#{index}"
      :ets.insert(ctx.keydir, {key, "gone", now - 1, LFU.initial(), 0, index, 4})
      assert :ok = LogicalKeyIndex.put(ctx.ordered, ctx.slots, key, "gone", now - 1)
    end)

    assert {:error, :logical_key_expiry_backlog} =
             LogicalKeyIndex.random_key(ctx.ordered, ctx.slots, ctx.keydir)

    remaining = LogicalKeyIndex.slot_count(ctx.ordered, ctx.slots)
    assert remaining < 1_000
    assert remaining >= 680

    result =
      Enum.reduce_while(1..10, nil, fn _attempt, _last_result ->
        case LogicalKeyIndex.random_key(ctx.ordered, ctx.slots, ctx.keydir) do
          {:error, :logical_key_expiry_backlog} = backlog -> {:cont, backlog}
          {:ok, nil} = empty -> {:halt, empty}
        end
      end)

    assert {:ok, nil} = result
    assert 0 = LogicalKeyIndex.slot_count(ctx.ordered, ctx.slots)
  end

  test "readers never observe a partially published slot mutation", ctx do
    assert :ok = LogicalKeyIndex.reset(ctx.ordered, ctx.slots)
    parent = self()

    writer =
      Task.async(fn ->
        send(parent, :writer_started)

        Enum.each(1..20_000, fn _iteration ->
          :ok = LogicalKeyIndex.put(ctx.ordered, ctx.slots, "flip", "value", 0)
          :ok = LogicalKeyIndex.delete(ctx.ordered, ctx.slots, "flip")
        end)
      end)

    assert_receive :writer_started

    observed =
      Enum.reduce(1..50_000, MapSet.new(), fn _iteration, counts ->
        case LogicalKeyIndex.slot_count(ctx.ordered, ctx.slots) do
          count when count in [0, 1] ->
            MapSet.put(counts, count)

          {:error, :logical_key_index_busy} ->
            MapSet.put(counts, :busy)

          inconsistent ->
            flunk("observed partially published logical-key index: #{inspect(inconsistent)}")
        end
      end)

    Task.await(writer, 10_000)
    assert MapSet.subset?(observed, MapSet.new([0, 1, :busy]))
    assert 0 = LogicalKeyIndex.slot_count(ctx.ordered, ctx.slots)
  end

  test "rebuild fails closed on malformed keydir rows", ctx do
    :ets.insert(ctx.keydir, {:malformed, :row})

    assert {:error, {:invalid_keydir_row, {:malformed, :row}}} =
             LogicalKeyIndex.rebuild(ctx.ordered, ctx.slots, ctx.keydir)
  end

  defp drain_expired_random_slots(_ctx, 0), do: {:error, :cleanup_did_not_converge}

  defp drain_expired_random_slots(ctx, attempts) do
    if LogicalKeyIndex.slot_count(ctx.ordered, ctx.slots) == 2 do
      :ok
    else
      case LogicalKeyIndex.random_key(ctx.ordered, ctx.slots, ctx.keydir) do
        {:error, :logical_key_expiry_backlog} ->
          drain_expired_random_slots(ctx, attempts - 1)

        {:ok, key} when key in ["live:a", "live:b"] ->
          drain_expired_random_slots(ctx, attempts - 1)

        result ->
          result
      end
    end
  end
end
