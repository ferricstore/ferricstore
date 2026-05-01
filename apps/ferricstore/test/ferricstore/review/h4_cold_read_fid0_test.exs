defmodule Ferricstore.Review.H4ColdReadFid0Test do
  @moduledoc """
  Verifies that ets_insert uses :pending as fid for unflushed entries,
  preventing cold reads from misinterpreting them as disk locations.

  Previously ets_insert stored fid=0, which matched the cold read pattern
  and caused pread_at(path, 0) to return wrong data. Now uses :pending,
  which ets_lookup treats as a miss — falling through to await_in_flight.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @threshold 128

  setup do
    ctx =
      Ferricstore.Test.IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: @threshold
      )

    pid = Process.whereis(elem(ctx.shard_names, 0))
    keydir = elem(ctx.keydir_refs, 0)
    on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)

    %{shard: pid, index: 0, keydir: keydir, ctx: ctx}
  end

  describe "ets_insert uses :pending for unflushed entries" do
    test "large value ETS entry has fid=:pending", %{shard: shard, keydir: keydir} do
      large = String.duplicate("X", @threshold + 1)

      # Seed a small value and flush so offset 0 of 00000.log is occupied.
      :ok = GenServer.call(shard, {:put, "seed", "tiny", 0})
      :ok = GenServer.call(shard, :flush)

      # Force flush_in_flight so the next PUT's pending is NOT flushed.
      :sys.replace_state(shard, fn s -> %{s | flush_in_flight: 999_999} end)

      # Write a large value — ets_insert stores nil with fid=:pending.
      :ok = GenServer.call(shard, {:put, "big", large, 0})

      [{_key, ets_val, _exp, _lfu, fid, off, vsize}] = :ets.lookup(keydir, "big")

      assert ets_val == nil, "large value should be nil in ETS (cold)"
      assert fid == :pending, "expected fid=:pending, got #{inspect(fid)}"
      assert off == 0
      assert vsize == 0
    end
  end

  describe "GET on unflushed large value works correctly" do
    test "read triggers flush and returns correct value", %{shard: shard} do
      large = String.duplicate("L", @threshold + 100)

      # Write large value — goes to pending with fid=:pending.
      :ok = GenServer.call(shard, {:put, "big", large, 0})

      # GET should trigger flush_pending_sync and return the correct value.
      result = GenServer.call(shard, {:get, "big"})
      assert result == large
    end

    test "read waits for a truly pending large value", %{shard: shard, keydir: keydir} do
      large = String.duplicate("W", @threshold + 100)

      :sys.replace_state(shard, fn s -> %{s | flush_in_flight: 999_999} end)
      :ok = GenServer.call(shard, {:put, "pending_big", large, 0})

      assert [{"pending_big", nil, _exp, _lfu, :pending, 0, 0}] =
               :ets.lookup(keydir, "pending_big")

      send(shard, {:tokio_complete, 999_999, :ok, :ok})

      assert large == GenServer.call(shard, {:get, "pending_big"})
    end

    test "read of pending large value does not force a durability fsync",
         %{shard: shard, keydir: keydir, ctx: ctx} do
      large = String.duplicate("D", @threshold + 100)

      :atomics.put(ctx.checkpoint_flags, 1, 0)
      :sys.replace_state(shard, fn s -> %{s | flush_in_flight: 999_999} end)
      :ok = GenServer.call(shard, {:put, "pending_dirty_read", large, 0})

      assert [{"pending_dirty_read", nil, _exp, _lfu, :pending, 0, 0}] =
               :ets.lookup(keydir, "pending_dirty_read")

      send(shard, {:tokio_complete, 999_999, :ok, :ok})

      assert large == GenServer.call(shard, {:get, "pending_dirty_read"})
      assert :atomics.get(ctx.checkpoint_flags, 1) == 1
    end

    test "get_meta waits for a truly pending large value", %{shard: shard, keydir: keydir} do
      large = String.duplicate("M", @threshold + 100)
      expire_at_ms = Ferricstore.HLC.now_ms() + 60_000

      :sys.replace_state(shard, fn s -> %{s | flush_in_flight: 999_999} end)
      :ok = GenServer.call(shard, {:put, "pending_meta", large, expire_at_ms})

      assert [{"pending_meta", nil, ^expire_at_ms, _lfu, :pending, 0, 0}] =
               :ets.lookup(keydir, "pending_meta")

      send(shard, {:tokio_complete, 999_999, :ok, :ok})

      assert {^large, ^expire_at_ms} = GenServer.call(shard, {:get_meta, "pending_meta"})
    end

    test "get_file_ref waits for a truly pending large value", %{shard: shard, keydir: keydir} do
      large = String.duplicate("F", @threshold + 100)

      :sys.replace_state(shard, fn s -> %{s | flush_in_flight: 999_999} end)
      :ok = GenServer.call(shard, {:put, "pending_file_ref", large, 0})

      assert [{"pending_file_ref", nil, _exp, _lfu, :pending, 0, 0}] =
               :ets.lookup(keydir, "pending_file_ref")

      send(shard, {:tokio_complete, 999_999, :ok, :ok})

      assert {path, offset, size} = GenServer.call(shard, {:get_file_ref, "pending_file_ref"})
      assert is_binary(path)
      assert offset > 0
      assert size == byte_size(large)
    end
  end

  describe "prefix scan on unflushed large value" do
    test "scan waits for pending large value instead of treating :pending as a file id",
         %{shard: shard, keydir: keydir} do
      prefix = "H:scan_pending" <> <<0>>
      field_key = prefix <> "field"
      large = String.duplicate("P", @threshold + 100)

      :sys.replace_state(shard, fn s -> %{s | flush_in_flight: 999_999} end)
      :ok = GenServer.call(shard, {:put, field_key, large, 0})

      assert [{^field_key, nil, _exp, _lfu, :pending, 0, 0}] = :ets.lookup(keydir, field_key)

      send(shard, {:tokio_complete, 999_999, :ok, :ok})

      assert [{"field", ^large}] =
               GenServer.call(shard, {:scan_prefix, prefix})
    end

    test "compound scan waits for pending large value before HSCAN-style reads",
         %{shard: shard, keydir: keydir} do
      redis_key = "scan_hash"
      prefix = "H:" <> redis_key <> <<0>>
      field_key = prefix <> "field"
      large = String.duplicate("C", @threshold + 100)

      :sys.replace_state(shard, fn s -> %{s | flush_in_flight: 999_999} end)
      :ok = GenServer.call(shard, {:compound_put, redis_key, field_key, large, 0})

      assert [{^field_key, nil, _exp, _lfu, :pending, 0, 0}] = :ets.lookup(keydir, field_key)

      send(shard, {:tokio_complete, 999_999, :ok, :ok})

      assert [{"field", ^large}] =
               GenServer.call(shard, {:compound_scan, redis_key, prefix})
    end

    test "raw prefix helper skips pending cold entries instead of crashing",
         %{keydir: keydir, ctx: ctx} do
      prefix = "H:raw_pending" <> <<0>>
      field_key = prefix <> "field"
      shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)

      :ets.insert(keydir, {field_key, nil, 0, 1, :pending, 0, 0})

      assert [] == ShardETS.prefix_scan_entries(keydir, prefix, shard_path)
    end

    test "scan without pending cold entries does not checkpoint dirty data",
         %{shard: shard, ctx: ctx} do
      prefix = "H:clean_scan" <> <<0>>

      :atomics.put(ctx.checkpoint_flags, 1, 1)

      assert [] == GenServer.call(shard, {:scan_prefix, prefix})
      assert :atomics.get(ctx.checkpoint_flags, 1) == 1
    end
  end

  describe "after flush, ETS has real disk location" do
    test "fid and vsize are updated after flush", %{shard: shard, keydir: keydir} do
      large = String.duplicate("A", @threshold + 50)

      :ok = GenServer.call(shard, {:put, "flushed_big", large, 0})
      :ok = GenServer.call(shard, :flush)

      [{_, nil, _, _, fid, _off, vsize}] = :ets.lookup(keydir, "flushed_big")

      assert fid != :pending, "fid should be updated from :pending after flush"
      assert is_integer(fid)
      assert vsize == byte_size(large)

      # GET returns correct value via cold read with real offset.
      assert large == GenServer.call(shard, {:get, "flushed_big"})
    end
  end
end
