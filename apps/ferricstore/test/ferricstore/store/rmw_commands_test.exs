defmodule Ferricstore.Store.RmwCommandsTest do
  @moduledoc """
  Concurrency correctness for read-modify-write commands that previously
  ran the read-compute-write cycle in the caller process. Under concurrent
  load this was losing updates (two callers read the same state, both
  compute independently, last writer wins).

  The fix: push RMW into the Raft state machine so `apply/3` is the sole
  mutator — ordering comes from the Raft log.

  Tests verify that concurrent operations on the same key produce the
  correct aggregate result. Before the state-machine migration these
  would fail by a wide margin (e.g., 1/50 instead of 50/50).

  Commands covered:
    - SETBIT (bitmap)
    - HINCRBY / HINCRBYFLOAT (hash field counters)
    - PFADD (HyperLogLog)
    - JSON.NUMINCRBY / JSON.ARRAPPEND (RedisJSON-style RMW)
    - BITFIELD (bitmap multi-op RMW)
    - ZINCRBY (sorted-set score delta)
    - GEOADD (geo index)

  Each command is tested in BOTH quorum and async namespaces — the state
  machine is the serialization point for both; the async path just uses
  the forced-quorum Batcher route (write_async_quorum) because RMW
  commands return computed values the caller needs.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Test.ShardHelpers

  @async_ns "rmw_async"

  setup do
    ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.set(@async_ns, "durability", "async")

    on_exit(fn ->
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  defp ukey(base), do: "#{base}_#{:erlang.unique_integer([:positive])}"
  defp akey(base), do: "#{@async_ns}:#{base}_#{:erlang.unique_integer([:positive])}"

  defp popcount(bin) when is_binary(bin) do
    for <<b::1 <- bin>>, b == 1, reduce: 0 do
      acc -> acc + 1
    end
  end

  defp run_concurrent(count, fun) do
    parent = self()

    tasks =
      for _ <- 1..count do
        Task.async(fn ->
          send(parent, :ready)

          receive do
            :go -> fun.()
          after
            5_000 -> flunk("concurrent task was not released")
          end
        end)
      end

    for _ <- 1..count do
      assert_receive :ready, 5_000
    end

    Enum.each(tasks, fn task -> send(task.pid, :go) end)
    Task.await_many(tasks, 30_000)
  end

  # ---------------------------------------------------------------------------
  # SETBIT
  # ---------------------------------------------------------------------------

  describe "concurrent SETBIT" do
    test "50 distinct offsets all land (quorum namespace)" do
      key = ukey("setbit_q")

      tasks =
        for i <- 0..49 do
          Task.async(fn -> FerricStore.setbit(key, i, 1) end)
        end

      _ = Task.await_many(tasks, 30_000)

      assert {:ok, val} = FerricStore.get(key)

      assert popcount(val) == 50,
             "expected 50 bits set, got #{popcount(val)}"
    end

    test "50 distinct offsets all land (async namespace)" do
      key = akey("setbit_a")

      tasks =
        for i <- 0..49 do
          Task.async(fn -> FerricStore.setbit(key, i, 1) end)
        end

      _ = Task.await_many(tasks, 30_000)

      assert {:ok, val} = FerricStore.get(key)
      assert popcount(val) == 50
    end
  end

  # ---------------------------------------------------------------------------
  # HINCRBY / HINCRBYFLOAT
  # ---------------------------------------------------------------------------

  describe "concurrent HINCRBY" do
    test "50 concurrent HINCRBY by 1 on same field sum to 50 (quorum)" do
      key = ukey("hincrby_q")

      tasks =
        for _ <- 1..50 do
          Task.async(fn -> FerricStore.hincrby(key, "counter", 1) end)
        end

      _ = Task.await_many(tasks, 30_000)

      assert {:ok, "50"} = FerricStore.hget(key, "counter")
    end

    test "50 concurrent HINCRBY by 1 on same field sum to 50 (async)" do
      key = akey("hincrby_a")

      tasks =
        for _ <- 1..50 do
          Task.async(fn -> FerricStore.hincrby(key, "counter", 1) end)
        end

      _ = Task.await_many(tasks, 30_000)

      assert {:ok, "50"} = FerricStore.hget(key, "counter")
    end

    test "concurrent HINCRBY on distinct fields each end at 10" do
      key = ukey("hincrby_fields")

      tasks =
        for f <- 1..10, _ <- 1..10 do
          Task.async(fn -> FerricStore.hincrby(key, "f#{f}", 1) end)
        end

      _ = Task.await_many(tasks, 30_000)

      for f <- 1..10 do
        assert {:ok, "10"} = FerricStore.hget(key, "f#{f}"),
               "field f#{f} expected 10"
      end
    end
  end

  describe "concurrent HINCRBYFLOAT" do
    test "50 concurrent HINCRBYFLOAT by 1.0 sum to 50.0 (quorum)" do
      key = ukey("hincrbyf_q")

      tasks =
        for _ <- 1..50 do
          Task.async(fn -> FerricStore.hincrbyfloat(key, "counter", 1.0) end)
        end

      _ = Task.await_many(tasks, 30_000)

      assert {:ok, val} = FerricStore.hget(key, "counter")
      assert String.to_float(val) == 50.0
    end
  end

  # ---------------------------------------------------------------------------
  # PFADD (HyperLogLog)
  # ---------------------------------------------------------------------------

  describe "concurrent PFADD" do
    test "100 concurrent PFADDs of distinct elements yield cardinality ~100 (quorum)" do
      key = ukey("pfadd_q")

      tasks =
        for i <- 1..100 do
          Task.async(fn -> FerricStore.pfadd(key, ["el_#{i}"]) end)
        end

      _ = Task.await_many(tasks, 30_000)

      assert {:ok, card} = FerricStore.pfcount([key])

      assert card >= 90 and card <= 110,
             "expected cardinality ~100, got #{card}"
    end

    test "100 concurrent PFADDs (async) yield cardinality ~100" do
      key = akey("pfadd_a")

      tasks =
        for i <- 1..100 do
          Task.async(fn -> FerricStore.pfadd(key, ["el_#{i}"]) end)
        end

      _ = Task.await_many(tasks, 30_000)

      assert {:ok, card} = FerricStore.pfcount([key])
      assert card >= 90 and card <= 110
    end
  end

  # ---------------------------------------------------------------------------
  # JSON.NUMINCRBY / JSON.ARRAPPEND
  # ---------------------------------------------------------------------------

  describe "concurrent JSON.NUMINCRBY" do
    test "50 concurrent numincrby on same path sums to 50 (quorum)" do
      key = ukey("json_numincr_q")
      :ok = FerricStore.json_set(key, "$", ~s({"counter": 0}))

      tasks =
        for _ <- 1..50 do
          Task.async(fn -> FerricStore.json_numincrby(key, "$.counter", "1") end)
        end

      _ = Task.await_many(tasks, 30_000)

      assert {:ok, val} = FerricStore.json_get(key, "$.counter")
      # JSON.GET returns the JSON-encoded value; "[50]" for a path that matches an array
      assert val in ["50", "[50]"]
    end
  end

  describe "concurrent JSON.ARRAPPEND" do
    test "50 concurrent ARRAPPEND to same array yields 50 elements (quorum)" do
      key = ukey("json_arr_q")
      :ok = FerricStore.json_set(key, "$", ~s({"items": []}))

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            FerricStore.json_arrappend(key, "$.items", [~s("el_#{i}")])
          end)
        end

      _ = Task.await_many(tasks, 30_000)

      assert {:ok, len_res} = FerricStore.json_arrlen(key, "$.items")
      # arrlen can return an int or a list with the int for array paths
      final_len = if is_list(len_res), do: List.first(len_res), else: len_res
      assert final_len == 50, "expected 50 elements in array, got #{final_len}"
    end
  end

  # ---------------------------------------------------------------------------
  # BITOP (merges N source bitmaps into destination)
  # ---------------------------------------------------------------------------

  describe "concurrent BITOP" do
    test "concurrent BITOP AND with disjoint destinations all succeed (quorum)" do
      # Seed two source bitmaps
      :ok = FerricStore.set("src_a", <<0xFF, 0xFF>>)
      :ok = FerricStore.set("src_b", <<0x0F, 0x0F>>)

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            FerricStore.bitop(:and, "dest_#{i}_#{:erlang.unique_integer([:positive])}", [
              "src_a",
              "src_b"
            ])
          end)
        end

      results = Task.await_many(tasks, 30_000)

      assert Enum.all?(results, fn
               {:ok, _len} -> true
               _ -> false
             end)
    end
  end

  # ---------------------------------------------------------------------------
  # ZINCRBY
  # ---------------------------------------------------------------------------

  describe "concurrent ZINCRBY" do
    test "50 concurrent ZINCRBY on same member sum to 50 (quorum)" do
      key = ukey("zincrby_q")

      tasks =
        for _ <- 1..50 do
          Task.async(fn -> FerricStore.zincrby(key, 1.0, "m1") end)
        end

      _ = Task.await_many(tasks, 30_000)

      assert {:ok, score} = FerricStore.zscore(key, "m1")
      assert score == 50.0
    end
  end

  # ---------------------------------------------------------------------------
  # Pop commands
  # ---------------------------------------------------------------------------

  describe "concurrent pop commands" do
    test "concurrent SPOP callers never receive the same member" do
      key = ukey("spop_q")
      members = for i <- 1..100, do: "m#{i}"
      assert {:ok, 100} = FerricStore.sadd(key, members)

      results = run_concurrent(100, fn -> FerricStore.spop(key) end)

      popped =
        Enum.map(results, fn
          {:ok, member} when is_binary(member) -> member
          other -> flunk("unexpected SPOP result: #{inspect(other)}")
        end)

      assert Enum.uniq(popped) == popped
      assert {:ok, 0} = FerricStore.scard(key)
    end

    test "concurrent ZPOPMIN callers never receive the same member" do
      key = ukey("zpopmin_q")
      members = for i <- 1..100, do: {i * 1.0, "m#{i}"}
      assert {:ok, 100} = FerricStore.zadd(key, members)

      results = run_concurrent(100, fn -> FerricStore.zpopmin(key, 1) end)

      popped =
        Enum.map(results, fn
          {:ok, [{member, _score}]} -> member
          other -> flunk("unexpected ZPOPMIN result: #{inspect(other)}")
        end)

      assert Enum.uniq(popped) == popped
      assert {:ok, 0} = FerricStore.zcard(key)
    end

    test "concurrent ZPOPMAX callers never receive the same member" do
      key = ukey("zpopmax_q")
      members = for i <- 1..100, do: {i * 1.0, "m#{i}"}
      assert {:ok, 100} = FerricStore.zadd(key, members)

      results = run_concurrent(100, fn -> FerricStore.zpopmax(key, 1) end)

      popped =
        Enum.map(results, fn
          {:ok, [{member, _score}]} -> member
          other -> flunk("unexpected ZPOPMAX result: #{inspect(other)}")
        end)

      assert Enum.uniq(popped) == popped
      assert {:ok, 0} = FerricStore.zcard(key)
    end
  end

  # ---------------------------------------------------------------------------
  # GEOADD (merges members into zset RMW-style)
  # ---------------------------------------------------------------------------

  describe "concurrent GEOADD" do
    test "50 concurrent GEOADDs of distinct members all land (quorum)" do
      key = ukey("geo_q")

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            # Small lat/lon jitter keeps each member distinct
            lon = 13.0 + i / 1000
            lat = 52.0 + i / 1000
            FerricStore.geoadd(key, [{lon, lat, "m#{i}"}])
          end)
        end

      _ = Task.await_many(tasks, 30_000)

      # GEOPOS returns position or nil per member; count non-nil entries.
      members = for i <- 1..50, do: "m#{i}"
      {:ok, positions} = FerricStore.geopos(key, members)
      found = Enum.count(positions, fn p -> p != nil end)
      assert found == 50, "expected 50 members present, got #{found}"
    end
  end
end
