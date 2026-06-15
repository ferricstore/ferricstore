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
    - BITFIELD (bitmap multi-op RMW)
    - ZINCRBY (sorted-set score delta)
    - GEOADD (geo index)

  Some tests use a distinct key prefix to prove the same serialized quorum
  path works for prefixed keys. The removed namespace durability switch is not
  part of this coverage.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers

  @prefixed_ns "rmw_prefixed"

  setup do
    ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.reset(@prefixed_ns)

    on_exit(fn ->
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  defp ukey(base), do: "#{base}_#{:erlang.unique_integer([:positive])}"
  defp pkey(base), do: "#{@prefixed_ns}:#{base}_#{:erlang.unique_integer([:positive])}"

  defp popcount(bin) when is_binary(bin) do
    for <<b::1 <- bin>>, b == 1, reduce: 0 do
      acc -> acc + 1
    end
  end

  defp run_concurrent(count, fun) do
    run_concurrent(count, fn _item -> fun.() end, List.duplicate(:ok, count))
  end

  defp run_concurrent(count, fun, items) do
    parent = self()

    tasks =
      for item <- items do
        Task.async(fn ->
          send(parent, :ready)

          receive do
            :go -> fun.(item)
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

    test "50 distinct offsets all land (prefixed key)" do
      key = pkey("setbit_p")

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

    test "50 concurrent HINCRBY by 1 on same field sum to 50 (prefixed key)" do
      key = pkey("hincrby_p")

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

    test "100 concurrent PFADDs (prefixed key) yield cardinality ~100" do
      key = pkey("pfadd_p")

      tasks =
        for i <- 1..100 do
          Task.async(fn -> FerricStore.pfadd(key, ["el_#{i}"]) end)
        end

      _ = Task.await_many(tasks, 30_000)

      assert {:ok, card} = FerricStore.pfcount([key])
      assert card >= 90 and card <= 110
    end
  end

  describe "concurrent PFMERGE" do
    test "concurrent merges into the same destination preserve all source sketches" do
      dest = ukey("pfmerge_dest")

      source_keys =
        for source_idx <- 1..20 do
          source = ukey("pfmerge_src_#{source_idx}")
          elements = for elem_idx <- 1..100, do: "s#{source_idx}:#{elem_idx}"
          assert {:ok, true} = FerricStore.pfadd(source, elements)
          source
        end

      results =
        run_concurrent(
          length(source_keys),
          fn source ->
            FerricStore.pfmerge(dest, [source])
          end,
          source_keys
        )

      assert Enum.all?(results, &(&1 == :ok))
      assert {:ok, card} = FerricStore.pfcount([dest])
      assert card >= 1_800 and card <= 2_200, "expected cardinality ~2000, got #{card}"
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
