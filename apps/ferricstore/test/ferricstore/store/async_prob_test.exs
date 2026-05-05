defmodule Ferricstore.Store.AsyncProbTest do
  @moduledoc """
  Correctness tests for prob commands on async-configured namespaces.

  Per the design decision in docs/async-compound-list-prob-design.md
  (Group C expanded), probabilistic commands (bloom/cuckoo/cms/topk)
  always take the forced-quorum path regardless of namespace config:

  - They return computed values the caller needs (`{:ok, 0|1}` for add,
    `{:ok, [counts]}` for query, evicted items for topk_add). Async
    "fire and forget" semantics would lose these.
  - The NIFs do 14-15 file syscalls per call (pread/pwrite per bit),
    not a cheap mmap write. Raft overhead isn't the dominant cost.
  - Serialization for concurrent bit-write safety is handled by the
    state machine's mailbox — moving it to a latch + inline path
    requires care and has modest latency payoff.

  The regression guard in this file is that prob commands in an async
  namespace return the correct computed value (NOT `{:ok, :ok}`, which
  was the pre-fix bug where the Batcher replied :ok prematurely for any
  command on an async-durability slot).
  """
  use ExUnit.Case, async: false
  @moduletag skip: "async durability feature removed; quorum is the only supported durability"

  alias Ferricstore.Test.ShardHelpers

  @ns "prob_async"

  setup do
    ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.set(@ns, "durability", "async")

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set(@ns, "durability", "quorum")
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  defp ukey(base), do: "#{@ns}:#{base}_#{:erlang.unique_integer([:positive])}"

  # ---------------------------------------------------------------------------
  # Bloom filter — BF.ADD / BF.MADD / BF.EXISTS
  # ---------------------------------------------------------------------------

  describe "uncontended Bloom" do
    test "bf_add on nonexistent key auto-creates and adds" do
      key = ukey("bf_auto")
      assert {:ok, 1} = FerricStore.bf_add(key, "hello")
      assert {:ok, 1} = FerricStore.bf_exists(key, "hello")
      assert {:ok, 0} = FerricStore.bf_exists(key, "missing")
    end

    test "bf_madd adds multiple elements" do
      key = ukey("bf_madd")
      assert {:ok, [1, 1, 1]} = FerricStore.bf_madd(key, ["a", "b", "c"])
      assert {:ok, 1} = FerricStore.bf_exists(key, "b")
    end
  end

  # ---------------------------------------------------------------------------
  # Cuckoo filter — CF.ADD / CF.EXISTS / CF.DEL
  # ---------------------------------------------------------------------------

  describe "uncontended Cuckoo" do
    test "cf_add on nonexistent key auto-creates and adds" do
      key = ukey("cf_auto")
      assert {:ok, 1} = FerricStore.cf_add(key, "hello")
      assert {:ok, 1} = FerricStore.cf_exists(key, "hello")
      assert {:ok, 0} = FerricStore.cf_exists(key, "missing")
    end

    test "cf_addnx returns 0 on duplicate" do
      key = ukey("cf_nx")
      assert {:ok, 1} = FerricStore.cf_addnx(key, "x")
      assert {:ok, 0} = FerricStore.cf_addnx(key, "x")
    end

    test "cf_del removes element" do
      key = ukey("cf_del")
      {:ok, 1} = FerricStore.cf_add(key, "x")
      assert {:ok, 1} = FerricStore.cf_del(key, "x")
      assert {:ok, 0} = FerricStore.cf_exists(key, "x")
    end
  end

  # ---------------------------------------------------------------------------
  # Count-Min Sketch — CMS.INCRBY / CMS.QUERY
  # ---------------------------------------------------------------------------

  describe "uncontended CMS" do
    test "cms_incrby increments counters" do
      key = ukey("cms")
      # cms_initbydim returns :ok on success; don't pattern-match on tuple
      _ = FerricStore.cms_initbydim(key, 1000, 5)
      assert {:ok, [5, 3]} = FerricStore.cms_incrby(key, [{"apple", 5}, {"banana", 3}])
      assert {:ok, [5, 3]} = FerricStore.cms_query(key, ["apple", "banana"])
    end
  end

  # ---------------------------------------------------------------------------
  # TopK — TOPK.ADD / TOPK.QUERY
  # ---------------------------------------------------------------------------

  describe "uncontended TopK" do
    test "topk_add tracks items" do
      key = ukey("topk")
      _ = FerricStore.topk_reserve(key, 3)
      # topk_add returns list of evicted items wrapped in {:ok, ...}.
      assert {:ok, _evicted} = FerricStore.topk_add(key, ["a", "b", "c"])
    end
  end

  # ---------------------------------------------------------------------------
  # Path telemetry — uncontended prob_write takes the latch path
  # ---------------------------------------------------------------------------

  describe "result correctness in async namespace" do
    test "bf_add in async namespace returns {:ok, 1} (not {:ok, :ok})" do
      # Regression guard: prob commands under async namespace previously
      # returned `{:ok, :ok}` because the Batcher replied :ok prematurely
      # for async-namespace writes. Now they route through the forced-
      # quorum path so the computed result flows back.
      key = ukey("bf_result_shape")
      assert {:ok, 1} = FerricStore.bf_add(key, "hello")
      assert {:ok, 0} = FerricStore.bf_add(key, "hello")  # duplicate
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent adds — bloom accepts duplicates; no crashes
  # ---------------------------------------------------------------------------

  describe "concurrent bloom adds" do
    test "50 concurrent bf_adds with different elements all visible" do
      key = ukey("bf_concurrent")

      tasks =
        for i <- 1..50 do
          Task.async(fn -> FerricStore.bf_add(key, "el_#{i}") end)
        end

      results = Task.await_many(tasks, 30_000)
      # Each add returns {:ok, 0|1} (0=already present, 1=newly added).
      # Under bloom semantics, some may report 0 due to hash collisions.
      # Main requirement: no crashes, no errors.
      assert Enum.all?(results, fn
               {:ok, n} when n in [0, 1] -> true
               _ -> false
             end)

      # Every added element should be reported present.
      for i <- 1..50 do
        assert {:ok, 1} = FerricStore.bf_exists(key, "el_#{i}")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Namespace routing — quorum parent key still works
  # ---------------------------------------------------------------------------

  describe "namespace routing" do
    test "quorum-namespace prob key still works end-to-end" do
      quorum_key = "quorum_prob_#{:erlang.unique_integer([:positive])}"
      assert {:ok, 1} = FerricStore.bf_add(quorum_key, "x")
      assert {:ok, 1} = FerricStore.bf_exists(quorum_key, "x")
    end
  end
end
