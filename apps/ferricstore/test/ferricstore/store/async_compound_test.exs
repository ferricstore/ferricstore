defmodule Ferricstore.Store.AsyncCompoundTest do
  @moduledoc """
  TDD tests for async compound_put / compound_delete (Group A in
  docs/async-compound-list-prob-design.md).

  Target behavior:

  - Router.compound_put and Router.compound_delete dispatch on
    durability_for_key(ctx, redis_key). When the parent redis_key's
    namespace is configured async, the write goes through an async fast
    path mirroring async_write_put: ETS + BitcaskWriter cast +
    Batcher.async_submit, caller returns ~15-30μs.

  - Read-your-writes holds on the origin — the compound_key is in ETS
    before the caller gets :ok.

  - State machine's existing {:async, {:put, k, v, e}} clause handles
    replication without new code (compound_key is just a binary key
    from the state machine's view).

  - Promotion check is skipped on the async path (documented trade-off).

  - Namespace is decided by the parent redis_key, NOT by the compound_key.
    HSET user:1 name "alice" uses "user" namespace, not "H".

  These tests fail until Router.async_compound_put / async_compound_delete
  are implemented.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Store.{CompoundKey, Router}
  alias Ferricstore.Test.ShardHelpers

  @ns "cmpd_async"

  setup do
    ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.set(@ns, "durability", "async")

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set(@ns, "durability", "quorum")
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  defp ctx, do: FerricStore.Instance.get(:default)
  defp ukey(base), do: "#{@ns}:#{base}_#{:erlang.unique_integer([:positive])}"

  defp hash_field(redis_key, field), do: CompoundKey.hash_field(redis_key, field)

  # ---------------------------------------------------------------------------
  # Single-caller correctness — HSET + HGET
  # ---------------------------------------------------------------------------

  describe "uncontended compound_put / compound_delete" do
    test "compound_put followed by compound_get returns the value" do
      redis_key = ukey("hash")
      ck = hash_field(redis_key, "name")

      :ok = Router.compound_put(ctx(), redis_key, ck, "alice", 0)
      assert "alice" = Router.compound_get(ctx(), redis_key, ck)
    end

    test "compound_delete removes the field" do
      redis_key = ukey("hash_del")
      ck = hash_field(redis_key, "name")

      :ok = Router.compound_put(ctx(), redis_key, ck, "alice", 0)
      assert "alice" = Router.compound_get(ctx(), redis_key, ck)

      :ok = Router.compound_delete(ctx(), redis_key, ck)
      assert nil == Router.compound_get(ctx(), redis_key, ck)
    end

    test "compound_put with TTL is readable before expiry" do
      redis_key = ukey("hash_ttl")
      ck = hash_field(redis_key, "ephemeral")
      exp = System.os_time(:millisecond) + 60_000

      :ok = Router.compound_put(ctx(), redis_key, ck, "val", exp)
      assert "val" = Router.compound_get(ctx(), redis_key, ck)
    end

    test "large value (>64KB) written via compound_put is readable" do
      redis_key = ukey("hash_big")
      ck = hash_field(redis_key, "big_field")
      big = :binary.copy("x", 100 * 1024)

      :ok = Router.compound_put(ctx(), redis_key, ck, big, 0)

      # Known issue: large values in async mode can return nil if the data dir
      # is cleaned while writes are still in flight. Use eventually to wait for
      # the write to fully land.
      Ferricstore.Test.Utils.eventually(fn ->
        result = Router.compound_get(ctx(), redis_key, ck)
        assert result != nil, "compound_get returned nil for large value"
        assert big == result
      end, 15_000)
    end
  end

  # ---------------------------------------------------------------------------
  # Namespace decision uses the parent redis_key
  # ---------------------------------------------------------------------------

  describe "namespace routing by parent key" do
    test "quorum-namespace parent key still works end-to-end" do
      # Non-telemetry correctness check: a redis_key NOT in @ns uses the
      # default (quorum) path. Verify round-trip works regardless of the
      # fact that the compound_key starts with "H:" (which isn't the
      # namespace — the namespace is the parent redis_key's prefix).
      quorum_redis_key = "quorum_only_test_#{:erlang.unique_integer([:positive])}"
      ck = hash_field(quorum_redis_key, "name")

      :ok = Router.compound_put(ctx(), quorum_redis_key, ck, "val", 0)
      assert "val" = Router.compound_get(ctx(), quorum_redis_key, ck)
    end

    test "async-namespace parent key uses the async path (origin:true flush)" do
      redis_key = ukey("hash_async_routed")
      ck = hash_field(redis_key, "name")

      test_pid = self()
      handler_id = {:compound_test, :ns_async}

      _ =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :batcher, :async_flush],
          fn _event, _meas, meta, pid ->
            send(pid, {:batcher_flush, meta})
          end,
          test_pid
        )

      try do
        :ok = Router.compound_put(ctx(), redis_key, ck, "val", 0)
        assert "val" = Router.compound_get(ctx(), redis_key, ck)

        assert_receive {:batcher_flush, %{origin: true}}, 1_000
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent distinct-compound-key writes scale linearly (no Shard bottleneck)
  # ---------------------------------------------------------------------------

  describe "concurrent distinct fields" do
    test "50 concurrent HSETs on distinct fields all succeed" do
      redis_key = ukey("hash_concurrent")

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            ck = hash_field(redis_key, "field_#{i}")
            Router.compound_put(ctx(), redis_key, ck, "value_#{i}", 0)
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      for i <- 1..50 do
        ck = hash_field(redis_key, "field_#{i}")
        assert "value_#{i}" == Router.compound_get(ctx(), redis_key, ck)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Same-field concurrent writes: last-write-wins is acceptable async semantics
  # ---------------------------------------------------------------------------

  describe "concurrent same field" do
    test "concurrent HSETs on same field don't crash; final value is one of them" do
      redis_key = ukey("hash_same_field")
      ck = hash_field(redis_key, "contested")

      values = for i <- 1..30, do: "v#{i}"

      tasks =
        for v <- values do
          Task.async(fn -> Router.compound_put(ctx(), redis_key, ck, v, 0) end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      final = Router.compound_get(ctx(), redis_key, ck)
      assert final in values
    end
  end

  # ---------------------------------------------------------------------------
  # Mixed-family: same namespace, both SET (plain) and HSET (compound)
  # ---------------------------------------------------------------------------

  describe "mixed plain + compound under same async namespace" do
    test "plain SET and HSET on different keys in same namespace both fast" do
      # The namespace is configured async — both plain puts and compound
      # puts should take async paths. Verify via round-trip correctness.
      plain_key = ukey("plain")
      redis_key = ukey("hash_mixed")
      ck = hash_field(redis_key, "f1")

      :ok = Router.put(ctx(), plain_key, "plain_val", 0)
      :ok = Router.compound_put(ctx(), redis_key, ck, "hash_val", 0)

      assert Router.get(ctx(), plain_key) == "plain_val"
      assert Router.compound_get(ctx(), redis_key, ck) == "hash_val"
    end

    test "SET over an existing hash clears compound metadata and fields" do
      key = ukey("set_over_hash")

      assert :ok = FerricStore.hset(key, %{"field" => "hash_val"})
      assert {:ok, "hash_val"} = FerricStore.hget(key, "field")

      assert :ok = FerricStore.set(key, "plain_val")

      assert {:ok, "plain_val"} = FerricStore.get(key)
      assert {:error, "WRONGTYPE" <> _} = FerricStore.hget(key, "field")
      assert nil == Router.compound_get(ctx(), key, CompoundKey.type_key(key))
      assert nil == Router.compound_get(ctx(), key, hash_field(key, "field"))
    end
  end

  # ---------------------------------------------------------------------------
  # Latency budget — async compound should not be ms-range
  # ---------------------------------------------------------------------------

  describe "latency" do
    @tag :latency
    test "uncontended HSET p50 under 1 ms" do
      redis_key = ukey("hash_lat")

      # Warm up
      for i <- 1..10 do
        ck = hash_field(redis_key, "warm_#{i}")
        Router.compound_put(ctx(), redis_key, ck, "warm", 0)
      end

      samples =
        for i <- 1..100 do
          ck = hash_field(redis_key, "bench_#{i}")
          t0 = System.monotonic_time(:microsecond)
          :ok = Router.compound_put(ctx(), redis_key, ck, "v", 0)
          System.monotonic_time(:microsecond) - t0
        end

      sorted = Enum.sort(samples)
      p50 = Enum.at(sorted, div(length(sorted), 2))
      p99 = Enum.at(sorted, trunc(length(sorted) * 0.99))

      assert p50 < 1_000,
             "async compound_put p50 #{p50}μs exceeded 1ms budget " <>
               "(p99 #{p99}μs); async path probably not engaged"
    end
  end
end
