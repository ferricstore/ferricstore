defmodule Ferricstore.PipelineBatchTest do
  @moduledoc """
  Tests that FerricStore.pipeline/1 submits commands as a single batch Raft
  entry per shard via the Coordinator, rather than executing them one at a time.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  describe "basic pipeline batching" do
    test "pipeline with 3 SETs returns [:ok, :ok, :ok]" do
      assert {:ok, [:ok, :ok, :ok]} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("pb:a", "1")
                 |> FerricStore.Pipe.set("pb:b", "2")
                 |> FerricStore.Pipe.set("pb:c", "3")
               end)

      assert {:ok, "1"} = FerricStore.get("pb:a")
      assert {:ok, "2"} = FerricStore.get("pb:b")
      assert {:ok, "3"} = FerricStore.get("pb:c")
    end

    test "pipeline with SET then GET returns [:ok, value]" do
      assert {:ok, [:ok, {:ok, "hello"}]} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("pb:sg", "hello")
                 |> FerricStore.Pipe.get("pb:sg")
               end)
    end

    test "pipeline with INCR returns correct increment values" do
      assert {:ok, [{:ok, 1}, {:ok, 2}, {:ok, 3}]} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.incr("pb:cnt")
                 |> FerricStore.Pipe.incr("pb:cnt")
                 |> FerricStore.Pipe.incr("pb:cnt")
               end)
    end

    test "pipeline with DEL returns correct delete counts" do
      FerricStore.set("pb:del1", "val")

      assert {:ok, [:ok, {:ok, nil}]} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.del("pb:del1")
                 |> FerricStore.Pipe.get("pb:del1")
               end)
    end

    test "pipeline with mixed operations (SET, GET, INCR, DEL)" do
      FerricStore.set("pb:mix_existing", "old")

      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("pb:mix_new", "fresh")
                 |> FerricStore.Pipe.get("pb:mix_existing")
                 |> FerricStore.Pipe.incr("pb:mix_counter")
                 |> FerricStore.Pipe.del("pb:mix_existing")
               end)

      assert [:ok, {:ok, "old"}, {:ok, 1}, :ok] = results
    end
  end

  describe "batching verification" do
    test "pipeline with multiple writes to same shard — all visible atomically" do
      # Use hash tags to force same shard
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("{pb:shard}:k1", "v1")
                 |> FerricStore.Pipe.set("{pb:shard}:k2", "v2")
                 |> FerricStore.Pipe.set("{pb:shard}:k3", "v3")
                 |> FerricStore.Pipe.get("{pb:shard}:k1")
                 |> FerricStore.Pipe.get("{pb:shard}:k2")
                 |> FerricStore.Pipe.get("{pb:shard}:k3")
               end)

      assert [:ok, :ok, :ok, {:ok, "v1"}, {:ok, "v2"}, {:ok, "v3"}] = results
    end

    test "pipeline with writes across shards still works" do
      # These keys likely hash to different shards (no hash tag)
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("pb:cross_alpha", "a")
                 |> FerricStore.Pipe.set("pb:cross_beta", "b")
                 |> FerricStore.Pipe.set("pb:cross_gamma", "c")
               end)

      assert [:ok, :ok, :ok] = results
      assert {:ok, "a"} = FerricStore.get("pb:cross_alpha")
      assert {:ok, "b"} = FerricStore.get("pb:cross_beta")
      assert {:ok, "c"} = FerricStore.get("pb:cross_gamma")
    end
  end

  describe "edge cases" do
    test "empty pipeline returns []" do
      assert {:ok, []} =
               FerricStore.pipeline(fn pipe ->
                 pipe
               end)
    end

    test "pipeline with single command works" do
      assert {:ok, [:ok]} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("pb:single", "only")
               end)

      assert {:ok, "only"} = FerricStore.get("pb:single")
    end

    test "pipeline with hash operations (HSET, HGET)" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.hset("pb:hash", %{"f1" => "v1", "f2" => "v2"})
                 |> FerricStore.Pipe.hget("pb:hash", "f1")
                 |> FerricStore.Pipe.hget("pb:hash", "f2")
               end)

      assert [:ok, {:ok, "v1"}, {:ok, "v2"}] = results
    end

    test "pipeline with list operations (LPUSH, RPUSH)" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.lpush("pb:list", ["a", "b"])
                 |> FerricStore.Pipe.rpush("pb:list", ["c", "d"])
               end)

      assert [{:ok, 2}, {:ok, 4}] = results
    end

    test "pipeline with set operations (SADD)" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.sadd("pb:set", ["m1", "m2", "m3"])
               end)

      assert [{:ok, 3}] = results
    end

    test "pipeline with sorted set operations (ZADD)" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.zadd("pb:zset", [{1.0, "a"}, {2.0, "b"}])
               end)

      assert [{:ok, 2}] = results
    end

    test "pipeline with INCR_BY" do
      assert {:ok, [{:ok, 10}, {:ok, 25}]} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.incr_by("pb:incrby", 10)
                 |> FerricStore.Pipe.incr_by("pb:incrby", 15)
               end)
    end

    test "pipeline with EXPIRE" do
      FerricStore.set("pb:expiry", "val")

      assert {:ok, [{:ok, true}]} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.expire("pb:expiry", 60_000)
               end)
    end

    test "pipeline with SET with TTL option" do
      assert {:ok, [:ok]} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("pb:ttl", "value", ttl: :timer.hours(1))
               end)

      assert {:ok, "value"} = FerricStore.get("pb:ttl")
    end

    test "all-get fast path returns WRONGTYPE for compound keys" do
      assert :ok = FerricStore.hset("pb:get:hash", %{"field" => "value"})
      assert :ok = FerricStore.set("pb:get:string", "value")

      assert {:ok, [{:error, "WRONGTYPE" <> _}, {:ok, "value"}]} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.get("pb:get:hash")
                 |> FerricStore.Pipe.get("pb:get:string")
               end)
    end

    test "all-set fast path replaces compound key with string" do
      assert :ok = FerricStore.hset("pb:set:hash", %{"field" => "old"})

      assert {:ok, [:ok]} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("pb:set:hash", "string")
               end)

      assert {:ok, "string"} = FerricStore.get("pb:set:hash")
      assert {:ok, "string"} = FerricStore.type("pb:set:hash")
      assert {:error, "WRONGTYPE" <> _} = FerricStore.hget("pb:set:hash", "field")
    end

    test "pipeline commands execute in order" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("pb:order", "first")
                 |> FerricStore.Pipe.get("pb:order")
                 |> FerricStore.Pipe.set("pb:order", "second")
                 |> FerricStore.Pipe.get("pb:order")
               end)

      assert [:ok, {:ok, "first"}, :ok, {:ok, "second"}] = results
    end

    test "mixed fast path preserves GET before SET order on same key" do
      assert :ok = FerricStore.set("pb:get_before_set", "old")

      assert {:ok, [{:ok, "old"}, :ok]} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.get("pb:get_before_set")
                 |> FerricStore.Pipe.set("pb:get_before_set", "new")
               end)

      assert {:ok, "new"} = FerricStore.get("pb:get_before_set")
    end
  end

  describe "performance" do
    test "pipeline of 100 SETs is faster than 100 individual SETs (at least 2x)" do
      n = 100

      # Warm up
      FerricStore.set("pb:warmup", "val")

      # Measure individual SETs
      individual_us =
        :timer.tc(fn ->
          for i <- 1..n do
            FerricStore.set("pb:ind:#{i}", "value_#{i}")
          end
        end)
        |> elem(0)

      # Measure pipeline SETs
      pipeline_us =
        :timer.tc(fn ->
          FerricStore.pipeline(fn pipe ->
            Enum.reduce(1..n, pipe, fn i, acc ->
              FerricStore.Pipe.set(acc, "pb:pipe:#{i}", "value_#{i}")
            end)
          end)
        end)
        |> elem(0)

      # Pipeline should be at least 2x faster
      assert pipeline_us < individual_us,
             "pipeline (#{pipeline_us}us) should be faster than individual (#{individual_us}us)"

      speedup = individual_us / max(pipeline_us, 1)

      assert speedup >= 2.0,
             "pipeline speedup #{Float.round(speedup, 1)}x should be >= 2x " <>
               "(individual: #{individual_us}us, pipeline: #{pipeline_us}us)"

      # Verify all values written correctly
      for i <- 1..n do
        expected = "value_#{i}"
        assert {:ok, ^expected} = FerricStore.get("pb:pipe:#{i}")
      end
    end
  end
end
