defmodule Ferricstore.Cluster.RateLimitTest do
  @moduledoc """
  Distributed rate limit cluster tests from test plan Section 14.

  Verifies rate limiting semantics across multiple FerricStore peer nodes:

  - Rate limit counter works correctly per-node (Raft-durable)
  - Concurrent requests on a single node: at most `limit` succeed
  - Rate limit state survives (persisted through Raft WAL)

  ## Return Value Format

  `RATELIMIT.ADD` returns `[status, count, remaining, ms_until_reset]` where:
  - `status` is `"allowed"` or `"denied"`
  - `count` is the effective count after this request
  - `remaining` is how many more requests are allowed
  - `ms_until_reset` is milliseconds until the window resets

  ## Running

      mix test test/ferricstore/cluster/ --include cluster
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.ClusterHelper

  @moduletag :cluster

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    nodes = ClusterHelper.start_cluster(3)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

    %{nodes: nodes}
  end

  # Helper: execute a Router function on a remote peer node with ctx.
  # Uses two MFA-form :erpc calls to avoid sending anonymous functions
  # (which would fail with :undef on peer nodes that lack test module code).
  defp remote_router(node_name, fun, args) do
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default])
    :erpc.call(node_name, Ferricstore.Store.Router, fun, [ctx | args])
  end

  # ---------------------------------------------------------------------------
  # Section 14: Rate Limit Correctness Per Node
  # ---------------------------------------------------------------------------

  describe "rate limit consistency" do
    @tag :cluster
    test "rate limit enforced correctly on each node", %{nodes: nodes} do
      Enum.each(nodes, fn node ->
        key = "rl:pernode:#{node.index}"
        window = 60_000
        limit = 5

        # Send exactly `limit` requests -- all should be allowed
        for i <- 1..limit do
          result =
            remote_router(node.name, :ratelimit_add, [
              key,
              window,
              limit,
              1
            ])

          [status | _] = result

          assert status == "allowed",
                 "request #{i} of #{limit} on #{node.name} should be allowed, got #{inspect(result)}"
        end

        # Next request should be denied
        result =
          remote_router(node.name, :ratelimit_add, [
            key,
            window,
            limit,
            1
          ])

        [status | _] = result

        assert status == "denied",
               "request #{limit + 1} on #{node.name} should be denied, got #{inspect(result)}"
      end)
    end

    @tag :cluster
    test "rate limit returns correct remaining count", %{nodes: nodes} do
      [n1 | _] = nodes
      key = "rl:remaining:test"
      window = 60_000
      limit = 10

      # First request
      [status, count, remaining, _ttl] =
        remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])

      assert status == "allowed"
      assert count == 1
      assert remaining == 9

      # Fifth request
      for _ <- 2..5 do
        remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])
      end

      [status, count, remaining, _ttl] =
        remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])

      assert status == "allowed"
      assert count == 6
      assert remaining == 4
    end
  end

  # ---------------------------------------------------------------------------
  # Section 14: Concurrent Rate Limiting
  # ---------------------------------------------------------------------------

  describe "concurrent rate limiting on single node" do
    @tag :cluster
    test "concurrent requests respect the limit", %{nodes: nodes} do
      [n1 | _] = nodes
      key = "rl:concurrent:burst"
      window = 60_000
      limit = 20

      # 50 concurrent requests on the same node
      tasks =
        for _i <- 1..50 do
          Task.async(fn ->
            remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])
          end)
        end

      results = Task.await_many(tasks, 30_000)

      # Filter valid results (exclude any RPC errors)
      valid_results =
        Enum.filter(results, fn
          [_status | _] -> true
          _ -> false
        end)

      allowed_count =
        Enum.count(valid_results, fn [status | _] -> status == "allowed" end)

      denied_count =
        Enum.count(valid_results, fn [status | _] -> status == "denied" end)

      # The Raft-serialized counter ensures at most `limit` succeed
      assert allowed_count <= limit,
             "allowed count (#{allowed_count}) should not exceed limit (#{limit})"

      assert allowed_count + denied_count == length(valid_results),
             "all valid results should be either allowed or denied"

      # We sent 2.5x the limit, so some should be denied
      assert denied_count > 0, "some requests should be denied"
    end

    @tag :cluster
    test "concurrent requests spread across all nodes respect per-node limits", %{nodes: nodes} do
      window = 60_000
      limit = 10

      # Each node gets its own rate limit key and 25 concurrent requests
      tasks =
        Enum.flat_map(nodes, fn node ->
          key = "rl:spread:#{node.index}"

          for _i <- 1..25 do
            Task.async(fn ->
              result =
                remote_router(node.name, :ratelimit_add, [key, window, limit, 1])

              {node.index, result}
            end)
          end
        end)

      results = Task.await_many(tasks, 30_000)

      # Group results by node
      by_node = Enum.group_by(results, &elem(&1, 0), &elem(&1, 1))

      Enum.each(by_node, fn {node_idx, node_results} ->
        valid =
          Enum.filter(node_results, fn
            [_status | _] -> true
            _ -> false
          end)

        allowed = Enum.count(valid, fn [status | _] -> status == "allowed" end)

        assert allowed <= limit,
               "node #{node_idx}: allowed (#{allowed}) should not exceed limit (#{limit})"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Section 14: Rate Limit Durability
  # ---------------------------------------------------------------------------

  describe "rate limit state persistence" do
    @tag :cluster
    test "rate limit counter is durable through Raft on node", %{nodes: nodes} do
      [n1 | _] = nodes
      key = "rl:durable:counter"
      window = 60_000
      limit = 10

      # Use 8 of 10 allowed requests
      for _ <- 1..8 do
        [status | _] =
          remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])

        assert status == "allowed"
      end

      # Verify 2 more are allowed
      [status | _] =
        remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])

      assert status == "allowed"

      [status | _] =
        remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])

      assert status == "allowed"

      # 11th should be denied
      [status | _] =
        remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])

      assert status == "denied",
             "11th request should be denied (used 8 + 2 = 10, limit is 10)"
    end

    @tag :cluster
    test "different keys are independent across nodes", %{nodes: nodes} do
      [n1, n2 | _] = nodes
      window = 60_000

      # Exhaust rate limit on one key on n1
      for _ <- 1..5 do
        remote_router(n1.name, :ratelimit_add, [
          "rl:indep:api:user1",
          window,
          5,
          1
        ])
      end

      # That key is exhausted on n1
      [status | _] =
        remote_router(n1.name, :ratelimit_add, [
          "rl:indep:api:user1",
          window,
          5,
          1
        ])

      assert status == "denied"

      # Different key on n2 is independent
      [status2 | _] =
        remote_router(n2.name, :ratelimit_add, [
          "rl:indep:search:user1",
          window,
          5,
          1
        ])

      assert status2 == "allowed",
             "different key should be independent"
    end
  end

  # ---------------------------------------------------------------------------
  # Section 14: Rate Limit Counter Consistent Across Nodes
  #
  # Test plan: increment on different nodes, counter stays consistent.
  #
  # ClusterHelper.start_cluster/2 forms real multi-node Raft groups. A rate
  # limit key must therefore have one replicated counter, regardless of which
  # node receives the client request.
  # ---------------------------------------------------------------------------

  describe "rate limit counter consistency across nodes" do
    @tag :cluster
    test "increments through different nodes share one replicated counter", %{nodes: nodes} do
      [n1, n2, n3] = nodes
      key = "rl:cross:counter"
      window = 60_000
      limit = 10

      # Send 3 requests through n1
      for _ <- 1..3 do
        [status | _] =
          remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])

        assert status == "allowed"
      end

      # Check n1's effective count
      [_status, n1_count, _remaining, _ttl] =
        remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])

      assert n1_count == 4, "n1 should have count of 4 after 4 requests"

      # The same key is owned by one replicated Raft shard, even when the
      # client request enters through a different node.
      [status2, n2_count, _remaining2, _ttl2] =
        remote_router(n2.name, :ratelimit_add, [key, window, limit, 1])

      assert status2 == "allowed"

      assert n2_count == 5,
             "n2 should observe n1's replicated counter and advance it to 5 (got #{n2_count})"

      # Same for n3
      [status3, n3_count, _remaining3, _ttl3] =
        remote_router(n3.name, :ratelimit_add, [key, window, limit, 1])

      assert status3 == "allowed"
      assert n3_count == 6, "n3 should observe the replicated counter at 6"
    end

    @tag :cluster
    test "exhausting through one node denies requests through other nodes", %{
      nodes: nodes
    } do
      [n1, n2, _n3] = nodes
      key = "rl:cross:exhaust"
      window = 60_000
      limit = 5

      # Exhaust rate limit on n1
      for _ <- 1..5 do
        [status | _] =
          remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])

        assert status == "allowed"
      end

      # n1 is exhausted
      [denied_status | _] =
        remote_router(n1.name, :ratelimit_add, [key, window, limit, 1])

      assert denied_status == "denied", "n1 should be rate-limited"

      [n2_status | _] =
        remote_router(n2.name, :ratelimit_add, [key, window, limit, 1])

      assert n2_status == "denied",
             "n2 should observe the replicated exhausted counter"
    end

    @tag :cluster
    test "concurrent increments on different nodes stay within the global replicated limit", %{
      nodes: nodes
    } do
      window = 60_000
      limit = 15

      # Each node sends 30 concurrent requests for the same key. The global
      # Raft state machine should still allow only `limit` total.
      tasks =
        Enum.flat_map(nodes, fn node ->
          for _i <- 1..30 do
            Task.async(fn ->
              result =
                remote_router(node.name, :ratelimit_add, ["rl:cross:concurrent", window, limit, 1])

              {node.index, result}
            end)
          end
        end)

      results = Task.await_many(tasks, 30_000)

      replies = Enum.map(results, &elem(&1, 1))

      valid =
        Enum.filter(replies, fn
          [_status | _] -> true
          _ -> false
        end)

      allowed = Enum.count(valid, fn [status | _] -> status == "allowed" end)
      denied = Enum.count(valid, fn [status | _] -> status == "denied" end)

      assert length(valid) == length(replies),
             "all cluster responses should be rate-limit replies, got #{inspect(replies -- valid)}"

      assert allowed == limit,
             "globally allowed requests (#{allowed}) should equal limit (#{limit})"

      assert denied == length(replies) - limit,
             "all requests above the global limit should be denied"
    end
  end
end
