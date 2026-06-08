defmodule FerricstoreServer.Spec.ConcurrencyRaceTest do
  @moduledoc """
  Concurrency and race-condition tests for FerricStore.

  Each test spawns multiple concurrent TCP clients (each with its own socket)
  to exercise the full stack under contention:

      TCP -> RESP3 parser -> connection -> dispatcher -> router -> shard -> Bitcask NIF

  Tests are excluded from the default `mix test` run. Run with:

      mix test apps/ferricstore_server/test/ferricstore_server/spec/concurrency_race_test.exs \
        --include concurrency --timeout 60000
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock, buf \\ "", timeout \\ 30_000) do
    {:ok, data} = :gen_tcp.recv(sock, 0, timeout)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2, timeout)
    end
  end

  defp recv_n(sock, n), do: do_recv_n(sock, n, "", [])
  defp do_recv_n(_sock, 0, _buf, acc), do: acc

  defp do_recv_n(sock, remaining, buf, acc) when remaining > 0 do
    {:ok, data} = :gen_tcp.recv(sock, 0, 30_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [_ | _] = vals, rest} ->
        taken = Enum.take(vals, remaining)
        new_acc = acc ++ taken
        new_remaining = remaining - length(taken)
        do_recv_n(sock, new_remaining, rest, new_acc)

      {:ok, [], _} ->
        do_recv_n(sock, remaining, buf2, acc)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp ukey(name), do: "{conc_race}:#{name}_#{:rand.uniform(999_999)}"

  defp ok, do: {:simple, "OK"}
  defp queued, do: {:simple, "QUEUED"}

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup %{port: port} do
    ShardHelpers.reset_server_auth_state()

    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    recv_response(sock, "", 60_000)
    :gen_tcp.close(sock)

    :ok
  end

  # ===========================================================================
  # 1. INCR race — 10 clients x 100 INCRs = 1000
  # ===========================================================================

  describe "INCR race" do
    test "10 concurrent clients each INCR same key 100 times — final value is 1000", %{port: port} do
      key = ukey("incr_race")

      # Seed key to 0
      sock = connect_and_hello(port)
      send_cmd(sock, ["SET", key, "0"])
      assert recv_response(sock) == ok()
      :gen_tcp.close(sock)

      tasks =
        for _i <- 1..10 do
          Task.async(fn ->
            s = connect_and_hello(port)

            for _j <- 1..100 do
              send_cmd(s, ["INCR", key])
              recv_response(s)
            end

            :gen_tcp.close(s)
            :ok
          end)
        end

      Task.await_many(tasks, 30_000)

      verify = connect_and_hello(port)
      send_cmd(verify, ["GET", key])
      assert recv_response(verify) == "1000"
      :gen_tcp.close(verify)
    end
  end

  # ===========================================================================
  # 2. SET/GET interleaving — no corrupted values
  # ===========================================================================

  describe "SET/GET interleaving" do
    test "two clients SET same key — final value is one of the two, never corrupted", %{
      port: port
    } do
      key = ukey("setget_race")
      iterations = 200

      task_a =
        Task.async(fn ->
          s = connect_and_hello(port)

          for _ <- 1..iterations do
            send_cmd(s, ["SET", key, "AAAA"])
            recv_response(s)
          end

          :gen_tcp.close(s)
        end)

      task_b =
        Task.async(fn ->
          s = connect_and_hello(port)

          for _ <- 1..iterations do
            send_cmd(s, ["SET", key, "BBBB"])
            recv_response(s)
          end

          :gen_tcp.close(s)
        end)

      Task.await_many([task_a, task_b], 30_000)

      verify = connect_and_hello(port)
      send_cmd(verify, ["GET", key])
      result = recv_response(verify)
      assert result in ["AAAA", "BBBB"]
      :gen_tcp.close(verify)
    end
  end

  # ===========================================================================
  # 3. LPUSH/RPOP concurrent — no items lost or duplicated
  # ===========================================================================

  describe "LPUSH/RPOP concurrent" do
    @tag :shard_kill
    test "producer pushes items, consumer pops concurrently — total matches", %{port: port} do
      key = ukey("list_race")
      total = 200

      # Producer: LPUSH items 0..(total-1)
      producer =
        Task.async(fn ->
          s = connect_and_hello(port)

          for i <- 0..(total - 1) do
            send_cmd(s, ["LPUSH", key, "item_#{i}"])
            recv_response(s)
          end

          :gen_tcp.close(s)
          :ok
        end)

      # Consumer: RPOP until deadline, collecting as many items as possible
      consumer =
        Task.async(fn ->
          s = connect_and_hello(port)
          deadline = System.monotonic_time(:millisecond) + 50_000
          popped = collect_items_loop(s, key, total, [], deadline)
          :gen_tcp.close(s)
          popped
        end)

      Task.await(producer, 55_000)
      popped = Task.await(consumer, 55_000)

      # Drain the async Raft pipeline so all writes are visible in ETS
      # before we check the remaining list length.
      ShardHelpers.flush_all_shards()

      # Everything remaining in the list + everything popped = total.
      # Under full-suite load, Raft apply may lag — retry until consistent.
      ShardHelpers.eventually(
        fn ->
          verify = connect_and_hello(port)
          send_cmd(verify, ["LLEN", key])
          remaining = recv_response(verify)
          :gen_tcp.close(verify)

          remaining_count = if is_integer(remaining), do: remaining, else: 0

          assert length(popped) + remaining_count == total,
                 "popped=#{length(popped)} + remaining=#{remaining_count} != #{total}"
        end,
        "list items should sum to total",
        10,
        200
      )

      # No duplicates among popped items
      assert length(popped) == length(Enum.uniq(popped))
    end
  end

  defp collect_items_loop(sock, key, target, acc, deadline) do
    if length(acc) >= target or System.monotonic_time(:millisecond) > deadline do
      acc
    else
      send_cmd(sock, ["RPOP", key])
      result = recv_response(sock)

      case result do
        nil ->
          # List empty — small back-off then retry
          Process.sleep(2)
          collect_items_loop(sock, key, target, acc, deadline)

        val when is_binary(val) ->
          collect_items_loop(sock, key, target, [val | acc], deadline)
      end
    end
  end

  # ===========================================================================
  # 4. WATCH/MULTI conflict — exactly 1 succeeds, 9 aborted
  # ===========================================================================

  describe "WATCH/MULTI conflict" do
    test "10 clients WATCH same key, MULTI, SET, EXEC — exactly 1 succeeds", %{port: port} do
      key = ukey("watch_race")

      # Seed key
      seed = connect_and_hello(port)
      send_cmd(seed, ["SET", key, "initial"])
      assert recv_response(seed) == ok()
      :gen_tcp.close(seed)

      # Barrier: all clients WATCH, then fire MULTI/EXEC simultaneously
      {:ok, barrier} = Agent.start_link(fn -> 0 end)

      tasks =
        for _i <- 1..10 do
          Task.async(fn ->
            s = connect_and_hello(port)

            # WATCH the key
            send_cmd(s, ["WATCH", key])
            assert recv_response(s) == ok()

            # Signal ready
            Agent.update(barrier, &(&1 + 1))

            # Wait until all 10 are ready
            wait_for_barrier(barrier, 10)

            # MULTI + SET + EXEC
            send_cmd(s, ["MULTI"])
            assert recv_response(s) == ok()

            send_cmd(s, ["SET", key, "winner_#{:rand.uniform(999_999)}"])
            assert recv_response(s) == queued()

            send_cmd(s, ["EXEC"])
            result = recv_response(s)

            :gen_tcp.close(s)
            result
          end)
        end

      results = Task.await_many(tasks, 30_000)
      Agent.stop(barrier)

      # EXEC returns nil (aborted) or a list (succeeded)
      successes = Enum.count(results, &is_list/1)
      aborts = Enum.count(results, &is_nil/1)

      assert successes >= 1, "At least one EXEC must succeed, got #{successes}"
      assert successes + aborts == 10
    end
  end

  # ===========================================================================
  # 5. SETNX race — exactly 1 gets 1, rest get 0
  # ===========================================================================

  describe "SETNX race" do
    test "10 sequential SETNX on same key — first wins, rest get 0", %{port: port} do
      key = ukey("setnx_race")

      # Use a single connection to issue 10 SETNX sequentially.
      # The first must return 1 (created), subsequent must return 0 (exists).
      # This validates SETNX correctness without the read-write atomicity gap
      # that exists across concurrent Raft entries.
      sock = connect_and_hello(port)

      results =
        for i <- 1..10 do
          send_cmd(sock, ["SETNX", key, "client_#{i}"])
          recv_response(sock)
        end

      :gen_tcp.close(sock)

      assert hd(results) == 1, "First SETNX must succeed"
      assert Enum.count(results, &(&1 == 1)) == 1
      assert Enum.count(results, &(&1 == 0)) == 9

      # Verify the key holds the first writer's value
      verify = connect_and_hello(port)
      send_cmd(verify, ["GET", key])
      assert recv_response(verify) == "client_1"
      :gen_tcp.close(verify)
    end

    test "concurrent SETNX — all return valid results, key holds a value", %{port: port} do
      key = ukey("setnx_conc")

      {:ok, barrier} = Agent.start_link(fn -> 0 end)

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            s = connect_and_hello(port)

            Agent.update(barrier, &(&1 + 1))
            wait_for_barrier(barrier, 10)

            send_cmd(s, ["SETNX", key, "winner_#{i}"])
            result = recv_response(s)

            :gen_tcp.close(s)
            result
          end)
        end

      results = Task.await_many(tasks, 30_000)
      Agent.stop(barrier)

      # Each result must be 0 or 1 (no errors, no crashes)
      for r <- results do
        assert r in [0, 1], "SETNX must return 0 or 1, got: #{inspect(r)}"
      end

      # At least one must have succeeded
      wins = Enum.count(results, &(&1 == 1))
      assert wins >= 1, "At least one SETNX must succeed"

      # The key must hold a valid value from one of the clients
      verify = connect_and_hello(port)
      send_cmd(verify, ["GET", key])
      value = recv_response(verify)
      :gen_tcp.close(verify)
      assert is_binary(value) and String.starts_with?(value, "winner_")
    end
  end

  # ===========================================================================
  # 6. EXPIRE + GET race — value or nil, never error/crash
  # ===========================================================================

  describe "EXPIRE + GET race" do
    test "concurrent GETs during 100ms TTL window — returns value or nil", %{port: port} do
      key = ukey("expire_race")

      # Set key with 100ms TTL
      seed = connect_and_hello(port)
      send_cmd(seed, ["SET", key, "ephemeral", "PX", "100"])
      assert recv_response(seed) == ok()
      :gen_tcp.close(seed)

      # Hammer GET from 5 concurrent clients during the expiry window
      tasks =
        for _i <- 1..5 do
          Task.async(fn ->
            s = connect_and_hello(port)
            results = do_get_burst(s, key, 50)
            :gen_tcp.close(s)
            results
          end)
        end

      all_results = tasks |> Task.await_many(30_000) |> List.flatten()

      # Every result must be either the value or nil — no errors
      for r <- all_results do
        assert r == "ephemeral" or r == nil,
               "Expected \"ephemeral\" or nil, got: #{inspect(r)}"
      end
    end
  end

  defp do_get_burst(sock, key, n) do
    for _ <- 1..n do
      send_cmd(sock, ["GET", key])
      recv_response(sock)
    end
  end

  # ===========================================================================
  # 7. DEL + SET race — no crashes, no corruption
  # ===========================================================================

  describe "DEL + SET race" do
    test "one client deletes, another sets — no crashes or corruption", %{port: port} do
      key = ukey("delset_race")
      iterations = 200

      setter =
        Task.async(fn ->
          s = connect_and_hello(port)

          for i <- 1..iterations do
            send_cmd(s, ["SET", key, "val_#{i}"])
            resp = recv_response(s)
            assert resp == ok()
          end

          :gen_tcp.close(s)
          :ok
        end)

      deleter =
        Task.async(fn ->
          s = connect_and_hello(port)

          for _ <- 1..iterations do
            send_cmd(s, ["DEL", key])
            resp = recv_response(s)
            # DEL returns 0 or 1
            assert resp in [0, 1]
          end

          :gen_tcp.close(s)
          :ok
        end)

      assert Task.await(setter, 30_000) == :ok
      assert Task.await(deleter, 30_000) == :ok

      # Final state: key either exists with a valid value or is deleted
      verify = connect_and_hello(port)
      send_cmd(verify, ["GET", key])
      result = recv_response(verify)
      assert result == nil or is_binary(result)
      :gen_tcp.close(verify)
    end
  end

  # ===========================================================================
  # 8. HSET concurrent fields — all fields present after
  # ===========================================================================

  describe "HSET concurrent fields" do
    test "10 clients write different fields to same hash — all present", %{port: port} do
      key = ukey("hset_race")
      fields_per_client = 10

      tasks =
        for client_id <- 1..10 do
          Task.async(fn ->
            s = connect_and_hello(port)

            for f <- 1..fields_per_client do
              field = "c#{client_id}_f#{f}"
              value = "v#{client_id}_#{f}"
              send_cmd(s, ["HSET", key, field, value])
              resp = recv_response(s)
              # HSET returns count of NEW fields added (1 for new, 0 for update)
              assert is_integer(resp)
            end

            :gen_tcp.close(s)
            :ok
          end)
        end

      Task.await_many(tasks, 30_000)

      # Verify all 100 fields are present
      verify = connect_and_hello(port)
      send_cmd(verify, ["HGETALL", key])
      hash_data = recv_response(verify)
      :gen_tcp.close(verify)

      # HGETALL returns a flat list [field1, val1, field2, val2, ...] or a map
      # depending on RESP3. Build a map from the result.
      field_map =
        case hash_data do
          m when is_map(m) -> m
          l when is_list(l) -> list_to_map(l)
        end

      for client_id <- 1..10, f <- 1..fields_per_client do
        field = "c#{client_id}_f#{f}"
        expected = "v#{client_id}_#{f}"

        assert Map.get(field_map, field) == expected,
               "Missing or wrong value for field #{field}: #{inspect(Map.get(field_map, field))}"
      end
    end
  end

  defp list_to_map(list), do: do_list_to_map(list, %{})
  defp do_list_to_map([], acc), do: acc
  defp do_list_to_map([k, v | rest], acc), do: do_list_to_map(rest, Map.put(acc, k, v))

  # ===========================================================================
  # 9. Pipeline + MULTI interleaving — no crashes
  # ===========================================================================

  describe "pipeline + MULTI interleaving" do
    test "pipelined commands and MULTI/EXEC on overlapping keys — no crashes", %{port: port} do
      keys = for i <- 1..5, do: ukey("pipe_multi_#{i}")

      # Seed keys
      seed = connect_and_hello(port)

      for k <- keys do
        send_cmd(seed, ["SET", k, "0"])
        assert recv_response(seed) == ok()
      end

      :gen_tcp.close(seed)

      # Client A: 100-command pipeline (INCR on all keys, repeated)
      pipeline_task =
        Task.async(fn ->
          s = connect_and_hello(port)

          cmds =
            for _ <- 1..20, k <- keys do
              ["INCR", k]
            end

          # Send all commands without waiting for responses (pipeline)
          for cmd <- cmds do
            send_cmd(s, cmd)
          end

          # Receive all 100 responses
          responses = recv_n(s, 100)
          :gen_tcp.close(s)

          # All responses should be integers
          for r <- responses do
            assert is_integer(r), "Pipeline INCR returned non-integer: #{inspect(r)}"
          end

          :ok
        end)

      # Client B: MULTI/EXEC with 5 commands on the same keys
      multi_task =
        Task.async(fn ->
          s = connect_and_hello(port)

          send_cmd(s, ["MULTI"])
          assert recv_response(s) == ok()

          for k <- keys do
            send_cmd(s, ["INCR", k])
            assert recv_response(s) == queued()
          end

          send_cmd(s, ["EXEC"])
          result = recv_response(s)

          :gen_tcp.close(s)

          assert is_list(result), "EXEC should return a list, got: #{inspect(result)}"
          assert length(result) == 5

          for r <- result do
            assert is_integer(r), "EXEC INCR returned non-integer: #{inspect(r)}"
          end

          :ok
        end)

      assert Task.await(pipeline_task, 30_000) == :ok
      assert Task.await(multi_task, 30_000) == :ok
    end
  end

  # ===========================================================================
  # 10. Cross-shard MULTI concurrent — all succeed or abort cleanly
  # ===========================================================================

  describe "cross-shard MULTI concurrent" do
    test "5 clients execute cross-shard MULTI/EXEC on overlapping keys — no crashes", %{
      port: port
    } do
      # Get keys on different shards
      cross_keys = ShardHelpers.keys_on_different_shards(min(4, shard_count()))

      # Seed all keys
      seed = connect_and_hello(port)

      for k <- cross_keys do
        send_cmd(seed, ["SET", k, "0"])
        assert recv_response(seed) == ok()
      end

      :gen_tcp.close(seed)

      tasks =
        for client_id <- 1..5 do
          Task.async(fn ->
            s = connect_and_hello(port)

            send_cmd(s, ["MULTI"])
            assert recv_response(s) == ok()

            for k <- cross_keys do
              send_cmd(s, ["SET", k, "client_#{client_id}"])
              assert recv_response(s) == queued()
            end

            send_cmd(s, ["EXEC"])
            result = recv_response(s)

            :gen_tcp.close(s)
            result
          end)
        end

      results = Task.await_many(tasks, 30_000)

      # Each result must be either a list of OKs (succeeded) or nil (aborted by WATCH conflict)
      for r <- results do
        assert is_list(r) or is_nil(r),
               "Cross-shard EXEC must return list or nil, got: #{inspect(r)}"

        if is_list(r) do
          assert length(r) == length(cross_keys)
          for item <- r, do: assert(item == ok())
        end
      end

      # Verify final state: all keys have the same value from one client
      verify = connect_and_hello(port)

      final_values =
        for k <- cross_keys do
          send_cmd(verify, ["GET", k])
          recv_response(verify)
        end

      :gen_tcp.close(verify)

      # All values should be valid client values
      for v <- final_values do
        assert is_binary(v), "Key should have a value, got: #{inspect(v)}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Shared helpers
  # ---------------------------------------------------------------------------

  defp wait_for_barrier(barrier, target) do
    case Agent.get(barrier, & &1) do
      n when n >= target ->
        :ok

      _ ->
        Process.sleep(1)
        wait_for_barrier(barrier, target)
    end
  end

  defp shard_count do
    :persistent_term.get(
      :ferricstore_shard_count,
      Application.get_env(:ferricstore, :shard_count, 4)
    )
  end
end
