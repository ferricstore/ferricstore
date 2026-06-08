defmodule FerricstoreServer.Integration.PerformanceTest do
  @moduledoc """
  Performance and load tests for FerricStore.

  These tests exercise the full stack under realistic workloads: sequential
  throughput, pipelining, concurrent clients, large values, TTL expiry under
  load, connection churn, and shard distribution.

  All tests are tagged `@moduletag :bench` so they can be excluded in CI with
  `--exclude bench` and included locally with `--include bench`.

  Run with:

      mix test test/ferricstore/integration/performance_test.exs --include bench
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  @moduletag :bench

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Store.Router

  @shard_count 4

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp connect(port) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [
        :binary,
        active: false,
        packet: :raw,
        nodelay: true,
        sndbuf: 1_048_576,
        recbuf: 1_048_576
      ])

    sock
  end

  defp send_cmd(sock, cmd) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(cmd)))
  end

  defp send_raw(sock, data) do
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_one(sock, buf \\ "") do
    case Parser.parse(buf) do
      {:ok, [val | _rest], _rem} ->
        val

      {:ok, [], _rem} ->
        case :gen_tcp.recv(sock, 0, 30_000) do
          {:ok, data} -> recv_one(sock, buf <> data)
          {:error, reason} -> {:recv_error, reason}
        end
    end
  end

  # Drains exactly `n` RESP3 responses from `sock`.
  #
  # Uses a process-dictionary buffer so that parsed-but-unconsumed values
  # and partial binary data survive across loop iterations without threading
  # buffer state through every callsite.
  @drain_buf_key :__perf_drain_buf__
  @drain_vals_key :__perf_drain_vals__

  defp drain_responses(sock, n) do
    Process.put(@drain_buf_key, "")
    Process.put(@drain_vals_key, [])
    results = do_drain(sock, n, [])
    Process.delete(@drain_buf_key)
    Process.delete(@drain_vals_key)
    results
  end

  defp do_drain(_sock, 0, acc), do: Enum.reverse(acc)

  defp do_drain(sock, remaining, acc) when remaining > 0 do
    # Check if we have pre-parsed values from a previous parse batch
    case Process.get(@drain_vals_key, []) do
      [val | rest] ->
        Process.put(@drain_vals_key, rest)
        do_drain(sock, remaining - 1, [val | acc])

      [] ->
        buf = Process.get(@drain_buf_key, "")

        case Parser.parse(buf) do
          {:ok, [val | rest_vals], rest_bin} ->
            Process.put(@drain_buf_key, rest_bin)
            Process.put(@drain_vals_key, rest_vals)
            do_drain(sock, remaining - 1, [val | acc])

          {:ok, [], _incomplete} ->
            case :gen_tcp.recv(sock, 0, 60_000) do
              {:ok, data} ->
                Process.put(@drain_buf_key, buf <> data)
                do_drain(sock, remaining, acc)

              {:error, reason} ->
                flunk(
                  "drain_responses: recv error #{inspect(reason)}, got #{length(acc)} of #{length(acc) + remaining}"
                )
            end
        end
    end
  end

  defp hello3(sock) do
    send_cmd(sock, ["HELLO", "3"])
    recv_one(sock)
  end

  defp connect_and_hello(port) do
    sock = connect(port)
    hello3(sock)
    sock
  end

  defp measure(fun) do
    start = System.monotonic_time(:millisecond)
    result = fun.()
    elapsed = System.monotonic_time(:millisecond) - start
    {elapsed, result}
  end

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup %{port: port} do
    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    recv_one(sock)
    :gen_tcp.close(sock)
    :ok
  end

  # ===========================================================================
  # 1. TCP throughput (sequential)
  # ===========================================================================

  describe "TCP throughput (sequential)" do
    @tag :bench
    test "1000 sequential PINGs complete within 5 seconds", %{port: port} do
      sock = connect_and_hello(port)

      {elapsed, _} =
        measure(fn ->
          for _ <- 1..1_000 do
            send_cmd(sock, ["PING"])
            assert {:simple, "PONG"} == recv_one(sock)
          end
        end)

      :gen_tcp.close(sock)

      assert elapsed < 5_000,
        "1000 sequential PINGs took #{elapsed}ms, expected < 5000ms"
    end

    @tag :bench
    test "1000 sequential SETs complete within 10 seconds", %{port: port} do
      sock = connect_and_hello(port)

      {elapsed, _} =
        measure(fn ->
          for i <- 1..1_000 do
            send_cmd(sock, ["SET", "seq_set_#{i}", "value_#{i}"])
            assert {:simple, "OK"} == recv_one(sock)
          end
        end)

      :gen_tcp.close(sock)

      assert elapsed < 10_000,
        "1000 sequential SETs took #{elapsed}ms, expected < 10000ms"
    end

    @tag :bench
    test "1000 sequential GETs complete within 10 seconds", %{port: port} do
      sock = connect_and_hello(port)

      # Pre-fill 1000 keys
      for i <- 1..1_000 do
        send_cmd(sock, ["SET", "seq_get_#{i}", "value_#{i}"])
        {:simple, "OK"} = recv_one(sock)
      end

      {elapsed, _} =
        measure(fn ->
          for i <- 1..1_000 do
            send_cmd(sock, ["GET", "seq_get_#{i}"])
            result = recv_one(sock)
            assert result == "value_#{i}"
          end
        end)

      :gen_tcp.close(sock)

      assert elapsed < 10_000,
        "1000 sequential GETs took #{elapsed}ms, expected < 10000ms"
    end
  end

  # ===========================================================================
  # 2. TCP throughput (pipelining)
  # ===========================================================================

  describe "TCP throughput (pipelining)" do
    @tag :bench
    test "pipeline 1000 PING commands in one TCP write completes within 2 seconds", %{
      port: port
    } do
      sock = connect_and_hello(port)

      pipeline =
        1..1_000
        |> Enum.map(fn _ -> Encoder.encode(["PING"]) end)
        |> IO.iodata_to_binary()

      {elapsed, responses} =
        measure(fn ->
          send_raw(sock, pipeline)
          drain_responses(sock, 1_000)
        end)

      :gen_tcp.close(sock)

      assert length(responses) == 1_000
      assert Enum.all?(responses, &(&1 == {:simple, "PONG"}))

      assert elapsed < 2_000,
        "Pipeline 1000 PINGs took #{elapsed}ms, expected < 2000ms"
    end

    @tag :bench
    test "pipeline 5000 SET commands in batches of 1000", %{port: port} do
      sock = connect_and_hello(port)

      {elapsed, _} =
        measure(fn ->
          for batch <- 0..4 do
            offset = batch * 1_000

            pipeline =
              1..1_000
              |> Enum.map(fn i ->
                idx = offset + i
                Encoder.encode(["SET", "pipe_batch_#{idx}", "val_#{idx}"])
              end)
              |> IO.iodata_to_binary()

            send_raw(sock, pipeline)
            responses = drain_responses(sock, 1_000)
            assert length(responses) == 1_000
            assert Enum.all?(responses, &(&1 == {:simple, "OK"}))
          end
        end)

      :gen_tcp.close(sock)

      # 5000 pipelined SETs -- generous threshold for slow machines
      assert elapsed < 30_000,
        "5000 pipelined SETs (5 batches) took #{elapsed}ms, expected < 30000ms"
    end

    @tag :bench
    test "pipeline is faster than sequential for 1000 PINGs", %{port: port} do
      # Sequential measurement
      sock_seq = connect_and_hello(port)

      {seq_elapsed, _} =
        measure(fn ->
          for _ <- 1..1_000 do
            send_cmd(sock_seq, ["PING"])
            {:simple, "PONG"} = recv_one(sock_seq)
          end
        end)

      :gen_tcp.close(sock_seq)

      # Pipeline measurement
      sock_pipe = connect_and_hello(port)

      pipeline =
        1..1_000
        |> Enum.map(fn _ -> Encoder.encode(["PING"]) end)
        |> IO.iodata_to_binary()

      {pipe_elapsed, responses} =
        measure(fn ->
          send_raw(sock_pipe, pipeline)
          drain_responses(sock_pipe, 1_000)
        end)

      :gen_tcp.close(sock_pipe)

      assert length(responses) == 1_000

      assert pipe_elapsed < seq_elapsed,
        "Pipeline (#{pipe_elapsed}ms) should be faster than sequential (#{seq_elapsed}ms)"
    end
  end

  # ===========================================================================
  # 3. Concurrent client throughput
  # ===========================================================================

  describe "concurrent client throughput" do
    @tag :bench
    test "10 clients each send 100 SETs concurrently within 10 seconds", %{port: port} do
      {elapsed, results} =
        measure(fn ->
          1..10
          |> Task.async_stream(
            fn client_id ->
              sock = connect_and_hello(port)

              for i <- 1..100 do
                key = "conc10_#{client_id}_#{i}"
                send_cmd(sock, ["SET", key, "val_#{client_id}_#{i}"])
                {:simple, "OK"} = recv_one(sock)
              end

              :gen_tcp.close(sock)
              :ok
            end,
            max_concurrency: 10,
            timeout: 30_000
          )
          |> Enum.to_list()
        end)

      assert length(results) == 10
      assert Enum.all?(results, &match?({:ok, :ok}, &1))

      assert elapsed < 10_000,
        "10 concurrent clients x 100 SETs took #{elapsed}ms, expected < 10000ms"
    end

    @tag :bench
    test "50 clients each send 10 GETs concurrently", %{port: port} do
      # Pre-fill keys
      sock = connect_and_hello(port)

      for i <- 1..10 do
        send_cmd(sock, ["SET", "conc50_get_#{i}", "prefill_#{i}"])
        {:simple, "OK"} = recv_one(sock)
      end

      :gen_tcp.close(sock)

      {elapsed, results} =
        measure(fn ->
          1..50
          |> Task.async_stream(
            fn _client_id ->
              sock = connect_and_hello(port)

              for i <- 1..10 do
                send_cmd(sock, ["GET", "conc50_get_#{i}"])
                result = recv_one(sock)
                assert result == "prefill_#{i}"
              end

              :gen_tcp.close(sock)
              :ok
            end,
            max_concurrency: 50,
            timeout: 30_000
          )
          |> Enum.to_list()
        end)

      assert length(results) == 50
      assert Enum.all?(results, &match?({:ok, :ok}, &1))

      assert elapsed < 15_000,
        "50 concurrent clients x 10 GETs took #{elapsed}ms, expected < 15000ms"
    end
  end

  # ===========================================================================
  # 4. Memory stability
  # ===========================================================================

  describe "memory stability" do
    @tag timeout: 180_000
    @tag :bench
    test "write 10000 keys via Router, verify DBSIZE, delete all, verify DBSIZE returns to baseline" do
      # Use Router directly to avoid TCP pipeline complexity.
      # The point of this test is memory stability, not TCP throughput.

      baseline = Router.dbsize(FerricStore.Instance.get(:default))

      # Write 10,000 keys directly through the Router
      for i <- 1..10_000 do
        Router.put(FerricStore.Instance.get(:default), "mem_#{i}", "val_#{i}")
      end

      # Verify DBSIZE reflects the new keys
      after_write = Router.dbsize(FerricStore.Instance.get(:default))
      assert after_write >= baseline + 10_000

      # Delete all 10,000 keys
      for i <- 1..10_000 do
        Router.delete(FerricStore.Instance.get(:default), "mem_#{i}")
      end

      # Verify DBSIZE is back to baseline
      after_delete = Router.dbsize(FerricStore.Instance.get(:default))
      assert after_delete == baseline
    end
  end

  # ===========================================================================
  # 5. Large value performance
  # ===========================================================================

  describe "large value performance" do
    @tag :bench
    test "SET and GET a 1MB value completes within 2 seconds", %{port: port} do
      sock = connect_and_hello(port)
      large_1mb = :binary.copy("A", 1_000_000)

      {elapsed, _} =
        measure(fn ->
          send_cmd(sock, ["SET", "large_1mb", large_1mb])
          assert {:simple, "OK"} == recv_one(sock)

          send_cmd(sock, ["GET", "large_1mb"])
          result = recv_one(sock)
          assert result == large_1mb
        end)

      :gen_tcp.close(sock)

      assert elapsed < 2_000,
        "1MB SET+GET took #{elapsed}ms, expected < 2000ms"
    end

    @tag :bench
    test "SET and GET a 10MB value completes within 10 seconds", %{port: port} do
      sock = connect_and_hello(port)
      large_10mb = :binary.copy("B", 10_000_000)

      {elapsed, _} =
        measure(fn ->
          send_cmd(sock, ["SET", "large_10mb", large_10mb])
          assert {:simple, "OK"} == recv_one(sock)

          send_cmd(sock, ["GET", "large_10mb"])
          result = recv_one(sock)
          assert result == large_10mb
        end)

      :gen_tcp.close(sock)

      assert elapsed < 10_000,
        "10MB SET+GET took #{elapsed}ms, expected < 10000ms"
    end
  end

  # ===========================================================================
  # 6. Hot path: ETS cache speedup
  # ===========================================================================

  describe "ETS cache hot path" do
    @tag :bench
    test "1000 Router.get calls on a cached key average < 100 microseconds each" do
      key = "ets_hot_#{:rand.uniform(9_999_999)}"

      # Write via Router to populate both Bitcask and ETS
      Router.put(FerricStore.Instance.get(:default), key, "hot_value")

      # First get warms the ETS cache
      assert Router.get(FerricStore.Instance.get(:default), key) == "hot_value"

      # Time 1000 gets -- all should be ETS cache hits
      {elapsed_us, _} =
        :timer.tc(fn ->
          for _ <- 1..1_000 do
            Router.get(FerricStore.Instance.get(:default), key)
          end
        end)

      avg_us = elapsed_us / 1_000

      # Clean up
      Router.delete(FerricStore.Instance.get(:default), key)

      assert avg_us < 100,
        "Average Router.get from ETS cache was #{Float.round(avg_us, 2)}us, expected < 100us"
    end
  end

  # ===========================================================================
  # 7. Shard distribution
  # ===========================================================================

  describe "shard distribution" do
    @tag :bench
    test "1000 keys are distributed across all #{@shard_count} shards with no shard > 60%" do
      keys = for i <- 1..1_000, do: "shard_dist_#{i}"

      # Count how many keys map to each shard
      shard_counts =
        keys
        |> Enum.group_by(fn key -> Router.shard_for(FerricStore.Instance.get(:default), key) end)
        |> Enum.map(fn {shard, shard_keys} -> {shard, length(shard_keys)} end)
        |> Map.new()

      # Assert all shards have at least one key
      for shard_idx <- 0..(@shard_count - 1) do
        count = Map.get(shard_counts, shard_idx, 0)

        assert count > 0,
          "Shard #{shard_idx} has 0 keys -- distribution is broken"
      end

      # Assert no shard has > 60% of the keys
      for {shard_idx, count} <- shard_counts do
        pct = count / 1_000 * 100

        assert pct <= 60.0,
          "Shard #{shard_idx} has #{count} keys (#{Float.round(pct, 1)}%), expected <= 60%"
      end
    end
  end

  # ===========================================================================
  # 8. TTL expiry under load
  # ===========================================================================

  describe "TTL expiry under load" do
    @tag :bench
    test "1000 keys with PX 200ms TTL are all expired after waiting", %{port: port} do
      # Write 1000 keys with 200ms TTL concurrently across 10 clients
      results =
        1..10
        |> Task.async_stream(
          fn client_id ->
            sock = connect_and_hello(port)
            offset = (client_id - 1) * 100

            for i <- 1..100 do
              key = "ttl_load_#{offset + i}"
              send_cmd(sock, ["SET", key, "ephemeral", "PX", "200"])
              {:simple, "OK"} = recv_one(sock)
            end

            :gen_tcp.close(sock)
            :ok
          end,
          max_concurrency: 10,
          timeout: 30_000
        )
        |> Enum.to_list()

      assert Enum.all?(results, &match?({:ok, :ok}, &1))

      # Wait for TTL to expire (generous margin)
      Process.sleep(500)

      # Verify all 1000 keys are expired via sequential GETs
      sock = connect_and_hello(port)

      expired_count =
        Enum.count(1..1_000, fn i ->
          send_cmd(sock, ["GET", "ttl_load_#{i}"])
          recv_one(sock) == nil
        end)

      assert expired_count == 1_000,
        "Expected all 1000 keys expired, but #{1_000 - expired_count} are still alive"

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # 9. Connection rate
  # ===========================================================================

  describe "connection rate" do
    @tag :bench
    test "200 sequential connect-PING-disconnect cycles within 10 seconds", %{port: port} do
      {elapsed, _} =
        measure(fn ->
          for _ <- 1..200 do
            sock = connect_and_hello(port)
            send_cmd(sock, ["PING"])
            {:simple, "PONG"} = recv_one(sock)
            :gen_tcp.close(sock)
          end
        end)

      assert elapsed < 10_000,
        "200 connect-PING-disconnect cycles took #{elapsed}ms, expected < 10000ms"
    end
  end

  # ===========================================================================
  # 10. Pipeline correctness under load
  # ===========================================================================

  describe "pipeline correctness under load" do
    @tag :bench
    test "5 concurrent clients each pipeline 200 commands with correct responses", %{
      port: port
    } do
      results =
        1..5
        |> Task.async_stream(
          fn client_id ->
            sock = connect_and_hello(port)

            # Build a pipeline of 100 SET + 100 GET commands
            set_cmds =
              for i <- 1..100 do
                Encoder.encode(["SET", "plc_#{client_id}_#{i}", "cv_#{client_id}_#{i}"])
              end

            get_cmds =
              for i <- 1..100 do
                Encoder.encode(["GET", "plc_#{client_id}_#{i}"])
              end

            pipeline = IO.iodata_to_binary(set_cmds ++ get_cmds)
            send_raw(sock, pipeline)

            responses = drain_responses(sock, 200)
            assert length(responses) == 200

            # First 100 responses should all be OK (SETs)
            set_responses = Enum.take(responses, 100)

            assert Enum.all?(set_responses, &(&1 == {:simple, "OK"})),
              "Client #{client_id}: not all SETs returned OK"

            # Last 100 responses should be the correct values (GETs)
            get_responses = Enum.drop(responses, 100)

            for i <- 1..100 do
              expected = "cv_#{client_id}_#{i}"
              actual = Enum.at(get_responses, i - 1)

              assert actual == expected,
                "Client #{client_id}, key #{i}: expected #{inspect(expected)}, got #{inspect(actual)}"
            end

            :gen_tcp.close(sock)
            :ok
          end,
          max_concurrency: 5,
          timeout: 30_000
        )
        |> Enum.to_list()

      assert length(results) == 5
      assert Enum.all?(results, &match?({:ok, :ok}, &1))
    end
  end
end
