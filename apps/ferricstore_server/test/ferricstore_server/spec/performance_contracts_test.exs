defmodule FerricstoreServer.Spec.PerformanceContractsTest do
  @moduledoc """
  Spec section 7: Performance Contract sanity checks.

  These tests assert basic latency bounds for single-node operations. They are
  intentionally generous (much looser than the spec's hardware-specific p99
  targets) so they pass on CI and developer laptops. The real contract
  verification requires dedicated hardware, NVMe Gen4, and Benchee — see
  section 16 of the test plan.

  Each test measures wall-clock elapsed time for a batch of operations and
  asserts that the total stays within a budget. This catches catastrophic
  regressions (e.g. a hot path accidentally hitting disk, a missing ETS lookup,
  a GenServer bottleneck) without flaking on variable-speed CI runners.

  Tagged `@moduletag :bench` — excluded from the default `mix test` run.
  Run with:

      mix test test/ferricstore/spec/performance_contracts_test.exs --include bench

  """

  use ExUnit.Case, async: false

  @moduletag :bench
  @moduletag timeout: 120_000

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp connect do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", Listener.port(), [
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

  defp drain_responses(sock, n) do
    do_drain(sock, n, "", [])
  end

  defp do_drain(_sock, 0, _buf, acc), do: Enum.reverse(acc)

  defp do_drain(sock, remaining, buf, acc) when remaining > 0 do
    case Parser.parse(buf) do
      {:ok, vals, rest} when vals != [] ->
        take = min(length(vals), remaining)
        new_acc = Enum.reverse(Enum.take(vals, take)) ++ acc
        new_remaining = remaining - take

        if new_remaining == 0 do
          Enum.reverse(new_acc)
        else
          do_drain(sock, new_remaining, rest, new_acc)
        end

      {:ok, [], _incomplete} ->
        case :gen_tcp.recv(sock, 0, 60_000) do
          {:ok, data} ->
            do_drain(sock, remaining, buf <> data, acc)

          {:error, reason} ->
            flunk(
              "drain_responses: recv error #{inspect(reason)}, got #{length(acc)} of #{length(acc) + remaining}"
            )
        end
    end
  end

  defp connect_and_hello do
    sock = connect()
    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_one(sock)
    sock
  end

  defp ukey(base), do: "perf_#{base}_#{System.unique_integer([:positive])}"

  # Measures the elapsed wall-clock time of `fun` in milliseconds.
  defp measure_ms(fun) do
    t0 = System.monotonic_time(:millisecond)
    result = fun.()
    elapsed = System.monotonic_time(:millisecond) - t0
    {elapsed, result}
  end

  # Measures the elapsed wall-clock time of `fun` in microseconds.
  defp measure_us(fun) do
    t0 = System.monotonic_time(:microsecond)
    result = fun.()
    elapsed = System.monotonic_time(:microsecond) - t0
    {elapsed, result}
  end

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    ShardHelpers.wait_shards_alive()
    %{port: Listener.port()}
  end

  setup do
    sock = connect_and_hello()
    send_cmd(sock, ["FLUSHDB"])
    recv_one(sock)
    :gen_tcp.close(sock)
    :ok
  end

  # ===========================================================================
  # PC-001: GET latency on warm key < 1ms (single node, via Router)
  # ===========================================================================

  describe "PC-001: GET latency on warm key" do
    @tag :bench
    test "1000 Router.get calls on a warm key complete in < 1000ms (avg < 1ms)" do
      key = ukey("warm_get")
      Router.put(FerricStore.Instance.get(:default), key, "warm_value")

      # Warm the ETS hot cache
      for _ <- 1..100, do: Router.get(FerricStore.Instance.get(:default), key)

      {elapsed_us, _} =
        measure_us(fn ->
          for _ <- 1..1_000 do
            assert Router.get(FerricStore.Instance.get(:default), key) == "warm_value"
          end
        end)

      avg_us = elapsed_us / 1_000

      # Spec target: p99 < 10us for ETS hot hit.
      # CI budget: average < 1ms (1000us) per GET — extremely generous.
      assert avg_us < 1_000,
             "Average Router.get on warm key was #{Float.round(avg_us, 1)}us, expected < 1000us"

      # Clean up
      Router.delete(FerricStore.Instance.get(:default), key)
    end
  end

  # ===========================================================================
  # PC-002: SET latency < 5ms (single node, via TCP)
  # ===========================================================================

  describe "PC-002: SET latency via TCP" do
    @tag :bench
    test "100 sequential SETs complete in < 2000ms (avg < 20ms each)" do
      sock = connect_and_hello()

      {elapsed, _} =
        measure_ms(fn ->
          for i <- 1..100 do
            send_cmd(sock, ["SET", ukey("set_#{i}"), "value_#{i}"])
            assert {:simple, "OK"} == recv_one(sock)
          end
        end)

      :gen_tcp.close(sock)

      avg_ms = elapsed / 100

      # Sequential SETs go through the full Raft write path:
      # TCP -> RESP parse -> Dispatcher -> Batcher -> Raft commit -> ETS apply.
      # On a single-node Raft cluster this is fast, but the group-commit window
      # and fsync introduce latency. 20ms per sequential SET is generous for CI.
      assert avg_ms < 20,
             "Average SET latency was #{Float.round(avg_ms * 1.0, 2)}ms, expected < 20ms"
    end
  end

  # ===========================================================================
  # PC-003: Pipeline of 10 commands completes < 50ms
  # ===========================================================================

  describe "PC-003: pipeline of 10 commands" do
    @tag :bench
    test "pipeline of 10 SET+GET commands completes in < 50ms" do
      sock = connect_and_hello()

      keys = for i <- 1..5, do: ukey("pipe_#{i}")

      pipeline =
        (Enum.map(keys, fn k -> Encoder.encode(["SET", k, "pval"]) end) ++
           Enum.map(keys, fn k -> Encoder.encode(["GET", k]) end))
        |> IO.iodata_to_binary()

      {elapsed, responses} =
        measure_ms(fn ->
          send_raw(sock, pipeline)
          drain_responses(sock, 10)
        end)

      :gen_tcp.close(sock)

      assert length(responses) == 10

      # First 5 should be OK (SETs)
      set_responses = Enum.take(responses, 5)
      assert Enum.all?(set_responses, &(&1 == {:simple, "OK"}))

      # Last 5 should be the values (GETs)
      get_responses = Enum.drop(responses, 5)
      assert Enum.all?(get_responses, &(&1 == "pval"))

      assert elapsed < 50,
             "Pipeline of 10 commands took #{elapsed}ms, expected < 50ms"
    end

    @tag :bench
    test "pipeline of 10 PINGs completes in < 50ms" do
      sock = connect_and_hello()

      pipeline =
        1..10
        |> Enum.map(fn _ -> Encoder.encode(["PING"]) end)
        |> IO.iodata_to_binary()

      {elapsed, responses} =
        measure_ms(fn ->
          send_raw(sock, pipeline)
          drain_responses(sock, 10)
        end)

      :gen_tcp.close(sock)

      assert length(responses) == 10
      assert Enum.all?(responses, &(&1 == {:simple, "PONG"}))

      assert elapsed < 50,
             "Pipeline of 10 PINGs took #{elapsed}ms, expected < 50ms"
    end
  end

  # ===========================================================================
  # PC-004: SCAN 1000 keys completes < 500ms
  # ===========================================================================

  describe "PC-004: SCAN 1000 keys" do
    @tag :bench
    test "SCAN iterating over 1000 keys completes in < 500ms" do
      sock = connect_and_hello()

      # Pre-populate 1000 keys using pipelined SETs for speed.
      batch_size = 200

      for batch <- 0..4 do
        offset = batch * batch_size

        pipeline =
          1..batch_size
          |> Enum.map(fn i ->
            idx = offset + i
            Encoder.encode(["SET", "scan_perf_#{idx}", "v#{idx}"])
          end)
          |> IO.iodata_to_binary()

        send_raw(sock, pipeline)
        responses = drain_responses(sock, batch_size)
        assert length(responses) == batch_size
      end

      # Now measure SCAN iteration time.
      # SCAN returns [cursor, [keys...]] — iterate until cursor is "0".
      {elapsed, total_keys} =
        measure_ms(fn ->
          scan_all_keys(sock, "0", 0)
        end)

      :gen_tcp.close(sock)

      # We should have found at least 1000 keys (there may be more from
      # other tests if FLUSHDB didn't fully clear, but at least 1000).
      assert total_keys >= 1000,
             "SCAN found only #{total_keys} keys, expected >= 1000"

      assert elapsed < 500,
             "SCAN over #{total_keys} keys took #{elapsed}ms, expected < 500ms"
    end
  end

  # Iterates SCAN until the cursor returns to "0", counting total keys found.
  defp scan_all_keys(sock, cursor, count) do
    send_cmd(sock, ["SCAN", cursor, "COUNT", "200"])
    result = recv_one(sock)

    case result do
      [new_cursor, keys] when is_list(keys) ->
        new_count = count + length(keys)

        if new_cursor == "0" do
          new_count
        else
          scan_all_keys(sock, new_cursor, new_count)
        end

      _other ->
        # Unexpected response shape — return what we have
        count
    end
  end

  # ===========================================================================
  # PC-005: ETS hot cache is measurably faster than cold path
  # ===========================================================================

  describe "PC-005: ETS hot cache speedup" do
    @tag :bench
    test "warm Router.get is faster than cold GenServer.call path" do
      key = ukey("hot_cold")
      Router.put(FerricStore.Instance.get(:default), key, "test_value")

      # First get to warm the hot cache
      Router.get(FerricStore.Instance.get(:default), key)

      # Measure warm reads (ETS path)
      {warm_us, _} =
        measure_us(fn ->
          for _ <- 1..1_000, do: Router.get(FerricStore.Instance.get(:default), key)
        end)

      # Delete from keydir to force cold path (but leave in Bitcask)
      idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{idx}"

      # Record cold path time by doing repeated GenServer calls
      # (Force cache miss by using a key pattern that bypasses ETS)
      cold_keys = for i <- 1..100, do: ukey("cold_#{i}")

      Enum.each(cold_keys, fn k ->
        Router.put(FerricStore.Instance.get(:default), k, "cold_value")
      end)

      # Evict cold keys from keydir to force cold reads
      Enum.each(cold_keys, fn k ->
        try do
          :ets.delete(keydir, k)
        rescue
          _ -> :ok
        end
      end)

      {cold_us, _} =
        measure_us(fn ->
          for k <- cold_keys, do: Router.get(FerricStore.Instance.get(:default), k)
        end)

      warm_avg = warm_us / 1_000
      cold_avg = cold_us / 100

      # The warm path should be meaningfully faster.
      # On any hardware, ETS lookup should be at least 2x faster than
      # a GenServer call that hits Bitcask.
      assert warm_avg < cold_avg,
             "Warm avg=#{Float.round(warm_avg, 1)}us should be faster than cold avg=#{Float.round(cold_avg, 1)}us"

      # Clean up
      Router.delete(FerricStore.Instance.get(:default), key)
      Enum.each(cold_keys, fn k -> Router.delete(FerricStore.Instance.get(:default), k) end)
    end
  end
end
