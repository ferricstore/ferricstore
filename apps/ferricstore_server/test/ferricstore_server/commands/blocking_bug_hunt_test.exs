defmodule FerricstoreServer.Commands.BlockingBugHuntTest do
  @moduledoc """
  Targeted tests that probe blocking list commands for correctness bugs.

  These tests cover edge cases and race conditions in:

    * `Ferricstore.Commands.Blocking` -- argument parsing and non-blocking dispatch
    * `Ferricstore.Waiters` -- ETS waiter registry accuracy
    * `FerricstoreServer.Connection` -- blocking dispatch over TCP

  TCP-level tests use a real Ranch listener (the one started by the application
  supervisor in test) and raw `:gen_tcp` connections with RESP3 encoding.

  ## Bug areas probed

  1. **BLPOP with float timeout (0.5)** -- `parse_timeout/1` uses `trunc/1`
     which truncates `0.5 * 1000` to 500ms. Verify the conversion is correct
     for various fractional second values.
  2. **BLPOP on multiple keys** -- first non-empty key should win immediately.
  3. **BLPOP timeout then normal command** -- connection must remain usable
     after a blocking timeout expires.
  4. **BRPOP wakes on RPUSH** -- BRPOP should pop from the right, not the left.
  5. **BLMOVE with empty source** -- should block (not crash) and return nil on
     timeout.
  6. **BLMPOP with COUNT > list length** -- should return all available elements,
     not error.
  7. **Two clients BLPOP same key** -- only one should wake per push (FIFO).
  8. **BLPOP then client disconnect** -- waiter entry must be cleaned up from
     the ETS table.
  9. **Waiters.count accuracy** -- after register/unregister cycles, the count
     must be exact.
  10. **BLPOP with negative timeout** -- must return an error, never block.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Blocking, List}
  alias FerricstoreServer.Connection.Blocking, as: ConnBlocking
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.MockStore
  alias Ferricstore.Waiters

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup_all do
    # Discover the listener port. If the listener isn't running, skip TCP tests.
    {port, tcp_healthy} =
      try do
        port = Listener.port()

        # Probe whether TCP commands actually work (the working tree may have
        # broken shard code that causes connection crashes). If we cannot
        # complete a PING, mark TCP as unhealthy.
        healthy =
          try do
            {:ok, sock} =
              :gen_tcp.connect(
                {127, 0, 0, 1},
                port,
                [:binary, active: false, packet: :raw],
                2_000
              )

            send_cmd_raw(sock, ["HELLO", "3"])

            case recv_raw(sock, 2_000) do
              {:ok, _} ->
                send_cmd_raw(sock, ["PING"])

                case recv_raw(sock, 2_000) do
                  {:ok, _} ->
                    :gen_tcp.close(sock)
                    true

                  _ ->
                    :gen_tcp.close(sock)
                    false
                end

              _ ->
                :gen_tcp.close(sock)
                false
            end
          rescue
            _ -> false
          catch
            _, _ -> false
          end

        {port, healthy}
      rescue
        _ -> {0, false}
      catch
        _, _ -> {0, false}
      end

    %{port: port, tcp_healthy: tcp_healthy}
  end

  setup do
    # Ensure ETS table exists
    if :ets.whereis(:ferricstore_waiters) == :undefined do
      Waiters.init()
    end

    # Clean up stale waiters from prior tests
    :ets.delete_all_objects(:ferricstore_waiters)

    :ok
  end

  # ===========================================================================
  # TCP helpers
  # ===========================================================================

  test "blocking list dispatch converts storage raises into error replies" do
    ctx = FerricStore.Instance.get(:default)
    key = "blocking_raise"
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    meta_key = Ferricstore.Store.CompoundKey.list_meta_key(key)

    with_raw_store(ctx, fn raw ->
      raw
      |> Map.put(:compound_get, fn
        ^key, ^type_key -> "list"
        ^key, ^meta_key -> :erlang.term_to_binary({1, 0, 0})
        _redis_key, _compound_key -> nil
      end)
      |> Map.put(:compound_scan, fn ^key, _prefix -> raise "async key latch timeout after 5ms" end)
    end)

    try do
      assert {:continue, response, ^state} =
               ConnBlocking.dispatch_blpop_ast([key], 1, state)

      assert IO.iodata_to_binary(response) =~ "-ERR internal error:"
    after
      restore_raw_store(ctx)
    end
  end

  # Raw helpers used in setup_all (before context is available)
  defp send_cmd_raw(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :gen_tcp.send(sock, data)
  end

  defp recv_raw(sock, timeout) do
    recv_raw_buf(sock, "", timeout)
  end

  defp recv_raw_buf(sock, buf, timeout) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val], _rest} -> {:ok, val}
          {:ok, [], _} -> recv_raw_buf(sock, buf2, timeout)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock, timeout \\ 5_000) do
    recv_response_buf(sock, "", timeout)
  end

  defp recv_response_buf(sock, buf, timeout) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val], _rest} -> {:ok, val}
          {:ok, [], _} -> recv_response_buf(sock, buf2, timeout)
        end

      {:error, :timeout} ->
        {:error, :timeout}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    {:ok, _greeting} = recv_response(sock)
    sock
  end

  defp ukey(name), do: "bughunt_#{name}_#{:erlang.unique_integer([:positive])}"

  defp with_raw_store(ctx, update_fn) do
    raw_store_key = raw_store_key(ctx)
    old_raw_store = :persistent_term.get(raw_store_key, :missing)
    Process.put({__MODULE__, :old_raw_store, ctx.name}, old_raw_store)

    ctx
    |> FerricstoreServer.Connection.Store.build_raw_store()
    |> update_fn.()
    |> then(&:persistent_term.put(raw_store_key, &1))
  end

  defp restore_raw_store(ctx) do
    raw_store_key = raw_store_key(ctx)

    case Process.delete({__MODULE__, :old_raw_store, ctx.name}) do
      :missing -> :persistent_term.erase(raw_store_key)
      nil -> :persistent_term.erase(raw_store_key)
      store -> :persistent_term.put(raw_store_key, store)
    end
  end

  defp raw_store_key(ctx), do: {:ferricstore_raw_store, ctx.name}

  # Skips a test if TCP is not healthy (shard crash loop in working tree).
  defp require_tcp!(%{tcp_healthy: true}), do: :ok

  defp require_tcp!(%{tcp_healthy: false}) do
    ExUnit.Assertions.flunk(
      "TCP infrastructure unhealthy (shard crash loop in working tree) -- skipping TCP test"
    )
  end

  # ===========================================================================
  # 1. BLPOP with timeout 0.5 (float seconds)
  # ===========================================================================

  describe "BLPOP with float timeout 0.5" do
    test "parse_timeout converts 0.5 seconds to 500 milliseconds" do
      assert {:ok, ["k"], 500} = Blocking.parse_blpop_args(["k", "0.5"])
    end

    test "parse_timeout converts 1.5 seconds to 1500 milliseconds" do
      assert {:ok, ["k"], 1500} = Blocking.parse_blpop_args(["k", "1.5"])
    end

    test "parse_timeout converts 0.1 seconds to 100 milliseconds" do
      assert {:ok, ["k"], 100} = Blocking.parse_blpop_args(["k", "0.1"])
    end

    test "parse_timeout converts 0.001 to 1 millisecond (sub-ms precision truncated)" do
      # 0.001 * 1000 = 1.0, trunc(1.0) = 1
      assert {:ok, ["k"], 1} = Blocking.parse_blpop_args(["k", "0.001"])
    end

    test "parse_timeout converts 0.0001 to 0 milliseconds (sub-ms)" do
      # 0.0001 * 1000 = 0.1, trunc(0.1) = 0
      assert {:ok, ["k"], 0} = Blocking.parse_blpop_args(["k", "0.0001"])
    end

    test "BLPOP with 0.5s timeout expires after ~500ms over TCP", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      key = ukey("float_timeout")

      start = System.monotonic_time(:millisecond)
      send_cmd(sock, ["BLPOP", key, "0.5"])
      {:ok, result} = recv_response(sock, 3_000)
      elapsed = System.monotonic_time(:millisecond) - start

      assert result == nil
      assert elapsed >= 300, "Timed out too fast: #{elapsed}ms"
      assert elapsed <= 1500, "Timed out too slow: #{elapsed}ms"

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # 2. BLPOP on multiple keys -- first non-empty wins
  # ===========================================================================

  describe "BLPOP on multiple keys -- first non-empty wins" do
    test "non-blocking: returns value from first non-empty compound list key" do
      store = MockStore.make()
      # k2 has data, k1 and k3 are empty
      List.handle("RPUSH", ["k2", "from_k2"], store)
      List.handle("RPUSH", ["k3", "from_k3"], store)

      result = Blocking.handle("BLPOP", ["k1", "k2", "k3", "1"], store)

      assert result == ["k2", "from_k2"]
    end

    test "returns value from first non-empty key immediately over TCP", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      k1 = ukey("multi_k1")
      k2 = ukey("multi_k2")
      k3 = ukey("multi_k3")

      send_cmd(sock, ["RPUSH", k2, "from_k2"])
      {:ok, _} = recv_response(sock)
      send_cmd(sock, ["RPUSH", k3, "from_k3"])
      {:ok, _} = recv_response(sock)

      send_cmd(sock, ["BLPOP", k1, k2, k3, "1"])
      {:ok, result} = recv_response(sock, 3_000)

      assert result == [k2, "from_k2"]

      :gen_tcp.close(sock)
    end

    test "blocks and wakes when push arrives on second key over TCP", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      k1 = ukey("multi_wake_k1")
      k2 = ukey("multi_wake_k2")

      send_cmd(sock, ["BLPOP", k1, k2, "5"])

      sock2 = connect_and_hello(context.port)
      Process.sleep(50)
      send_cmd(sock2, ["RPUSH", k2, "woke_up"])
      {:ok, _push_result} = recv_response(sock2)

      {:ok, result} = recv_response(sock, 3_000)
      assert result == [k2, "woke_up"]

      :gen_tcp.close(sock)
      :gen_tcp.close(sock2)
    end
  end

  # ===========================================================================
  # 3. BLPOP timeout then normal command -- connection still works
  # ===========================================================================

  describe "BLPOP timeout then normal command -- connection still works" do
    test "connection remains usable after blocking timeout expires", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      key = ukey("post_timeout")

      send_cmd(sock, ["BLPOP", key, "0.3"])
      {:ok, result} = recv_response(sock, 10_000)
      assert result == nil

      send_cmd(sock, ["SET", key, "alive"])
      {:ok, set_result} = recv_response(sock, 5_000)
      assert set_result == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      {:ok, get_result} = recv_response(sock, 5_000)
      assert get_result == "alive"

      :gen_tcp.close(sock)
    end

    test "PING works immediately after BLPOP timeout", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      key = ukey("post_timeout_ping")

      send_cmd(sock, ["BLPOP", key, "0.2"])
      {:ok, nil} = recv_response(sock, 10_000)

      send_cmd(sock, ["PING"])
      {:ok, pong} = recv_response(sock)
      assert pong == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # 4. BRPOP wakes on RPUSH (not just LPUSH)
  # ===========================================================================

  describe "BRPOP wakes on RPUSH" do
    test "BRPOP pops from the right side of the list over TCP", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      key = ukey("brpop_side")

      send_cmd(sock, ["RPUSH", key, "first", "second", "third"])
      {:ok, 3} = recv_response(sock)

      send_cmd(sock, ["BRPOP", key, "1"])
      {:ok, result} = recv_response(sock, 3_000)
      assert result == [key, "third"]

      :gen_tcp.close(sock)
    end

    test "BRPOP wakes when RPUSH happens on empty key", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      key = ukey("brpop_wake")

      send_cmd(sock, ["BRPOP", key, "5"])

      sock2 = connect_and_hello(context.port)
      Process.sleep(50)
      send_cmd(sock2, ["RPUSH", key, "pushed_val"])
      {:ok, _} = recv_response(sock2)

      {:ok, result} = recv_response(sock, 3_000)
      assert result == [key, "pushed_val"]

      :gen_tcp.close(sock)
      :gen_tcp.close(sock2)
    end

    test "BRPOP wakes when LPUSH happens on empty key", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      key = ukey("brpop_lpush_wake")

      send_cmd(sock, ["BRPOP", key, "5"])

      sock2 = connect_and_hello(context.port)
      Process.sleep(50)
      send_cmd(sock2, ["LPUSH", key, "lpushed"])
      {:ok, _} = recv_response(sock2)

      {:ok, result} = recv_response(sock, 3_000)
      assert result == [key, "lpushed"]

      :gen_tcp.close(sock)
      :gen_tcp.close(sock2)
    end

    test "BRPOP pops rightmost after waking on multi-element RPUSH", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      key = ukey("brpop_multi_push")

      send_cmd(sock, ["BRPOP", key, "5"])

      sock2 = connect_and_hello(context.port)
      Process.sleep(50)
      send_cmd(sock2, ["RPUSH", key, "a", "b", "c"])
      {:ok, 3} = recv_response(sock2)

      {:ok, result} = recv_response(sock, 3_000)
      assert result == [key, "c"]

      send_cmd(sock, ["LRANGE", key, "0", "-1"])
      {:ok, remaining} = recv_response(sock)
      assert remaining == ["a", "b"]

      :gen_tcp.close(sock)
      :gen_tcp.close(sock2)
    end
  end

  # ===========================================================================
  # 5. BLMOVE with empty source -- blocks correctly
  # ===========================================================================

  describe "BLMOVE with empty source -- blocks correctly" do
    test "non-blocking handle returns nil for empty source" do
      store = MockStore.make()
      result = Blocking.handle("BLMOVE", ["src", "dst", "LEFT", "RIGHT", "0"], store)
      assert result == nil
    end

    test "BLMOVE blocks and returns nil on timeout when source stays empty", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      src = ukey("blmove_empty_src")
      dst = ukey("blmove_empty_dst")

      send_cmd(sock, ["BLMOVE", src, dst, "LEFT", "RIGHT", "0.3"])
      {:ok, result} = recv_response(sock, 3_000)

      assert result == nil

      :gen_tcp.close(sock)
    end

    test "BLMOVE wakes and moves when source gets data", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      src = ukey("blmove_wake_src")
      dst = ukey("blmove_wake_dst")

      send_cmd(sock, ["BLMOVE", src, dst, "LEFT", "RIGHT", "5"])

      sock2 = connect_and_hello(context.port)
      Process.sleep(50)
      send_cmd(sock2, ["RPUSH", src, "moved_val"])
      {:ok, _} = recv_response(sock2)

      {:ok, result} = recv_response(sock, 3_000)
      assert result == "moved_val"

      send_cmd(sock, ["LRANGE", dst, "0", "-1"])
      {:ok, dst_contents} = recv_response(sock)
      assert dst_contents == ["moved_val"]

      :gen_tcp.close(sock)
      :gen_tcp.close(sock2)
    end
  end

  # ===========================================================================
  # 6. BLMPOP with COUNT > list length
  # ===========================================================================

  describe "BLMPOP with COUNT > list length" do
    test "non-blocking: returns all elements when COUNT exceeds list length" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)

      result = Blocking.handle("BLMPOP", ["0", "1", "mylist", "LEFT", "COUNT", "10"], store)
      assert result == ["mylist", ["a", "b"]]
    end

    test "non-blocking: returns single element when COUNT is 1" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "x", "y", "z"], store)

      result = Blocking.handle("BLMPOP", ["0", "1", "mylist", "LEFT"], store)
      assert result == ["mylist", ["x"]]
    end

    test "BLMPOP over TCP with COUNT > list length returns all available", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      key = ukey("blmpop_count_exceed")

      send_cmd(sock, ["RPUSH", key, "a", "b", "c"])
      {:ok, 3} = recv_response(sock)

      send_cmd(sock, ["BLMPOP", "0", "1", key, "LEFT", "COUNT", "100"])
      {:ok, result} = recv_response(sock, 3_000)

      assert result == [key, ["a", "b", "c"]]

      :gen_tcp.close(sock)
    end

    test "BLMPOP COUNT 0 is rejected", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      key = ukey("blmpop_count_zero")

      send_cmd(sock, ["RPUSH", key, "a"])
      {:ok, _} = recv_response(sock)

      send_cmd(sock, ["BLMPOP", "0", "1", key, "LEFT", "COUNT", "0"])
      {:ok, result} = recv_response(sock, 3_000)

      assert match?({:error, _}, result)

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # 7. Two clients BLPOP same key -- only one wakes
  # ===========================================================================

  describe "two clients BLPOP same key -- only one wakes" do
    test "single notify_push wakes exactly one waiter (FIFO)" do
      key = ukey("two_waiters")
      me = self()

      waiter1 =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} -> send(me, {:woken, 1})
          after
            2000 -> send(me, {:timeout, 1})
          end
        end)

      Process.sleep(5)

      _waiter2 =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} -> send(me, {:woken, 2})
          after
            2000 -> send(me, {:timeout, 2})
          end
        end)

      Process.sleep(10)

      # One push should wake only waiter1 (FIFO)
      notified = Waiters.notify_push(key)
      assert notified == waiter1

      assert_receive {:woken, 1}, 500
      refute_receive {:woken, 2}, 200
    end

    test "notify_push skips dead stale waiters and wakes the oldest live waiter" do
      key = ukey("dead_waiter_fifo")
      me = self()

      dead_waiter = spawn(fn -> Process.sleep(:infinity) end)
      Waiters.register(key, dead_waiter, 0)
      Process.exit(dead_waiter, :kill)

      live_waiter =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} -> send(me, {:woken, self()})
          after
            1000 -> send(me, {:timeout, self()})
          end
        end)

      Process.sleep(20)

      assert Waiters.notify_push(key) == live_waiter
      assert_receive {:woken, ^live_waiter}, 500
      refute_receive {:timeout, ^live_waiter}, 100
      assert Waiters.count(key) == 0
    end

    test "single RPUSH wakes exactly one blocked client (FIFO) over TCP", context do
      require_tcp!(context)
      key = ukey("two_clients")

      sock1 = connect_and_hello(context.port)
      send_cmd(sock1, ["BLPOP", key, "5"])

      Process.sleep(50)

      sock2 = connect_and_hello(context.port)
      send_cmd(sock2, ["BLPOP", key, "5"])

      Process.sleep(50)

      sock3 = connect_and_hello(context.port)
      send_cmd(sock3, ["RPUSH", key, "prize"])
      {:ok, 1} = recv_response(sock3)

      {:ok, result1} = recv_response(sock1, 3_000)
      assert result1 == [key, "prize"]

      assert {:error, :timeout} = recv_response(sock2, 500)

      :gen_tcp.close(sock1)
      :gen_tcp.close(sock2)
      :gen_tcp.close(sock3)
    end

    test "two RPUSHes wake both clients in FIFO order over TCP", context do
      require_tcp!(context)
      key = ukey("two_clients_both")

      sock1 = connect_and_hello(context.port)
      send_cmd(sock1, ["BLPOP", key, "5"])
      Process.sleep(50)

      sock2 = connect_and_hello(context.port)
      send_cmd(sock2, ["BLPOP", key, "5"])
      Process.sleep(50)

      sock3 = connect_and_hello(context.port)
      send_cmd(sock3, ["RPUSH", key, "val1"])
      {:ok, _} = recv_response(sock3)

      Process.sleep(50)

      send_cmd(sock3, ["RPUSH", key, "val2"])
      {:ok, _} = recv_response(sock3)

      {:ok, result1} = recv_response(sock1, 3_000)
      {:ok, result2} = recv_response(sock2, 3_000)

      assert result1 == [key, "val1"]
      assert result2 == [key, "val2"]

      :gen_tcp.close(sock1)
      :gen_tcp.close(sock2)
      :gen_tcp.close(sock3)
    end
  end

  # ===========================================================================
  # 8. BLPOP then client disconnect -- waiter cleaned up
  # ===========================================================================

  describe "BLPOP then client disconnect -- waiter cleaned up" do
    test "waiter is removed from ETS after client disconnects during block", context do
      require_tcp!(context)
      key = ukey("disconnect_waiter")

      :ets.delete_all_objects(:ferricstore_waiters)

      sock = connect_and_hello(context.port)
      send_cmd(sock, ["BLPOP", key, "30"])

      Process.sleep(100)

      initial_count = Waiters.count(key)
      assert initial_count >= 1, "Expected at least 1 waiter, got #{initial_count}"

      :gen_tcp.close(sock)

      Process.sleep(500)

      final_count = Waiters.count(key)

      assert final_count == 0,
             "Waiter leak detected: #{final_count} waiters remain for key #{key} after disconnect"
    end

    test "Waiters.cleanup removes all entries for a given pid" do
      k1 = ukey("cleanup_k1")
      k2 = ukey("cleanup_k2")
      k3 = ukey("cleanup_k3")

      pid = spawn(fn -> Process.sleep(10_000) end)

      Waiters.register(k1, pid, 0)
      Waiters.register(k2, pid, 0)
      Waiters.register(k3, pid, 0)

      assert Waiters.count(k1) == 1
      assert Waiters.count(k2) == 1
      assert Waiters.count(k3) == 1

      Waiters.cleanup(pid)

      assert Waiters.count(k1) == 0
      assert Waiters.count(k2) == 0
      assert Waiters.count(k3) == 0

      Process.exit(pid, :kill)
    end
  end

  # ===========================================================================
  # 9. Waiters.count accuracy after register/unregister
  # ===========================================================================

  describe "Waiters.count accuracy after register/unregister" do
    test "count is 0 after all registrations are unregistered" do
      key = ukey("count_accuracy")

      pids =
        for _ <- 1..5 do
          pid = spawn(fn -> Process.sleep(10_000) end)
          Waiters.register(key, pid, 0)
          pid
        end

      assert Waiters.count(key) == 5

      Enum.each(pids, fn pid -> Waiters.unregister(key, pid) end)
      assert Waiters.count(key) == 0

      Enum.each(pids, &Process.exit(&1, :kill))
    end

    test "count is correct after partial unregistration" do
      key = ukey("count_partial")

      pids =
        for _ <- 1..4 do
          pid = spawn(fn -> Process.sleep(10_000) end)
          Waiters.register(key, pid, 0)
          pid
        end

      assert Waiters.count(key) == 4

      [p1, p2 | _rest] = pids
      Waiters.unregister(key, p1)
      Waiters.unregister(key, p2)

      assert Waiters.count(key) == 2

      Enum.each(pids, &Process.exit(&1, :kill))
    end

    test "total_count reflects all keys" do
      k1 = ukey("total_k1")
      k2 = ukey("total_k2")

      initial_total = Waiters.total_count()

      p1 = spawn(fn -> Process.sleep(10_000) end)
      p2 = spawn(fn -> Process.sleep(10_000) end)
      p3 = spawn(fn -> Process.sleep(10_000) end)

      Waiters.register(k1, p1, 0)
      Waiters.register(k1, p2, 0)
      Waiters.register(k2, p3, 0)

      assert Waiters.total_count() == initial_total + 3

      Waiters.cleanup(p1)
      assert Waiters.total_count() == initial_total + 2

      Waiters.cleanup(p2)
      Waiters.cleanup(p3)
      assert Waiters.total_count() == initial_total

      Process.exit(p1, :kill)
      Process.exit(p2, :kill)
      Process.exit(p3, :kill)
    end

    test "count is accurate when same pid registers on multiple keys" do
      k1 = ukey("multi_key_a")
      k2 = ukey("multi_key_b")

      pid = spawn(fn -> Process.sleep(10_000) end)

      Waiters.register(k1, pid, 0)
      Waiters.register(k2, pid, 0)

      assert Waiters.count(k1) == 1
      assert Waiters.count(k2) == 1

      Waiters.cleanup(pid)
      assert Waiters.count(k1) == 0
      assert Waiters.count(k2) == 0

      Process.exit(pid, :kill)
    end

    test "notify_push decrements count by exactly 1" do
      key = ukey("notify_count")

      p1 = spawn(fn -> Process.sleep(10_000) end)
      p2 = spawn(fn -> Process.sleep(10_000) end)

      Waiters.register(key, p1, 0)
      Process.sleep(1)
      Waiters.register(key, p2, 0)

      assert Waiters.count(key) == 2

      Waiters.notify_push(key)
      assert Waiters.count(key) == 1

      Waiters.notify_push(key)
      assert Waiters.count(key) == 0

      Process.exit(p1, :kill)
      Process.exit(p2, :kill)
    end
  end

  # ===========================================================================
  # 10. BLPOP with negative timeout -- error
  # ===========================================================================

  describe "BLPOP with negative timeout -- error" do
    test "parse_blpop_args rejects negative integer timeout" do
      assert {:error, "ERR timeout is negative"} = Blocking.parse_blpop_args(["key", "-1"])
    end

    test "parse_blpop_args rejects negative float timeout" do
      assert {:error, "ERR timeout is negative"} = Blocking.parse_blpop_args(["key", "-0.5"])
    end

    test "parse_blpop_args rejects large negative timeout" do
      assert {:error, "ERR timeout is negative"} = Blocking.parse_blpop_args(["key", "-999"])
    end

    test "BLPOP with negative timeout returns error over TCP", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      key = ukey("neg_timeout")

      send_cmd(sock, ["BLPOP", key, "-1"])
      {:ok, result} = recv_response(sock, 3_000)

      assert match?({:error, _msg}, result)

      send_cmd(sock, ["PING"])
      {:ok, pong} = recv_response(sock)
      assert pong == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "BRPOP with negative timeout returns error over TCP", context do
      require_tcp!(context)
      sock = connect_and_hello(context.port)
      key = ukey("neg_timeout_brpop")

      send_cmd(sock, ["BRPOP", key, "-5"])
      {:ok, result} = recv_response(sock, 3_000)

      assert match?({:error, _msg}, result)

      :gen_tcp.close(sock)
    end

    test "BLMOVE with negative timeout returns error" do
      assert {:error, "ERR timeout is negative"} =
               Blocking.parse_blmove_args(["src", "dst", "LEFT", "RIGHT", "-1"])
    end

    test "BLMPOP with negative timeout returns error" do
      assert {:error, "ERR timeout is negative"} =
               Blocking.parse_blmpop_args(["-1", "1", "mylist", "LEFT"])
    end
  end
end
