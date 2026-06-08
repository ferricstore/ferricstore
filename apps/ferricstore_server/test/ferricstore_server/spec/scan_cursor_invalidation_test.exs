defmodule FerricstoreServer.Spec.ScanCursorInvalidationTest do
  @moduledoc """
  Spec section 2G.5: SCAN cursor invalidation tests.

  Verifies that SCAN cursors are invalidated (or effectively reset) after
  FLUSHDB. The spec requires that a cursor obtained before FLUSHDB should
  not continue returning stale keys after the flush.

  Tests:
    - Start a SCAN, get cursor, FLUSHDB, continue SCAN -> returns empty or resets
    - SCAN after FLUSHDB returns cursor "0" with empty results
    - Full SCAN iteration consistency before and after FLUSHDB
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag timeout: 90_000

  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 30_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp ukey(base), do: "scan_inv_#{base}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup do
    ShardHelpers.flush_all_keys()
    ShardHelpers.flush_global_state()

    on_exit(fn ->
      ShardHelpers.reset_server_auth_state()
      ShardHelpers.reset_memory_guard_pressure()
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Section 2G.5: SCAN cursor invalidation on FLUSHDB
  # ---------------------------------------------------------------------------

  describe "SCAN cursor invalidation on FLUSHDB (spec 2G.5)" do
    test "SCAN after FLUSHDB returns no keys from pre-flush data", %{port: port} do
      sock = connect_and_hello(port)

      # Populate keys
      for i <- 1..20 do
        k = ukey("flush_#{i}")
        send_cmd(sock, ["SET", k, "val_#{i}"])
        assert recv_response(sock) == {:simple, "OK"}
      end

      # Start a SCAN with small COUNT to get a partial result
      send_cmd(sock, ["SCAN", "0", "COUNT", "5", "MATCH", "scan_inv_*"])
      [cursor, first_batch] = recv_response(sock)

      assert is_binary(cursor)
      assert is_list(first_batch)
      # Should have returned some keys
      assert first_batch != []

      # Now FLUSHDB
      send_cmd(sock, ["FLUSHDB"])
      assert recv_response(sock) == {:simple, "OK"}

      # Continue SCAN with the old cursor — should return empty or reset
      send_cmd(sock, ["SCAN", cursor, "COUNT", "100", "MATCH", "scan_inv_*"])
      [next_cursor, next_batch] = recv_response(sock)

      # After FLUSHDB, there are no keys. The SCAN implementation re-evaluates
      # the key set on each call, so continuing with an old cursor over an empty
      # keyspace should return no keys and cursor "0".
      assert next_batch == []
      assert next_cursor == "0"

      :gen_tcp.close(sock)
    end

    test "SCAN from cursor 0 after FLUSHDB returns empty", %{port: port} do
      sock = connect_and_hello(port)

      # Populate and then flush
      for i <- 1..10 do
        k = ukey("empty_#{i}")
        send_cmd(sock, ["SET", k, "v_#{i}"])
        assert recv_response(sock) == {:simple, "OK"}
      end

      send_cmd(sock, ["FLUSHDB"])
      assert recv_response(sock) == {:simple, "OK"}

      # SCAN from 0 should immediately find nothing
      send_cmd(sock, ["SCAN", "0", "MATCH", "scan_inv_*"])
      [cursor, batch] = recv_response(sock)

      assert cursor == "0"
      assert batch == []

      :gen_tcp.close(sock)
    end

    test "new keys added after FLUSHDB are visible to SCAN", %{port: port} do
      sock = connect_and_hello(port)

      # Populate, flush, then add new keys
      for i <- 1..5 do
        send_cmd(sock, ["SET", ukey("pre_#{i}"), "old"])
        assert recv_response(sock) == {:simple, "OK"}
      end

      send_cmd(sock, ["FLUSHDB"])
      assert recv_response(sock) == {:simple, "OK"}

      # Add new keys
      new_keys =
        for i <- 1..3 do
          k = ukey("post_#{i}")
          send_cmd(sock, ["SET", k, "new_#{i}"])
          assert recv_response(sock) == {:simple, "OK"}
          k
        end

      # Full SCAN should find exactly the new keys
      all_keys = scan_all(sock, "scan_inv_*")

      # The new keys should be found
      Enum.each(new_keys, fn k ->
        assert k in all_keys,
               "Expected key #{inspect(k)} in scan results: #{inspect(all_keys)}"
      end)

      # No pre-flush keys should appear
      all_keys
      |> Enum.filter(&String.contains?(&1, "_pre_"))
      |> then(fn stale ->
        assert stale == [],
               "Pre-flush keys should not appear after FLUSHDB: #{inspect(stale)}"
      end)

      :gen_tcp.close(sock)
    end

    test "FLUSHDB mid-iteration resets effective cursor", %{port: port} do
      sock = connect_and_hello(port)

      # Create enough keys to require multiple SCAN iterations
      for i <- 1..30 do
        send_cmd(sock, [
          "SET",
          ukey("mid_#{String.pad_leading(Integer.to_string(i), 3, "0")}"),
          "v"
        ])

        assert recv_response(sock) == {:simple, "OK"}
      end

      # First SCAN pass — get a non-zero cursor
      send_cmd(sock, ["SCAN", "0", "COUNT", "5", "MATCH", "scan_inv_mid_*"])
      [cursor1, batch1] = recv_response(sock)
      assert batch1 != []

      # FLUSHDB
      send_cmd(sock, ["FLUSHDB"])
      assert recv_response(sock) == {:simple, "OK"}

      # Add a few new keys with a different sub-prefix
      for i <- 1..3 do
        send_cmd(sock, ["SET", ukey("new_mid_#{i}"), "fresh"])
        assert recv_response(sock) == {:simple, "OK"}
      end

      # Continue with old cursor — should not return any old "mid_" keys
      send_cmd(sock, ["SCAN", cursor1, "COUNT", "100", "MATCH", "scan_inv_mid_*"])
      [_cursor2, batch2] = recv_response(sock)

      # Any keys in batch2 must be from the post-flush set (none should match "mid_" prefix
      # since the old mid_ keys were flushed and only "new_mid_" exist)
      # Actually, old "mid_" keys are gone, so this should return empty
      assert batch2 == []

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Helper: full SCAN iteration
  # ---------------------------------------------------------------------------

  defp scan_all(sock, match_pattern) do
    scan_all(sock, "0", match_pattern, [])
  end

  defp scan_all(sock, cursor, match_pattern, acc) do
    send_cmd(sock, ["SCAN", cursor, "MATCH", match_pattern, "COUNT", "100"])
    [next_cursor, batch] = recv_response(sock)

    new_acc = acc ++ batch

    if next_cursor == "0" do
      new_acc
    else
      scan_all(sock, next_cursor, match_pattern, new_acc)
    end
  end
end
