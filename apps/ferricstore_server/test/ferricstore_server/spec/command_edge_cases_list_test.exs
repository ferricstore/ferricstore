defmodule FerricstoreServer.Spec.CommandEdgeCasesListTest do
  @moduledoc """
  Edge-case tests for LIST commands over TCP/RESP3.
  Split from CommandEdgeCasesComprehensiveTest.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

  @moduletag timeout: 120_000

  setup_all do
    ShardHelpers.wait_shards_alive()
    %{port: Listener.port()}
  end

  setup %{port: port} do
    Process.delete(:edge_parsed_queue)
    Process.delete(:edge_binary_buf)

    sock = connect_and_hello(port)
    ShardHelpers.flush_all_keys()
    on_exit(fn -> :gen_tcp.close(sock) end)
    %{sock: sock, port: port}
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [
        :binary, active: false, packet: :raw, nodelay: true,
        recbuf: 4 * 1024 * 1024, sndbuf: 4 * 1024 * 1024
      ])
    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_direct(sock, "", 5_000)
    sock
  end

  defp recv_direct(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | _], _rest} -> val
      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_direct(sock, buf <> data, timeout)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  defp send_cmd(sock, cmd) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(cmd)))
  end

  @parsed_key :edge_parsed_queue
  @binary_key :edge_binary_buf

  defp recv_response(sock, timeout \\ 10_000) do
    case Process.get(@parsed_key, []) do
      [val | rest] ->
        Process.put(@parsed_key, rest)
        val
      [] -> recv_loop(sock, Process.get(@binary_key, ""), timeout)
    end
  end

  defp recv_loop(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | rest_vals], rest_bin} ->
        Process.put(@parsed_key, rest_vals)
        Process.put(@binary_key, rest_bin)
        val
      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_loop(sock, buf <> data, timeout)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  defp cmd(sock, args) do
    send_cmd(sock, args)
    recv_response(sock)
  end

  defp ukey(base), do: "{edgetest}:#{base}_#{:rand.uniform(9_999_999)}"

  defp assert_error_contains(result, substring) do
    assert {:error, msg} = result
    assert String.contains?(msg, substring), "Expected error containing #{inspect(substring)}, got: #{inspect(msg)}"
  end

  # ===========================================================================
  # LIST COMMANDS EDGE CASES
  # ===========================================================================

  describe "List edge cases" do
    test "LPUSH creates list from scratch", %{sock: sock} do
      k = ukey("lpush_new")
      assert cmd(sock, ["LPUSH", k, "a", "b", "c"]) == 3
    end

    test "RPUSH creates list from scratch", %{sock: sock} do
      k = ukey("rpush_new")
      assert cmd(sock, ["RPUSH", k, "a", "b"]) == 2
    end

    test "LRANGE on non-existent key returns empty list", %{sock: sock} do
      k = ukey("lrange_nil")
      assert cmd(sock, ["LRANGE", k, "0", "-1"]) == []
    end

    test "LRANGE with start > end returns empty list", %{sock: sock} do
      k = ukey("lrange_inverted")
      cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert cmd(sock, ["LRANGE", k, "5", "1"]) == []
    end

    test "LRANGE with indices beyond list length clamps to bounds", %{sock: sock} do
      k = ukey("lrange_oob")
      cmd(sock, ["RPUSH", k, "a", "b"])
      assert cmd(sock, ["LRANGE", k, "0", "100"]) == ["a", "b"]
    end

    test "LINDEX out of bounds returns nil", %{sock: sock} do
      k = ukey("lindex_oob")
      cmd(sock, ["RPUSH", k, "a", "b"])
      assert cmd(sock, ["LINDEX", k, "100"]) == nil
    end

    test "LINDEX negative index", %{sock: sock} do
      k = ukey("lindex_neg")
      cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert cmd(sock, ["LINDEX", k, "-1"]) == "c"
      assert cmd(sock, ["LINDEX", k, "-3"]) == "a"
    end

    test "LINDEX negative index beyond list returns nil", %{sock: sock} do
      k = ukey("lindex_neg_oob")
      cmd(sock, ["RPUSH", k, "a"])
      assert cmd(sock, ["LINDEX", k, "-100"]) == nil
    end

    test "LPOP from non-existent key returns nil", %{sock: sock} do
      k = ukey("lpop_nil")
      assert cmd(sock, ["LPOP", k]) == nil
    end

    test "RPOP from non-existent key returns nil", %{sock: sock} do
      k = ukey("rpop_nil")
      assert cmd(sock, ["RPOP", k]) == nil
    end

    test "LPOP from empty list (all elements popped) returns nil", %{sock: sock} do
      k = ukey("lpop_empty")
      cmd(sock, ["RPUSH", k, "a"])
      cmd(sock, ["LPOP", k])
      assert cmd(sock, ["LPOP", k]) == nil
    end

    test "LPOP with count returns list", %{sock: sock} do
      k = ukey("lpop_count")
      cmd(sock, ["RPUSH", k, "a", "b", "c"])
      result = cmd(sock, ["LPOP", k, "2"])
      assert result == ["a", "b"]
    end

    test "LPOP with count > list length returns available elements", %{sock: sock} do
      k = ukey("lpop_count_big")
      cmd(sock, ["RPUSH", k, "a"])
      result = cmd(sock, ["LPOP", k, "100"])
      assert result == ["a"]
    end

    test "LPOP with count 0 returns empty list", %{sock: sock} do
      k = ukey("lpop_count_zero")
      cmd(sock, ["RPUSH", k, "a"])
      result = cmd(sock, ["LPOP", k, "0"])
      assert result == []
    end

    test "LLEN on non-existent key returns 0", %{sock: sock} do
      k = ukey("llen_nil")
      assert cmd(sock, ["LLEN", k]) == 0
    end

    test "LSET out of bounds returns error", %{sock: sock} do
      k = ukey("lset_oob")
      cmd(sock, ["RPUSH", k, "a"])
      result = cmd(sock, ["LSET", k, "5", "x"])
      assert_error_contains(result, "index out of range")
    end

    test "LMOVE with non-existent source returns nil", %{sock: sock} do
      k1 = ukey("lmove_nil_src")
      k2 = ukey("lmove_nil_dst")
      assert cmd(sock, ["LMOVE", k1, k2, "LEFT", "RIGHT"]) == nil
    end

    test "RPOPLPUSH with same src/dst rotates the list", %{sock: sock} do
      k = ukey("rpoplpush_self")
      cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert cmd(sock, ["RPOPLPUSH", k, k]) == "c"
      assert cmd(sock, ["LRANGE", k, "0", "-1"]) == ["c", "a", "b"]
    end

    test "LPUSHX on non-existent key does nothing", %{sock: sock} do
      k = ukey("lpushx_nil")
      assert cmd(sock, ["LPUSHX", k, "a"]) == 0
      assert cmd(sock, ["LLEN", k]) == 0
    end

    test "RPUSHX on non-existent key does nothing", %{sock: sock} do
      k = ukey("rpushx_nil")
      assert cmd(sock, ["RPUSHX", k, "a"]) == 0
    end

    test "LREM removes correct count of elements", %{sock: sock} do
      k = ukey("lrem_multi")
      cmd(sock, ["RPUSH", k, "a", "b", "a", "c", "a"])
      assert cmd(sock, ["LREM", k, "2", "a"]) == 2
      assert cmd(sock, ["LRANGE", k, "0", "-1"]) == ["b", "c", "a"]
    end

    test "LINSERT BEFORE pivot", %{sock: sock} do
      k = ukey("linsert_before")
      cmd(sock, ["RPUSH", k, "a", "c"])
      assert cmd(sock, ["LINSERT", k, "BEFORE", "c", "b"]) == 3
      assert cmd(sock, ["LRANGE", k, "0", "-1"]) == ["a", "b", "c"]
    end

    test "LINSERT with non-existent pivot returns -1", %{sock: sock} do
      k = ukey("linsert_nopivot")
      cmd(sock, ["RPUSH", k, "a", "b"])
      assert cmd(sock, ["LINSERT", k, "BEFORE", "z", "x"]) == -1
    end

    test "LTRIM beyond list length empties it", %{sock: sock} do
      k = ukey("ltrim_beyond")
      cmd(sock, ["RPUSH", k, "a", "b"])
      cmd(sock, ["LTRIM", k, "5", "10"])
      assert cmd(sock, ["LLEN", k]) == 0
    end

    test "LPUSH wrong arity", %{sock: sock} do
      result = cmd(sock, ["LPUSH", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "LRANGE wrong arity", %{sock: sock} do
      result = cmd(sock, ["LRANGE", "k", "0"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end
end
