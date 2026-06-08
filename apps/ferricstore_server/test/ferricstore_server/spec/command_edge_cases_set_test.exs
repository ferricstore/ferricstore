defmodule FerricstoreServer.Spec.CommandEdgeCasesSetTest do
  @moduledoc """
  Edge-case tests for SET commands over TCP/RESP3.
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
        :binary,
        active: false,
        packet: :raw,
        nodelay: true,
        recbuf: 4 * 1024 * 1024,
        sndbuf: 4 * 1024 * 1024
      ])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_direct(sock, "", 5_000)
    sock
  end

  defp recv_direct(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | _], _rest} ->
        val

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

      [] ->
        recv_loop(sock, Process.get(@binary_key, ""), timeout)
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

    assert String.contains?(msg, substring),
           "Expected error containing #{inspect(substring)}, got: #{inspect(msg)}"
  end

  # ===========================================================================
  # SET COMMANDS EDGE CASES
  # ===========================================================================

  describe "Set edge cases" do
    test "SMEMBERS on non-existent key returns empty list", %{sock: sock} do
      k = ukey("smembers_nil")
      assert cmd(sock, ["SMEMBERS", k]) == []
    end

    test "SISMEMBER on non-existent key returns 0", %{sock: sock} do
      k = ukey("sismember_nil")
      assert cmd(sock, ["SISMEMBER", k, "member"]) == 0
    end

    test "SREM non-existent member returns 0", %{sock: sock} do
      k = ukey("srem_nil")
      cmd(sock, ["SADD", k, "a"])
      assert cmd(sock, ["SREM", k, "nonexistent"]) == 0
    end

    test "SREM on non-existent key returns 0", %{sock: sock} do
      k = ukey("srem_nil_key")
      assert cmd(sock, ["SREM", k, "member"]) == 0
    end

    test "SCARD on non-existent key returns 0", %{sock: sock} do
      k = ukey("scard_nil")
      assert cmd(sock, ["SCARD", k]) == 0
    end

    test "SADD duplicate member returns 0 (not added)", %{sock: sock} do
      k = ukey("sadd_dup")
      cmd(sock, ["SADD", k, "a"])
      assert cmd(sock, ["SADD", k, "a"]) == 0
      assert cmd(sock, ["SCARD", k]) == 1
    end

    test "SRANDMEMBER on non-existent key returns nil", %{sock: sock} do
      k = ukey("srandmember_nil")
      assert cmd(sock, ["SRANDMEMBER", k]) == nil
    end

    test "SRANDMEMBER with count > set size returns all (no dup)", %{sock: sock} do
      k = ukey("srandmember_big")
      cmd(sock, ["SADD", k, "a", "b"])
      result = cmd(sock, ["SRANDMEMBER", k, "10"])
      assert is_list(result)
      assert length(result) == 2
      assert Enum.sort(result) == ["a", "b"]
    end

    test "SRANDMEMBER with negative count allows duplicates", %{sock: sock} do
      k = ukey("srandmember_neg")
      cmd(sock, ["SADD", k, "only"])
      result = cmd(sock, ["SRANDMEMBER", k, "-5"])
      assert is_list(result)
      assert length(result) == 5
      assert Enum.all?(result, &(&1 == "only"))
    end

    test "SRANDMEMBER with count 0 returns empty list", %{sock: sock} do
      k = ukey("srandmember_zero")
      cmd(sock, ["SADD", k, "a"])
      assert cmd(sock, ["SRANDMEMBER", k, "0"]) == []
    end

    test "SPOP from non-existent key returns nil", %{sock: sock} do
      k = ukey("spop_nil")
      assert cmd(sock, ["SPOP", k]) == nil
    end

    test "SPOP with count returns list", %{sock: sock} do
      k = ukey("spop_count")
      cmd(sock, ["SADD", k, "a", "b", "c"])
      result = cmd(sock, ["SPOP", k, "2"])
      assert is_list(result)
      assert length(result) == 2
    end

    test "SMISMEMBER returns correct membership flags", %{sock: sock} do
      k = ukey("smismember")
      cmd(sock, ["SADD", k, "a", "c"])
      assert cmd(sock, ["SMISMEMBER", k, "a", "b", "c"]) == [1, 0, 1]
    end

    test "SINTER with non-existent key returns empty", %{sock: sock} do
      k1 = ukey("sinter_a")
      k2 = ukey("sinter_nil")
      cmd(sock, ["SADD", k1, "a", "b"])
      result = cmd(sock, ["SINTER", k1, k2])
      assert result == []
    end

    test "SUNION with non-existent key returns other set", %{sock: sock} do
      k1 = ukey("sunion_a")
      k2 = ukey("sunion_nil")
      cmd(sock, ["SADD", k1, "a", "b"])
      result = cmd(sock, ["SUNION", k1, k2])
      assert Enum.sort(result) == ["a", "b"]
    end

    test "SDIFF with non-existent key returns first set", %{sock: sock} do
      k1 = ukey("sdiff_a")
      k2 = ukey("sdiff_nil")
      cmd(sock, ["SADD", k1, "a", "b"])
      result = cmd(sock, ["SDIFF", k1, k2])
      assert Enum.sort(result) == ["a", "b"]
    end

    test "SMOVE between sets", %{sock: sock} do
      k1 = ukey("smove_src")
      k2 = ukey("smove_dst")
      cmd(sock, ["SADD", k1, "a", "b"])
      cmd(sock, ["SADD", k2, "c"])
      assert cmd(sock, ["SMOVE", k1, k2, "a"]) == 1
      assert cmd(sock, ["SISMEMBER", k1, "a"]) == 0
      assert cmd(sock, ["SISMEMBER", k2, "a"]) == 1
    end

    test "SMOVE non-existent member returns 0", %{sock: sock} do
      k1 = ukey("smove_nil")
      k2 = ukey("smove_dst2")
      cmd(sock, ["SADD", k1, "a"])
      assert cmd(sock, ["SMOVE", k1, k2, "nonexistent"]) == 0
    end

    test "SADD wrong arity", %{sock: sock} do
      result = cmd(sock, ["SADD", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "SREM wrong arity", %{sock: sock} do
      result = cmd(sock, ["SREM", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "DEL removes set key", %{sock: sock} do
      k = ukey("del_set")
      cmd(sock, ["SADD", k, "a", "b"])
      assert cmd(sock, ["DEL", k]) == 1
      assert cmd(sock, ["SMEMBERS", k]) == []
    end
  end
end
