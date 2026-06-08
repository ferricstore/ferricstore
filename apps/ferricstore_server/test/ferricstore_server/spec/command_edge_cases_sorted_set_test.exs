defmodule FerricstoreServer.Spec.CommandEdgeCasesSortedSetTest do
  @moduledoc """
  Edge-case tests for SORTED SET commands over TCP/RESP3.
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
  # SORTED SET COMMANDS EDGE CASES
  # ===========================================================================

  describe "Sorted set edge cases" do
    test "ZADD with non-numeric score returns error", %{sock: sock} do
      k = ukey("zadd_nan")
      result = cmd(sock, ["ZADD", k, "notanumber", "member"])
      assert_error_contains(result, "not a valid float")
    end

    test "ZADD with integer score succeeds", %{sock: sock} do
      k = ukey("zadd_int")
      assert cmd(sock, ["ZADD", k, "5", "member"]) == 1
      score = cmd(sock, ["ZSCORE", k, "member"])
      assert is_binary(score)
      {val, ""} = Float.parse(score)
      assert val == 5.0
    end

    test "ZSCORE on non-existent member returns nil", %{sock: sock} do
      k = ukey("zscore_nil")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZSCORE", k, "missing"]) == nil
    end

    test "ZSCORE on non-existent key returns nil", %{sock: sock} do
      k = ukey("zscore_nil_key")
      assert cmd(sock, ["ZSCORE", k, "member"]) == nil
    end

    test "ZRANK on non-existent member returns nil", %{sock: sock} do
      k = ukey("zrank_nil")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZRANK", k, "missing"]) == nil
    end

    test "ZRANK on non-existent key returns nil", %{sock: sock} do
      k = ukey("zrank_nil_key")
      assert cmd(sock, ["ZRANK", k, "member"]) == nil
    end

    test "ZREVRANK returns correct reverse rank", %{sock: sock} do
      k = ukey("zrevrank")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      assert cmd(sock, ["ZREVRANK", k, "c"]) == 0
      assert cmd(sock, ["ZREVRANK", k, "a"]) == 2
    end

    test "ZREVRANK on non-existent member returns nil", %{sock: sock} do
      k = ukey("zrevrank_nil")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZREVRANK", k, "missing"]) == nil
    end

    test "ZCARD on non-existent key returns 0", %{sock: sock} do
      k = ukey("zcard_nil")
      assert cmd(sock, ["ZCARD", k]) == 0
    end

    test "ZREM non-existent member returns 0", %{sock: sock} do
      k = ukey("zrem_nil")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZREM", k, "missing"]) == 0
    end

    test "ZINCRBY on non-existent key creates sorted set", %{sock: sock} do
      k = ukey("zincrby_nil")
      result = cmd(sock, ["ZINCRBY", k, "5", "member"])
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert val == 5.0
    end

    test "ZINCRBY with non-numeric increment returns error", %{sock: sock} do
      k = ukey("zincrby_nan")
      result = cmd(sock, ["ZINCRBY", k, "abc", "member"])
      assert_error_contains(result, "not a valid float")
    end

    test "ZRANGEBYSCORE with inverted min/max returns empty", %{sock: sock} do
      k = ukey("zrangebyscore_inv")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      assert cmd(sock, ["ZRANGEBYSCORE", k, "3", "1"]) == []
    end

    test "ZRANGEBYSCORE with -inf/+inf returns all", %{sock: sock} do
      k = ukey("zrangebyscore_inf")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      result = cmd(sock, ["ZRANGEBYSCORE", k, "-inf", "+inf"])
      assert result == ["a", "b", "c"]
    end

    test "ZRANGEBYSCORE with exclusive bounds using '(' prefix", %{sock: sock} do
      k = ukey("zrangebyscore_excl")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      result = cmd(sock, ["ZRANGEBYSCORE", k, "(1", "(3"])
      assert result == ["b"]
    end

    test "ZRANGEBYSCORE with non-numeric min returns error", %{sock: sock} do
      k = ukey("zrangebyscore_nan")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      result = cmd(sock, ["ZRANGEBYSCORE", k, "abc", "10"])
      assert_error_contains(result, "not a float")
    end

    test "ZCOUNT with -inf/+inf", %{sock: sock} do
      k = ukey("zcount_inf")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      assert cmd(sock, ["ZCOUNT", k, "-inf", "+inf"]) == 3
    end

    test "ZCOUNT on non-existent key returns 0", %{sock: sock} do
      k = ukey("zcount_nil")
      assert cmd(sock, ["ZCOUNT", k, "-inf", "+inf"]) == 0
    end

    test "ZRANGE with start > end returns empty", %{sock: sock} do
      k = ukey("zrange_inv")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZRANGE", k, "5", "1"]) == []
    end

    test "ZRANGE with WITHSCORES", %{sock: sock} do
      k = ukey("zrange_ws")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b"])
      result = cmd(sock, ["ZRANGE", k, "0", "-1", "WITHSCORES"])
      assert is_list(result)
      assert length(result) == 4
      assert Enum.at(result, 0) == "a"
      assert Enum.at(result, 2) == "b"
    end

    test "ZRANGE with negative indices", %{sock: sock} do
      k = ukey("zrange_neg")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b", "3.0", "c"])
      result = cmd(sock, ["ZRANGE", k, "-2", "-1"])
      assert result == ["b", "c"]
    end

    test "ZPOPMIN on non-existent key returns empty", %{sock: sock} do
      k = ukey("zpopmin_nil")
      assert cmd(sock, ["ZPOPMIN", k]) == []
    end

    test "ZPOPMAX on non-existent key returns empty", %{sock: sock} do
      k = ukey("zpopmax_nil")
      assert cmd(sock, ["ZPOPMAX", k]) == []
    end

    test "ZPOPMIN removes element with lowest score", %{sock: sock} do
      k = ukey("zpopmin_ok")
      cmd(sock, ["ZADD", k, "3.0", "c", "1.0", "a", "2.0", "b"])
      result = cmd(sock, ["ZPOPMIN", k])
      assert is_list(result)
      assert length(result) == 2
      assert Enum.at(result, 0) == "a"
    end

    test "ZPOPMAX removes element with highest score", %{sock: sock} do
      k = ukey("zpopmax_ok")
      cmd(sock, ["ZADD", k, "3.0", "c", "1.0", "a", "2.0", "b"])
      result = cmd(sock, ["ZPOPMAX", k])
      assert is_list(result)
      assert length(result) == 2
      assert Enum.at(result, 0) == "c"
    end

    test "ZADD NX only adds new, does not update existing", %{sock: sock} do
      k = ukey("zadd_nx")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["ZADD", k, "NX", "5.0", "a"]) == 0
      score = cmd(sock, ["ZSCORE", k, "a"])
      {val, ""} = Float.parse(score)
      assert val == 1.0
    end

    test "ZADD XX only updates existing, does not add new", %{sock: sock} do
      k = ukey("zadd_xx")
      assert cmd(sock, ["ZADD", k, "XX", "5.0", "new_member"]) == 0
      assert cmd(sock, ["ZSCORE", k, "new_member"]) == nil
    end

    test "ZADD GT only updates if new score > existing", %{sock: sock} do
      k = ukey("zadd_gt")
      cmd(sock, ["ZADD", k, "5.0", "a"])
      cmd(sock, ["ZADD", k, "GT", "3.0", "a"])
      score = cmd(sock, ["ZSCORE", k, "a"])
      {val, ""} = Float.parse(score)
      assert val == 5.0

      cmd(sock, ["ZADD", k, "GT", "10.0", "a"])
      score2 = cmd(sock, ["ZSCORE", k, "a"])
      {val2, ""} = Float.parse(score2)
      assert val2 == 10.0
    end

    test "ZADD LT only updates if new score < existing", %{sock: sock} do
      k = ukey("zadd_lt")
      cmd(sock, ["ZADD", k, "5.0", "a"])
      cmd(sock, ["ZADD", k, "LT", "10.0", "a"])
      score = cmd(sock, ["ZSCORE", k, "a"])
      {val, ""} = Float.parse(score)
      assert val == 5.0

      cmd(sock, ["ZADD", k, "LT", "1.0", "a"])
      score2 = cmd(sock, ["ZSCORE", k, "a"])
      {val2, ""} = Float.parse(score2)
      assert val2 == 1.0
    end

    test "ZADD CH returns added + changed count", %{sock: sock} do
      k = ukey("zadd_ch")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      result = cmd(sock, ["ZADD", k, "CH", "2.0", "a", "3.0", "b"])
      assert result == 2
    end

    test "ZMSCORE with mix of existing and non-existing members", %{sock: sock} do
      k = ukey("zmscore_mix")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b"])
      result = cmd(sock, ["ZMSCORE", k, "a", "missing", "b"])
      assert is_list(result)
      assert length(result) == 3
      assert Enum.at(result, 1) == nil
    end

    test "ZRANDMEMBER on non-existent key returns nil", %{sock: sock} do
      k = ukey("zrandmember_nil")
      assert cmd(sock, ["ZRANDMEMBER", k]) == nil
    end

    test "ZRANDMEMBER with negative count allows duplicates", %{sock: sock} do
      k = ukey("zrandmember_neg")
      cmd(sock, ["ZADD", k, "1.0", "only"])
      result = cmd(sock, ["ZRANDMEMBER", k, "-5"])
      assert is_list(result)
      assert length(result) == 5
    end

    test "ZADD wrong arity", %{sock: sock} do
      result = cmd(sock, ["ZADD", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "ZADD odd score-member pairs returns error", %{sock: sock} do
      k = ukey("zadd_odd")
      result = cmd(sock, ["ZADD", k, "1.0"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "DEL removes sorted set key", %{sock: sock} do
      k = ukey("del_zset")
      cmd(sock, ["ZADD", k, "1.0", "a", "2.0", "b"])
      assert cmd(sock, ["DEL", k]) == 1
      assert cmd(sock, ["ZCARD", k]) == 0
    end
  end
end
