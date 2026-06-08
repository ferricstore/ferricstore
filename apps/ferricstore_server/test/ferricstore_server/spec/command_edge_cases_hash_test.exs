defmodule FerricstoreServer.Spec.CommandEdgeCasesHashTest do
  @moduledoc """
  Edge-case tests for HASH commands over TCP/RESP3.
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
  # HASH COMMANDS EDGE CASES
  # ===========================================================================

  describe "Hash edge cases" do
    test "HSET with empty field name succeeds", %{sock: sock} do
      k = ukey("hset_empty_field")
      assert cmd(sock, ["HSET", k, "", "val"]) == 1
      assert cmd(sock, ["HGET", k, ""]) == "val"
    end

    test "HGET non-existent field returns nil", %{sock: sock} do
      k = ukey("hget_nil_field")
      cmd(sock, ["HSET", k, "f1", "v1"])
      assert cmd(sock, ["HGET", k, "missing"]) == nil
    end

    test "HGET on non-existent key returns nil", %{sock: sock} do
      k = ukey("hget_nil_key")
      assert cmd(sock, ["HGET", k, "field"]) == nil
    end

    test "HDEL non-existent field returns 0", %{sock: sock} do
      k = ukey("hdel_nil_field")
      cmd(sock, ["HSET", k, "f1", "v1"])
      assert cmd(sock, ["HDEL", k, "missing"]) == 0
    end

    test "HDEL on non-existent key returns 0", %{sock: sock} do
      k = ukey("hdel_nil_key")
      assert cmd(sock, ["HDEL", k, "field"]) == 0
    end

    test "HINCRBY on non-numeric field returns error", %{sock: sock} do
      k = ukey("hincrby_nan")
      cmd(sock, ["HSET", k, "f1", "hello"])
      result = cmd(sock, ["HINCRBY", k, "f1", "1"])
      assert_error_contains(result, "not an integer")
    end

    test "HINCRBY on non-existent key creates hash with field=increment", %{sock: sock} do
      k = ukey("hincrby_nil")
      assert cmd(sock, ["HINCRBY", k, "f1", "5"]) == 5
      assert cmd(sock, ["HGET", k, "f1"]) == "5"
    end

    test "HINCRBY on non-existent field initializes to 0", %{sock: sock} do
      k = ukey("hincrby_nil_field")
      cmd(sock, ["HSET", k, "other", "x"])
      assert cmd(sock, ["HINCRBY", k, "newfield", "10"]) == 10
    end

    test "HINCRBYFLOAT on non-numeric field returns error", %{sock: sock} do
      k = ukey("hincrbyfloat_nan")
      cmd(sock, ["HSET", k, "f1", "hello"])
      result = cmd(sock, ["HINCRBYFLOAT", k, "f1", "1.5"])
      assert_error_contains(result, "not a valid float")
    end

    test "HINCRBYFLOAT on non-existent field initializes to 0", %{sock: sock} do
      k = ukey("hincrbyfloat_nil")
      result = cmd(sock, ["HINCRBYFLOAT", k, "f1", "2.5"])
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert val == 2.5
    end

    test "HGETALL on non-existent key returns empty list", %{sock: sock} do
      k = ukey("hgetall_nil")
      assert cmd(sock, ["HGETALL", k]) == []
    end

    test "HLEN on non-existent key returns 0", %{sock: sock} do
      k = ukey("hlen_nil")
      assert cmd(sock, ["HLEN", k]) == 0
    end

    test "HEXISTS on non-existent key returns 0", %{sock: sock} do
      k = ukey("hexists_nil")
      assert cmd(sock, ["HEXISTS", k, "field"]) == 0
    end

    test "HKEYS on non-existent key returns empty list", %{sock: sock} do
      k = ukey("hkeys_nil")
      assert cmd(sock, ["HKEYS", k]) == []
    end

    test "HVALS on non-existent key returns empty list", %{sock: sock} do
      k = ukey("hvals_nil")
      assert cmd(sock, ["HVALS", k]) == []
    end

    test "HSETNX on existing field returns 0", %{sock: sock} do
      k = ukey("hsetnx_exist")
      cmd(sock, ["HSET", k, "f1", "old"])
      assert cmd(sock, ["HSETNX", k, "f1", "new"]) == 0
      assert cmd(sock, ["HGET", k, "f1"]) == "old"
    end

    test "HSETNX on new field returns 1", %{sock: sock} do
      k = ukey("hsetnx_new")
      assert cmd(sock, ["HSETNX", k, "f1", "val"]) == 1
      assert cmd(sock, ["HGET", k, "f1"]) == "val"
    end

    test "HSET with multiple field-value pairs", %{sock: sock} do
      k = ukey("hset_multi")
      result = cmd(sock, ["HSET", k, "f1", "v1", "f2", "v2", "f3", "v3"])
      assert result == 3
      assert cmd(sock, ["HLEN", k]) == 3
    end

    test "HSET with odd field-value pairs returns error", %{sock: sock} do
      k = ukey("hset_odd")
      result = cmd(sock, ["HSET", k, "f1", "v1", "f2"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "HMGET with mix of existing and non-existing fields", %{sock: sock} do
      k = ukey("hmget_mix")
      cmd(sock, ["HSET", k, "f1", "v1"])
      result = cmd(sock, ["HMGET", k, "f1", "f2"])
      assert result == ["v1", nil]
    end

    test "HSTRLEN on non-existent field returns 0", %{sock: sock} do
      k = ukey("hstrlen_nil")
      cmd(sock, ["HSET", k, "f1", "abc"])
      assert cmd(sock, ["HSTRLEN", k, "missing"]) == 0
    end

    test "HRANDFIELD on empty/non-existent hash returns nil", %{sock: sock} do
      k = ukey("hrandfield_nil")
      assert cmd(sock, ["HRANDFIELD", k]) == nil
    end

    test "HRANDFIELD with count > hash size returns all fields (no duplicates)", %{sock: sock} do
      k = ukey("hrandfield_big")
      cmd(sock, ["HSET", k, "f1", "v1", "f2", "v2"])
      result = cmd(sock, ["HRANDFIELD", k, "10"])
      assert is_list(result)
      assert length(result) == 2
    end

    test "HRANDFIELD with negative count allows duplicates", %{sock: sock} do
      k = ukey("hrandfield_neg")
      cmd(sock, ["HSET", k, "f1", "v1"])
      result = cmd(sock, ["HRANDFIELD", k, "-5"])
      assert is_list(result)
      assert length(result) == 5
      assert Enum.all?(result, &(&1 == "f1"))
    end

    test "HSET wrong arity (no field-value)", %{sock: sock} do
      result = cmd(sock, ["HSET", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "HGET wrong arity", %{sock: sock} do
      result = cmd(sock, ["HGET", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "HDEL wrong arity", %{sock: sock} do
      result = cmd(sock, ["HDEL", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "DEL removes hash key and all fields", %{sock: sock} do
      k = ukey("del_hash")
      cmd(sock, ["HSET", k, "f1", "v1", "f2", "v2"])
      assert cmd(sock, ["HLEN", k]) == 2
      assert cmd(sock, ["DEL", k]) == 1
      assert cmd(sock, ["HLEN", k]) == 0
      assert cmd(sock, ["HGET", k, "f1"]) == nil
    end
  end
end
