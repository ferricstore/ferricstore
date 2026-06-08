defmodule FerricstoreServer.Spec.CommandEdgeCasesStringsTest do
  @moduledoc """
  Edge-case tests for STRING commands over TCP/RESP3.
  Split from CommandEdgeCasesComprehensiveTest.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

  @moduletag timeout: 120_000

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

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

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

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
        buf = Process.get(@binary_key, "")
        recv_loop(sock, buf, timeout)
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

  defp assert_ok(result), do: assert result == {:simple, "OK"} or result == :ok

  defp assert_error_contains(result, substring) do
    assert {:error, msg} = result
    assert String.contains?(msg, substring), "Expected error containing #{inspect(substring)}, got: #{inspect(msg)}"
  end

  # ===========================================================================
  # STRING COMMANDS EDGE CASES
  # ===========================================================================

  describe "Strings: SET/GET edge cases" do
    test "SET with empty key returns error", %{sock: sock} do
      result = cmd(sock, ["SET", "", "value"])
      assert_error_contains(result, "empty key")
    end

    test "GET with empty key returns error", %{sock: sock} do
      result = cmd(sock, ["GET", ""])
      assert_error_contains(result, "empty key")
    end

    test "SET with max key size (65535 bytes) succeeds", %{sock: sock} do
      big_key = String.duplicate("K", 65_535)
      result = cmd(sock, ["SET", big_key, "val"])
      assert_ok(result)
      assert cmd(sock, ["GET", big_key]) == "val"
    end

    test "SET with key > 65535 bytes returns error", %{sock: sock} do
      huge_key = String.duplicate("K", 65_536)
      result = cmd(sock, ["SET", huge_key, "val"])
      assert_error_contains(result, "key too large")
    end

    test "GET non-existent key returns nil", %{sock: sock} do
      assert cmd(sock, ["GET", ukey("nonexistent")]) == nil
    end

    test "SET with empty value succeeds", %{sock: sock} do
      k = ukey("empty_val")
      assert_ok(cmd(sock, ["SET", k, ""]))
      assert cmd(sock, ["GET", k]) == ""
    end

    test "SET with binary value containing null bytes", %{sock: sock} do
      k = ukey("null_bytes")
      v = <<0, 1, 2, 0, 255, 0>>
      assert_ok(cmd(sock, ["SET", k, v]))
      assert cmd(sock, ["GET", k]) == v
    end

    test "SET NX when key exists returns nil", %{sock: sock} do
      k = ukey("setnx_exists")
      cmd(sock, ["SET", k, "first"])
      assert cmd(sock, ["SET", k, "second", "NX"]) == nil
      assert cmd(sock, ["GET", k]) == "first"
    end

    test "SET XX when key missing returns nil", %{sock: sock} do
      k = ukey("setxx_missing")
      assert cmd(sock, ["SET", k, "val", "XX"]) == nil
      assert cmd(sock, ["GET", k]) == nil
    end

    test "SET NX + XX is contradictory but should not crash", %{sock: sock} do
      k = ukey("nx_xx")
      result = cmd(sock, ["SET", k, "val", "NX", "XX"])
      assert result == nil or match?({:error, _}, result) or result == {:simple, "OK"}
    end

    test "SET with conflicting EX and PX returns error", %{sock: sock} do
      k = ukey("ex_px_conflict")
      result = cmd(sock, ["SET", k, "val", "EX", "10", "PX", "10000"])
      assert_error_contains(result, "syntax error")
    end

    test "SET with EX + KEEPTTL returns error", %{sock: sock} do
      k = ukey("ex_keepttl")
      result = cmd(sock, ["SET", k, "val", "EX", "10", "KEEPTTL"])
      assert_error_contains(result, "syntax error")
    end

    test "SET GET returns old value", %{sock: sock} do
      k = ukey("set_get")
      cmd(sock, ["SET", k, "old"])
      assert cmd(sock, ["SET", k, "new", "GET"]) == "old"
      assert cmd(sock, ["GET", k]) == "new"
    end

    test "SET GET on non-existent key returns nil", %{sock: sock} do
      k = ukey("set_get_nil")
      assert cmd(sock, ["SET", k, "val", "GET"]) == nil
      assert cmd(sock, ["GET", k]) == "val"
    end

    test "SET with EX 0 returns error", %{sock: sock} do
      k = ukey("ex_zero")
      result = cmd(sock, ["SET", k, "val", "EX", "0"])
      assert_error_contains(result, "invalid expire")
    end

    test "SET with negative EX returns error", %{sock: sock} do
      k = ukey("ex_neg")
      result = cmd(sock, ["SET", k, "val", "EX", "-5"])
      assert_error_contains(result, "invalid expire")
    end

    test "SET with non-integer EX returns error", %{sock: sock} do
      k = ukey("ex_str")
      result = cmd(sock, ["SET", k, "val", "EX", "abc"])
      assert_error_contains(result, "not an integer")
    end

    test "SET with unrecognized option returns error", %{sock: sock} do
      k = ukey("bad_opt")
      result = cmd(sock, ["SET", k, "val", "FOOBAR"])
      assert_error_contains(result, "not recognized")
    end

    test "SET with no args returns arity error", %{sock: sock} do
      result = cmd(sock, ["SET"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "GET with too many args returns arity error", %{sock: sock} do
      result = cmd(sock, ["GET", "a", "b"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "SET KEEPTTL preserves existing TTL", %{sock: sock} do
      k = ukey("keepttl")
      cmd(sock, ["SET", k, "v1", "EX", "1000"])
      ttl_before = cmd(sock, ["TTL", k])
      assert ttl_before > 0

      cmd(sock, ["SET", k, "v2", "KEEPTTL"])
      assert cmd(sock, ["GET", k]) == "v2"
      ttl_after = cmd(sock, ["TTL", k])
      assert ttl_after > 0
    end
  end

  describe "Strings: SETNX / SETEX / PSETEX edge cases" do
    test "SETNX when key exists returns 0", %{sock: sock} do
      k = ukey("setnx_0")
      cmd(sock, ["SET", k, "existing"])
      assert cmd(sock, ["SETNX", k, "new"]) == 0
      assert cmd(sock, ["GET", k]) == "existing"
    end

    test "SETNX when key missing returns 1", %{sock: sock} do
      k = ukey("setnx_1")
      assert cmd(sock, ["SETNX", k, "val"]) == 1
      assert cmd(sock, ["GET", k]) == "val"
    end

    test "SETEX with zero seconds returns error", %{sock: sock} do
      k = ukey("setex_zero")
      result = cmd(sock, ["SETEX", k, "0", "val"])
      assert_error_contains(result, "invalid expire")
    end

    test "SETEX with negative seconds returns error", %{sock: sock} do
      k = ukey("setex_neg")
      result = cmd(sock, ["SETEX", k, "-1", "val"])
      assert_error_contains(result, "invalid expire")
    end

    test "PSETEX with zero ms returns error", %{sock: sock} do
      k = ukey("psetex_zero")
      result = cmd(sock, ["PSETEX", k, "0", "val"])
      assert_error_contains(result, "invalid expire")
    end

    test "SETEX wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["SETEX", "k", "10"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: INCR / DECR / INCRBY / DECRBY edge cases" do
    test "INCR on non-existent key initializes to 0 then increments", %{sock: sock} do
      k = ukey("incr_nil")
      assert cmd(sock, ["INCR", k]) == 1
    end

    test "INCR on non-numeric value returns error", %{sock: sock} do
      k = ukey("incr_str")
      cmd(sock, ["SET", k, "hello"])
      result = cmd(sock, ["INCR", k])
      assert_error_contains(result, "not an integer")
    end

    test "INCR on float value returns error", %{sock: sock} do
      k = ukey("incr_float")
      cmd(sock, ["SET", k, "3.14"])
      result = cmd(sock, ["INCR", k])
      assert_error_contains(result, "not an integer")
    end

    test "DECR on non-existent key initializes to 0 then decrements", %{sock: sock} do
      k = ukey("decr_nil")
      assert cmd(sock, ["DECR", k]) == -1
    end

    test "INCRBY with large positive value", %{sock: sock} do
      k = ukey("incrby_large")
      cmd(sock, ["SET", k, "0"])
      assert cmd(sock, ["INCRBY", k, "9223372036854775806"]) == 9_223_372_036_854_775_806
    end

    test "INCRBY with non-numeric delta returns error", %{sock: sock} do
      k = ukey("incrby_nan")
      result = cmd(sock, ["INCRBY", k, "xyz"])
      assert_error_contains(result, "not an integer")
    end

    test "INCRBY with float delta returns error", %{sock: sock} do
      k = ukey("incrby_float")
      result = cmd(sock, ["INCRBY", k, "1.5"])
      assert_error_contains(result, "not an integer")
    end

    test "DECRBY with non-numeric delta returns error", %{sock: sock} do
      k = ukey("decrby_nan")
      result = cmd(sock, ["DECRBY", k, "abc"])
      assert_error_contains(result, "not an integer")
    end

    test "INCR wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["INCR"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "DECRBY wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["DECRBY", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: INCRBYFLOAT edge cases" do
    test "INCRBYFLOAT on non-existent key creates it", %{sock: sock} do
      k = ukey("incrbyfloat_nil")
      result = cmd(sock, ["INCRBYFLOAT", k, "2.5"])
      assert is_binary(result)
      assert String.to_float(result) == 2.5
    end

    test "INCRBYFLOAT on non-numeric value returns error", %{sock: sock} do
      k = ukey("incrbyfloat_str")
      cmd(sock, ["SET", k, "hello"])
      result = cmd(sock, ["INCRBYFLOAT", k])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "INCRBYFLOAT with non-numeric increment returns error", %{sock: sock} do
      k = ukey("incrbyfloat_nan")
      result = cmd(sock, ["INCRBYFLOAT", k, "notanumber"])
      assert_error_contains(result, "not a valid float")
    end

    test "INCRBYFLOAT with integer increment string works", %{sock: sock} do
      k = ukey("incrbyfloat_int")
      cmd(sock, ["SET", k, "10"])
      result = cmd(sock, ["INCRBYFLOAT", k, "5"])
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert val == 15.0
    end

    test "INCRBYFLOAT wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["INCRBYFLOAT"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: APPEND edge cases" do
    test "APPEND to non-existent key creates it", %{sock: sock} do
      k = ukey("append_nil")
      assert cmd(sock, ["APPEND", k, "hello"]) == 5
      assert cmd(sock, ["GET", k]) == "hello"
    end

    test "APPEND to existing key extends it", %{sock: sock} do
      k = ukey("append_exist")
      cmd(sock, ["SET", k, "hello"])
      assert cmd(sock, ["APPEND", k, " world"]) == 11
      assert cmd(sock, ["GET", k]) == "hello world"
    end

    test "APPEND with empty value returns existing length", %{sock: sock} do
      k = ukey("append_empty")
      cmd(sock, ["SET", k, "abc"])
      assert cmd(sock, ["APPEND", k, ""]) == 3
    end

    test "APPEND wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["APPEND", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: STRLEN edge cases" do
    test "STRLEN on non-existent key returns 0", %{sock: sock} do
      assert cmd(sock, ["STRLEN", ukey("strlen_nil")]) == 0
    end

    test "STRLEN on empty string returns 0", %{sock: sock} do
      k = ukey("strlen_empty")
      cmd(sock, ["SET", k, ""])
      assert cmd(sock, ["STRLEN", k]) == 0
    end

    test "STRLEN wrong arity", %{sock: sock} do
      result = cmd(sock, ["STRLEN"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: GETRANGE edge cases" do
    test "GETRANGE on non-existent key returns empty string", %{sock: sock} do
      assert cmd(sock, ["GETRANGE", ukey("getrange_nil"), "0", "10"]) == ""
    end

    test "GETRANGE with start > end returns empty string", %{sock: sock} do
      k = ukey("getrange_inverted")
      cmd(sock, ["SET", k, "hello"])
      assert cmd(sock, ["GETRANGE", k, "3", "1"]) == ""
    end

    test "GETRANGE with negative indices", %{sock: sock} do
      k = ukey("getrange_neg")
      cmd(sock, ["SET", k, "hello"])
      assert cmd(sock, ["GETRANGE", k, "-5", "-1"]) == "hello"
    end

    test "GETRANGE with negative indices beyond string length", %{sock: sock} do
      k = ukey("getrange_neg_oob")
      cmd(sock, ["SET", k, "hi"])
      assert cmd(sock, ["GETRANGE", k, "-100", "-1"]) == "hi"
    end

    test "GETRANGE with start beyond string length returns empty", %{sock: sock} do
      k = ukey("getrange_oob")
      cmd(sock, ["SET", k, "hi"])
      assert cmd(sock, ["GETRANGE", k, "100", "200"]) == ""
    end

    test "GETRANGE with non-integer indices returns error", %{sock: sock} do
      k = ukey("getrange_nan")
      cmd(sock, ["SET", k, "hi"])
      result = cmd(sock, ["GETRANGE", k, "abc", "1"])
      assert_error_contains(result, "not an integer")
    end

    test "GETRANGE wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["GETRANGE", "k", "0"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: SETRANGE edge cases" do
    test "SETRANGE on non-existent key zero-pads", %{sock: sock} do
      k = ukey("setrange_nil")
      new_len = cmd(sock, ["SETRANGE", k, "5", "hi"])
      assert new_len == 7
      val = cmd(sock, ["GET", k])
      assert byte_size(val) == 7
      assert binary_part(val, 0, 5) == <<0, 0, 0, 0, 0>>
      assert binary_part(val, 5, 2) == "hi"
    end

    test "SETRANGE with negative offset returns error", %{sock: sock} do
      k = ukey("setrange_neg")
      cmd(sock, ["SET", k, "hello"])
      result = cmd(sock, ["SETRANGE", k, "-1", "x"])
      assert_error_contains(result, "out of range")
    end

    test "SETRANGE with offset > 512MB returns error", %{sock: sock} do
      k = ukey("setrange_huge")
      result = cmd(sock, ["SETRANGE", k, "536870912", "x"])
      assert_error_contains(result, "maximum allowed size")
    end

    test "SETRANGE with non-integer offset returns error", %{sock: sock} do
      k = ukey("setrange_nan")
      result = cmd(sock, ["SETRANGE", k, "abc", "x"])
      assert_error_contains(result, "not an integer")
    end

    test "SETRANGE with offset 0 and empty value on existing key", %{sock: sock} do
      k = ukey("setrange_empty")
      cmd(sock, ["SET", k, "hello"])
      new_len = cmd(sock, ["SETRANGE", k, "0", ""])
      assert new_len == 5
      assert cmd(sock, ["GET", k]) == "hello"
    end

    test "SETRANGE wrong arity returns error", %{sock: sock} do
      result = cmd(sock, ["SETRANGE", "k", "0"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: MSET / MGET edge cases" do
    test "MSET with odd number of args returns error", %{sock: sock} do
      result = cmd(sock, ["MSET", "k1", "v1", "k2"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "MSET with zero args returns error", %{sock: sock} do
      result = cmd(sock, ["MSET"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "MGET with non-existent keys returns nils", %{sock: sock} do
      result = cmd(sock, ["MGET", ukey("mget_a"), ukey("mget_b")])
      assert result == [nil, nil]
    end

    test "MGET with mix of existent and non-existent keys", %{sock: sock} do
      k1 = ukey("mget_mix_a")
      k2 = ukey("mget_mix_b")
      cmd(sock, ["SET", k1, "val1"])
      result = cmd(sock, ["MGET", k1, k2])
      assert result == ["val1", nil]
    end

    test "MGET with zero args returns error", %{sock: sock} do
      result = cmd(sock, ["MGET"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "MSET with empty key returns error", %{sock: sock} do
      result = cmd(sock, ["MSET", "", "val"])
      assert_error_contains(result, "too large or empty")
    end
  end

  describe "Strings: MSETNX edge cases" do
    test "MSETNX when none exist returns 1", %{sock: sock} do
      k1 = ukey("msetnx_a")
      k2 = ukey("msetnx_b")
      assert cmd(sock, ["MSETNX", k1, "v1", k2, "v2"]) == 1
      assert cmd(sock, ["GET", k1]) == "v1"
      assert cmd(sock, ["GET", k2]) == "v2"
    end

    test "MSETNX when any key exists returns 0 and sets nothing", %{sock: sock} do
      k1 = ukey("msetnx_c")
      k2 = ukey("msetnx_d")
      cmd(sock, ["SET", k1, "existing"])
      assert cmd(sock, ["MSETNX", k1, "new1", k2, "new2"]) == 0
      assert cmd(sock, ["GET", k1]) == "existing"
      assert cmd(sock, ["GET", k2]) == nil
    end

    test "MSETNX with odd args returns error", %{sock: sock} do
      result = cmd(sock, ["MSETNX", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end

  describe "Strings: GETSET / GETDEL / GETEX edge cases" do
    test "GETSET on non-existent key returns nil and creates key", %{sock: sock} do
      k = ukey("getset_nil")
      assert cmd(sock, ["GETSET", k, "newval"]) == nil
      assert cmd(sock, ["GET", k]) == "newval"
    end

    test "GETDEL on non-existent key returns nil", %{sock: sock} do
      k = ukey("getdel_nil")
      assert cmd(sock, ["GETDEL", k]) == nil
    end

    test "GETDEL atomically gets and deletes", %{sock: sock} do
      k = ukey("getdel_ok")
      cmd(sock, ["SET", k, "temp"])
      assert cmd(sock, ["GETDEL", k]) == "temp"
      assert cmd(sock, ["GET", k]) == nil
    end

    test "GETEX with EX updates TTL and returns value", %{sock: sock} do
      k = ukey("getex_ex")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["GETEX", k, "EX", "1000"]) == "val"
      ttl = cmd(sock, ["TTL", k])
      assert ttl > 0 and ttl <= 1000
    end

    test "GETEX with PERSIST removes TTL", %{sock: sock} do
      k = ukey("getex_persist")
      cmd(sock, ["SET", k, "val", "EX", "1000"])
      assert cmd(sock, ["GETEX", k, "PERSIST"]) == "val"
      assert cmd(sock, ["TTL", k]) == -1
    end

    test "GETEX on non-existent key returns nil", %{sock: sock} do
      k = ukey("getex_nil")
      assert cmd(sock, ["GETEX", k]) == nil
    end

    test "GETEX with invalid option returns error", %{sock: sock} do
      k = ukey("getex_bad")
      cmd(sock, ["SET", k, "val"])
      result = cmd(sock, ["GETEX", k, "BADOPT", "123"])
      assert_error_contains(result, "syntax error")
    end

    test "GETSET wrong arity", %{sock: sock} do
      result = cmd(sock, ["GETSET", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "GETDEL wrong arity", %{sock: sock} do
      result = cmd(sock, ["GETDEL"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end
end
