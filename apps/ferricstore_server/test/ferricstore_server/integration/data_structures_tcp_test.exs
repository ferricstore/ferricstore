defmodule FerricstoreServer.Integration.DataStructuresTcpTest do
  @moduledoc """
  End-to-end TCP integration tests for HASH, SET, and SORTED SET commands.

  Follows the same pattern as CommandsTcpTest: connects over a real TCP socket,
  sends RESP3-encoded commands, and verifies responses through the full stack.
  """

  use ExUnit.Case, async: false
  @moduletag :integration

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

  defp ukey(name), do: "#{name}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup %{port: port} do
    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    recv_response(sock)
    :gen_tcp.close(sock)
    :ok
  end

  # ===========================================================================
  # HASH commands
  # ===========================================================================

  describe "HSET and HGET over TCP" do
    test "sets and gets hash field", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hash")
      send_cmd(sock, ["HSET", key, "field1", "value1"])
      assert recv_response(sock) == 1
      send_cmd(sock, ["HGET", key, "field1"])
      assert recv_response(sock) == "value1"
      :gen_tcp.close(sock)
    end
  end

  describe "HGETALL over TCP" do
    test "returns all fields and values", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hgetall")
      send_cmd(sock, ["HSET", key, "a", "1", "b", "2"])
      recv_response(sock)
      send_cmd(sock, ["HGETALL", key])
      result = recv_response(sock)
      assert is_list(result)
      pairs = Enum.chunk_every(result, 2) |> Map.new(fn [k, v] -> {k, v} end)
      assert pairs == %{"a" => "1", "b" => "2"}
      :gen_tcp.close(sock)
    end
  end

  describe "HDEL over TCP" do
    test "deletes a hash field", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hdel")
      send_cmd(sock, ["HSET", key, "f1", "v1"])
      recv_response(sock)
      send_cmd(sock, ["HDEL", key, "f1"])
      assert recv_response(sock) == 1
      send_cmd(sock, ["HGET", key, "f1"])
      assert recv_response(sock) == nil
      :gen_tcp.close(sock)
    end
  end

  describe "HEXISTS over TCP" do
    test "returns 1 for existing field, 0 for missing", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hexists")
      send_cmd(sock, ["HSET", key, "f1", "v1"])
      recv_response(sock)
      send_cmd(sock, ["HEXISTS", key, "f1"])
      assert recv_response(sock) == 1
      send_cmd(sock, ["HEXISTS", key, "missing"])
      assert recv_response(sock) == 0
      :gen_tcp.close(sock)
    end
  end

  describe "HLEN over TCP" do
    test "returns number of fields", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hlen")
      send_cmd(sock, ["HSET", key, "a", "1", "b", "2", "c", "3"])
      recv_response(sock)
      send_cmd(sock, ["HLEN", key])
      assert recv_response(sock) == 3
      :gen_tcp.close(sock)
    end
  end

  describe "HKEYS over TCP" do
    test "returns all field names", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hkeys")
      send_cmd(sock, ["HSET", key, "x", "1", "y", "2"])
      recv_response(sock)
      send_cmd(sock, ["HKEYS", key])
      assert Enum.sort(recv_response(sock)) == ["x", "y"]
      :gen_tcp.close(sock)
    end
  end

  describe "HVALS over TCP" do
    test "returns all values", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hvals")
      send_cmd(sock, ["HSET", key, "x", "10", "y", "20"])
      recv_response(sock)
      send_cmd(sock, ["HVALS", key])
      assert Enum.sort(recv_response(sock)) == ["10", "20"]
      :gen_tcp.close(sock)
    end
  end

  describe "HMGET over TCP" do
    test "returns values for multiple fields", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hmget")
      send_cmd(sock, ["HSET", key, "a", "1", "b", "2"])
      recv_response(sock)
      send_cmd(sock, ["HMGET", key, "a", "b", "missing"])
      assert recv_response(sock) == ["1", "2", nil]
      :gen_tcp.close(sock)
    end
  end

  describe "HINCRBY over TCP" do
    test "increments integer field value", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hincrby")
      send_cmd(sock, ["HSET", key, "counter", "10"])
      recv_response(sock)
      send_cmd(sock, ["HINCRBY", key, "counter", "5"])
      assert recv_response(sock) == 15
      :gen_tcp.close(sock)
    end
  end

  describe "HINCRBYFLOAT over TCP" do
    test "increments float field value", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hincrbyfloat")
      send_cmd(sock, ["HSET", key, "price", "10.5"])
      recv_response(sock)
      send_cmd(sock, ["HINCRBYFLOAT", key, "price", "1.5"])
      result = recv_response(sock)
      assert is_binary(result)
      {val, ""} = Float.parse(result)
      assert_in_delta val, 12.0, 0.001
      :gen_tcp.close(sock)
    end
  end

  describe "HSETNX over TCP" do
    test "sets field only if not exists", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hsetnx")
      send_cmd(sock, ["HSETNX", key, "f1", "original"])
      assert recv_response(sock) == 1
      send_cmd(sock, ["HSETNX", key, "f1", "overwrite"])
      assert recv_response(sock) == 0
      send_cmd(sock, ["HGET", key, "f1"])
      assert recv_response(sock) == "original"
      :gen_tcp.close(sock)
    end
  end

  describe "HSTRLEN over TCP" do
    test "returns byte length of field value", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hstrlen")
      send_cmd(sock, ["HSET", key, "f1", "hello"])
      recv_response(sock)
      send_cmd(sock, ["HSTRLEN", key, "f1"])
      assert recv_response(sock) == 5
      :gen_tcp.close(sock)
    end
  end

  describe "HSCAN over TCP" do
    test "scans hash fields", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hscan")
      send_cmd(sock, ["HSET", key, "a", "1", "b", "2", "c", "3"])
      recv_response(sock)
      send_cmd(sock, ["HSCAN", key, "0"])
      [cursor, elements] = recv_response(sock)
      assert cursor == "0"
      assert is_list(elements)
      assert length(elements) == 6
      :gen_tcp.close(sock)
    end
  end

  describe "HRANDFIELD over TCP" do
    test "returns a random field name", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("hrandfield")
      send_cmd(sock, ["HSET", key, "a", "1", "b", "2", "c", "3"])
      recv_response(sock)
      send_cmd(sock, ["HRANDFIELD", key])
      result = recv_response(sock)
      assert result in ["a", "b", "c"]
      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # SET commands
  # ===========================================================================

  describe "SADD and SMEMBERS over TCP" do
    test "adds members and retrieves them", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("set")
      send_cmd(sock, ["SADD", key, "a", "b", "c"])
      assert recv_response(sock) == 3
      send_cmd(sock, ["SMEMBERS", key])
      assert Enum.sort(recv_response(sock)) == ["a", "b", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "SREM over TCP" do
    test "removes members from set", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("srem")
      send_cmd(sock, ["SADD", key, "a", "b", "c"])
      recv_response(sock)
      send_cmd(sock, ["SREM", key, "b"])
      assert recv_response(sock) == 1
      send_cmd(sock, ["SMEMBERS", key])
      assert Enum.sort(recv_response(sock)) == ["a", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "SCARD over TCP" do
    test "returns set cardinality", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("scard")
      send_cmd(sock, ["SADD", key, "x", "y", "z"])
      recv_response(sock)
      send_cmd(sock, ["SCARD", key])
      assert recv_response(sock) == 3
      :gen_tcp.close(sock)
    end
  end

  describe "SISMEMBER over TCP" do
    test "checks membership", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("sismember")
      send_cmd(sock, ["SADD", key, "a", "b"])
      recv_response(sock)
      send_cmd(sock, ["SISMEMBER", key, "a"])
      assert recv_response(sock) == 1
      send_cmd(sock, ["SISMEMBER", key, "missing"])
      assert recv_response(sock) == 0
      :gen_tcp.close(sock)
    end
  end

  describe "SMISMEMBER over TCP" do
    test "checks multiple members", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("smismember")
      send_cmd(sock, ["SADD", key, "a", "b"])
      recv_response(sock)
      send_cmd(sock, ["SMISMEMBER", key, "a", "missing", "b"])
      assert recv_response(sock) == [1, 0, 1]
      :gen_tcp.close(sock)
    end
  end

  describe "SDIFF over TCP" do
    test "returns difference between sets", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("sdiff1")
      k2 = ukey("sdiff2")
      send_cmd(sock, ["SADD", k1, "a", "b", "c"])
      recv_response(sock)
      send_cmd(sock, ["SADD", k2, "b", "d"])
      recv_response(sock)
      send_cmd(sock, ["SDIFF", k1, k2])
      assert Enum.sort(recv_response(sock)) == ["a", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "SINTER over TCP" do
    test "returns intersection of sets", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("sinter1")
      k2 = ukey("sinter2")
      send_cmd(sock, ["SADD", k1, "a", "b", "c"])
      recv_response(sock)
      send_cmd(sock, ["SADD", k2, "b", "c", "d"])
      recv_response(sock)
      send_cmd(sock, ["SINTER", k1, k2])
      assert Enum.sort(recv_response(sock)) == ["b", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "SUNION over TCP" do
    test "returns union of sets", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("sunion1")
      k2 = ukey("sunion2")
      send_cmd(sock, ["SADD", k1, "a", "b"])
      recv_response(sock)
      send_cmd(sock, ["SADD", k2, "b", "c"])
      recv_response(sock)
      send_cmd(sock, ["SUNION", k1, k2])
      assert Enum.sort(recv_response(sock)) == ["a", "b", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "SDIFFSTORE over TCP" do
    test "stores difference into destination", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("sdiffstore_s1")
      k2 = ukey("sdiffstore_s2")
      dest = ukey("sdiffstore_d")
      send_cmd(sock, ["SADD", k1, "a", "b", "c"])
      recv_response(sock)
      send_cmd(sock, ["SADD", k2, "b"])
      recv_response(sock)
      send_cmd(sock, ["SDIFFSTORE", dest, k1, k2])
      assert recv_response(sock) == 2
      send_cmd(sock, ["SMEMBERS", dest])
      assert Enum.sort(recv_response(sock)) == ["a", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "SINTERSTORE over TCP" do
    test "stores intersection into destination", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("sinterstore_s1")
      k2 = ukey("sinterstore_s2")
      dest = ukey("sinterstore_d")
      send_cmd(sock, ["SADD", k1, "a", "b", "c"])
      recv_response(sock)
      send_cmd(sock, ["SADD", k2, "b", "c", "d"])
      recv_response(sock)
      send_cmd(sock, ["SINTERSTORE", dest, k1, k2])
      assert recv_response(sock) == 2
      send_cmd(sock, ["SMEMBERS", dest])
      assert Enum.sort(recv_response(sock)) == ["b", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "SUNIONSTORE over TCP" do
    test "stores union into destination", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("sunionstore_s1")
      k2 = ukey("sunionstore_s2")
      dest = ukey("sunionstore_d")
      send_cmd(sock, ["SADD", k1, "a", "b"])
      recv_response(sock)
      send_cmd(sock, ["SADD", k2, "b", "c"])
      recv_response(sock)
      send_cmd(sock, ["SUNIONSTORE", dest, k1, k2])
      assert recv_response(sock) == 3
      send_cmd(sock, ["SMEMBERS", dest])
      assert Enum.sort(recv_response(sock)) == ["a", "b", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "SPOP over TCP" do
    test "removes and returns a random member", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("spop")
      send_cmd(sock, ["SADD", key, "a", "b", "c"])
      recv_response(sock)
      send_cmd(sock, ["SPOP", key])
      popped = recv_response(sock)
      assert popped in ["a", "b", "c"]
      send_cmd(sock, ["SCARD", key])
      assert recv_response(sock) == 2
      :gen_tcp.close(sock)
    end
  end

  describe "SRANDMEMBER over TCP" do
    test "returns a random member without removing", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("srandmember")
      send_cmd(sock, ["SADD", key, "a", "b", "c"])
      recv_response(sock)
      send_cmd(sock, ["SRANDMEMBER", key])
      result = recv_response(sock)
      assert result in ["a", "b", "c"]
      send_cmd(sock, ["SCARD", key])
      assert recv_response(sock) == 3
      :gen_tcp.close(sock)
    end
  end

  describe "SMOVE over TCP" do
    test "moves member between sets", %{port: port} do
      sock = connect_and_hello(port)
      src = ukey("smove_src")
      dst = ukey("smove_dst")
      send_cmd(sock, ["SADD", src, "a", "b"])
      recv_response(sock)
      send_cmd(sock, ["SADD", dst, "c"])
      recv_response(sock)
      send_cmd(sock, ["SMOVE", src, dst, "a"])
      assert recv_response(sock) == 1
      send_cmd(sock, ["SMEMBERS", src])
      assert recv_response(sock) == ["b"]
      send_cmd(sock, ["SMEMBERS", dst])
      assert Enum.sort(recv_response(sock)) == ["a", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "SSCAN over TCP" do
    test "scans set members", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("sscan")
      send_cmd(sock, ["SADD", key, "a", "b", "c"])
      recv_response(sock)
      send_cmd(sock, ["SSCAN", key, "0"])
      [cursor, elements] = recv_response(sock)
      assert cursor == "0"
      assert Enum.sort(elements) == ["a", "b", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "SINTERCARD over TCP" do
    test "returns cardinality of intersection", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("sintercard1")
      k2 = ukey("sintercard2")
      send_cmd(sock, ["SADD", k1, "a", "b", "c"])
      recv_response(sock)
      send_cmd(sock, ["SADD", k2, "b", "c", "d"])
      recv_response(sock)
      send_cmd(sock, ["SINTERCARD", "2", k1, k2])
      assert recv_response(sock) == 2
      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # SORTED SET commands
  # ===========================================================================

  describe "ZADD and ZSCORE over TCP" do
    test "adds member with score and retrieves score", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zset")
      send_cmd(sock, ["ZADD", key, "1.5", "alice"])
      assert recv_response(sock) == 1
      send_cmd(sock, ["ZSCORE", key, "alice"])
      score = recv_response(sock)
      {val, ""} = Float.parse(score)
      assert_in_delta val, 1.5, 0.001
      :gen_tcp.close(sock)
    end
  end

  describe "ZCARD over TCP" do
    test "returns sorted set cardinality", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zcard")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c"])
      recv_response(sock)
      send_cmd(sock, ["ZCARD", key])
      assert recv_response(sock) == 3
      :gen_tcp.close(sock)
    end
  end

  describe "ZCOUNT over TCP" do
    test "counts members in score range", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zcount")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c", "4", "d"])
      recv_response(sock)
      send_cmd(sock, ["ZCOUNT", key, "2", "3"])
      assert recv_response(sock) == 2
      :gen_tcp.close(sock)
    end
  end

  describe "ZRANK over TCP" do
    test "returns rank of member (0-based, ascending)", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zrank")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c"])
      recv_response(sock)
      send_cmd(sock, ["ZRANK", key, "b"])
      assert recv_response(sock) == 1
      :gen_tcp.close(sock)
    end
  end

  describe "ZREVRANK over TCP" do
    test "returns rank of member (0-based, descending)", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zrevrank")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c"])
      recv_response(sock)
      send_cmd(sock, ["ZREVRANK", key, "a"])
      assert recv_response(sock) == 2
      :gen_tcp.close(sock)
    end
  end

  describe "ZRANGE over TCP" do
    test "returns members by index range", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zrange")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c"])
      recv_response(sock)
      send_cmd(sock, ["ZRANGE", key, "0", "-1"])
      assert recv_response(sock) == ["a", "b", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "ZREVRANGE over TCP" do
    test "returns members by index range in reverse", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zrevrange")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c"])
      recv_response(sock)
      send_cmd(sock, ["ZREVRANGE", key, "0", "-1"])
      assert recv_response(sock) == ["c", "b", "a"]
      :gen_tcp.close(sock)
    end
  end

  describe "ZRANGEBYSCORE over TCP" do
    test "returns members in score range", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zrangebyscore")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c", "4", "d"])
      recv_response(sock)
      send_cmd(sock, ["ZRANGEBYSCORE", key, "2", "3"])
      assert recv_response(sock) == ["b", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "ZREVRANGEBYSCORE over TCP" do
    test "returns members in score range in reverse", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zrevrangebyscore")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c", "4", "d"])
      recv_response(sock)
      send_cmd(sock, ["ZREVRANGEBYSCORE", key, "3", "2"])
      assert recv_response(sock) == ["c", "b"]
      :gen_tcp.close(sock)
    end
  end

  describe "ZINCRBY over TCP" do
    test "increments member score", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zincrby")
      send_cmd(sock, ["ZADD", key, "10", "alice"])
      recv_response(sock)
      send_cmd(sock, ["ZINCRBY", key, "5", "alice"])
      score = recv_response(sock)
      {val, ""} = Float.parse(score)
      assert_in_delta val, 15.0, 0.001
      :gen_tcp.close(sock)
    end
  end

  describe "ZREM over TCP" do
    test "removes member from sorted set", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zrem")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b"])
      recv_response(sock)
      send_cmd(sock, ["ZREM", key, "a"])
      assert recv_response(sock) == 1
      send_cmd(sock, ["ZRANGE", key, "0", "-1"])
      assert recv_response(sock) == ["b"]
      :gen_tcp.close(sock)
    end
  end

  describe "ZPOPMIN over TCP" do
    test "pops member with lowest score", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zpopmin")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c"])
      recv_response(sock)
      send_cmd(sock, ["ZPOPMIN", key])
      result = recv_response(sock)
      assert is_list(result)
      assert hd(result) == "a"
      send_cmd(sock, ["ZCARD", key])
      assert recv_response(sock) == 2
      :gen_tcp.close(sock)
    end
  end

  describe "ZPOPMAX over TCP" do
    test "pops member with highest score", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zpopmax")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c"])
      recv_response(sock)
      send_cmd(sock, ["ZPOPMAX", key])
      result = recv_response(sock)
      assert is_list(result)
      assert hd(result) == "c"
      send_cmd(sock, ["ZCARD", key])
      assert recv_response(sock) == 2
      :gen_tcp.close(sock)
    end
  end

  describe "ZRANDMEMBER over TCP" do
    test "returns a random member", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zrandmember")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c"])
      recv_response(sock)
      send_cmd(sock, ["ZRANDMEMBER", key])
      result = recv_response(sock)
      assert result in ["a", "b", "c"]
      :gen_tcp.close(sock)
    end
  end

  describe "ZSCAN over TCP" do
    test "scans sorted set members", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zscan")
      send_cmd(sock, ["ZADD", key, "1", "a", "2", "b", "3", "c"])
      recv_response(sock)
      send_cmd(sock, ["ZSCAN", key, "0"])
      [cursor, elements] = recv_response(sock)
      assert cursor == "0"
      # elements is [member, score, member, score, ...]
      assert is_list(elements)
      assert length(elements) == 6
      :gen_tcp.close(sock)
    end
  end

  describe "ZMSCORE over TCP" do
    test "returns scores for multiple members", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("zmscore")
      send_cmd(sock, ["ZADD", key, "1.0", "a", "2.0", "b"])
      recv_response(sock)
      send_cmd(sock, ["ZMSCORE", key, "a", "b", "missing"])
      result = recv_response(sock)
      assert is_list(result)
      assert length(result) == 3
      {a_score, ""} = Float.parse(Enum.at(result, 0))
      assert_in_delta a_score, 1.0, 0.001
      {b_score, ""} = Float.parse(Enum.at(result, 1))
      assert_in_delta b_score, 2.0, 0.001
      assert Enum.at(result, 2) == nil
      :gen_tcp.close(sock)
    end
  end
end
