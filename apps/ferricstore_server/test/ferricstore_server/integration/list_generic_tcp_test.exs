defmodule FerricstoreServer.Integration.ListGenericTcpTest do
  @moduledoc """
  End-to-end TCP integration tests for LIST, GENERIC, EXPIRY, and BITMAP commands.

  One happy-path test per command. Uses the same connection helpers and setup
  as CommandsTcpTest.
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
  # LIST commands
  # ===========================================================================

  describe "LPUSH" do
    test "pushes elements and returns new length", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("lpush")

      send_cmd(sock, ["LPUSH", k, "a", "b", "c"])
      assert recv_response(sock) == 3

      :gen_tcp.close(sock)
    end
  end

  describe "RPUSH" do
    test "pushes elements and returns new length", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("rpush")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert recv_response(sock) == 3

      :gen_tcp.close(sock)
    end
  end

  describe "LPOP" do
    test "pops leftmost element", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("lpop")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert recv_response(sock) == 3

      send_cmd(sock, ["LPOP", k])
      assert recv_response(sock) == "a"

      :gen_tcp.close(sock)
    end
  end

  describe "RPOP" do
    test "pops rightmost element", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("rpop")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert recv_response(sock) == 3

      send_cmd(sock, ["RPOP", k])
      assert recv_response(sock) == "c"

      :gen_tcp.close(sock)
    end
  end

  describe "LLEN" do
    test "returns list length", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("llen")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert recv_response(sock) == 3

      send_cmd(sock, ["LLEN", k])
      assert recv_response(sock) == 3

      :gen_tcp.close(sock)
    end
  end

  describe "LRANGE" do
    test "returns elements in range", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("lrange")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c", "d"])
      assert recv_response(sock) == 4

      send_cmd(sock, ["LRANGE", k, "0", "-1"])
      assert recv_response(sock) == ["a", "b", "c", "d"]

      :gen_tcp.close(sock)
    end
  end

  describe "LINDEX" do
    test "returns element at index", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("lindex")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert recv_response(sock) == 3

      send_cmd(sock, ["LINDEX", k, "1"])
      assert recv_response(sock) == "b"

      :gen_tcp.close(sock)
    end
  end

  describe "LINSERT" do
    test "inserts element before pivot", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("linsert")

      send_cmd(sock, ["RPUSH", k, "a", "c"])
      assert recv_response(sock) == 2

      send_cmd(sock, ["LINSERT", k, "BEFORE", "c", "b"])
      assert recv_response(sock) == 3

      send_cmd(sock, ["LRANGE", k, "0", "-1"])
      assert recv_response(sock) == ["a", "b", "c"]

      :gen_tcp.close(sock)
    end
  end

  describe "LSET" do
    test "sets element at index", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("lset")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert recv_response(sock) == 3

      send_cmd(sock, ["LSET", k, "1", "B"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["LINDEX", k, "1"])
      assert recv_response(sock) == "B"

      :gen_tcp.close(sock)
    end
  end

  describe "LREM" do
    test "removes matching elements", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("lrem")

      send_cmd(sock, ["RPUSH", k, "a", "b", "a", "c", "a"])
      assert recv_response(sock) == 5

      send_cmd(sock, ["LREM", k, "2", "a"])
      assert recv_response(sock) == 2

      send_cmd(sock, ["LRANGE", k, "0", "-1"])
      assert recv_response(sock) == ["b", "c", "a"]

      :gen_tcp.close(sock)
    end
  end

  describe "LTRIM" do
    test "trims list to range", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("ltrim")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c", "d", "e"])
      assert recv_response(sock) == 5

      send_cmd(sock, ["LTRIM", k, "1", "3"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["LRANGE", k, "0", "-1"])
      assert recv_response(sock) == ["b", "c", "d"]

      :gen_tcp.close(sock)
    end
  end

  describe "LMOVE" do
    test "moves element between lists", %{port: port} do
      sock = connect_and_hello(port)
      src = ukey("lmove_src")
      dst = ukey("lmove_dst")

      send_cmd(sock, ["RPUSH", src, "a", "b", "c"])
      assert recv_response(sock) == 3

      send_cmd(sock, ["LMOVE", src, dst, "LEFT", "RIGHT"])
      assert recv_response(sock) == "a"

      send_cmd(sock, ["LRANGE", src, "0", "-1"])
      assert recv_response(sock) == ["b", "c"]

      send_cmd(sock, ["LRANGE", dst, "0", "-1"])
      assert recv_response(sock) == ["a"]

      :gen_tcp.close(sock)
    end
  end

  describe "LPUSHX" do
    test "pushes only if key exists", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("lpushx")

      # LPUSHX on non-existent key returns 0
      send_cmd(sock, ["LPUSHX", k, "a"])
      assert recv_response(sock) == 0

      # Create the list, then LPUSHX works
      send_cmd(sock, ["RPUSH", k, "b"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["LPUSHX", k, "a"])
      assert recv_response(sock) == 2

      :gen_tcp.close(sock)
    end
  end

  describe "RPUSHX" do
    test "pushes only if key exists", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("rpushx")

      send_cmd(sock, ["RPUSHX", k, "a"])
      assert recv_response(sock) == 0

      send_cmd(sock, ["RPUSH", k, "b"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["RPUSHX", k, "c"])
      assert recv_response(sock) == 2

      :gen_tcp.close(sock)
    end
  end

  describe "LPOS" do
    test "returns position of element", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("lpos")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c", "b", "d"])
      assert recv_response(sock) == 5

      send_cmd(sock, ["LPOS", k, "b"])
      assert recv_response(sock) == 1

      :gen_tcp.close(sock)
    end
  end

  describe "RPOPLPUSH" do
    test "pops from right of source, pushes to left of destination", %{port: port} do
      sock = connect_and_hello(port)
      src = ukey("rpoplpush_src")
      dst = ukey("rpoplpush_dst")

      send_cmd(sock, ["RPUSH", src, "a", "b", "c"])
      assert recv_response(sock) == 3

      send_cmd(sock, ["RPOPLPUSH", src, dst])
      assert recv_response(sock) == "c"

      send_cmd(sock, ["LRANGE", dst, "0", "-1"])
      assert recv_response(sock) == ["c"]

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # GENERIC commands
  # ===========================================================================

  describe "DEL" do
    test "deletes key and returns count", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("del")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DEL", k])
      assert recv_response(sock) == 1

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  describe "EXISTS" do
    test "returns 1 for existing key", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("exists")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXISTS", k])
      assert recv_response(sock) == 1

      :gen_tcp.close(sock)
    end
  end

  describe "TYPE" do
    test "returns type of key", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("type")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["TYPE", k])
      assert recv_response(sock) == {:simple, "string"}

      :gen_tcp.close(sock)
    end
  end

  describe "RENAME" do
    test "renames key", %{port: port} do
      sock = connect_and_hello(port)
      old = ukey("rename_old")
      new_key = ukey("rename_new")

      send_cmd(sock, ["SET", old, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["RENAME", old, new_key])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", new_key])
      assert recv_response(sock) == "v"

      send_cmd(sock, ["EXISTS", old])
      assert recv_response(sock) == 0

      :gen_tcp.close(sock)
    end
  end

  describe "RENAMENX" do
    test "renames only if destination does not exist", %{port: port} do
      sock = connect_and_hello(port)
      src = ukey("renamenx_src")
      dst = ukey("renamenx_dst")

      send_cmd(sock, ["SET", src, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["RENAMENX", src, dst])
      assert recv_response(sock) == 1

      send_cmd(sock, ["GET", dst])
      assert recv_response(sock) == "v"

      :gen_tcp.close(sock)
    end
  end

  describe "COPY" do
    test "copies key to destination", %{port: port} do
      sock = connect_and_hello(port)
      src = ukey("copy_src")
      dst = ukey("copy_dst")

      send_cmd(sock, ["SET", src, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["COPY", src, dst])
      assert recv_response(sock) == 1

      send_cmd(sock, ["GET", dst])
      assert recv_response(sock) == "v"

      :gen_tcp.close(sock)
    end
  end

  describe "KEYS" do
    test "returns matching keys", %{port: port} do
      sock = connect_and_hello(port)
      prefix = "keys_test_#{:rand.uniform(999_999)}"
      k1 = "#{prefix}:a"
      k2 = "#{prefix}:b"

      send_cmd(sock, ["SET", k1, "1"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", k2, "2"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["KEYS", "#{prefix}:*"])
      keys = recv_response(sock)
      assert Enum.sort(keys) == Enum.sort([k1, k2])

      :gen_tcp.close(sock)
    end
  end

  describe "SCAN" do
    test "iterates over keys", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("scan")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SCAN", "0", "MATCH", "#{k}*", "COUNT", "100"])
      [_cursor, keys] = recv_response(sock)
      assert k in keys

      :gen_tcp.close(sock)
    end
  end

  describe "RANDOMKEY" do
    test "returns a key when store is non-empty", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("randomkey")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["RANDOMKEY"])
      result = recv_response(sock)
      assert is_binary(result)

      :gen_tcp.close(sock)
    end
  end

  describe "UNLINK" do
    test "deletes key and returns count", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("unlink")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["UNLINK", k])
      assert recv_response(sock) == 1

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  describe "OBJECT" do
    test "OBJECT ENCODING returns encoding type", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("object_enc")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["OBJECT", "ENCODING", k])
      result = recv_response(sock)
      assert result in ["embstr", "raw"]

      :gen_tcp.close(sock)
    end
  end

  describe "DBSIZE" do
    test "returns key count", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("dbsize")

      send_cmd(sock, ["DBSIZE"])
      before = recv_response(sock)

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DBSIZE"])
      after_count = recv_response(sock)
      assert after_count >= before + 1

      :gen_tcp.close(sock)
    end
  end

  describe "ECHO" do
    test "returns the message", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ECHO", "hello"])
      assert recv_response(sock) == "hello"

      :gen_tcp.close(sock)
    end
  end

  describe "PING" do
    test "returns PONG", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # EXPIRY commands
  # ===========================================================================

  describe "EXPIRE" do
    test "sets TTL on key", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("expire")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXPIRE", k, "60"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["TTL", k])
      ttl = recv_response(sock)
      assert is_integer(ttl) and ttl > 0

      :gen_tcp.close(sock)
    end
  end

  describe "PEXPIRE" do
    test "sets TTL in milliseconds", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("pexpire")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["PEXPIRE", k, "60000"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["PTTL", k])
      pttl = recv_response(sock)
      assert is_integer(pttl) and pttl > 0

      :gen_tcp.close(sock)
    end
  end

  describe "EXPIREAT" do
    test "sets absolute expiry timestamp", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("expireat")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      future_ts = System.os_time(:second) + 3600
      send_cmd(sock, ["EXPIREAT", k, Integer.to_string(future_ts)])
      assert recv_response(sock) == 1

      send_cmd(sock, ["TTL", k])
      ttl = recv_response(sock)
      assert is_integer(ttl) and ttl > 0

      :gen_tcp.close(sock)
    end
  end

  describe "PEXPIREAT" do
    test "sets absolute expiry in milliseconds", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("pexpireat")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      future_ts_ms = System.os_time(:millisecond) + 3_600_000
      send_cmd(sock, ["PEXPIREAT", k, Integer.to_string(future_ts_ms)])
      assert recv_response(sock) == 1

      send_cmd(sock, ["PTTL", k])
      pttl = recv_response(sock)
      assert is_integer(pttl) and pttl > 0

      :gen_tcp.close(sock)
    end
  end

  describe "TTL" do
    test "returns -1 for key without expiry", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("ttl_no_exp")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["TTL", k])
      assert recv_response(sock) == -1

      :gen_tcp.close(sock)
    end
  end

  describe "PTTL" do
    test "returns -1 for key without expiry", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("pttl_no_exp")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["PTTL", k])
      assert recv_response(sock) == -1

      :gen_tcp.close(sock)
    end
  end

  describe "PERSIST" do
    test "removes TTL from key", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("persist")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXPIRE", k, "60"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["PERSIST", k])
      assert recv_response(sock) == 1

      send_cmd(sock, ["TTL", k])
      assert recv_response(sock) == -1

      :gen_tcp.close(sock)
    end
  end

  describe "EXPIRETIME" do
    test "returns absolute expiry in seconds", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("expiretime")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXPIRE", k, "60"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["EXPIRETIME", k])
      ts = recv_response(sock)
      assert is_integer(ts) and ts > 0

      :gen_tcp.close(sock)
    end
  end

  describe "PEXPIRETIME" do
    test "returns absolute expiry in milliseconds", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("pexpiretime")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["PEXPIRE", k, "60000"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["PEXPIRETIME", k])
      ts = recv_response(sock)
      assert is_integer(ts) and ts > 0

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # BITMAP commands
  # ===========================================================================

  describe "SETBIT" do
    test "sets bit and returns old value", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("setbit")

      send_cmd(sock, ["SETBIT", k, "7", "1"])
      assert recv_response(sock) == 0

      send_cmd(sock, ["SETBIT", k, "7", "0"])
      assert recv_response(sock) == 1

      :gen_tcp.close(sock)
    end
  end

  describe "GETBIT" do
    test "returns bit value at offset", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("getbit")

      send_cmd(sock, ["SETBIT", k, "7", "1"])
      assert recv_response(sock) == 0

      send_cmd(sock, ["GETBIT", k, "7"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["GETBIT", k, "0"])
      assert recv_response(sock) == 0

      :gen_tcp.close(sock)
    end
  end

  describe "BITCOUNT" do
    test "counts set bits", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("bitcount")

      send_cmd(sock, ["SET", k, "foobar"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["BITCOUNT", k])
      count = recv_response(sock)
      assert is_integer(count) and count > 0

      :gen_tcp.close(sock)
    end
  end

  describe "BITPOS" do
    test "finds first set bit", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("bitpos")

      # Set bit 7 (LSB of first byte)
      send_cmd(sock, ["SETBIT", k, "7", "1"])
      assert recv_response(sock) == 0

      send_cmd(sock, ["BITPOS", k, "1"])
      assert recv_response(sock) == 7

      :gen_tcp.close(sock)
    end
  end

  describe "BITOP" do
    test "performs bitwise AND", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("bitop_a")
      k2 = ukey("bitop_b")
      dest = ukey("bitop_dest")

      # Set both keys to single-byte values
      send_cmd(sock, ["SET", k1, "a"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", k2, "b"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["BITOP", "AND", dest, k1, k2])
      result = recv_response(sock)
      # Returns the byte length of the result string
      assert is_integer(result) and result > 0

      :gen_tcp.close(sock)
    end
  end
end
