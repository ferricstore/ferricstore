defmodule FerricstoreServer.Integration.CommandsTcpTest.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Encoder
      alias FerricstoreServer.Resp.Parser
      alias FerricstoreServer.Listener

  describe "INCR over TCP" do
    test "increments integer value by 1", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incr")

      send_cmd(sock, ["SET", key, "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["INCR", key])
      assert recv_response(sock) == 11

      :gen_tcp.close(sock)
    end

    test "initializes missing key to 0 then increments", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incr_new")

      send_cmd(sock, ["INCR", key])
      assert recv_response(sock) == 1

      :gen_tcp.close(sock)
    end
  end
  describe "INCRBY over TCP" do
    test "increments by specified amount", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incrby")

      send_cmd(sock, ["SET", key, "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["INCRBY", key, "5"])
      assert recv_response(sock) == 15

      :gen_tcp.close(sock)
    end

    test "increments by negative amount", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incrby_neg")

      send_cmd(sock, ["SET", key, "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["INCRBY", key, "-3"])
      assert recv_response(sock) == 7

      :gen_tcp.close(sock)
    end
  end
  describe "DECR over TCP" do
    test "decrements integer value by 1", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("decr")

      send_cmd(sock, ["SET", key, "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DECR", key])
      assert recv_response(sock) == 9

      :gen_tcp.close(sock)
    end

    test "initializes missing key to 0 then decrements", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("decr_new")

      send_cmd(sock, ["DECR", key])
      assert recv_response(sock) == -1

      :gen_tcp.close(sock)
    end
  end
  describe "DECRBY over TCP" do
    test "decrements by specified amount", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("decrby")

      send_cmd(sock, ["SET", key, "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DECRBY", key, "3"])
      assert recv_response(sock) == 7

      :gen_tcp.close(sock)
    end
  end
  describe "INCRBYFLOAT over TCP" do
    test "increments by float amount", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incrbyfloat")

      send_cmd(sock, ["SET", key, "10.5"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["INCRBYFLOAT", key, "0.1"])
      result = recv_response(sock)
      # Result is a bulk string representation of the float
      assert result == "10.6"

      :gen_tcp.close(sock)
    end

    test "increments integer string by float", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("incrbyfloat_int")

      send_cmd(sock, ["SET", key, "5"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["INCRBYFLOAT", key, "2.5"])
      result = recv_response(sock)
      assert result == "7.5"

      :gen_tcp.close(sock)
    end
  end
  describe "MSETNX over TCP" do
    test "sets all keys when none exist", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("msetnx_a")
      k2 = ukey("msetnx_b")

      send_cmd(sock, ["MSETNX", k1, "val1", k2, "val2"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["MGET", k1, k2])
      assert recv_response(sock) == ["val1", "val2"]

      :gen_tcp.close(sock)
    end

    test "sets no keys when any already exist", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("msetnx_exists")
      k2 = ukey("msetnx_new")

      send_cmd(sock, ["SET", k1, "original"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MSETNX", k1, "overwrite", k2, "val2"])
      assert recv_response(sock) == 0

      # k1 should still be original, k2 should not exist
      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == "original"

      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end
  describe "GETSET over TCP" do
    test "sets new value and returns old value", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getset")

      send_cmd(sock, ["SET", key, "old"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GETSET", key, "new"])
      assert recv_response(sock) == "old"

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "new"

      :gen_tcp.close(sock)
    end

    test "returns nil for non-existing key", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getset_new")

      send_cmd(sock, ["GETSET", key, "value"])
      assert recv_response(sock) == nil

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "value"

      :gen_tcp.close(sock)
    end
  end
  describe "GETDEL over TCP" do
    test "returns value and deletes the key", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getdel")

      send_cmd(sock, ["SET", key, "ephemeral"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GETDEL", key])
      assert recv_response(sock) == "ephemeral"

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "returns nil for missing key", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getdel_missing")

      send_cmd(sock, ["GETDEL", key])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end
  describe "GETEX over TCP" do
    test "returns value and sets expiry with EX", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getex")

      send_cmd(sock, ["SET", key, "myval"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GETEX", key, "EX", "100"])
      assert recv_response(sock) == "myval"

      send_cmd(sock, ["TTL", key])
      ttl = recv_response(sock)
      assert is_integer(ttl)
      assert ttl > 0 and ttl <= 100

      :gen_tcp.close(sock)
    end

    test "returns value and removes expiry with PERSIST", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getex_persist")

      send_cmd(sock, ["SET", key, "myval", "EX", "100"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GETEX", key, "PERSIST"])
      assert recv_response(sock) == "myval"

      send_cmd(sock, ["TTL", key])
      assert recv_response(sock) == -1

      :gen_tcp.close(sock)
    end

    test "returns nil for missing key", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("getex_missing")

      send_cmd(sock, ["GETEX", key, "EX", "100"])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end
  describe "SETEX over TCP" do
    test "sets value with expiry in seconds", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("setex")

      send_cmd(sock, ["SETEX", key, "100", "myval"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "myval"

      send_cmd(sock, ["TTL", key])
      ttl = recv_response(sock)
      assert is_integer(ttl)
      assert ttl > 0 and ttl <= 100

      :gen_tcp.close(sock)
    end
  end
  describe "PSETEX over TCP" do
    test "sets value with expiry in milliseconds", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("psetex")

      send_cmd(sock, ["PSETEX", key, "100000", "myval"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "myval"

      send_cmd(sock, ["PTTL", key])
      pttl = recv_response(sock)
      assert is_integer(pttl)
      assert pttl > 0 and pttl <= 100_000

      :gen_tcp.close(sock)
    end
  end
  describe "SETNX over TCP" do
    test "sets value only when key does not exist", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("setnx")

      send_cmd(sock, ["SETNX", key, "first"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "first"

      send_cmd(sock, ["SETNX", key, "second"])
      assert recv_response(sock) == 0

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "first"

      :gen_tcp.close(sock)
    end
  end
  describe "SET with KEEPTTL over TCP" do
    test "SET with KEEPTTL preserves existing TTL", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("keepttl")

      send_cmd(sock, ["SET", key, "original", "EX", "100"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", key, "updated", "KEEPTTL"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "updated"

      send_cmd(sock, ["TTL", key])
      ttl = recv_response(sock)
      assert is_integer(ttl)
      assert ttl > 0 and ttl <= 100

      :gen_tcp.close(sock)
    end
  end
    end
  end
end
