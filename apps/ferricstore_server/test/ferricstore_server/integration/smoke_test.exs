defmodule FerricstoreServer.Integration.SmokeTest do
  @moduledoc """
  End-to-end smoke tests for FerricStore.

  Two complementary approaches:

  1. **`redis-cli` tests** — invoke `redis-cli` as an external process against the
     running listener and assert on its stdout.  These tests are skipped automatically
     when `redis-cli` is not in `$PATH` (CI without Redis tooling, fresh dev machines).

  2. **Pure-Elixir smoke tests** — exercise the same behaviours over a raw TCP socket
     using RESP3 framing.  These always run and provide full coverage of the
     human-facing command surface.

  All tests connect to the application-managed listener via `Listener.port/0`
  (ephemeral in test env, 6379 in production).
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  @redis_cli System.find_executable("redis-cli")

  defp redis_cli_available?, do: not is_nil(@redis_cli)

  # Run redis-cli with given args, return {stdout, exit_code}
  defp redis_cli(args) do
    port_num = Listener.port()
    cmd = [@redis_cli, "-p", Integer.to_string(port_num), "--no-auth-warning" | args]
    {output, code} = System.cmd(hd(cmd), tl(cmd), stderr_to_stdout: true)
    {String.trim(output), code}
  end

  defp connect do
    port = Listener.port()
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    sock
  end

  # Process dictionary keys for buffered state between recv_one calls
  @parsed_key :smoke_parsed_queue
  @binary_key :smoke_binary_buf

  defp cmd(sock, args) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(args)))
    recv_one(sock)
  end

  # Receives exactly one RESP3 value from `sock`, buffering leftovers so
  # that back-to-back calls consume them without extra socket reads.
  defp recv_one(sock) do
    case Process.get(@parsed_key, []) do
      [val | rest] ->
        Process.put(@parsed_key, rest)
        val

      [] ->
        buf = Process.get(@binary_key, "")
        fetch_and_recv(sock, buf)
    end
  end

  defp fetch_and_recv(sock, buf, retries \\ 2) do
    case Parser.parse(buf) do
      {:ok, [val | rest_vals], rest_bin} ->
        Process.put(@parsed_key, rest_vals)
        Process.put(@binary_key, rest_bin)
        val

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, 5_000) do
          {:ok, data} ->
            fetch_and_recv(sock, buf <> data, retries)

          {:error, :timeout} when retries > 0 ->
            # Under full suite load, the server may be slow. Retry.
            fetch_and_recv(sock, buf, retries - 1)

          {:error, :timeout} ->
            raise "TCP recv timed out after retries (buf size: #{byte_size(buf)})"
        end
    end
  end

  # Legacy alias used in pipelining tests
  defp recv(sock, _buf), do: recv_one(sock)

  defp ukey(base), do: "smoke_#{base}_#{:rand.uniform(9_999_999)}"

  # ---------------------------------------------------------------------------
  # redis-cli smoke tests
  # ---------------------------------------------------------------------------

  describe "redis-cli smoke tests" do
    @tag :redis_cli
    test "redis-cli PING returns PONG" do
      if redis_cli_available?() do
        {out, code} = redis_cli(["PING"])
        assert code == 0
        assert out == "PONG"
      else
        IO.puts("  [skip] redis-cli not found — skipping redis-cli smoke tests")
      end
    end

    @tag :redis_cli
    test "redis-cli SET and GET roundtrip" do
      if redis_cli_available?() do
        k = ukey("cli_rtt")
        {_, code} = redis_cli(["SET", k, "world"])
        assert code == 0

        {out, code} = redis_cli(["GET", k])
        assert code == 0
        assert out == "world"
      else
        IO.puts("  [skip] redis-cli not found")
      end
    end

    @tag :redis_cli
    test "redis-cli DEL removes key" do
      if redis_cli_available?() do
        k = ukey("cli_del")
        redis_cli(["SET", k, "to_delete"])

        {out, code} = redis_cli(["DEL", k])
        assert code == 0
        assert out == "1"

        {out, _} = redis_cli(["GET", k])
        assert out == ""
      else
        IO.puts("  [skip] redis-cli not found")
      end
    end

    @tag :redis_cli
    test "redis-cli EXPIRE then TTL shows countdown" do
      if redis_cli_available?() do
        k = ukey("cli_ttl")
        redis_cli(["SET", k, "ttl_val"])
        redis_cli(["EXPIRE", k, "100"])

        {out, code} = redis_cli(["TTL", k])
        assert code == 0
        ttl = String.to_integer(out)
        assert ttl > 0 and ttl <= 100
      else
        IO.puts("  [skip] redis-cli not found")
      end
    end

    @tag :redis_cli
    test "redis-cli DBSIZE reflects set keys" do
      if redis_cli_available?() do
        k1 = ukey("cli_dbsize_a")
        k2 = ukey("cli_dbsize_b")
        redis_cli(["SET", k1, "v1"])
        redis_cli(["SET", k2, "v2"])

        {out, code} = redis_cli(["DBSIZE"])
        assert code == 0
        count = String.to_integer(out)
        assert count >= 2
      else
        IO.puts("  [skip] redis-cli not found")
      end
    end

    @tag :redis_cli
    test "redis-cli HELLO 3 returns server map" do
      if redis_cli_available?() do
        {out, code} = redis_cli(["HELLO", "3"])
        assert code == 0
        assert String.contains?(out, "ferricstore")
        assert String.contains?(out, "server")
      else
        IO.puts("  [skip] redis-cli not found")
      end
    end

    @tag :redis_cli
    test "redis-cli KEYS * lists all keys" do
      if redis_cli_available?() do
        k = ukey("cli_keys_list")
        redis_cli(["SET", k, "v"])

        {out, code} = redis_cli(["KEYS", "*"])
        assert code == 0
        assert String.contains?(out, k)
      else
        IO.puts("  [skip] redis-cli not found")
      end
    end

    @tag :redis_cli
    test "redis-cli MSET and MGET" do
      if redis_cli_available?() do
        k1 = ukey("cli_mget_a")
        k2 = ukey("cli_mget_b")
        redis_cli(["MSET", k1, "aa", k2, "bb"])

        {out, code} = redis_cli(["MGET", k1, k2])
        assert code == 0
        assert String.contains?(out, "aa")
        assert String.contains?(out, "bb")
      else
        IO.puts("  [skip] redis-cli not found")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Pure-Elixir smoke tests — always run
  # ---------------------------------------------------------------------------

  describe "PING / PONG" do
    test "inline PING returns PONG" do
      sock = connect()
      assert cmd(sock, ["PING"]) == {:simple, "PONG"}
      :gen_tcp.close(sock)
    end

    test "PING with message echoes the message" do
      sock = connect()
      assert cmd(sock, ["PING", "hello"]) == "hello"
      :gen_tcp.close(sock)
    end

    test "ECHO returns the argument" do
      sock = connect()
      assert cmd(sock, ["ECHO", "ferricstore"]) == "ferricstore"
      :gen_tcp.close(sock)
    end
  end

  describe "SET / GET / DEL / EXISTS" do
    test "SET then GET returns the stored value" do
      sock = connect()
      k = ukey("get")
      assert cmd(sock, ["SET", k, "value1"]) == {:simple, "OK"}
      assert cmd(sock, ["GET", k]) == "value1"
      :gen_tcp.close(sock)
    end

    test "GET on missing key returns nil" do
      sock = connect()
      k = ukey("missing")
      assert cmd(sock, ["GET", k]) == nil
      :gen_tcp.close(sock)
    end

    test "SET overwrites existing value" do
      sock = connect()
      k = ukey("overwrite")
      cmd(sock, ["SET", k, "v1"])
      cmd(sock, ["SET", k, "v2"])
      assert cmd(sock, ["GET", k]) == "v2"
      :gen_tcp.close(sock)
    end

    test "DEL removes a key" do
      sock = connect()
      k = ukey("del")
      cmd(sock, ["SET", k, "to_delete"])
      assert cmd(sock, ["DEL", k]) == 1
      assert cmd(sock, ["GET", k]) == nil
      :gen_tcp.close(sock)
    end

    test "DEL returns 0 for missing key" do
      sock = connect()
      k = ukey("del_missing")
      assert cmd(sock, ["DEL", k]) == 0
      :gen_tcp.close(sock)
    end

    test "EXISTS returns 1 for present key, 0 for absent" do
      sock = connect()
      k = ukey("exists")
      cmd(sock, ["SET", k, "x"])
      assert cmd(sock, ["EXISTS", k]) == 1
      assert cmd(sock, ["EXISTS", ukey("nope")]) == 0
      :gen_tcp.close(sock)
    end

    test "SET NX succeeds on missing key, fails on existing" do
      sock = connect()
      k = ukey("nx")
      assert cmd(sock, ["SET", k, "first", "NX"]) == {:simple, "OK"}
      assert cmd(sock, ["SET", k, "second", "NX"]) == nil
      assert cmd(sock, ["GET", k]) == "first"
      :gen_tcp.close(sock)
    end

    test "SET XX fails on missing key, succeeds on existing" do
      sock = connect()
      k = ukey("xx")
      assert cmd(sock, ["SET", k, "v", "XX"]) == nil
      cmd(sock, ["SET", k, "initial"])
      assert cmd(sock, ["SET", k, "updated", "XX"]) == {:simple, "OK"}
      assert cmd(sock, ["GET", k]) == "updated"
      :gen_tcp.close(sock)
    end
  end

  describe "MSET / MGET" do
    test "MSET stores multiple keys atomically" do
      sock = connect()
      k1 = ukey("mset_a")
      k2 = ukey("mset_b")
      assert cmd(sock, ["MSET", k1, "alpha", k2, "beta"]) == {:simple, "OK"}
      assert cmd(sock, ["GET", k1]) == "alpha"
      assert cmd(sock, ["GET", k2]) == "beta"
      :gen_tcp.close(sock)
    end

    test "MGET returns values in order, nil for missing" do
      sock = connect()
      k1 = ukey("mget_present")
      k2 = ukey("mget_missing")
      cmd(sock, ["SET", k1, "found"])
      result = cmd(sock, ["MGET", k1, k2])
      assert result == ["found", nil]
      :gen_tcp.close(sock)
    end
  end

  describe "TTL / EXPIRE / PERSIST" do
    test "SET with EX, TTL returns remaining seconds" do
      sock = connect()
      k = ukey("ttl_ex")
      cmd(sock, ["SET", k, "v", "EX", "100"])
      ttl = cmd(sock, ["TTL", k])
      assert is_integer(ttl) and ttl > 0 and ttl <= 100
      :gen_tcp.close(sock)
    end

    test "SET with PX, PTTL returns remaining ms" do
      sock = connect()
      k = ukey("pttl_px")
      cmd(sock, ["SET", k, "v", "PX", "10000"])
      pttl = cmd(sock, ["PTTL", k])
      assert is_integer(pttl) and pttl > 0 and pttl <= 10_000
      :gen_tcp.close(sock)
    end

    test "TTL on persistent key returns -1" do
      sock = connect()
      k = ukey("ttl_persist")
      cmd(sock, ["SET", k, "v"])
      assert cmd(sock, ["TTL", k]) == -1
      :gen_tcp.close(sock)
    end

    test "TTL on missing key returns -2" do
      sock = connect()
      k = ukey("ttl_missing")
      assert cmd(sock, ["TTL", k]) == -2
      :gen_tcp.close(sock)
    end

    test "EXPIRE sets expiry, PERSIST removes it" do
      sock = connect()
      k = ukey("persist")
      cmd(sock, ["SET", k, "v"])
      cmd(sock, ["EXPIRE", k, "100"])
      assert cmd(sock, ["TTL", k]) > 0
      cmd(sock, ["PERSIST", k])
      assert cmd(sock, ["TTL", k]) == -1
      :gen_tcp.close(sock)
    end

    test "key expires naturally after PX window" do
      sock = connect()
      k = ukey("px_expire")
      cmd(sock, ["SET", k, "temp", "PX", "200"])
      assert cmd(sock, ["GET", k]) == "temp"
      Process.sleep(300)
      assert cmd(sock, ["GET", k]) == nil
      :gen_tcp.close(sock)
    end
  end

  describe "KEYS / DBSIZE" do
    test "KEYS * includes recently set key" do
      sock = connect()
      k = ukey("keys_star")
      cmd(sock, ["SET", k, "x"])
      keys = cmd(sock, ["KEYS", "*"])
      assert is_list(keys) and k in keys
      :gen_tcp.close(sock)
    end

    test "KEYS with glob pattern filters results" do
      sock = connect()
      prefix = "kglob_#{:rand.uniform(9_999_999)}"
      k1 = "#{prefix}_apple"
      k2 = "#{prefix}_banana"
      k3 = ukey("other")
      cmd(sock, ["SET", k1, "a"])
      cmd(sock, ["SET", k2, "b"])
      cmd(sock, ["SET", k3, "c"])
      keys = cmd(sock, ["KEYS", "#{prefix}_*"])
      assert is_list(keys)
      assert k1 in keys
      assert k2 in keys
      refute k3 in keys
      :gen_tcp.close(sock)
    end

    test "DBSIZE is non-negative integer" do
      sock = connect()
      result = cmd(sock, ["DBSIZE"])
      assert is_integer(result) and result >= 0
      :gen_tcp.close(sock)
    end

    test "DBSIZE increases after SET" do
      sock = connect()
      before = cmd(sock, ["DBSIZE"])
      cmd(sock, ["SET", ukey("dbsize_delta"), "v"])
      after_set = cmd(sock, ["DBSIZE"])
      assert after_set >= before + 1
      :gen_tcp.close(sock)
    end
  end

  describe "HELLO handshake" do
    test "HELLO 3 returns server info map" do
      sock = connect()
      result = cmd(sock, ["HELLO", "3"])
      assert is_map(result)
      assert result["server"] == "ferricstore"
      assert result["proto"] == 3
      assert Map.has_key?(result, "version")
      assert Map.has_key?(result, "id")
      :gen_tcp.close(sock)
    end

    test "HELLO without version returns server map" do
      sock = connect()
      result = cmd(sock, ["HELLO"])
      assert is_map(result)
      assert result["server"] == "ferricstore"
      :gen_tcp.close(sock)
    end

    test "HELLO 2 returns NOPROTO error" do
      sock = connect()
      result = cmd(sock, ["HELLO", "2"])
      assert {:error, msg} = result
      assert String.contains?(msg, "NOPROTO")
      :gen_tcp.close(sock)
    end
  end

  describe "connection commands" do
    test "QUIT closes the connection" do
      sock = connect()
      assert cmd(sock, ["QUIT"]) == {:simple, "OK"}
      # After QUIT the server closes — recv should return error or empty
      assert :gen_tcp.recv(sock, 0, 500) in [{:error, :closed}, {:error, :econnreset}]
      :gen_tcp.close(sock)
    end

    test "RESET returns RESET status" do
      sock = connect()
      assert cmd(sock, ["RESET"]) == {:simple, "RESET"}
      :gen_tcp.close(sock)
    end
  end

  describe "pipelining" do
    test "multiple commands in one TCP write, responses in order" do
      sock = connect()
      k1 = ukey("pipe_a")
      k2 = ukey("pipe_b")

      # Send 3 commands in a single write
      data =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k1, "1"]),
          Encoder.encode(["SET", k2, "2"]),
          Encoder.encode(["MGET", k1, k2])
        ])

      :ok = :gen_tcp.send(sock, data)

      assert recv(sock, "") == {:simple, "OK"}
      assert recv(sock, "") == {:simple, "OK"}
      assert recv(sock, "") == ["1", "2"]

      :gen_tcp.close(sock)
    end

    test "100-command pipeline" do
      sock = connect()
      prefix = "pipeline_#{:rand.uniform(9_999_999)}"

      # Send 100 SET commands
      sets =
        for i <- 1..100 do
          Encoder.encode(["SET", "#{prefix}_#{i}", "val#{i}"])
        end

      :ok = :gen_tcp.send(sock, IO.iodata_to_binary(sets))

      # Drain 100 responses
      for _ <- 1..100, do: recv(sock, "")

      # Spot-check a few
      assert cmd(sock, ["GET", "#{prefix}_1"]) == "val1"
      assert cmd(sock, ["GET", "#{prefix}_50"]) == "val50"
      assert cmd(sock, ["GET", "#{prefix}_100"]) == "val100"

      :gen_tcp.close(sock)
    end
  end

  describe "error handling" do
    test "unknown command returns error" do
      sock = connect()
      result = cmd(sock, ["NOTACOMMAND"])
      assert {:error, _msg} = result
      :gen_tcp.close(sock)
    end

    test "wrong arity returns error" do
      sock = connect()
      result = cmd(sock, ["GET"])
      assert {:error, _msg} = result
      :gen_tcp.close(sock)
    end

    test "SET with invalid EX returns error" do
      sock = connect()
      k = ukey("bad_ex")
      result = cmd(sock, ["SET", k, "v", "EX", "-1"])
      assert {:error, _msg} = result
      :gen_tcp.close(sock)
    end

    test "SET with non-integer EX returns error" do
      sock = connect()
      k = ukey("bad_ex_str")
      result = cmd(sock, ["SET", k, "v", "EX", "notanumber"])
      assert {:error, _msg} = result
      :gen_tcp.close(sock)
    end

    test "EXPIRE with non-integer seconds returns error" do
      sock = connect()
      k = ukey("bad_expire")
      cmd(sock, ["SET", k, "v"])
      result = cmd(sock, ["EXPIRE", k, "notanumber"])
      assert {:error, _msg} = result
      :gen_tcp.close(sock)
    end
  end

  describe "binary safety" do
    test "keys and values with spaces and special characters" do
      sock = connect()
      k = "smoke key with spaces #{:rand.uniform(9_999_999)}"
      v = "value\twith\nnewlines and \"quotes\""
      cmd(sock, ["SET", k, v])
      assert cmd(sock, ["GET", k]) == v
      :gen_tcp.close(sock)
    end

    test "empty string value" do
      sock = connect()
      k = ukey("empty_val")
      cmd(sock, ["SET", k, ""])
      assert cmd(sock, ["GET", k]) == ""
      :gen_tcp.close(sock)
    end

    test "large value (64KB)" do
      sock = connect()
      k = ukey("large_val")
      v = String.duplicate("x", 64 * 1024)
      cmd(sock, ["SET", k, v])
      result = cmd(sock, ["GET", k])
      assert result == v
      :gen_tcp.close(sock)
    end

    test "unicode value" do
      sock = connect()
      k = ukey("unicode_val")
      v = "こんにちは世界 🦀"
      cmd(sock, ["SET", k, v])
      assert cmd(sock, ["GET", k]) == v
      :gen_tcp.close(sock)
    end
  end
end
