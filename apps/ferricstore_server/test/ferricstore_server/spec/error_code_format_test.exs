defmodule FerricstoreServer.Spec.ErrorCodeFormatTest do
  @moduledoc """
  Spec section 4.6: Error Code Format tests.

  Verifies that all error responses match the spec-required format:
    - ERR prefix for generic errors
    - WRONGTYPE prefix for type mismatches
    - NOAUTH for unauthenticated commands
    - OOM for memory limit (MemoryGuard noeviction rejection)
    - BUSYKEY for RENAME conflicts

  These tests verify the error message format at the handler level (unit) and
  over TCP (integration) to ensure RESP encoding preserves the correct prefix.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, Generic, Strings}
  alias Ferricstore.Config
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.MockStore

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
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
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

  defp ukey(base), do: "err_fmt_#{base}_#{:rand.uniform(999_999)}"

  defp eventually(fun, attempts \\ 50) do
    if fun.() do
      :ok
    else
      if attempts > 0 do
        Process.sleep(20)
        eventually(fun, attempts - 1)
      else
        flunk("Condition not met after retries")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    on_exit(fn -> Config.set("requirepass", "") end)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Section 4.6: ERR prefix for generic errors
  # ---------------------------------------------------------------------------

  describe "ERR prefix for generic errors (spec 4.6)" do
    test "wrong number of arguments returns ERR prefix" do
      store = MockStore.make()

      # GET with no args
      assert {:error, "ERR wrong number of arguments" <> _} =
               Strings.handle("GET", [], store)

      # SET with no args
      assert {:error, "ERR wrong number of arguments" <> _} =
               Strings.handle("SET", [], store)

      # DEL with no args
      assert {:error, "ERR wrong number of arguments" <> _} =
               Strings.handle("DEL", [], store)

      # RENAME with wrong args
      assert {:error, "ERR wrong number of arguments" <> _} =
               Generic.handle("RENAME", [], store)
    end

    test "unknown command returns ERR prefix" do
      store = MockStore.make()

      assert {:error, "ERR unknown command" <> _} =
               Dispatcher.dispatch("NONEXISTENT", [], store)
    end

    test "syntax error returns ERR prefix" do
      store = MockStore.make()

      # COPY with invalid option
      assert {:error, "ERR syntax error"} =
               Generic.handle("COPY", ["src", "dst", "BADOPT"], store)
    end

    test "value not an integer returns ERR prefix" do
      store = MockStore.make(%{"k" => {"not_a_number", 0}})

      assert {:error, "ERR value is not an integer or out of range"} =
               Strings.handle("INCR", ["k"], store)
    end

    test "empty key returns ERR prefix" do
      store = MockStore.make()

      assert {:error, "ERR empty key"} = Strings.handle("GET", [""], store)
    end

    test "invalid expire time returns ERR prefix" do
      store = MockStore.make()

      assert {:error, msg} = Strings.handle("SET", ["k", "v", "EX", "0"], store)
      assert String.starts_with?(msg, "ERR")
    end

    test "ERR errors are encoded as RESP3 simple errors over TCP", %{port: port} do
      sock = connect_and_hello(port)

      # Send a command with wrong arity
      send_cmd(sock, ["GET"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert String.starts_with?(msg, "ERR")

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Section 4.6: WRONGTYPE prefix for type mismatches
  # ---------------------------------------------------------------------------

  describe "WRONGTYPE prefix for type mismatches (spec 4.6)" do
    test "LPUSH on a string key returns WRONGTYPE" do
      k = ukey("wrongtype_str")
      Router.put(FerricStore.Instance.get(:default), k, "string_value", 0)

      # Wait until key is visible in ETS (Raft commit may take time on slow CI)
      eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) != nil end)

      live_store = build_live_store()
      result = Dispatcher.dispatch("LPUSH", [k, "elem"], live_store)

      assert {:error, "WRONGTYPE" <> _} = result

      Router.delete(FerricStore.Instance.get(:default), k)
    end

    test "HSET on a list key returns WRONGTYPE", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("wrongtype_list")

      # Create a list key
      send_cmd(sock, ["RPUSH", k, "a", "b"])
      recv_response(sock)

      # Try a hash command on it
      send_cmd(sock, ["HSET", k, "field", "value"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert String.starts_with?(msg, "WRONGTYPE")

      # Cleanup
      send_cmd(sock, ["DEL", k])
      recv_response(sock)
      :gen_tcp.close(sock)
    end

    test "SADD on a hash key returns WRONGTYPE", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("wrongtype_hash")

      # Create a hash key
      send_cmd(sock, ["HSET", k, "f1", "v1"])
      recv_response(sock)

      # Try a set command on it
      send_cmd(sock, ["SADD", k, "member"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert String.starts_with?(msg, "WRONGTYPE")

      # Cleanup
      send_cmd(sock, ["DEL", k])
      recv_response(sock)
      :gen_tcp.close(sock)
    end

    test "WRONGTYPE errors are encoded as RESP3 simple errors over TCP", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("wrongtype_tcp")

      # Create a hash key via TCP
      send_cmd(sock, ["HSET", k, "field", "val"])
      recv_response(sock)

      # Try string GET on it — GET should work on any key (strings.ex reads raw)
      # But LPUSH on a hash should fail with WRONGTYPE
      send_cmd(sock, ["LPUSH", k, "elem"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert String.starts_with?(msg, "WRONGTYPE")

      send_cmd(sock, ["DEL", k])
      recv_response(sock)
      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Section 4.6: NOAUTH for unauthenticated commands
  # ---------------------------------------------------------------------------

  describe "NOAUTH for unauthenticated commands (spec 4.6)" do
    test "commands return NOAUTH when requirepass is set and client not authenticated", %{
      port: port
    } do
      # Set a password
      Config.set("requirepass", "secret123")

      # Open a new connection (not authenticated)
      sock = connect_and_hello(port)

      # Try to execute a command without AUTH
      send_cmd(sock, ["PING"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert String.starts_with?(msg, "NOAUTH")

      :gen_tcp.close(sock)
    end

    test "AUTH with wrong password returns WRONGPASS", %{port: port} do
      Config.set("requirepass", "secret123")

      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "wrongpassword"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert String.starts_with?(msg, "WRONGPASS")

      :gen_tcp.close(sock)
    end

    test "after successful AUTH, commands work normally", %{port: port} do
      Config.set("requirepass", "secret123")

      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "secret123"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "HELLO and AUTH are allowed without authentication", %{port: port} do
      Config.set("requirepass", "secret123")

      sock = connect_and_hello(port)

      # AUTH should be allowed
      send_cmd(sock, ["AUTH", "secret123"])
      response = recv_response(sock)
      assert response == {:simple, "OK"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Section 4.6: COPY no-op result and BUSYKEY for RENAME conflicts
  # ---------------------------------------------------------------------------

  describe "COPY no-op semantics (spec 4.6)" do
    test "COPY to existing key without REPLACE returns 0" do
      store =
        MockStore.make(%{
          "src" => {"value", 0},
          "dst" => {"existing", 0}
        })

      result = Generic.handle("COPY", ["src", "dst"], store)
      assert result == 0
    end

    test "COPY to existing key with REPLACE succeeds" do
      store =
        MockStore.make(%{
          "src" => {"value", 0},
          "dst" => {"existing", 0}
        })

      result = Generic.handle("COPY", ["src", "dst", "REPLACE"], store)
      assert result == 1
    end

    test "COPY from non-existent key returns 0" do
      store = MockStore.make()

      result = Generic.handle("COPY", ["nonexistent", "dst"], store)
      assert result == 0
    end

    test "COPY no-op result preserved over TCP", %{port: port} do
      sock = connect_and_hello(port)
      src = ukey("copy_src")
      dst = ukey("copy_dst")

      # Create both keys
      send_cmd(sock, ["SET", src, "sv"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", dst, "dv"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["COPY", src, dst])
      assert recv_response(sock) == 0

      # Cleanup
      send_cmd(sock, ["DEL", src, dst])
      recv_response(sock)
      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Section 4.6: ERR for RENAME on missing keys
  # ---------------------------------------------------------------------------

  describe "ERR for RENAME on missing source key (spec 4.6)" do
    test "RENAME with missing source returns ERR no such key" do
      store = MockStore.make()

      result = Generic.handle("RENAME", ["nonexistent", "newname"], store)
      assert {:error, "ERR no such key"} = result
    end

    test "RENAMENX with missing source returns ERR no such key" do
      store = MockStore.make()

      result = Generic.handle("RENAMENX", ["nonexistent", "newname"], store)
      assert {:error, "ERR no such key"} = result
    end
  end

  # ---------------------------------------------------------------------------
  # Section 4.6: General error format consistency
  # ---------------------------------------------------------------------------

  describe "error format consistency across commands (spec 4.6)" do
    test "all error responses use {:error, string} tuple format" do
      store = MockStore.make()

      error_cases = [
        {"GET", []},
        {"SET", []},
        {"DEL", []},
        {"RENAME", []},
        {"RENAMENX", []},
        {"COPY", []},
        {"TYPE", []},
        {"RANDOMKEY", ["extra"]},
        {"SCAN", []},
        {"WAIT", []}
      ]

      Enum.each(error_cases, fn {cmd, args} ->
        result = Dispatcher.dispatch(cmd, args, store)

        assert match?({:error, msg} when is_binary(msg), result),
               "#{cmd} #{inspect(args)} should return {:error, binary}, got: #{inspect(result)}"

        {:error, msg} = result
        # All errors should start with a known prefix
        assert String.starts_with?(msg, "ERR") or
                 String.starts_with?(msg, "WRONGTYPE") or
                 String.starts_with?(msg, "NOAUTH") or
                 String.starts_with?(msg, "OOM") or
                 String.starts_with?(msg, "NOPROTO"),
               "Error message for #{cmd} has unexpected prefix: #{inspect(msg)}"
      end)
    end

    test "RESP3 encoding of errors produces '-' prefix on wire" do
      # Verify that {:error, msg} is encoded as a RESP3 simple error
      encoded = Encoder.encode({:error, "ERR test error"}) |> IO.iodata_to_binary()
      assert encoded == "-ERR test error\r\n"

      encoded2 = Encoder.encode({:error, "WRONGTYPE bad type"}) |> IO.iodata_to_binary()
      assert encoded2 == "-WRONGTYPE bad type\r\n"

      encoded3 =
        Encoder.encode({:error, "NOAUTH Authentication required."}) |> IO.iodata_to_binary()

      assert encoded3 == "-NOAUTH Authentication required.\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Private: build a live store (wrapping Router)
  # ---------------------------------------------------------------------------

  defp build_live_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      flush: fn ->
        Enum.each(Router.keys(FerricStore.Instance.get(:default)), fn k ->
          Router.delete(FerricStore.Instance.get(:default), k)
        end)

        :ok
      end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end,
      incr: fn k, d -> Router.incr(FerricStore.Instance.get(:default), k, d) end,
      incr_float: fn k, d -> Router.incr_float(FerricStore.Instance.get(:default), k, d) end,
      append: fn k, s -> Router.append(FerricStore.Instance.get(:default), k, s) end,
      getset: fn k, v -> Router.getset(FerricStore.Instance.get(:default), k, v) end,
      getdel: fn k -> Router.getdel(FerricStore.Instance.get(:default), k) end,
      getex: fn k, e -> Router.getex(FerricStore.Instance.get(:default), k, e) end,
      setrange: fn k, o, v -> Router.setrange(FerricStore.Instance.get(:default), k, o, v) end,
      cas: fn k, e, n, t -> Router.cas(FerricStore.Instance.get(:default), k, e, n, t) end,
      lock: fn k, o, t -> Router.lock(FerricStore.Instance.get(:default), k, o, t) end,
      unlock: fn k, o -> Router.unlock(FerricStore.Instance.get(:default), k, o) end,
      extend: fn k, o, t -> Router.extend(FerricStore.Instance.get(:default), k, o, t) end,
      ratelimit_add: fn k, w, m, c ->
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, w, m, c)
      end,
      list_op: fn k, op -> Router.list_op(FerricStore.Instance.get(:default), k, op) end,
      compound_get: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:get, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:get_meta, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:put, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:delete, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:scan_prefix, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:count_prefix, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:delete_prefix, prefix})
      end
    }
  end
end
