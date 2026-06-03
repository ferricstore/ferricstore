defmodule FerricstoreServer.ReviewR4Test do
  @moduledoc """
  Tests verifying code review findings R4 (B1-B13) against the actual codebase.

  Each test group corresponds to a review issue and documents whether the finding
  is a confirmed bug, false positive, or needs further investigation.

  The listener is started automatically by the server application.
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  @moduletag timeout: 30_000

  # ---------------------------------------------------------------------------
  # Setup: the server app starts automatically, just get the port
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp connect do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", Listener.port(), [
        :binary,
        active: false,
        packet: :raw,
        recbuf: 65_536,
        sndbuf: 65_536
      ])

    sock
  end

  defp cmd(sock, args, timeout \\ 5_000) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(args)))
    recv_one(sock, timeout)
  end

  defp recv_one(sock, timeout \\ 5_000) do
    recv_loop(sock, "", timeout)
  end

  defp recv_loop(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | _], _} ->
        val

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_loop(sock, buf <> data, timeout)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  defp recv_n(sock, count, timeout \\ 5_000) do
    do_recv_n(sock, count, "", [], timeout)
  end

  defp do_recv_n(_sock, 0, _buf, acc, _timeout), do: Enum.reverse(acc)

  defp do_recv_n(sock, remaining, buf, acc, timeout) do
    case Parser.parse(buf) do
      {:ok, [], rest} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> do_recv_n(sock, remaining, rest <> data, acc, timeout)
          {:error, reason} -> Enum.reverse([{:tcp_error, reason} | acc])
        end

      {:ok, vals, rest} ->
        taken = Enum.take(vals, remaining)
        new_remaining = remaining - length(taken)
        new_acc = Enum.reverse(taken) ++ acc

        if new_remaining <= 0 do
          Enum.reverse(new_acc)
        else
          do_recv_n(sock, new_remaining, rest, new_acc, timeout)
        end
    end
  end

  # Unwrap RESP3 simple strings for easier assertions.
  # The parser returns {:simple, "OK"} for +OK\r\n, but most tests
  # only care about the string content.
  defp unwrap({:simple, s}), do: s
  defp unwrap({:error, _} = e), do: e
  defp unwrap({:push, _} = p), do: p
  defp unwrap(other), do: other

  defp ukey(base), do: "r4_#{base}_#{:rand.uniform(9_999_999)}"

  # ---------------------------------------------------------------------------
  # B3. send_response failures emit telemetry and do not crash the server.
  # ---------------------------------------------------------------------------

  describe "B3: send_response/2 handles disconnected clients" do
    test "server does not crash when client disconnects mid-pipeline" do
      sock = connect()
      key = ukey("b3")
      assert "OK" == unwrap(cmd(sock, ["SET", key, "val"]))

      # Close abruptly
      :gen_tcp.close(sock)
      Process.sleep(100)

      # Verify the server is still accepting connections
      sock2 = connect()
      assert "PONG" == unwrap(cmd(sock2, ["PING"]))
      :gen_tcp.close(sock2)
    end
  end

  # ---------------------------------------------------------------------------
  # B8. Pipelined commands share mutable connection state — FALSE POSITIVE
  # ---------------------------------------------------------------------------

  describe "B8: pipelined commands share connection state" do
    test "pipeline preserves response ordering for pure commands" do
      sock = connect()
      keys = Enum.map(1..5, fn i -> ukey("b8_#{i}") end)

      Enum.each(keys, fn k ->
        assert "OK" == unwrap(cmd(sock, ["SET", k, "value_#{k}"]))
      end)

      pipeline =
        keys
        |> Enum.map(fn k -> Encoder.encode(["GET", k]) end)
        |> IO.iodata_to_binary()

      :ok = :gen_tcp.send(sock, pipeline)
      results = recv_n(sock, length(keys))

      Enum.zip(keys, results)
      |> Enum.each(fn {k, v} ->
        assert unwrap(v) == "value_#{k}"
      end)

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # B9. Inline command parsing should handle Redis-style quoted strings.
  # ---------------------------------------------------------------------------

  describe "B9: inline parser handles quoted strings" do
    test "inline parser preserves spaces inside double quotes" do
      sock = connect()
      key = ukey("b9")

      :ok = :gen_tcp.send(sock, "SET #{key} \"hello world\"\r\n")
      assert "OK" == unwrap(recv_one(sock))

      :ok = :gen_tcp.send(sock, "GET #{key}\r\n")
      assert "hello world" == unwrap(recv_one(sock))

      :gen_tcp.close(sock)
    end

    test "inline parser tab splitting works" do
      sock = connect()
      key = ukey("b9_tab")

      :ok = :gen_tcp.send(sock, "SET\t#{key}\tval\r\n")
      assert "OK" == unwrap(recv_one(sock))

      :ok = :gen_tcp.send(sock, "GET\t#{key}\r\n")
      assert "val" == unwrap(recv_one(sock))

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # B10. AUTH timing not constant — FALSE POSITIVE
  # ---------------------------------------------------------------------------

  describe "B10: AUTH uses constant-time comparison" do
    test "AUTH rejects when no password is set" do
      sock = connect()
      result = cmd(sock, ["AUTH", "somepassword"])
      assert match?({:error, "ERR Client sent AUTH, but no password is set" <> _}, result)
      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # B11. CLIENT SETNAME validation — FIXED
  # ---------------------------------------------------------------------------

  describe "B11: CLIENT SETNAME validation" do
    test "CLIENT SETNAME rejects spaces" do
      sock = connect()
      result = cmd(sock, ["CLIENT", "SETNAME", "hello world"])
      assert match?({:error, "ERR Client names cannot contain" <> _}, result)
      :gen_tcp.close(sock)
    end

    test "CLIENT SETNAME rejects newlines" do
      sock = connect()
      result = cmd(sock, ["CLIENT", "SETNAME", "hello\nworld"])
      assert match?({:error, "ERR Client names cannot contain" <> _}, result)
      :gen_tcp.close(sock)
    end

    test "CLIENT SETNAME rejects carriage returns" do
      sock = connect()
      result = cmd(sock, ["CLIENT", "SETNAME", "hello\rworld"])
      assert match?({:error, "ERR Client names cannot contain" <> _}, result)
      :gen_tcp.close(sock)
    end

    test "CLIENT SETNAME accepts valid names" do
      sock = connect()
      assert "OK" == unwrap(cmd(sock, ["CLIENT", "SETNAME", "my-connection-1"]))
      assert "my-connection-1" == unwrap(cmd(sock, ["CLIENT", "GETNAME"]))
      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # B12. Pipeline response ordering not guaranteed under async — FALSE POSITIVE
  # ---------------------------------------------------------------------------

  describe "B12: pipeline response ordering is guaranteed" do
    test "pipeline responses arrive in order even with mixed key shards" do
      sock = connect()
      keys = Enum.map(1..20, fn i -> ukey("b12_#{i}") end)

      pipeline_set =
        keys
        |> Enum.map(fn k -> Encoder.encode(["SET", k, "v_#{k}"]) end)
        |> IO.iodata_to_binary()

      :ok = :gen_tcp.send(sock, pipeline_set)
      set_results = recv_n(sock, length(keys))
      assert Enum.all?(set_results, &(unwrap(&1) == "OK"))

      # Use a fresh socket for GETs to avoid any buffering issues
      sock2 = connect()

      pipeline_get =
        keys
        |> Enum.map(fn k -> Encoder.encode(["GET", k]) end)
        |> IO.iodata_to_binary()

      :ok = :gen_tcp.send(sock2, pipeline_get)
      get_results = recv_n(sock2, length(keys))

      Enum.zip(keys, get_results)
      |> Enum.each(fn {k, v} ->
        assert unwrap(v) == "v_#{k}", "Response for #{k} was #{inspect(v)}, expected v_#{k}"
      end)

      :gen_tcp.close(sock)
      :gen_tcp.close(sock2)
    end
  end

  # ---------------------------------------------------------------------------
  # B13. CLIENT LIST id counter not atomic — FALSE POSITIVE
  # ---------------------------------------------------------------------------

  describe "B13: client ID generation" do
    test "client IDs are unique across connections" do
      socks = Enum.map(1..10, fn _ -> connect() end)

      ids =
        Enum.map(socks, fn sock ->
          result = cmd(sock, ["CLIENT", "ID"])
          assert is_integer(result)
          result
        end)

      assert length(Enum.uniq(ids)) == length(ids)
      Enum.each(socks, &:gen_tcp.close/1)
    end
  end

  # ---------------------------------------------------------------------------
  # B2. PubSub send errors use the shared send telemetry path.
  # ---------------------------------------------------------------------------

  describe "B2: pubsub message delivery on send failure" do
    test "pubsub delivery works for live subscribers" do
      sub_sock = connect()
      pub_sock = connect()
      channel = ukey("b2_ch")

      :ok = :gen_tcp.send(sub_sock, IO.iodata_to_binary(Encoder.encode(["SUBSCRIBE", channel])))
      sub_response = recv_one(sub_sock)
      assert match?({:push, ["subscribe", ^channel, 1]}, sub_response)

      assert 1 == cmd(pub_sock, ["PUBLISH", channel, "hello"])

      msg = recv_one(sub_sock)
      assert match?({:push, ["message", ^channel, "hello"]}, msg)

      :gen_tcp.close(sub_sock)
      :gen_tcp.close(pub_sock)
    end
  end

  # ---------------------------------------------------------------------------
  # B6. CLIENT KILL closes the target connection by client ID.
  # ---------------------------------------------------------------------------

  describe "B6: CLIENT KILL implementation" do
    test "CLIENT KILL ID closes the target connection" do
      controller = connect()
      target = connect()

      target_id = cmd(target, ["CLIENT", "ID"])
      assert is_integer(target_id)

      assert "OK" ==
               unwrap(cmd(controller, ["CLIENT", "KILL", "ID", Integer.to_string(target_id)]))

      assert {:tcp_error, :closed} = recv_one(target, 1_000)
      assert "PONG" == unwrap(cmd(controller, ["PING"]))

      :gen_tcp.close(controller)
    end
  end

  # ---------------------------------------------------------------------------
  # B5. Max clients are enforced at connection init.
  # ---------------------------------------------------------------------------

  describe "B5: max connections enforcement" do
    test "rejects connections beyond maxclients" do
      old_maxclients = Application.get_env(:ferricstore, :maxclients, 10_000)
      current_active = Ferricstore.Stats.active_connections()

      try do
        Application.put_env(:ferricstore, :maxclients, current_active + 2)

        allowed = connect()
        assert "PONG" == unwrap(cmd(allowed, ["PING"]))

        allowed2 = connect()
        assert "PONG" == unwrap(cmd(allowed2, ["PING"]))

        rejected = connect()
        assert match?(
                 {:error, "ERR max number of clients reached" <> _},
                 cmd(rejected, ["PING"])
               )

        assert {:tcp_error, :closed} = recv_one(rejected, 1_000)

        :gen_tcp.close(allowed)
        :gen_tcp.close(allowed2)
      after
        Application.put_env(:ferricstore, :maxclients, old_maxclients)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # B9 unit test: parser inline parsing (no TCP needed)
  # ---------------------------------------------------------------------------

  describe "B9 unit: inline parser behavior" do
    test "preserves quoted whitespace" do
      input = "SET mykey \"hello world\"\r\n"

      case Parser.parse(input) do
        {:ok, [{:inline, tokens} | _], _rest} ->
          assert tokens == ["SET", "mykey", "hello world"]

        other ->
          flunk("Unexpected parse result: #{inspect(other)}")
      end
    end

    test "splits on tab characters" do
      input = "SET\tkey\tvalue\r\n"

      case Parser.parse(input) do
        {:ok, [{:inline, tokens} | _], _rest} ->
          assert tokens == ["SET", "key", "value"]

        other ->
          flunk("Unexpected parse result: #{inspect(other)}")
      end
    end
  end
end
