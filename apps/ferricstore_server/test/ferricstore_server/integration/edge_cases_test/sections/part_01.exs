defmodule FerricstoreServer.Integration.EdgeCasesTest.Sections.Part01 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Store.Router
      alias FerricstoreServer.Resp.{Encoder, Parser}
      alias FerricstoreServer.Listener

  describe "value size boundaries" do
    test "empty value (0 bytes) round-trips correctly" do
      k = ukey("empty")
      assert :ok == Router.put(FerricStore.Instance.get(:default), k, "", 0)
      assert "" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "1-byte value round-trips correctly" do
      k = ukey("one_byte")
      assert :ok == Router.put(FerricStore.Instance.get(:default), k, "x", 0)
      assert "x" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "value at exactly 1 MB round-trips correctly" do
      k = ukey("1mb")
      v = :binary.copy("A", 1_048_576)
      assert :ok == Router.put(FerricStore.Instance.get(:default), k, v, 0)
      assert v == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "value at exactly 10 MB round-trips correctly" do
      k = ukey("10mb")
      v = :binary.copy("B", 10_000_000)
      assert :ok == Router.put(FerricStore.Instance.get(:default), k, v, 0)
      assert v == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "value at 32 MB round-trips correctly" do
      k = ukey("32mb")
      v = :binary.copy("C", 32_000_000)
      assert :ok == Router.put(FerricStore.Instance.get(:default), k, v, 0)
      assert v == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "value content is byte-exact after round-trip at 10 MB" do
      k = ukey("byte_exact_10mb")
      # Use a non-repeating pattern to catch offset/truncation bugs
      v = for i <- 0..9_999_999, into: <<>>, do: <<rem(i, 251)>>
      assert :ok == Router.put(FerricStore.Instance.get(:default), k, v, 0)
      result = Router.get(FerricStore.Instance.get(:default), k)
      assert byte_size(result) == 10_000_000
      assert result == v
    end

    test "overwrite large value with small value, GET returns new value" do
      k = ukey("overwrite_large")
      big = :binary.copy("Z", 1_000_000)
      small = "tiny"
      Router.put(FerricStore.Instance.get(:default), k, big, 0)
      assert big == Router.get(FerricStore.Instance.get(:default), k)
      Router.put(FerricStore.Instance.get(:default), k, small, 0)
      assert small == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "overwrite small value with large value, GET returns new value" do
      k = ukey("overwrite_small")
      Router.put(FerricStore.Instance.get(:default), k, "tiny", 0)
      big = :binary.copy("Q", 500_000)
      Router.put(FerricStore.Instance.get(:default), k, big, 0)
      assert big == Router.get(FerricStore.Instance.get(:default), k)
    end

    # The Rust NIF guard caps values at 512 MiB. Anything larger is rejected
    # with {:error, "value too large: ..."} before any disk I/O occurs.
    test "value at 512 MiB limit is documented as the enforced ceiling" do
      assert @max_value_bytes == 512 * 1024 * 1024
    end

  end
  describe "TTL edge cases" do
    test "expire_at_ms = 0 means no expiry (key lives forever)" do
      k = ukey("no_expiry")
      Router.put(FerricStore.Instance.get(:default), k, "permanent", 0)
      # PUT with expire_at=0 is synchronous via Raft — no sleep needed
      assert "permanent" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "key expires before read returns nil" do
      k = ukey("past_expiry")
      past = System.os_time(:millisecond) - 1
      Router.put(FerricStore.Instance.get(:default), k, "ghost", past)
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "key expiring in 1ms: readable immediately, nil after expiry" do
      k = ukey("1ms_ttl")
      expire_at = System.os_time(:millisecond) + 1
      Router.put(FerricStore.Instance.get(:default), k, "ephemeral", expire_at)
      # May or may not be readable immediately depending on scheduling
      _ = Router.get(FerricStore.Instance.get(:default), k)

      Ferricstore.Test.ShardHelpers.eventually(fn ->
        nil == Router.get(FerricStore.Instance.get(:default), k)
      end, "key with 1ms TTL should expire", 20, 5)
    end

    test "key expiring soon is readable before expiry, nil after" do
      k = ukey("soon_ttl")
      expire_at = System.os_time(:millisecond) + 500
      Router.put(FerricStore.Instance.get(:default), k, "brief", expire_at)
      assert "brief" == Router.get(FerricStore.Instance.get(:default), k)

      Ferricstore.Test.ShardHelpers.eventually(fn ->
        nil == Router.get(FerricStore.Instance.get(:default), k)
      end, "key with short TTL should expire", 1_000, 10)
    end

    test "expired key is not included in Router.keys(FerricStore.Instance.get(:default))" do
      k = ukey("expired_keys")
      past = System.os_time(:millisecond) - 1
      Router.put(FerricStore.Instance.get(:default), k, "ghost", past)
      refute k in Router.keys(FerricStore.Instance.get(:default))
    end

    test "expired key is not counted in Router.dbsize(FerricStore.Instance.get(:default))" do
      k = ukey("expired_dbsize")
      past = System.os_time(:millisecond) - 1
      baseline = Router.dbsize(FerricStore.Instance.get(:default))
      Router.put(FerricStore.Instance.get(:default), k, "ghost", past)
      # dbsize may transiently include the key before the lazy eviction fires,
      # but after a GET (which triggers eviction) it must be excluded
      Router.get(FerricStore.Instance.get(:default), k)
      assert Router.dbsize(FerricStore.Instance.get(:default)) <= baseline
    end

    test "PUT then overwrite with no-expiry removes the TTL" do
      k = ukey("clear_ttl")
      expire_at = System.os_time(:millisecond) + 5_000
      Router.put(FerricStore.Instance.get(:default), k, "expiring", expire_at)
      assert "expiring" == Router.get(FerricStore.Instance.get(:default), k)
      # Overwrite with no expiry — synchronous via Raft, should persist the TTL removal
      Router.put(FerricStore.Instance.get(:default), k, "permanent", 0)

      Ferricstore.Test.ShardHelpers.eventually(fn ->
        "permanent" == Router.get(FerricStore.Instance.get(:default), k)
      end, "overwrite with no-expiry should clear TTL", 20, 10)
    end

    test "PUT then overwrite with earlier TTL takes effect" do
      k = ukey("earlier_ttl")
      far_future = System.os_time(:millisecond) + 60_000
      Router.put(FerricStore.Instance.get(:default), k, "far", far_future)
      past = System.os_time(:millisecond) - 1
      Router.put(FerricStore.Instance.get(:default), k, "past", past)
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "expire_at_ms at u64 max does not crash" do
      k = ukey("max_ttl")
      # u64::MAX — far future, should behave as no expiry in practice
      max_u64 = 18_446_744_073_709_551_615
      Router.put(FerricStore.Instance.get(:default), k, "max_future", max_u64)
      assert "max_future" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end
  describe "duplicate keys and update semantics" do
    test "multiple PUTs to same key: GET returns last value" do
      k = ukey("overwrite")
      for i <- 1..10, do: Router.put(FerricStore.Instance.get(:default), k, "val_#{i}", 0)
      assert "val_10" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "PUT then DELETE then PUT: GET returns new value" do
      k = ukey("del_then_put")
      Router.put(FerricStore.Instance.get(:default), k, "first", 0)
      Router.delete(FerricStore.Instance.get(:default), k)
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
      Router.put(FerricStore.Instance.get(:default), k, "second", 0)
      assert "second" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "DELETE of non-existent key returns :ok without error" do
      k = ukey("del_nonexist")
      assert :ok == Router.delete(FerricStore.Instance.get(:default), k)
    end

    test "DELETE then DELETE same key: both return :ok" do
      k = ukey("double_del")
      Router.put(FerricStore.Instance.get(:default), k, "v", 0)
      assert :ok == Router.delete(FerricStore.Instance.get(:default), k)
      assert :ok == Router.delete(FerricStore.Instance.get(:default), k)
    end

    test "MSET with duplicate keys in same call: last value wins" do
      k = ukey("mset_dup")
      sock = connect()
      cmd(sock, ["MSET", k, "first", k, "second"])
      result = cmd(sock, ["GET", k])
      assert result == "second"
      :gen_tcp.close(sock)
    end
  end
  describe "large value TCP round-trips" do
    # These tests exercise values above the default 1 MB max_value_size.
    # Temporarily raise the limit to 64 MB (the hard cap) for the duration.
    setup do
      original = Application.get_env(:ferricstore, :max_value_size)
      Application.put_env(:ferricstore, :max_value_size, 64 * 1024 * 1024)
      on_exit(fn ->
        if original, do: Application.put_env(:ferricstore, :max_value_size, original),
          else: Application.delete_env(:ferricstore, :max_value_size)
      end)
    end

    test "1 MB value SET and GET over TCP" do
      sock = connect()
      k = ukey("tcp_1mb")
      v = :binary.copy("A", 1_000_000)
      assert {:simple, "OK"} == cmd(sock, ["SET", k, v])
      assert v == cmd(sock, ["GET", k])
      :gen_tcp.close(sock)
    end

    test "10 MB value SET and GET over TCP" do
      sock = connect()
      k = ukey("tcp_10mb")
      v = :binary.copy("B", 10_000_000)
      assert {:simple, "OK"} == cmd(sock, ["SET", k, v], 30_000)
      assert v == cmd(sock, ["GET", k], 30_000)
      :gen_tcp.close(sock)
    end

    test "10 MB value content is byte-exact over TCP" do
      sock = connect()
      k = ukey("tcp_10mb_exact")
      # Non-repeating pattern -- catches any truncation or offset bugs
      v = for i <- 0..9_999_999, into: <<>>, do: <<rem(i, 251)>>
      assert {:simple, "OK"} == cmd(sock, ["SET", k, v], 30_000)
      result = cmd(sock, ["GET", k], 30_000)
      assert byte_size(result) == 10_000_000
      assert result == v
      :gen_tcp.close(sock)
    end

    test "multiple large values on same connection do not interfere" do
      sock = connect()
      pairs =
        for i <- 1..3 do
          k = ukey("multi_large_#{i}")
          v = :binary.copy(<<i>>, 500_000)
          {k, v}
        end

      for {k, v} <- pairs do
        assert {:simple, "OK"} == cmd(sock, ["SET", k, v], 15_000)
      end

      for {k, v} <- pairs do
        assert v == cmd(sock, ["GET", k], 15_000)
      end

      :gen_tcp.close(sock)
    end

    test "large value after small values on same connection" do
      sock = connect()
      k_small = ukey("before_large")
      k_large = ukey("large_after_small")
      cmd(sock, ["SET", k_small, "tiny"])
      v = :binary.copy("L", 2_000_000)
      assert {:simple, "OK"} == cmd(sock, ["SET", k_large, v], 15_000)
      assert "tiny" == cmd(sock, ["GET", k_small])
      assert v == cmd(sock, ["GET", k_large], 15_000)
      :gen_tcp.close(sock)
    end
  end
  describe "protocol stress" do
    test "pipeline of 1000 SET commands all succeed" do
      sock = connect()

      keys =
        for i <- 1..1000 do
          k = ukey("pipe_set_#{i}")
          :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(["SET", k, "v#{i}"])))
          k
        end

      responses = recv_n(sock, 1000, 30_000)

      assert Enum.all?(responses, &(&1 == {:simple, "OK"}))

      # Spot-check 10 random keys
      samples = Enum.take_random(Enum.with_index(keys, 1), 10)
      for {k, i} <- samples do
        assert "v#{i}" == cmd(sock, ["GET", k])
      end

      :gen_tcp.close(sock)
    end

    test "pipeline of 1000 PING commands all return PONG" do
      sock = connect()

      blob =
        1..1000
        |> Enum.map(fn _ -> Encoder.encode(["PING"]) end)
        |> IO.iodata_to_binary()

      :ok = :gen_tcp.send(sock, blob)

      responses = recv_n(sock, 1000, 30_000)
      assert Enum.all?(responses, &(&1 == {:simple, "PONG"}))
      :gen_tcp.close(sock)
    end

    test "interleaved SET and GET in a pipeline return correct values" do
      sock = connect()
      k = ukey("interleaved")

      # SET k v1, GET k, SET k v2, GET k
      commands =
        [
          Encoder.encode(["SET", k, "v1"]),
          Encoder.encode(["GET", k]),
          Encoder.encode(["SET", k, "v2"]),
          Encoder.encode(["GET", k])
        ]
        |> IO.iodata_to_binary()

      :ok = :gen_tcp.send(sock, commands)

      [r1, r2, r3, r4] = recv_n(sock, 4)
      assert r1 == {:simple, "OK"}
      assert r2 == "v1"
      assert r3 == {:simple, "OK"}
      assert r4 == "v2"

      :gen_tcp.close(sock)
    end

    test "connection survives a sequence of unknown commands" do
      sock = connect()

      for _ <- 1..5 do
        result = cmd(sock, ["UNKNOWNCMD", "arg1"])
        assert match?({:error, _}, result)
      end

      # Connection still functional
      assert {:simple, "PONG"} == cmd(sock, ["PING"])
      :gen_tcp.close(sock)
    end

    test "many small keys in MSET and MGET" do
      sock = connect()
      n = 200
      pairs = for i <- 1..n, do: {ukey("mset_k#{i}"), "mval_#{i}"}
      flat = Enum.flat_map(pairs, fn {k, v} -> [k, v] end)

      assert {:simple, "OK"} == cmd(sock, ["MSET" | flat])

      keys = Enum.map(pairs, fn {k, _} -> k end)
      values = cmd(sock, ["MGET" | keys])
      expected = Enum.map(pairs, fn {_, v} -> v end)
      assert values == expected

      :gen_tcp.close(sock)
    end
  end
  describe "concurrent write stress" do
    test "100 concurrent Router.put calls all succeed and are readable" do
      keys =
        for i <- 1..100 do
          k = ukey("conc_#{i}")
          v = "val_#{i}"
          {k, v}
        end

      results =
        keys
        |> Enum.map(fn {k, v} -> Task.async(fn -> Router.put(FerricStore.Instance.get(:default), k, v, 0) end) end)
        |> Task.await_many(15_000)

      assert Enum.all?(results, &(&1 == :ok))

      for {k, v} <- keys do
        assert v == Router.get(FerricStore.Instance.get(:default), k)
      end
    end

    test "50 concurrent writes to the same key: GET returns a valid value" do
      k = ukey("same_key_conc")

      results =
        1..50
        |> Enum.map(fn i -> Task.async(fn -> Router.put(FerricStore.Instance.get(:default), k, "val_#{i}", 0) end) end)
        |> Task.await_many(15_000)

      assert Enum.all?(results, &(&1 == :ok))

      value = Router.get(FerricStore.Instance.get(:default), k)
      assert is_binary(value)
      assert String.starts_with?(value, "val_")
    end

    test "concurrent writes and reads do not return corrupted data" do
      base_key = ukey("rw_conc")
      n = 30

      # Pre-seed
      for i <- 1..n, do: Router.put(FerricStore.Instance.get(:default), "#{base_key}_#{i}", "seed_#{i}", 0)

      write_tasks =
        Enum.map(1..n, fn i ->
          Task.async(fn -> Router.put(FerricStore.Instance.get(:default), "#{base_key}_#{i}", "updated_#{i}", 0) end)
        end)

      read_tasks =
        Enum.map(1..n, fn i ->
          Task.async(fn -> Router.get(FerricStore.Instance.get(:default), "#{base_key}_#{i}") end)
        end)

      write_results = Task.await_many(write_tasks, 15_000)
      read_results = Task.await_many(read_tasks, 15_000)

      assert Enum.all?(write_results, &(&1 == :ok))

      for v <- read_results do
        assert v in [nil | Enum.map(1..n, &"seed_#{&1}")] or
                 String.starts_with?(v || "", "updated_"),
               "Unexpected value: #{inspect(v)}"
      end
    end

    test "concurrent DEL and PUT on same key: store remains consistent" do
      k = ukey("del_put_race")
      Router.put(FerricStore.Instance.get(:default), k, "initial", 0)

      tasks =
        Enum.map(1..20, fn i ->
          Task.async(fn ->
            if rem(i, 2) == 0,
              do: Router.put(FerricStore.Instance.get(:default), k, "v#{i}", 0),
              else: Router.delete(FerricStore.Instance.get(:default), k)
          end)
        end)

      Task.await_many(tasks, 15_000)

      # After the race, value must be either nil or a valid string — never a crash
      result = Router.get(FerricStore.Instance.get(:default), k)
      assert is_nil(result) or is_binary(result)
    end
  end
  describe "protocol-level size guards" do
    setup do
      sock = connect()
      on_exit(fn -> :gen_tcp.close(sock) end)
      {:ok, sock: sock}
    end

    test "SET with empty key returns ERR response", %{sock: sock} do
      resp = cmd(sock, ["SET", "", "value"])
      assert match?({:error, _}, resp), "Expected error for empty key, got: #{inspect(resp)}"
    end

    test "SET with key over 65,535 bytes returns ERR response", %{sock: sock} do
      big_key = :binary.copy("k", @max_key_bytes + 1)
      resp = cmd(sock, ["SET", big_key, "value"])
      assert match?({:error, _}, resp), "Expected error for oversized key, got: #{inspect(resp)}"
    end

    test "GET with empty key returns ERR response", %{sock: sock} do
      resp = cmd(sock, ["GET", ""])
      assert match?({:error, _}, resp), "Expected error for empty key, got: #{inspect(resp)}"
    end

    test "GET with key over 65,535 bytes returns ERR response", %{sock: sock} do
      big_key = :binary.copy("k", @max_key_bytes + 1)
      resp = cmd(sock, ["GET", big_key])
      assert match?({:error, _}, resp), "Expected error for oversized key, got: #{inspect(resp)}"
    end

    test "MSET with oversized key returns ERR response", %{sock: sock} do
      big_key = :binary.copy("k", @max_key_bytes + 1)
      resp = cmd(sock, ["MSET", big_key, "value"])
      assert match?({:error, _}, resp), "Expected error for oversized key, got: #{inspect(resp)}"
    end

    @tag :large_alloc
    test "SET with oversized value disconnects (TOOBIG per spec)", %{sock: sock} do
      # 513 MiB — over the 512 MiB guard.
      # Per spec section 4.6: "Connection is disconnected immediately after this error."
      oversized_value = :binary.copy("v", @max_value_bytes + 1)
      :gen_tcp.send(sock, IO.iodata_to_binary(FerricstoreServer.Resp.Encoder.encode(["SET", "guard_key", oversized_value])))
      # Connection should close (TOOBIG disconnects)
      case :gen_tcp.recv(sock, 0, 10_000) do
        {:error, :closed} -> :ok
        {:ok, data} ->
          # Server may send error before closing
          assert data =~ "ERR" or data =~ "too large" or data =~ "TOOBIG"
      end
    end

    @tag :large_alloc
    test "MSET with oversized value disconnects (TOOBIG per spec)", %{sock: sock} do
      oversized_value = :binary.copy("v", @max_value_bytes + 1)
      :gen_tcp.send(sock, IO.iodata_to_binary(FerricstoreServer.Resp.Encoder.encode(["MSET", "guard_key2", oversized_value])))
      case :gen_tcp.recv(sock, 0, 10_000) do
        {:error, :closed} -> :ok
        {:ok, data} ->
          assert data =~ "ERR" or data =~ "too large" or data =~ "TOOBIG"
      end
    end

    test "SET with valid key and value at max sizes succeeds", %{sock: sock} do
      max_key = :binary.copy("k", @max_key_bytes)
      # Use a smaller value to avoid memory pressure in CI; the value limit is tested separately
      resp = cmd(sock, ["SET", max_key, "boundary_value"])
      assert resp == {:simple, "OK"}
      resp2 = cmd(sock, ["GET", max_key])
      assert resp2 == "boundary_value"
    end
  end
  describe "protocol edge cases" do
    test "truncated RESP3 bulk string header: server waits for more data, then completes" do
      # Send a partial bulk string header (e.g. "$5\r\nhe" without the rest).
      # Then complete it. The server should buffer and complete successfully.
      sock = connect_and_hello()

      full = IO.iodata_to_binary(Encoder.encode(["PING"]))
      # Split in the middle
      {part1, part2} = String.split_at(full, 3)

      :ok = send_raw(sock, part1)
      # intentional delay — testing grace/timeout behavior (server must buffer partial data)
      Process.sleep(50)
      :ok = send_raw(sock, part2)

      result = recv_one(sock, 5_000)
      assert result == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "wrong RESP type marker followed by valid command: error then recovery" do
      # Send bytes that start with a bad type marker, terminated by \r\n
      # so the inline parser can process them, then send a valid command.
      sock = connect()

      # The inline parser will try to interpret this as an inline command
      # with token "\xFF\xFE" which is an unknown command -> error.
      :ok = send_raw(sock, <<0xFF, 0xFE, "\r\n">>)

      case recv_raw(sock, 2_000) do
        {:ok, data} ->
          # Server sent an error response or processed as inline
          assert String.contains?(data, "-") or byte_size(data) > 0

        {:error, reason} ->
          # Server closed the connection -- reconnect
          assert reason in [:closed, :econnreset]
      end

      # Server should still accept new connections regardless
      fresh = connect_and_hello()
      assert {:simple, "PONG"} == cmd(fresh, ["PING"])
      :gen_tcp.close(fresh)
    end

    test "send inline command (not RESP3 array): server handles it" do
      # Inline commands are plain text terminated by \r\n.
      # The parser returns {:inline, ["PING"]} which the connection handler
      # normalises and dispatches.
      sock = connect_and_hello()

      :ok = send_raw(sock, "PING\r\n")
      result = recv_one(sock, 5_000)
      assert result == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "send inline SET command with spaces: parsed correctly" do
      sock = connect_and_hello()
      k = ukey("inline_set")

      :ok = send_raw(sock, "SET #{k} inline_value\r\n")
      result = recv_one(sock, 5_000)
      assert result == {:simple, "OK"}

      assert "inline_value" == cmd(sock, ["GET", k])

      :gen_tcp.close(sock)
    end

    test "pipeline 100+ commands in one send" do
      sock = connect_and_hello()

      count = 150
      blob =
        1..count
        |> Enum.map(fn i -> Encoder.encode(["PING", "p#{i}"]) end)
        |> IO.iodata_to_binary()

      :ok = send_raw(sock, blob)

      responses = recv_n(sock, count, 30_000)
      assert length(responses) == count

      for i <- 1..count do
        assert Enum.at(responses, i - 1) == "p#{i}"
      end

      :gen_tcp.close(sock)
    end

    test "HELLO 2 (RESP2) is rejected with NOPROTO" do
      sock = connect()

      resp = cmd(sock, ["HELLO", "2"])
      assert match?({:error, "NOPROTO" <> _}, resp),
             "Expected NOPROTO error for RESP2, got: #{inspect(resp)}"

      # Connection should still be usable after the rejected HELLO
      resp2 = cmd(sock, ["PING"])
      assert resp2 == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "HELLO with unsupported version 99 is rejected with NOPROTO" do
      sock = connect()

      resp = cmd(sock, ["HELLO", "99"])
      assert match?({:error, "NOPROTO" <> _}, resp),
             "Expected NOPROTO error, got: #{inspect(resp)}"

      :gen_tcp.close(sock)
    end

    test "command at max value size is handled without crash" do
      # Create a SET command with a value at exactly the max_value_size limit (1MB).
      # The server should process it successfully.
      sock = connect_and_hello()
      k = ukey("big_cmd")
      big_val = :binary.copy("X", 1_048_576)

      resp = cmd(sock, ["SET", k, big_val], 30_000)
      assert resp == {:simple, "OK"}

      result = cmd(sock, ["GET", k], 30_000)
      assert result == big_val

      :gen_tcp.close(sock)
    end

    test "command exceeding max value size returns error and closes connection" do
      # A SET with a value larger than max_value_size (default 1MB) should be
      # rejected at the parser level with a value_too_large error.
      sock = connect_and_hello()
      big_val = :binary.copy("X", 1_100_000)

      send_raw(sock, IO.iodata_to_binary(Encoder.encode(["SET", "over_limit", big_val])))

      # Server sends an error response then closes the connection.
      # Depending on timing, we either get the error data or :closed.
      case recv_raw(sock, 10_000) do
        {:ok, data} ->
          assert data =~ "value too large"

        {:error, :closed} ->
          # Server closed before we read the error — acceptable behavior.
          # The important thing is the server didn't crash.
          :ok
      end
    end
  end
  describe "connection edge cases" do
    test "close connection mid-command: server does not crash, new connections work" do
      sock = connect_and_hello()

      # Send a partial RESP command (the beginning of a bulk string SET)
      partial = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nmy"
      :ok = send_raw(sock, partial)

      # Abruptly close without completing the command
      :gen_tcp.close(sock)

      # Server must still accept new connections
      fresh = connect_and_hello()
      assert {:simple, "PONG"} == cmd(fresh, ["PING"])
      :gen_tcp.close(fresh)
    end

    test "multiple HELLO 3 handshakes on same connection" do
      sock = connect()

      # First HELLO 3
      greeting1 = cmd(sock, ["HELLO", "3"])
      assert is_map(greeting1)
      assert greeting1["server"] == "ferricstore"
      assert greeting1["proto"] == 3
      id1 = greeting1["id"]

      # Second HELLO 3
      greeting2 = cmd(sock, ["HELLO", "3"])
      assert is_map(greeting2)
      assert greeting2["server"] == "ferricstore"
      assert greeting2["proto"] == 3
      # Same connection should keep same client ID
      assert greeting2["id"] == id1

      # Commands still work after multiple HELLOs
      k = ukey("multi_hello")
      assert {:simple, "OK"} == cmd(sock, ["SET", k, "after_multi_hello"])
      assert "after_multi_hello" == cmd(sock, ["GET", k])

      :gen_tcp.close(sock)
    end

    test "HELLO with no version returns server info" do
      sock = connect()

      greeting = cmd(sock, ["HELLO"])
      assert is_map(greeting)
      assert greeting["server"] == "ferricstore"

      :gen_tcp.close(sock)
    end

    test "QUIT mid-transaction: MULTI then QUIT closes connection" do
      sock = connect_and_hello()
      k = ukey("quit_mid_txn")

      # Begin a transaction
      assert {:simple, "OK"} == cmd(sock, ["MULTI"])

      # Queue a command
      assert {:simple, "QUEUED"} == cmd(sock, ["SET", k, "txn_value"])

      # QUIT before EXEC -- should close connection, transaction is discarded
      assert {:simple, "OK"} == cmd(sock, ["QUIT"])

      # Connection should be closed
      result = recv_raw(sock, 1_000)
      assert result == {:error, :closed} or result == {:error, :econnreset}

      # Verify the queued SET was NOT executed
      fresh = connect_and_hello()
      assert nil == cmd(fresh, ["GET", k])
      :gen_tcp.close(fresh)
    end

    test "RESET clears transaction state mid-MULTI" do
      sock = connect_and_hello()
      k = ukey("reset_mid_txn")

      # Begin a transaction
      assert {:simple, "OK"} == cmd(sock, ["MULTI"])
      assert {:simple, "QUEUED"} == cmd(sock, ["SET", k, "should_not_persist"])

      # RESET clears the transaction state
      resp = cmd(sock, ["RESET"])
      assert resp == {:simple, "RESET"}

      # Now we're in normal mode again; EXEC should fail since MULTI was cleared
      exec_resp = cmd(sock, ["EXEC"])
      assert match?({:error, _}, exec_resp)

      # Verify the queued command was NOT executed
      assert nil == cmd(sock, ["GET", k])

      :gen_tcp.close(sock)
    end
  end
    end
  end
end
