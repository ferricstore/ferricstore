defmodule FerricstoreServer.Spec.EdgeCasesTest do
  @moduledoc """
  Final edge-case bug hunt: scenarios that fall between the cracks of existing
  test coverage.

  Covers:
    - Binary keys containing only null bytes (through Router and TCP)
    - Empty MULTI/EXEC with interleaved valid commands
    - SUBSCRIBE then immediate UNSUBSCRIBE (same pipeline, no messages between)
    - CONFIG SET on a key that was just SET (namespace confusion)
    - SUBSCRIBE/UNSUBSCRIBE count bookkeeping edge cases
    - MULTI/EXEC with only read commands (no writes)
    - CONFIG GET after CONFIG SET round-trip
    - Keys with unusual byte patterns through full TCP stack
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  # PubSub module used indirectly via ETS table lookups for verification.

  @moduletag timeout: 60_000

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp connect do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", Listener.port(), [
        :binary,
        active: false,
        packet: :raw,
        nodelay: true,
        recbuf: 1_048_576,
        sndbuf: 1_048_576
      ])

    sock
  end

  defp send_cmd(sock, cmd) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(cmd)))
  end

  defp recv_one(sock, timeout \\ 10_000) do
    recv_loop(sock, "", timeout)
  end

  defp recv_loop(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | _], _rest} ->
        val

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_loop(sock, buf <> data, timeout)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  defp recv_n(sock, count, timeout \\ 10_000) do
    recv_n_loop(sock, count, "", timeout, [])
  end

  defp recv_n_loop(_sock, 0, _buf, _timeout, acc), do: Enum.reverse(acc)

  defp recv_n_loop(sock, remaining, buf, timeout, acc) do
    case Parser.parse(buf) do
      {:ok, vals, rest} when vals != [] ->
        take = min(length(vals), remaining)
        new_acc = Enum.reverse(Enum.take(vals, take)) ++ acc
        new_remaining = remaining - take

        if new_remaining == 0 do
          Enum.reverse(new_acc)
        else
          recv_n_loop(sock, new_remaining, rest, timeout, new_acc)
        end

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_n_loop(sock, remaining, buf <> data, timeout, acc)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  defp connect_and_hello do
    sock = connect()
    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_one(sock)
    sock
  end

  # Hash tag ensures all generated keys co-locate on the same shard.
  defp ukey(base), do: "{edge}:#{base}_#{System.unique_integer([:positive])}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    ShardHelpers.wait_shards_alive()
    %{port: Listener.port()}
  end

  setup do
    ShardHelpers.flush_all_keys()

    # Clean PubSub ETS tables between tests
    if :ets.whereis(:ferricstore_pubsub) != :undefined do
      :ets.delete_all_objects(:ferricstore_pubsub)
    end

    if :ets.whereis(:ferricstore_pubsub_patterns) != :undefined do
      :ets.delete_all_objects(:ferricstore_pubsub_patterns)
    end

    if :ets.whereis(:ferricstore_pubsub_channel_cache) != :undefined do
      :ets.delete_all_objects(:ferricstore_pubsub_channel_cache)
    end

    :ok
  end

  # ===========================================================================
  # 1. Binary keys containing only null bytes
  # ===========================================================================

  describe "binary keys containing only null bytes" do
    test "SET and GET a key that is a single null byte via TCP" do
      sock = connect_and_hello()

      # A key that is exactly one null byte: <<0>>
      key = <<0>>
      value = "null_byte_key_value"

      send_cmd(sock, ["SET", key, value])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["GET", key])
      assert value == recv_one(sock)

      # Clean up
      send_cmd(sock, ["DEL", key])
      recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "SET and GET a key that is 64 null bytes via TCP" do
      sock = connect_and_hello()

      key = :binary.copy(<<0>>, 64)
      value = "sixty_four_nulls"

      send_cmd(sock, ["SET", key, value])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["GET", key])
      assert value == recv_one(sock)

      # Verify EXISTS sees it
      send_cmd(sock, ["EXISTS", key])
      assert 1 == recv_one(sock)

      # Verify DEL removes it
      send_cmd(sock, ["DEL", key])
      assert 1 == recv_one(sock)

      send_cmd(sock, ["GET", key])
      assert nil == recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "all-null-byte key round-trips through Router" do
      key = :binary.copy(<<0>>, 32)
      value = "router_null_key"

      Router.put(FerricStore.Instance.get(:default), key, value)
      assert Router.get(FerricStore.Instance.get(:default), key) == value
      assert Router.exists?(FerricStore.Instance.get(:default), key) == true

      Router.delete(FerricStore.Instance.get(:default), key)
      assert Router.get(FerricStore.Instance.get(:default), key) == nil
    end

    test "all-null-byte key and all-null-byte value round-trip" do
      sock = connect_and_hello()

      key = :binary.copy(<<0>>, 8)
      value = :binary.copy(<<0>>, 128)

      send_cmd(sock, ["SET", key, value])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["GET", key])
      result = recv_one(sock)
      assert result == value
      assert byte_size(result) == 128

      # Clean up
      send_cmd(sock, ["DEL", key])
      recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "STRLEN on all-null-byte value returns correct length" do
      sock = connect_and_hello()

      key = ukey("strlen_null")
      value = :binary.copy(<<0>>, 42)

      send_cmd(sock, ["SET", key, value])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["STRLEN", key])
      assert 42 == recv_one(sock)

      send_cmd(sock, ["DEL", key])
      recv_one(sock)

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # 2. Empty MULTI/EXEC (no commands queued)
  # ===========================================================================

  describe "empty MULTI/EXEC edge cases" do
    test "MULTI then immediate EXEC returns empty array" do
      sock = connect_and_hello()

      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["EXEC"])
      result = recv_one(sock)
      assert result == []

      # Connection should be back in normal mode
      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} == recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "MULTI then DISCARD then MULTI then EXEC returns empty array" do
      sock = connect_and_hello()

      # First MULTI/DISCARD cycle
      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["DISCARD"])
      assert {:simple, "OK"} == recv_one(sock)

      # Second MULTI/EXEC cycle (empty)
      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["EXEC"])
      assert [] == recv_one(sock)

      # Connection should be normal
      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} == recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "MULTI with only read commands queued executes correctly" do
      sock = connect_and_hello()

      key = ukey("multi_read")
      send_cmd(sock, ["SET", key, "before_multi"])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} == recv_one(sock)

      # Queue only reads — no writes
      send_cmd(sock, ["GET", key])
      assert {:simple, "QUEUED"} == recv_one(sock)

      send_cmd(sock, ["EXISTS", key])
      assert {:simple, "QUEUED"} == recv_one(sock)

      send_cmd(sock, ["STRLEN", key])
      assert {:simple, "QUEUED"} == recv_one(sock)

      send_cmd(sock, ["EXEC"])
      result = recv_one(sock)

      assert is_list(result)
      assert length(result) == 3
      assert Enum.at(result, 0) == "before_multi"
      assert Enum.at(result, 1) == 1
      assert Enum.at(result, 2) == byte_size("before_multi")

      # Clean up
      send_cmd(sock, ["DEL", key])
      recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "three consecutive empty MULTI/EXEC cycles all return empty arrays" do
      sock = connect_and_hello()

      for _cycle <- 1..3 do
        send_cmd(sock, ["MULTI"])
        assert {:simple, "OK"} == recv_one(sock)

        send_cmd(sock, ["EXEC"])
        assert [] == recv_one(sock)
      end

      # Connection still functional
      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} == recv_one(sock)

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # 3. SUBSCRIBE then immediate UNSUBSCRIBE
  # ===========================================================================

  describe "SUBSCRIBE then immediate UNSUBSCRIBE" do
    test "SUBSCRIBE to channel then immediately UNSUBSCRIBE from it" do
      sock = connect_and_hello()
      channel = ukey("sub_unsub")

      # Subscribe
      send_cmd(sock, ["SUBSCRIBE", channel])
      sub_resp = recv_one(sock)
      assert {:push, ["subscribe", ^channel, 1]} = sub_resp

      # Immediately unsubscribe — no messages published in between
      send_cmd(sock, ["UNSUBSCRIBE", channel])
      unsub_resp = recv_one(sock)
      assert {:push, ["unsubscribe", ^channel, 0]} = unsub_resp

      # Verify PubSub ETS has no entries for this channel
      # The connection process PID differs from self() (it's a separate Ranch
      # process), so we verify cleanup via a publish test instead:

      # Open a second connection to try publishing — count should be 0
      sock2 = connect_and_hello()
      send_cmd(sock2, ["PUBLISH", channel, "should_not_arrive"])
      pub_count = recv_one(sock2)
      assert pub_count == 0

      :gen_tcp.close(sock)
      :gen_tcp.close(sock2)
    end

    test "SUBSCRIBE to 3 channels then UNSUBSCRIBE from all at once" do
      sock = connect_and_hello()
      ch1 = ukey("batch_unsub_1")
      ch2 = ukey("batch_unsub_2")
      ch3 = ukey("batch_unsub_3")

      # Subscribe to all 3
      send_cmd(sock, ["SUBSCRIBE", ch1, ch2, ch3])
      responses = recv_n(sock, 3)

      assert {:push, ["subscribe", ^ch1, 1]} = Enum.at(responses, 0)
      assert {:push, ["subscribe", ^ch2, 2]} = Enum.at(responses, 1)
      assert {:push, ["subscribe", ^ch3, 3]} = Enum.at(responses, 2)

      # Immediately unsubscribe from all 3
      send_cmd(sock, ["UNSUBSCRIBE", ch1, ch2, ch3])
      unsub_responses = recv_n(sock, 3)

      # Each unsubscribe should decrement the count
      assert {:push, ["unsubscribe", ^ch1, 2]} = Enum.at(unsub_responses, 0)
      assert {:push, ["unsubscribe", ^ch2, 1]} = Enum.at(unsub_responses, 1)
      assert {:push, ["unsubscribe", ^ch3, 0]} = Enum.at(unsub_responses, 2)

      # Verify all channels are empty
      sock2 = connect_and_hello()

      for ch <- [ch1, ch2, ch3] do
        send_cmd(sock2, ["PUBLISH", ch, "post_unsub"])
        assert 0 == recv_one(sock2)
      end

      :gen_tcp.close(sock)
      :gen_tcp.close(sock2)
    end

    test "SUBSCRIBE then UNSUBSCRIBE with no args unsubscribes from all" do
      sock = connect_and_hello()
      ch1 = ukey("unsub_all_1")
      ch2 = ukey("unsub_all_2")

      # Subscribe to 2 channels
      send_cmd(sock, ["SUBSCRIBE", ch1, ch2])
      _sub_responses = recv_n(sock, 2)

      # UNSUBSCRIBE with no args — Redis semantics: unsubscribe from all
      send_cmd(sock, ["UNSUBSCRIBE"])
      unsub_responses = recv_n(sock, 2)

      # Both channels should be unsubscribed
      channels_unsubbed =
        Enum.map(unsub_responses, fn {:push, ["unsubscribe", ch, _count]} -> ch end)

      assert Enum.sort(channels_unsubbed) == Enum.sort([ch1, ch2])

      # Final count should be 0
      final_counts =
        Enum.map(unsub_responses, fn {:push, ["unsubscribe", _ch, count]} -> count end)

      assert Enum.min(final_counts) == 0

      :gen_tcp.close(sock)
    end

    test "SUBSCRIBE then UNSUBSCRIBE then SUBSCRIBE again works" do
      sock = connect_and_hello()
      channel = ukey("resub")

      # First subscribe
      send_cmd(sock, ["SUBSCRIBE", channel])
      assert {:push, ["subscribe", ^channel, 1]} = recv_one(sock)

      # Unsubscribe
      send_cmd(sock, ["UNSUBSCRIBE", channel])
      assert {:push, ["unsubscribe", ^channel, 0]} = recv_one(sock)

      # Re-subscribe to the same channel
      send_cmd(sock, ["SUBSCRIBE", channel])
      assert {:push, ["subscribe", ^channel, 1]} = recv_one(sock)

      # Verify messages arrive again
      sock2 = connect_and_hello()
      send_cmd(sock2, ["PUBLISH", channel, "after_resub"])
      pub_count = recv_one(sock2)
      assert pub_count >= 1

      msg = recv_one(sock, 5_000)
      assert {:push, ["message", ^channel, "after_resub"]} = msg

      :gen_tcp.close(sock)
      :gen_tcp.close(sock2)
    end
  end

  # ===========================================================================
  # 4. CONFIG SET on a key name that was just SET as data
  # ===========================================================================

  describe "CONFIG SET vs data SET namespace isolation" do
    test "CONFIG SET parameter does not interfere with data key of same name" do
      sock = connect_and_hello()

      # SET a data key called "hz" (which is also a CONFIG parameter name)
      send_cmd(sock, ["SET", "hz", "data_value_for_hz"])
      assert {:simple, "OK"} == recv_one(sock)

      # CONFIG SET hz to a different value
      send_cmd(sock, ["CONFIG", "SET", "hz", "20"])
      assert {:simple, "OK"} == recv_one(sock)

      # The data key should still have its original value
      send_cmd(sock, ["GET", "hz"])
      assert "data_value_for_hz" == recv_one(sock)

      # CONFIG GET should return the config value, not the data value
      send_cmd(sock, ["CONFIG", "GET", "hz"])
      result = recv_one(sock)
      assert ["hz", "20"] == result

      # Clean up
      send_cmd(sock, ["DEL", "hz"])
      recv_one(sock)

      # Reset CONFIG to default
      send_cmd(sock, ["CONFIG", "SET", "hz", "10"])
      recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "CONFIG SET maxmemory-policy does not create a data key" do
      sock = connect_and_hello()

      send_cmd(sock, ["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
      assert {:simple, "OK"} == recv_one(sock)

      # "maxmemory-policy" as a data key should not exist
      send_cmd(sock, ["GET", "maxmemory-policy"])
      assert nil == recv_one(sock)

      # CONFIG GET should return the configured value
      send_cmd(sock, ["CONFIG", "GET", "maxmemory-policy"])
      assert ["maxmemory-policy", "allkeys-lru"] == recv_one(sock)

      # Reset
      send_cmd(sock, ["CONFIG", "SET", "maxmemory-policy", "volatile-lru"])
      recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "SET a data key with CONFIG-like name, then CONFIG GET returns config not data" do
      sock = connect_and_hello()

      # SET data key "slowlog-log-slower-than"
      send_cmd(sock, ["SET", "slowlog-log-slower-than", "9999999"])
      assert {:simple, "OK"} == recv_one(sock)

      # CONFIG GET should return the actual config, not the data key
      send_cmd(sock, ["CONFIG", "GET", "slowlog-log-slower-than"])
      result = recv_one(sock)
      assert ["slowlog-log-slower-than", config_val] = result
      # The config value should be the default (10000), not "9999999"
      assert config_val != "9999999"

      # GET should return the data value
      send_cmd(sock, ["GET", "slowlog-log-slower-than"])
      assert "9999999" == recv_one(sock)

      # Clean up
      send_cmd(sock, ["DEL", "slowlog-log-slower-than"])
      recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "CONFIG SET then GET round-trips correctly for notify-keyspace-events" do
      sock = connect_and_hello()

      # Get original value
      send_cmd(sock, ["CONFIG", "GET", "notify-keyspace-events"])
      ["notify-keyspace-events", original] = recv_one(sock)

      # Set to a new value
      send_cmd(sock, ["CONFIG", "SET", "notify-keyspace-events", "KEA"])
      assert {:simple, "OK"} == recv_one(sock)

      # Read it back
      send_cmd(sock, ["CONFIG", "GET", "notify-keyspace-events"])
      assert ["notify-keyspace-events", "KEA"] == recv_one(sock)

      # Reset to original
      send_cmd(sock, ["CONFIG", "SET", "notify-keyspace-events", original])
      recv_one(sock)

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # 5. Additional edge cases — unusual key patterns via TCP
  # ===========================================================================

  describe "unusual key patterns via TCP" do
    test "key containing only CRLF bytes via TCP" do
      sock = connect_and_hello()

      key = "\r\n\r\n"
      value = "crlf_key_value"

      send_cmd(sock, ["SET", key, value])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["GET", key])
      assert value == recv_one(sock)

      send_cmd(sock, ["DEL", key])
      recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "key containing mix of null bytes and printable chars via TCP" do
      sock = connect_and_hello()

      key = "prefix\x00middle\x00suffix"
      value = "mixed_null_key"

      send_cmd(sock, ["SET", key, value])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["GET", key])
      assert value == recv_one(sock)

      send_cmd(sock, ["DEL", key])
      recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "very long key (1000 bytes) SET and GET via TCP" do
      sock = connect_and_hello()

      key = :binary.copy("k", 1000)
      value = "long_key_value"

      send_cmd(sock, ["SET", key, value])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["GET", key])
      assert value == recv_one(sock)

      send_cmd(sock, ["DEL", key])
      recv_one(sock)

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # 6. MULTI/EXEC with WATCH on a key that does not exist
  # ===========================================================================

  describe "WATCH edge cases" do
    test "WATCH a nonexistent key, then EXEC without touching it succeeds" do
      sock = connect_and_hello()
      key = ukey("watch_missing")

      # WATCH a key that doesn't exist
      send_cmd(sock, ["WATCH", key])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["SET", key, "watched_value"])
      assert {:simple, "QUEUED"} == recv_one(sock)

      send_cmd(sock, ["EXEC"])
      result = recv_one(sock)
      assert is_list(result)
      assert length(result) == 1
      assert Enum.at(result, 0) == {:simple, "OK"}

      # Verify the key was set
      send_cmd(sock, ["GET", key])
      assert "watched_value" == recv_one(sock)

      # Clean up
      send_cmd(sock, ["DEL", key])
      recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "WATCH a nonexistent key, another client creates it, EXEC returns nil" do
      sock1 = connect_and_hello()
      sock2 = connect_and_hello()
      key = ukey("watch_race")

      # Client 1: WATCH the key
      send_cmd(sock1, ["WATCH", key])
      assert {:simple, "OK"} == recv_one(sock1)

      # Client 2: create the key
      send_cmd(sock2, ["SET", key, "created_by_other"])
      assert {:simple, "OK"} == recv_one(sock2)

      # Client 1: MULTI/EXEC should be aborted
      send_cmd(sock1, ["MULTI"])
      assert {:simple, "OK"} == recv_one(sock1)

      send_cmd(sock1, ["SET", key, "should_not_win"])
      assert {:simple, "QUEUED"} == recv_one(sock1)

      send_cmd(sock1, ["EXEC"])
      result = recv_one(sock1)
      # Redis returns nil for aborted transaction
      assert result == nil

      # Verify the other client's value persisted
      send_cmd(sock1, ["GET", key])
      assert "created_by_other" == recv_one(sock1)

      # Clean up
      send_cmd(sock1, ["DEL", key])
      recv_one(sock1)

      :gen_tcp.close(sock1)
      :gen_tcp.close(sock2)
    end
  end

  # ===========================================================================
  # 7. PSUBSCRIBE then immediate PUNSUBSCRIBE
  # ===========================================================================

  describe "PSUBSCRIBE then immediate PUNSUBSCRIBE" do
    test "pattern subscribe then immediate unsubscribe leaves no state" do
      sock = connect_and_hello()
      pattern = ukey("pat_unsub") <> "*"

      # PSUBSCRIBE
      send_cmd(sock, ["PSUBSCRIBE", pattern])
      sub_resp = recv_one(sock)
      assert {:push, ["psubscribe", ^pattern, 1]} = sub_resp

      # Immediately PUNSUBSCRIBE
      send_cmd(sock, ["PUNSUBSCRIBE", pattern])
      unsub_resp = recv_one(sock)
      assert {:push, ["punsubscribe", ^pattern, 0]} = unsub_resp

      # Verify pattern is gone — publish to a matching channel
      matching_channel = String.replace_trailing(pattern, "*", "test_channel")
      sock2 = connect_and_hello()
      send_cmd(sock2, ["PUBLISH", matching_channel, "should_not_arrive"])
      pub_count = recv_one(sock2)
      assert pub_count == 0

      :gen_tcp.close(sock)
      :gen_tcp.close(sock2)
    end
  end

  # ===========================================================================
  # 8. CONFIG SET on read-only parameters returns error
  # ===========================================================================

  describe "CONFIG SET on read-only parameters" do
    test "CONFIG SET maxmemory returns error" do
      sock = connect_and_hello()

      send_cmd(sock, ["CONFIG", "SET", "maxmemory", "999"])
      result = recv_one(sock)
      assert {:error, msg} = result
      assert msg =~ "read-only" or msg =~ "Unsupported"

      :gen_tcp.close(sock)
    end

    test "CONFIG SET tcp-port returns error" do
      sock = connect_and_hello()

      send_cmd(sock, ["CONFIG", "SET", "tcp-port", "9999"])
      result = recv_one(sock)
      assert {:error, msg} = result
      assert msg =~ "read-only" or msg =~ "Unsupported"

      :gen_tcp.close(sock)
    end

    test "CONFIG SET nonexistent-param returns error" do
      sock = connect_and_hello()

      send_cmd(sock, ["CONFIG", "SET", "totally-bogus-param", "123"])
      result = recv_one(sock)
      assert {:error, _msg} = result

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # 9. MULTI/EXEC after connection RESET
  # ===========================================================================

  describe "MULTI/EXEC after RESET" do
    test "RESET during MULTI clears transaction state" do
      sock = connect_and_hello()

      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} == recv_one(sock)

      send_cmd(sock, ["SET", ukey("reset_mid_multi"), "val"])
      assert {:simple, "QUEUED"} == recv_one(sock)

      # RESET should clear the MULTI state
      send_cmd(sock, ["RESET"])
      result = recv_one(sock)
      assert {:simple, "RESET"} = result

      # EXEC should now fail (not in MULTI mode)
      send_cmd(sock, ["EXEC"])
      assert {:error, "ERR EXEC without MULTI"} = recv_one(sock)

      # Normal commands should work
      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} == recv_one(sock)

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # 10. DEL on multiple keys where some don't exist
  # ===========================================================================

  describe "DEL edge cases" do
    test "DEL on mix of existing and nonexistent keys returns correct count" do
      sock = connect_and_hello()

      k1 = ukey("del_exists_1")
      k2 = ukey("del_exists_2")
      k3 = ukey("del_missing_1")
      k4 = ukey("del_missing_2")

      # Create only k1 and k2
      send_cmd(sock, ["SET", k1, "v1"])
      assert {:simple, "OK"} == recv_one(sock)
      send_cmd(sock, ["SET", k2, "v2"])
      assert {:simple, "OK"} == recv_one(sock)

      # DEL all 4 — should return 2 (only k1 and k2 existed)
      send_cmd(sock, ["DEL", k1, k2, k3, k4])
      assert 2 == recv_one(sock)

      # Verify both are gone
      send_cmd(sock, ["EXISTS", k1])
      assert 0 == recv_one(sock)
      send_cmd(sock, ["EXISTS", k2])
      assert 0 == recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "DEL with zero keys returns error" do
      sock = connect_and_hello()

      send_cmd(sock, ["DEL"])
      result = recv_one(sock)
      assert {:error, _} = result

      :gen_tcp.close(sock)
    end
  end
end
