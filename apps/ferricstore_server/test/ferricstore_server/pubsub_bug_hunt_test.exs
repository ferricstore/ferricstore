defmodule FerricstoreServer.PubSubBugHuntTest do
  @moduledoc """
  Targeted Pub/Sub bug-hunt tests.

  Exercises SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH,
  and PUBSUB subcommands (CHANNELS, NUMSUB, NUMPAT) over real TCP
  connections. Each test isolates a specific contract and is designed to
  surface subtle bugs: duplicate ETS entries, stale state after RESET,
  missing cleanup, incorrect counts, etc.

  All tests are TCP-level integration tests running against the full stack:

      TCP -> RESP3 parser -> Connection (pubsub dispatch) -> PubSub ETS
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.PubSub

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
        recbuf: 256 * 1024,
        sndbuf: 256 * 1024
      ])

    sock
  end

  defp connect_and_hello do
    sock = connect()
    assert {:simple, "OK"} = cmd(sock, ["HELLO", "3"]) |> normalize_hello()
    sock
  end

  defp normalize_hello(resp) when is_map(resp), do: {:simple, "OK"}
  defp normalize_hello(resp), do: resp

  defp cmd(sock, args, timeout \\ 10_000) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(args)))
    recv_one(sock, timeout)
  end

  defp send_cmd(sock, args) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(args)))
  end

  defp recv_one(sock, timeout \\ 10_000) do
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

  # Receive exactly `count` RESP responses from `sock`.
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

  # Attempt to receive a push message; returns nil if nothing arrives within
  # the timeout (useful for asserting no message is delivered).
  defp recv_push_or_nil(sock, timeout) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, data} ->
        case Parser.parse(data) do
          {:ok, [val | _], _} -> val
          _ -> nil
        end

      {:error, :timeout} ->
        nil
    end
  end

  defp ukey(base), do: "pubsub_hunt_#{base}_#{:rand.uniform(9_999_999)}"

  # Clean the PubSub ETS tables between tests so leaked subscriptions from
  # a prior test do not pollute subsequent ones.
  setup do
    PubSub.cleanup(self())

    # Clear ALL entries from both PubSub ETS tables so tests start clean.
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

  # ---------------------------------------------------------------------------
  # 1. SUBSCRIBE to 100 channels — all confirmed
  # ---------------------------------------------------------------------------

  describe "SUBSCRIBE to 100 channels" do
    test "all 100 subscriptions are confirmed with incrementing counts" do
      sock = connect_and_hello()
      channels = Enum.map(1..100, fn i -> ukey("mass_#{i}") end)

      # Subscribe to all 100 at once (SUBSCRIBE ch1 ch2 ... ch100)
      send_cmd(sock, ["SUBSCRIBE" | channels])
      responses = recv_n(sock, 100)

      assert length(responses) == 100

      Enum.with_index(responses, 1)
      |> Enum.each(fn {resp, i} ->
        expected_ch = Enum.at(channels, i - 1)
        assert {:push, ["subscribe", ^expected_ch, ^i]} = resp
      end)

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 2. PUBLISH to channel with no subscribers — returns 0
  # ---------------------------------------------------------------------------

  describe "PUBLISH to channel with no subscribers" do
    test "returns 0 when no one is subscribed" do
      sock = connect_and_hello()
      channel = ukey("ghost")

      result = cmd(sock, ["PUBLISH", channel, "hello_void"])
      assert result == 0

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 3. PSUBSCRIBE pattern matching: "foo*" matches "foobar"
  # ---------------------------------------------------------------------------

  describe "PSUBSCRIBE pattern matching" do
    test "pattern 'foo*' matches channel 'foobar'" do
      prefix = ukey("pat")
      pattern = "#{prefix}foo*"
      channel = "#{prefix}foobar"

      # Client A: subscribe to pattern
      sock_a = connect_and_hello()
      send_cmd(sock_a, ["PSUBSCRIBE", pattern])
      sub_resp = recv_one(sock_a)
      assert {:push, ["psubscribe", ^pattern, 1]} = sub_resp

      # Client B: publish to matching channel
      sock_b = connect_and_hello()
      pub_count = cmd(sock_b, ["PUBLISH", channel, "pattern_hit"])
      assert pub_count >= 1

      # Client A: should receive pmessage push
      msg = recv_one(sock_a, 5_000)
      assert {:push, ["pmessage", ^pattern, ^channel, "pattern_hit"]} = msg

      :gen_tcp.close(sock_a)
      :gen_tcp.close(sock_b)
    end

    test "pattern 'foo*' does NOT match channel 'barfoo'" do
      prefix = ukey("pat_neg")
      pattern = "#{prefix}foo*"
      channel = "#{prefix}barfoo"

      sock_a = connect_and_hello()
      send_cmd(sock_a, ["PSUBSCRIBE", pattern])
      _sub = recv_one(sock_a)

      sock_b = connect_and_hello()
      pub_count = cmd(sock_b, ["PUBLISH", channel, "should_not_arrive"])
      assert pub_count == 0

      # No message should arrive
      assert recv_push_or_nil(sock_a, 300) == nil

      :gen_tcp.close(sock_a)
      :gen_tcp.close(sock_b)
    end
  end

  # ---------------------------------------------------------------------------
  # 4. UNSUBSCRIBE from channel not subscribed to — no error
  # ---------------------------------------------------------------------------

  describe "UNSUBSCRIBE from channel not subscribed to" do
    test "returns unsubscribe push with count 0 and no error" do
      sock = connect_and_hello()
      channel = ukey("never_subbed")

      # Need to enter pubsub mode first by subscribing to something,
      # then unsubscribe from a channel we never subscribed to.
      dummy = ukey("dummy")
      send_cmd(sock, ["SUBSCRIBE", dummy])
      _sub = recv_one(sock)

      send_cmd(sock, ["UNSUBSCRIBE", channel])
      resp = recv_one(sock)

      # Should be a push, NOT an error. Count stays 1 (still subscribed to dummy).
      assert {:push, ["unsubscribe", ^channel, 1]} = resp

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 5. PUNSUBSCRIBE from pattern not subscribed to — no error
  # ---------------------------------------------------------------------------

  describe "PUNSUBSCRIBE from pattern not subscribed to" do
    test "returns punsubscribe push with no error" do
      sock = connect_and_hello()
      pattern = ukey("never_psubbed_*")

      # Enter pubsub mode via a dummy subscription
      dummy = ukey("dummy_p")
      send_cmd(sock, ["SUBSCRIBE", dummy])
      _sub = recv_one(sock)

      send_cmd(sock, ["PUNSUBSCRIBE", pattern])
      resp = recv_one(sock)

      # Should be a push, NOT an error. Count stays 1.
      assert {:push, ["punsubscribe", ^pattern, 1]} = resp

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 6. Subscribe, publish, unsubscribe, publish — no message after unsub
  # ---------------------------------------------------------------------------

  describe "no messages after unsubscribe" do
    test "publish after unsubscribe delivers nothing" do
      sock_sub = connect_and_hello()
      sock_pub = connect_and_hello()
      channel = ukey("unsub_test")

      # Subscribe
      send_cmd(sock_sub, ["SUBSCRIBE", channel])
      sub_resp = recv_one(sock_sub)
      assert {:push, ["subscribe", ^channel, 1]} = sub_resp

      # First publish — should be received
      pub1 = cmd(sock_pub, ["PUBLISH", channel, "msg1"])
      assert pub1 >= 1

      msg1 = recv_one(sock_sub, 5_000)
      assert {:push, ["message", ^channel, "msg1"]} = msg1

      # Unsubscribe
      send_cmd(sock_sub, ["UNSUBSCRIBE", channel])
      unsub_resp = recv_one(sock_sub)
      assert {:push, ["unsubscribe", ^channel, 0]} = unsub_resp

      # Second publish — should NOT be received (count=0)
      pub2 = cmd(sock_pub, ["PUBLISH", channel, "msg2"])
      assert pub2 == 0

      # Verify nothing arrives on the subscriber socket
      assert recv_push_or_nil(sock_sub, 300) == nil

      :gen_tcp.close(sock_sub)
      :gen_tcp.close(sock_pub)
    end
  end

  # ---------------------------------------------------------------------------
  # 7. Two subscribers same channel — both receive
  # ---------------------------------------------------------------------------

  describe "two subscribers same channel" do
    test "both subscribers receive the published message" do
      channel = ukey("dual_sub")

      sock_a = connect_and_hello()
      sock_b = connect_and_hello()
      sock_pub = connect_and_hello()

      # Both subscribe
      send_cmd(sock_a, ["SUBSCRIBE", channel])
      assert {:push, ["subscribe", ^channel, 1]} = recv_one(sock_a)

      send_cmd(sock_b, ["SUBSCRIBE", channel])
      assert {:push, ["subscribe", ^channel, 1]} = recv_one(sock_b)

      # Publish
      count = cmd(sock_pub, ["PUBLISH", channel, "both_get_this"])
      assert count == 2

      # Both should receive
      msg_a = recv_one(sock_a, 5_000)
      assert {:push, ["message", ^channel, "both_get_this"]} = msg_a

      msg_b = recv_one(sock_b, 5_000)
      assert {:push, ["message", ^channel, "both_get_this"]} = msg_b

      :gen_tcp.close(sock_a)
      :gen_tcp.close(sock_b)
      :gen_tcp.close(sock_pub)
    end
  end

  # ---------------------------------------------------------------------------
  # 8. PUBSUB CHANNELS returns subscribed channels
  # ---------------------------------------------------------------------------

  describe "PUBSUB CHANNELS" do
    test "lists currently subscribed channels" do
      channel_a = ukey("listed_a")
      channel_b = ukey("listed_b")

      sock_sub = connect_and_hello()

      # Subscribe to two channels
      send_cmd(sock_sub, ["SUBSCRIBE", channel_a, channel_b])
      _responses = recv_n(sock_sub, 2)

      # Use a separate connection to query PUBSUB CHANNELS
      sock_query = connect_and_hello()
      result = cmd(sock_query, ["PUBSUB", "CHANNELS"])

      assert is_list(result)
      assert channel_a in result
      assert channel_b in result

      :gen_tcp.close(sock_sub)
      :gen_tcp.close(sock_query)
    end

    test "PUBSUB CHANNELS with pattern filters results" do
      prefix = ukey("filter")
      channel_match = "#{prefix}_yes"
      channel_no = ukey("other_no")

      sock_sub = connect_and_hello()
      send_cmd(sock_sub, ["SUBSCRIBE", channel_match, channel_no])
      _responses = recv_n(sock_sub, 2)

      sock_query = connect_and_hello()
      result = cmd(sock_query, ["PUBSUB", "CHANNELS", "#{prefix}*"])

      assert is_list(result)
      assert channel_match in result
      refute channel_no in result

      :gen_tcp.close(sock_sub)
      :gen_tcp.close(sock_query)
    end
  end

  # ---------------------------------------------------------------------------
  # 9. PUBSUB NUMSUB returns correct counts
  # ---------------------------------------------------------------------------

  describe "PUBSUB NUMSUB" do
    test "returns correct subscriber counts for queried channels" do
      channel = ukey("numsub")

      sock_a = connect_and_hello()
      sock_b = connect_and_hello()

      # Two subscribers on same channel
      send_cmd(sock_a, ["SUBSCRIBE", channel])
      _sub_a = recv_one(sock_a)

      send_cmd(sock_b, ["SUBSCRIBE", channel])
      _sub_b = recv_one(sock_b)

      # Query from a third connection
      sock_query = connect_and_hello()
      result = cmd(sock_query, ["PUBSUB", "NUMSUB", channel])

      # Expected: [channel, 2]
      assert result == [channel, 2]

      :gen_tcp.close(sock_a)
      :gen_tcp.close(sock_b)
      :gen_tcp.close(sock_query)
    end

    test "returns 0 for channels with no subscribers" do
      channel = ukey("numsub_empty")

      sock_query = connect_and_hello()
      result = cmd(sock_query, ["PUBSUB", "NUMSUB", channel])

      assert result == [channel, 0]

      :gen_tcp.close(sock_query)
    end
  end

  # ---------------------------------------------------------------------------
  # 10. Subscribe then RESET — clears subscriptions
  #
  # BUG DETECTED: The RESET command handler in Connection does NOT clear
  # pubsub_channels / pubsub_patterns from the connection state, and does
  # NOT call PubSub.cleanup/1 or PubSub.unsubscribe/2. After RESET, the
  # ETS tables still hold stale entries and PUBLISH still delivers messages
  # to the "reset" connection's pid.
  # ---------------------------------------------------------------------------

  describe "RESET clears subscriptions" do
    test "after RESET, PUBSUB NUMSUB shows 0 for previously subscribed channel" do
      channel = ukey("reset_test")

      sock_sub = connect_and_hello()

      # Subscribe
      send_cmd(sock_sub, ["SUBSCRIBE", channel])
      sub_resp = recv_one(sock_sub)
      assert {:push, ["subscribe", ^channel, 1]} = sub_resp

      # Verify subscription exists
      sock_query = connect_and_hello()
      numsub = cmd(sock_query, ["PUBSUB", "NUMSUB", channel])
      assert numsub == [channel, 1]

      # RESET the subscribed connection
      send_cmd(sock_sub, ["RESET"])
      reset_resp = recv_one(sock_sub)
      assert {:simple, "RESET"} = reset_resp

      # BUG: After RESET, the subscription should be gone.
      # The ETS entry should be removed and NUMSUB should return 0.
      numsub_after = cmd(sock_query, ["PUBSUB", "NUMSUB", channel])

      assert numsub_after == [channel, 0],
             "BUG: RESET did not clear pubsub subscriptions. " <>
               "NUMSUB still reports #{inspect(numsub_after)} instead of [#{inspect(channel)}, 0]. " <>
               "The RESET handler must clear pubsub_channels/pubsub_patterns and call PubSub.cleanup/1."

      :gen_tcp.close(sock_sub)
      :gen_tcp.close(sock_query)
    end

    test "after RESET, PUBLISH delivers 0 (no stale subscribers)" do
      channel = ukey("reset_pub")

      sock_sub = connect_and_hello()

      # Subscribe
      send_cmd(sock_sub, ["SUBSCRIBE", channel])
      _sub = recv_one(sock_sub)

      # RESET
      send_cmd(sock_sub, ["RESET"])
      _reset = recv_one(sock_sub)

      # Publish from another connection — should hit 0 subscribers
      sock_pub = connect_and_hello()
      count = cmd(sock_pub, ["PUBLISH", channel, "after_reset"])

      assert count == 0,
             "BUG: PUBLISH after RESET still delivered to #{count} subscriber(s). " <>
               "The RESET handler does not call PubSub.unsubscribe/2 or PubSub.cleanup/1, " <>
               "leaving stale entries in the ETS table."

      :gen_tcp.close(sock_sub)
      :gen_tcp.close(sock_pub)
    end

    test "after RESET, PSUBSCRIBE patterns are also cleared" do
      pattern = ukey("reset_pat_*")

      sock_sub = connect_and_hello()

      # Pattern subscribe
      send_cmd(sock_sub, ["PSUBSCRIBE", pattern])
      psub_resp = recv_one(sock_sub)
      assert {:push, ["psubscribe", ^pattern, 1]} = psub_resp

      # Verify pattern exists via PUBSUB NUMPAT
      sock_query = connect_and_hello()
      numpat_before = cmd(sock_query, ["PUBSUB", "NUMPAT"])
      assert numpat_before >= 1

      # RESET
      send_cmd(sock_sub, ["RESET"])
      _reset = recv_one(sock_sub)

      # NUMPAT should decrease (pattern should be removed)
      numpat_after = cmd(sock_query, ["PUBSUB", "NUMPAT"])

      assert numpat_after == numpat_before - 1,
             "BUG: RESET did not clear pattern subscriptions. " <>
               "NUMPAT was #{numpat_before} before RESET, still #{numpat_after} after. " <>
               "The RESET handler must call PubSub.punsubscribe/2 or PubSub.cleanup/1."

      :gen_tcp.close(sock_sub)
      :gen_tcp.close(sock_query)
    end
  end
end
