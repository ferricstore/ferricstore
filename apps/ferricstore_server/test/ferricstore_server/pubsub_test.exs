defmodule FerricstoreServer.PubSubTest do
  @moduledoc """
  Tests for the `Ferricstore.PubSub` ETS-based pub/sub registry.

  These tests verify the core pub/sub operations (subscribe, unsubscribe,
  publish, pattern matching, cleanup) at the BEAM messaging level, without
  going through TCP or RESP encoding.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.PubSub
  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # TCP helpers (shared with integration tests)
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp send_pipeline(sock, commands) do
    data =
      commands
      |> Enum.map(&Encoder.encode/1)
      |> IO.iodata_to_binary()

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

  defp recv_n(sock, n) do
    do_recv_n(sock, n, "", [])
  end

  defp do_recv_n(_sock, 0, _buf, acc), do: acc

  defp do_recv_n(sock, remaining, buf, acc) when remaining > 0 do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [_ | _] = vals, rest} ->
        taken = Enum.take(vals, remaining)
        new_acc = acc ++ taken
        new_remaining = remaining - length(taken)
        do_recv_n(sock, new_remaining, rest, new_acc)

      {:ok, [], _} ->
        do_recv_n(sock, remaining, buf2, acc)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp eventually(fun, timeout_ms \\ 500) when is_function(fun, 0) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    try do
      fun.()
    rescue
      error in [ExUnit.AssertionError] ->
        if System.monotonic_time(:millisecond) >= deadline do
          reraise error, __STACKTRACE__
        else
          Process.sleep(10)
          do_eventually(fun, deadline)
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
    reset_server_auth_state()
    on_exit(fn -> reset_server_auth_state() end)

    # Clean ETS tables between tests to avoid cross-test interference
    clear_table(:ferricstore_pubsub)
    clear_table(:ferricstore_pubsub_patterns)
    clear_table(:ferricstore_pubsub_channel_cache)
    :ok
  end

  defp reset_server_auth_state do
    Ferricstore.Config.set("requirepass", "")
    Acl.reset!()
    ConnAuth.broadcast_acl_invalidation(:all)
    :ok
  end

  defp clear_table(table) do
    case :ets.whereis(table) do
      :undefined -> :ok
      _tid -> :ets.delete_all_objects(table)
    end
  end

  # ---------------------------------------------------------------------------
  # Unit tests — PubSub module directly
  # ---------------------------------------------------------------------------

  test "connection Pub/Sub dispatch uses bulk registry APIs" do
    source = File.read!(Path.expand("../../lib/ferricstore_server/connection/pubsub.ex", __DIR__))

    assert source =~ "PS.subscribe_many(self())"
    assert source =~ "PS.psubscribe_many(self())"
    assert source =~ "PS.unsubscribe_many(channels, self())"
    assert source =~ "PS.punsubscribe_many(patterns, self())"
    refute source =~ "PS.subscribe(ch, self())"
    refute source =~ "PS.psubscribe(pat, self())"
  end

  test "exact publish uses the channel pid cache" do
    source = File.read!(Path.expand("../../../ferricstore/lib/ferricstore/pubsub.ex", __DIR__))
    [publish_source] = Regex.run(~r/def publish\(channel, message\).*?^  end/ms, source)

    assert source =~ "@channel_cache_table :ferricstore_pubsub_channel_cache"
    assert publish_source =~ ":ets.lookup(@channel_cache_table, channel)"
    refute publish_source =~ ":ets.lookup(@channels_table, channel)"
  end

  describe "subscribe and receive message" do
    test "bulk exact subscribe and unsubscribe update registry idempotently" do
      assert :ok = PubSub.subscribe_many(["bulk", "bulk", "other"], self())

      assert PubSub.numsub(["bulk", "other"]) == ["bulk", 1, "other", 1]
      assert PubSub.publish("bulk", "hello") == 1
      assert_receive {:pubsub_message, "bulk", "hello"}, 1000

      assert :ok = PubSub.unsubscribe_many(["bulk", "bulk"], self())
      assert PubSub.numsub(["bulk", "other"]) == ["bulk", 0, "other", 1]
    end

    test "subscriber receives message on exact channel" do
      PubSub.subscribe("news", self())
      PubSub.publish("news", "hello world")

      assert_receive {:pubsub_message, "news", "hello world"}, 1000
    end

    test "duplicate exact subscription is idempotent" do
      PubSub.subscribe("news", self())
      PubSub.subscribe("news", self())

      assert PubSub.numsub(["news"]) == ["news", 1]
      assert PubSub.publish("news", "hello world") == 1

      assert_receive {:pubsub_message, "news", "hello world"}, 1000
      refute_receive {:pubsub_message, "news", "hello world"}, 100
    end

    test "bulk duplicate exact subscriptions keep publish count and delivery idempotent" do
      PubSub.subscribe_many(["news", "news", "news"], self())

      assert PubSub.numsub(["news"]) == ["news", 1]
      assert PubSub.publish("news", "cached") == 1

      assert_receive {:pubsub_message, "news", "cached"}, 1000
      refute_receive {:pubsub_message, "news", "cached"}, 100
    end

    test "subscriber does not receive message on different channel" do
      PubSub.subscribe("news", self())
      PubSub.publish("sports", "goal!")

      refute_receive {:pubsub_message, _, _}, 100
    end
  end

  describe "pattern subscribe and receive message" do
    test "bulk pattern subscribe and unsubscribe update registry idempotently" do
      assert :ok = PubSub.psubscribe_many(["bulk.*", "bulk.*", "other.*"], self())

      assert PubSub.numpat() == 2
      assert PubSub.publish("bulk.1", "hello") == 1
      assert_receive {:pubsub_pmessage, "bulk.*", "bulk.1", "hello"}, 1000

      assert :ok = PubSub.punsubscribe_many(["bulk.*", "bulk.*"], self())
      assert PubSub.numpat() == 1
      assert PubSub.publish("bulk.1", "miss") == 0
    end

    test "pattern subscriber receives message matching pattern" do
      PubSub.psubscribe("news.*", self())
      PubSub.publish("news.tech", "AI breakthrough")

      assert_receive {:pubsub_pmessage, "news.*", "news.tech", "AI breakthrough"}, 1000
    end

    test "duplicate pattern subscription is idempotent" do
      PubSub.psubscribe("news.*", self())
      PubSub.psubscribe("news.*", self())

      assert PubSub.numpat() == 1
      assert PubSub.publish("news.tech", "AI breakthrough") == 1

      assert_receive {:pubsub_pmessage, "news.*", "news.tech", "AI breakthrough"}, 1000
      refute_receive {:pubsub_pmessage, "news.*", "news.tech", "AI breakthrough"}, 100
    end

    test "pattern subscriber does not receive non-matching message" do
      PubSub.psubscribe("news.*", self())
      PubSub.publish("sports.football", "goal!")

      refute_receive {:pubsub_pmessage, _, _, _}, 100
    end

    test "wildcard pattern matches everything" do
      PubSub.psubscribe("*", self())
      PubSub.publish("anything", "data")

      assert_receive {:pubsub_pmessage, "*", "anything", "data"}, 1000
    end

    test "question mark pattern matches single character" do
      PubSub.psubscribe("ch?t", self())
      PubSub.publish("chat", "hello")
      PubSub.publish("chit", "hello")
      PubSub.publish("choot", "hello")

      assert_receive {:pubsub_pmessage, "ch?t", "chat", "hello"}, 1000
      assert_receive {:pubsub_pmessage, "ch?t", "chit", "hello"}, 1000
      refute_receive {:pubsub_pmessage, "ch?t", "choot", _}, 100
    end
  end

  describe "publish returns recipient count" do
    test "returns 0 when no subscribers" do
      assert PubSub.publish("empty_channel", "hello") == 0
    end

    test "returns 1 for one subscriber" do
      PubSub.subscribe("ch1", self())
      assert PubSub.publish("ch1", "msg") == 1
    end

    test "returns total count of channel + pattern subscribers" do
      PubSub.subscribe("events.log", self())
      PubSub.psubscribe("events.*", self())
      assert PubSub.publish("events.log", "msg") == 2
    end
  end

  describe "multiple subscribers receive same message" do
    test "two processes both receive the published message" do
      parent = self()

      pid1 =
        spawn(fn ->
          receive do
            {:pubsub_message, ch, msg} -> send(parent, {:got1, ch, msg})
          end
        end)

      pid2 =
        spawn(fn ->
          receive do
            {:pubsub_message, ch, msg} -> send(parent, {:got2, ch, msg})
          end
        end)

      PubSub.subscribe("shared", pid1)
      PubSub.subscribe("shared", pid2)

      count = PubSub.publish("shared", "broadcast")
      assert count == 2

      assert_receive {:got1, "shared", "broadcast"}, 1000
      assert_receive {:got2, "shared", "broadcast"}, 1000
    end
  end

  describe "unsubscribe" do
    test "unsubscribed process no longer receives messages" do
      PubSub.subscribe("temp", self())
      PubSub.unsubscribe("temp", self())
      PubSub.publish("temp", "missed")

      refute_receive {:pubsub_message, _, _}, 100
    end

    test "unsubscribe from non-subscribed channel is a no-op" do
      assert PubSub.unsubscribe("nonexistent", self()) == :ok
    end
  end

  describe "punsubscribe" do
    test "pattern unsubscribe stops pattern messages" do
      PubSub.psubscribe("news.*", self())
      PubSub.punsubscribe("news.*", self())
      PubSub.publish("news.tech", "missed")

      refute_receive {:pubsub_pmessage, _, _, _}, 100
    end
  end

  describe "channels/1" do
    test "returns empty list when no subscriptions" do
      assert PubSub.channels() == []
    end

    test "returns active channels" do
      PubSub.subscribe("ch1", self())
      PubSub.subscribe("ch2", self())

      result = PubSub.channels()
      assert Enum.sort(result) == ["ch1", "ch2"]
    end

    test "filters channels by glob pattern" do
      PubSub.subscribe("news.tech", self())
      PubSub.subscribe("news.sports", self())
      PubSub.subscribe("other.stuff", self())

      result = PubSub.channels("news.*")
      assert Enum.sort(result) == ["news.sports", "news.tech"]
    end

    test "returns unique channels even with multiple subscribers" do
      parent = self()
      pid2 = spawn(fn -> Process.sleep(:infinity) end)

      PubSub.subscribe("dup", parent)
      PubSub.subscribe("dup", pid2)

      assert PubSub.channels() == ["dup"]

      Process.exit(pid2, :kill)
    end
  end

  describe "numsub/1" do
    test "returns subscriber counts per channel" do
      PubSub.subscribe("a", self())

      pid2 = spawn(fn -> Process.sleep(:infinity) end)
      PubSub.subscribe("a", pid2)
      PubSub.subscribe("b", self())

      result = PubSub.numsub(["a", "b", "c"])
      assert result == ["a", 2, "b", 1, "c", 0]

      Process.exit(pid2, :kill)
    end

    test "returns empty list for empty input" do
      assert PubSub.numsub([]) == []
    end
  end

  describe "numpat/0" do
    test "returns 0 when no pattern subscriptions" do
      assert PubSub.numpat() == 0
    end

    test "returns count of active pattern subscriptions" do
      PubSub.psubscribe("news.*", self())
      PubSub.psubscribe("sports.*", self())

      assert PubSub.numpat() == 2
    end
  end

  describe "cleanup on disconnect" do
    test "cleanup removes all subscriptions for a pid" do
      PubSub.subscribe("ch1", self())
      PubSub.subscribe("ch2", self())
      PubSub.psubscribe("pat.*", self())

      PubSub.cleanup(self())

      assert PubSub.channels() == []
      assert PubSub.numpat() == 0
      assert PubSub.publish("ch1", "missed") == 0
    end

    test "dead exact subscribers are removed without explicit cleanup" do
      pid = spawn(fn -> Process.sleep(:infinity) end)

      PubSub.subscribe("stale", pid)
      assert PubSub.numsub(["stale"]) == ["stale", 1]

      Process.exit(pid, :kill)

      eventually(fn ->
        assert PubSub.numsub(["stale"]) == ["stale", 0]
        assert PubSub.publish("stale", "missed") == 0
      end)
    end

    test "dead pattern subscribers are removed without explicit cleanup" do
      pid = spawn(fn -> Process.sleep(:infinity) end)

      PubSub.psubscribe("stale.*", pid)
      assert PubSub.numpat() == 1

      Process.exit(pid, :kill)

      eventually(fn ->
        assert PubSub.numpat() == 0
        assert PubSub.publish("stale.channel", "missed") == 0
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # TCP integration tests for pub/sub
  # ---------------------------------------------------------------------------

  describe "SUBSCRIBE over TCP" do
    test "subscribe returns push with channel and count", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SUBSCRIBE", "test_ch"])
      response = recv_response(sock)

      assert {:push, ["subscribe", "test_ch", 1]} = response

      :gen_tcp.close(sock)
    end

    test "subscribe to multiple channels returns push per channel", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SUBSCRIBE", "ch_a", "ch_b", "ch_c"])
      responses = recv_n(sock, 3)

      assert {:push, ["subscribe", "ch_a", 1]} = Enum.at(responses, 0)
      assert {:push, ["subscribe", "ch_b", 2]} = Enum.at(responses, 1)
      assert {:push, ["subscribe", "ch_c", 3]} = Enum.at(responses, 2)

      :gen_tcp.close(sock)
    end
  end

  describe "SUBSCRIBE and receive published message over TCP" do
    test "subscriber receives message from publisher", %{port: port} do
      sub_sock = connect_and_hello(port)
      pub_sock = connect_and_hello(port)

      # Subscribe
      send_cmd(sub_sock, ["SUBSCRIBE", "events"])
      {:push, ["subscribe", "events", 1]} = recv_response(sub_sock)

      # Publish
      send_cmd(pub_sock, ["PUBLISH", "events", "hello"])
      pub_resp = recv_response(pub_sock)
      assert pub_resp == 1

      # Subscriber receives push message
      msg = recv_response(sub_sock)
      assert {:push, ["message", "events", "hello"]} = msg

      :gen_tcp.close(sub_sock)
      :gen_tcp.close(pub_sock)
    end

    test "multiple subscribers receive same published message", %{port: port} do
      sub1 = connect_and_hello(port)
      sub2 = connect_and_hello(port)
      pub = connect_and_hello(port)

      send_cmd(sub1, ["SUBSCRIBE", "shared_ch"])
      {:push, ["subscribe", "shared_ch", 1]} = recv_response(sub1)

      send_cmd(sub2, ["SUBSCRIBE", "shared_ch"])
      {:push, ["subscribe", "shared_ch", 1]} = recv_response(sub2)

      send_cmd(pub, ["PUBLISH", "shared_ch", "broadcast"])
      count = recv_response(pub)
      assert count == 2

      assert {:push, ["message", "shared_ch", "broadcast"]} = recv_response(sub1)
      assert {:push, ["message", "shared_ch", "broadcast"]} = recv_response(sub2)

      :gen_tcp.close(sub1)
      :gen_tcp.close(sub2)
      :gen_tcp.close(pub)
    end
  end

  describe "PSUBSCRIBE over TCP" do
    test "pattern subscriber receives matching message", %{port: port} do
      sub = connect_and_hello(port)
      pub = connect_and_hello(port)

      send_cmd(sub, ["PSUBSCRIBE", "news.*"])
      {:push, ["psubscribe", "news.*", 1]} = recv_response(sub)

      send_cmd(pub, ["PUBLISH", "news.tech", "AI update"])
      assert recv_response(pub) == 1

      msg = recv_response(sub)
      assert {:push, ["pmessage", "news.*", "news.tech", "AI update"]} = msg

      :gen_tcp.close(sub)
      :gen_tcp.close(pub)
    end
  end

  describe "UNSUBSCRIBE over TCP" do
    test "unsubscribe reduces subscription count", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SUBSCRIBE", "a", "b"])
      _subs = recv_n(sock, 2)

      send_cmd(sock, ["UNSUBSCRIBE", "a"])
      unsub = recv_response(sock)
      assert {:push, ["unsubscribe", "a", 1]} = unsub

      :gen_tcp.close(sock)
    end

    test "unsubscribe all channels", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SUBSCRIBE", "x", "y"])
      _subs = recv_n(sock, 2)

      send_cmd(sock, ["UNSUBSCRIBE"])
      responses = recv_n(sock, 2)

      # After unsubscribing all, the last response should have count 0
      counts = Enum.map(responses, fn {:push, [_, _, count]} -> count end)
      assert Enum.min(counts) == 0

      :gen_tcp.close(sock)
    end

    test "unsubscribe with no subscriptions returns null channel count zero", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["UNSUBSCRIBE"])
      assert {:push, ["unsubscribe", nil, 0]} = recv_response(sock)

      :gen_tcp.close(sock)
    end

    test "connection exits pub/sub mode after unsubscribing all", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SUBSCRIBE", "temp"])
      {:push, ["subscribe", "temp", 1]} = recv_response(sock)

      send_cmd(sock, ["UNSUBSCRIBE", "temp"])
      {:push, ["unsubscribe", "temp", 0]} = recv_response(sock)

      # Should be able to use normal commands again
      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} = recv_response(sock)

      :gen_tcp.close(sock)
    end
  end

  describe "PUNSUBSCRIBE over TCP" do
    test "pattern unsubscribe reduces count", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["PSUBSCRIBE", "a.*", "b.*"])
      _psubs = recv_n(sock, 2)

      send_cmd(sock, ["PUNSUBSCRIBE", "a.*"])
      unsub = recv_response(sock)
      assert {:push, ["punsubscribe", "a.*", 1]} = unsub

      :gen_tcp.close(sock)
    end

    test "punsubscribe with no subscriptions returns null pattern count zero", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["PUNSUBSCRIBE"])
      assert {:push, ["punsubscribe", nil, 0]} = recv_response(sock)

      :gen_tcp.close(sock)
    end
  end

  describe "pub/sub mode restrictions" do
    test "non-pubsub commands return error in pub/sub mode", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SUBSCRIBE", "locked"])
      {:push, ["subscribe", "locked", 1]} = recv_response(sock)

      send_cmd(sock, ["GET", "somekey"])
      response = recv_response(sock)
      assert {:error, "ERR Can't execute 'get'" <> _} = response

      :gen_tcp.close(sock)
    end

    test "commands after SUBSCRIBE in the same pipeline stay restricted", %{port: port} do
      sock = connect_and_hello(port)
      key = "pubsub:pipeline:restricted:#{System.unique_integer([:positive])}"

      send_pipeline(sock, [
        ["SUBSCRIBE", "locked-pipe"],
        ["SET", key, "must-not-write"]
      ])

      [subscribe_response, set_response] = recv_n(sock, 2)

      assert {:push, ["subscribe", "locked-pipe", 1]} = subscribe_response
      assert {:error, "ERR Can't execute 'set'" <> _} = set_response
      assert nil == Ferricstore.Store.Router.get(FerricStore.Instance.get(:default), key)

      :gen_tcp.close(sock)
    end

    test "PING is allowed in pub/sub mode", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SUBSCRIBE", "pingy"])
      {:push, ["subscribe", "pingy", 1]} = recv_response(sock)

      send_cmd(sock, ["PING"])
      response = recv_response(sock)
      assert {:simple, "PONG"} = response

      :gen_tcp.close(sock)
    end
  end

  describe "PUBLISH returns number of recipients" do
    test "PUBLISH to channel with no subscribers returns 0", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["PUBLISH", "ghost_channel", "hello"])
      assert recv_response(sock) == 0

      :gen_tcp.close(sock)
    end
  end

  describe "PUBSUB CHANNELS over TCP" do
    test "returns active channels", %{port: port} do
      sub = connect_and_hello(port)
      query = connect_and_hello(port)

      send_cmd(sub, ["SUBSCRIBE", "active1", "active2"])
      _subs = recv_n(sub, 2)

      send_cmd(query, ["PUBSUB", "CHANNELS"])
      channels = recv_response(query)
      assert is_list(channels)
      assert "active1" in channels
      assert "active2" in channels

      :gen_tcp.close(sub)
      :gen_tcp.close(query)
    end

    test "returns channels matching pattern filter", %{port: port} do
      sub = connect_and_hello(port)
      query = connect_and_hello(port)

      send_cmd(sub, ["SUBSCRIBE", "news.tech", "news.sports", "other.stuff"])
      _subs = recv_n(sub, 3)

      send_cmd(query, ["PUBSUB", "CHANNELS", "news.*"])
      channels = recv_response(query)
      assert is_list(channels)
      assert Enum.sort(channels) == ["news.sports", "news.tech"]

      :gen_tcp.close(sub)
      :gen_tcp.close(query)
    end
  end

  describe "PUBSUB NUMSUB over TCP" do
    test "returns subscriber counts for specific channels", %{port: port} do
      sub1 = connect_and_hello(port)
      sub2 = connect_and_hello(port)
      query = connect_and_hello(port)

      send_cmd(sub1, ["SUBSCRIBE", "numsub_a", "numsub_b"])
      _subs = recv_n(sub1, 2)

      send_cmd(sub2, ["SUBSCRIBE", "numsub_a"])
      _r3 = recv_response(sub2)

      send_cmd(query, ["PUBSUB", "NUMSUB", "numsub_a", "numsub_b", "numsub_missing"])
      result = recv_response(query)
      assert result == ["numsub_a", 2, "numsub_b", 1, "numsub_missing", 0]

      :gen_tcp.close(sub1)
      :gen_tcp.close(sub2)
      :gen_tcp.close(query)
    end
  end

  describe "PUBSUB NUMPAT over TCP" do
    test "returns pattern subscription count", %{port: port} do
      sub = connect_and_hello(port)
      query = connect_and_hello(port)

      send_cmd(sub, ["PSUBSCRIBE", "pat1.*", "pat2.*"])
      _psubs = recv_n(sub, 2)

      send_cmd(query, ["PUBSUB", "NUMPAT"])
      count = recv_response(query)
      assert is_integer(count)
      assert count >= 2

      :gen_tcp.close(sub)
      :gen_tcp.close(query)
    end
  end

  describe "cleanup on disconnect over TCP" do
    test "disconnecting subscriber cleans up ETS entries", %{port: port} do
      sub = connect_and_hello(port)
      query = connect_and_hello(port)

      send_cmd(sub, ["SUBSCRIBE", "cleanup_test"])
      {:push, ["subscribe", "cleanup_test", 1]} = recv_response(sub)

      # Verify channel exists
      send_cmd(query, ["PUBSUB", "NUMSUB", "cleanup_test"])
      assert recv_response(query) == ["cleanup_test", 1]

      # Close subscriber
      :gen_tcp.close(sub)

      # Give the connection process time to clean up
      Process.sleep(100)

      # Channel should have no subscribers now
      send_cmd(query, ["PUBSUB", "NUMSUB", "cleanup_test"])
      assert recv_response(query) == ["cleanup_test", 0]

      :gen_tcp.close(query)
    end
  end

  describe "ACL invalidation while subscribed" do
    setup do
      Acl.reset!()

      on_exit(fn ->
        Acl.reset!()
      end)

      :ok
    end

    test "deleting a subscribed user removes live subscriptions", %{port: port} do
      channel = "acl-events-#{System.unique_integer([:positive])}"

      assert :ok =
               Acl.set_user("temporary_subscriber", [
                 "on",
                 ">secret",
                 "-@all",
                 "+@pubsub",
                 "&acl-events-*"
               ])

      sub = connect_and_hello(port)
      send_cmd(sub, ["AUTH", "temporary_subscriber", "secret"])
      assert {:simple, "OK"} = recv_response(sub)

      send_cmd(sub, ["SUBSCRIBE", channel])
      assert {:push, ["subscribe", ^channel, 1]} = recv_response(sub)
      assert PubSub.numsub([channel]) == [channel, 1]

      assert :ok = Acl.del_user("temporary_subscriber")
      ConnAuth.broadcast_acl_invalidation("temporary_subscriber")

      eventually(fn ->
        assert PubSub.numsub([channel]) == [channel, 0]
      end)

      assert PubSub.publish(channel, "should-not-deliver") == 0

      :gen_tcp.close(sub)
    end
  end

  describe "Pub/Sub ACL channel patterns" do
    setup do
      Acl.reset!()

      on_exit(fn ->
        Acl.reset!()
      end)

      :ok
    end

    test "SUBSCRIBE is governed by channel ACL patterns, not key ACL patterns", %{port: port} do
      assert :ok =
               Acl.set_user("news_subscriber", [
                 "on",
                 ">secret",
                 "-@all",
                 "+subscribe",
                 "resetkeys",
                 "&news:*"
               ])

      sub = connect_and_hello(port)
      send_cmd(sub, ["AUTH", "news_subscriber", "secret"])
      assert {:simple, "OK"} = recv_response(sub)

      send_cmd(sub, ["SUBSCRIBE", "news:1"])
      assert {:push, ["subscribe", "news:1", 1]} = recv_response(sub)

      denied = connect_and_hello(port)
      send_cmd(denied, ["AUTH", "news_subscriber", "secret"])
      assert {:simple, "OK"} = recv_response(denied)

      send_cmd(denied, ["SUBSCRIBE", "private:1"])
      assert {:error, reason} = recv_response(denied)
      assert reason =~ "NOPERM"

      :gen_tcp.close(sub)
      :gen_tcp.close(denied)
    end

    test "PUBLISH is governed by channel ACL patterns, not key ACL patterns", %{port: port} do
      channel = "news:publish-acl"

      assert :ok =
               Acl.set_user("news_publisher", [
                 "on",
                 ">secret",
                 "-@all",
                 "+publish",
                 "resetkeys",
                 "&news:*"
               ])

      sub = connect_and_hello(port)
      send_cmd(sub, ["SUBSCRIBE", channel])
      assert {:push, ["subscribe", ^channel, 1]} = recv_response(sub)

      pub = connect_and_hello(port)
      send_cmd(pub, ["AUTH", "news_publisher", "secret"])
      assert {:simple, "OK"} = recv_response(pub)

      send_cmd(pub, ["PUBLISH", channel, "ok"])
      assert 1 = recv_response(pub)
      assert {:push, ["message", ^channel, "ok"]} = recv_response(sub)

      send_cmd(pub, ["PUBLISH", "private:publish-acl", "nope"])
      assert {:error, reason} = recv_response(pub)
      assert reason =~ "NOPERM"

      :gen_tcp.close(pub)
      :gen_tcp.close(sub)
    end
  end
end
