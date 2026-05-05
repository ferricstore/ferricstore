defmodule FerricstoreServer.KeyspaceNotificationsTest do
  @moduledoc false
  use ExUnit.Case, async: false
  alias Ferricstore.Config
  alias Ferricstore.KeyspaceNotifications
  alias Ferricstore.PubSub
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Setup -- reset notification config before each test
  # ---------------------------------------------------------------------------

  setup do
    Config.set("notify-keyspace-events", "")
    on_exit(fn -> Config.set("notify-keyspace-events", "") end)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Unit tests: KeyspaceNotifications module
  # ---------------------------------------------------------------------------

  describe "should_notify?/2" do
    test "returns false when no channel flags are set" do
      refute KeyspaceNotifications.should_notify?("set", "g$")
    end

    test "returns true for string event with K$" do
      assert KeyspaceNotifications.should_notify?("set", "K$")
    end

    test "returns true for generic event with Kg" do
      assert KeyspaceNotifications.should_notify?("del", "Kg")
    end

    test "returns true for expired event with Kx" do
      assert KeyspaceNotifications.should_notify?("expired", "Kx")
    end

    test "returns true with A flag for any event" do
      assert KeyspaceNotifications.should_notify?("set", "KA")
      assert KeyspaceNotifications.should_notify?("del", "KA")
      assert KeyspaceNotifications.should_notify?("hset", "KA")
      assert KeyspaceNotifications.should_notify?("lpush", "KA")
      assert KeyspaceNotifications.should_notify?("sadd", "KA")
      assert KeyspaceNotifications.should_notify?("zadd", "KA")
      assert KeyspaceNotifications.should_notify?("expired", "EA")
    end

    test "returns true for full string command family with $ flag" do
      for event <- ~w(set setrange incrby decrby incrbyfloat getdel getex mset setbit bitop) do
        assert KeyspaceNotifications.should_notify?(event, "K$")
      end
    end

    test "returns true for collection event category flags" do
      assert KeyspaceNotifications.should_notify?("hset", "Kh")
      assert KeyspaceNotifications.should_notify?("lpush", "Kl")
      assert KeyspaceNotifications.should_notify?("sadd", "Ks")
      assert KeyspaceNotifications.should_notify?("zadd", "Kz")
      assert KeyspaceNotifications.should_notify?("xadd", "Kt")
    end

    test "collection category flags do not cross-match other types" do
      refute KeyspaceNotifications.should_notify?("hset", "Kl")
      refute KeyspaceNotifications.should_notify?("lpush", "Ks")
      refute KeyspaceNotifications.should_notify?("sadd", "Kz")
      refute KeyspaceNotifications.should_notify?("zadd", "Kh")
      refute KeyspaceNotifications.should_notify?("xadd", "Kz")
    end

    test "returns false for string event with Kg (no $ flag)" do
      refute KeyspaceNotifications.should_notify?("set", "Kg")
    end

    test "returns false for generic event with K$ (no g flag)" do
      refute KeyspaceNotifications.should_notify?("del", "K$")
    end

    test "returns true for expire with Eg" do
      assert KeyspaceNotifications.should_notify?("expire", "Eg")
    end

    test "returns true for persist with Kg" do
      assert KeyspaceNotifications.should_notify?("persist", "Kg")
    end
  end

  # ---------------------------------------------------------------------------
  # Unit tests: notify/3 with explicit flags
  # ---------------------------------------------------------------------------

  describe "notify/3 with explicit flags" do
    test "does nothing when flags is empty" do
      PubSub.subscribe("__keyspace@0__:mykey", self())
      KeyspaceNotifications.notify("mykey", "set", "")

      refute_received {:pubsub_message, _, _}
    end

    test "publishes to keyspace channel with K flag" do
      PubSub.subscribe("__keyspace@0__:mykey", self())
      KeyspaceNotifications.notify("mykey", "set", "K$")

      assert_received {:pubsub_message, "__keyspace@0__:mykey", "set"}
    end

    test "publishes to keyevent channel with E flag" do
      PubSub.subscribe("__keyevent@0__:set", self())
      KeyspaceNotifications.notify("mykey", "set", "E$")

      assert_received {:pubsub_message, "__keyevent@0__:set", "mykey"}
    end

    test "publishes to both channels with KE flags" do
      PubSub.subscribe("__keyspace@0__:mykey", self())
      PubSub.subscribe("__keyevent@0__:set", self())
      KeyspaceNotifications.notify("mykey", "set", "KE$")

      assert_received {:pubsub_message, "__keyspace@0__:mykey", "set"}
      assert_received {:pubsub_message, "__keyevent@0__:set", "mykey"}
    end

    test "only K flag fires keyspace only" do
      PubSub.subscribe("__keyspace@0__:mykey", self())
      PubSub.subscribe("__keyevent@0__:del", self())
      KeyspaceNotifications.notify("mykey", "del", "Kg")

      assert_received {:pubsub_message, "__keyspace@0__:mykey", "del"}
      refute_received {:pubsub_message, "__keyevent@0__:del", _}
    end

    test "only E flag fires keyevent only" do
      PubSub.subscribe("__keyspace@0__:mykey", self())
      PubSub.subscribe("__keyevent@0__:del", self())
      KeyspaceNotifications.notify("mykey", "del", "Eg")

      refute_received {:pubsub_message, "__keyspace@0__:mykey", _}
      assert_received {:pubsub_message, "__keyevent@0__:del", "mykey"}
    end

    test "del fires with g flag" do
      PubSub.subscribe("__keyevent@0__:del", self())
      KeyspaceNotifications.notify("mykey", "del", "Eg")

      assert_received {:pubsub_message, "__keyevent@0__:del", "mykey"}
    end

    test "expire fires with g flag" do
      PubSub.subscribe("__keyevent@0__:expire", self())
      KeyspaceNotifications.notify("mykey", "expire", "Eg")

      assert_received {:pubsub_message, "__keyevent@0__:expire", "mykey"}
    end

    test "set fires with $ flag" do
      PubSub.subscribe("__keyevent@0__:set", self())
      KeyspaceNotifications.notify("mykey", "set", "E$")

      assert_received {:pubsub_message, "__keyevent@0__:set", "mykey"}
    end

    test "incr fires with $ flag" do
      PubSub.subscribe("__keyevent@0__:incr", self())
      KeyspaceNotifications.notify("mykey", "incr", "E$")

      assert_received {:pubsub_message, "__keyevent@0__:incr", "mykey"}
    end

    test "expired fires with x flag" do
      PubSub.subscribe("__keyevent@0__:expired", self())
      KeyspaceNotifications.notify("mykey", "expired", "Ex")

      assert_received {:pubsub_message, "__keyevent@0__:expired", "mykey"}
    end
  end

  # ---------------------------------------------------------------------------
  # Integration tests: CONFIG SET enables notifications, then commands fire them
  # ---------------------------------------------------------------------------

  describe "CONFIG SET notify-keyspace-events integration" do
    test "no notifications when config is empty (default)" do
      PubSub.subscribe("__keyspace@0__:cfg_test_key", self())
      PubSub.subscribe("__keyevent@0__:set", self())

      # Directly call notify which reads from config
      KeyspaceNotifications.notify("cfg_test_key", "set")

      refute_received {:pubsub_message, _, _}
    end

    test "CONFIG SET notify-keyspace-events KEg enables notifications" do
      Config.set("notify-keyspace-events", "KEg")

      PubSub.subscribe("__keyspace@0__:cfg_test_key2", self())
      PubSub.subscribe("__keyevent@0__:del", self())

      KeyspaceNotifications.notify("cfg_test_key2", "del")

      assert_received {:pubsub_message, "__keyspace@0__:cfg_test_key2", "del"}
      assert_received {:pubsub_message, "__keyevent@0__:del", "cfg_test_key2"}
    end

    test "CONFIG SET notify-keyspace-events KE$ enables string notifications" do
      Config.set("notify-keyspace-events", "KE$")

      PubSub.subscribe("__keyspace@0__:str_key", self())
      PubSub.subscribe("__keyevent@0__:set", self())

      KeyspaceNotifications.notify("str_key", "set")

      assert_received {:pubsub_message, "__keyspace@0__:str_key", "set"}
      assert_received {:pubsub_message, "__keyevent@0__:set", "str_key"}
    end
  end

  # ---------------------------------------------------------------------------
  # TCP integration: SET fires keyspace notification
  # ---------------------------------------------------------------------------

  describe "SET fires keyspace notifications over TCP" do
    test "SET fires keyspace notification on __keyspace@0__:key channel" do
      Config.set("notify-keyspace-events", "KE$")

      key = "ksn_tcp_set_#{:rand.uniform(999_999)}"
      PubSub.subscribe("__keyspace@0__:#{key}", self())

      port = Listener.port()
      sock = connect_and_hello(port)
      send_cmd(sock, ["SET", key, "hello"])
      _resp = recv_response(sock)

      assert_received {:pubsub_message, "__keyspace@0__:" <> ^key, "set"}

      :gen_tcp.close(sock)
    end

    test "SET fires keyevent notification on __keyevent@0__:set channel" do
      Config.set("notify-keyspace-events", "KE$")

      key = "ksn_tcp_set_evt_#{:rand.uniform(999_999)}"
      PubSub.subscribe("__keyevent@0__:set", self())

      port = Listener.port()
      sock = connect_and_hello(port)
      send_cmd(sock, ["SET", key, "hello"])
      _resp = recv_response(sock)

      assert_received {:pubsub_message, "__keyevent@0__:set", ^key}

      :gen_tcp.close(sock)
    end

    test "DEL fires generic event notification" do
      Config.set("notify-keyspace-events", "KEg")

      key = "ksn_tcp_del_#{:rand.uniform(999_999)}"

      port = Listener.port()
      sock = connect_and_hello(port)

      # First SET the key (no $ flag so SET won't fire -- only g is set)
      send_cmd(sock, ["SET", key, "v"])
      _resp = recv_response(sock)

      # Now subscribe for DEL
      PubSub.subscribe("__keyspace@0__:#{key}", self())
      PubSub.subscribe("__keyevent@0__:del", self())

      send_cmd(sock, ["DEL", key])
      _resp = recv_response(sock)

      assert_received {:pubsub_message, "__keyspace@0__:" <> ^key, "del"}
      assert_received {:pubsub_message, "__keyevent@0__:del", ^key}

      :gen_tcp.close(sock)
    end

    test "EXPIRE fires with g flag" do
      Config.set("notify-keyspace-events", "KEg")

      key = "ksn_tcp_expire_#{:rand.uniform(999_999)}"

      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", key, "v"])
      _resp = recv_response(sock)

      PubSub.subscribe("__keyspace@0__:#{key}", self())
      PubSub.subscribe("__keyevent@0__:expire", self())

      send_cmd(sock, ["EXPIRE", key, "100"])
      resp = recv_response(sock)
      assert resp == 1

      assert_received {:pubsub_message, "__keyspace@0__:" <> ^key, "expire"}
      assert_received {:pubsub_message, "__keyevent@0__:expire", ^key}

      :gen_tcp.close(sock)
    end

    test "no notifications when config is empty" do
      Config.set("notify-keyspace-events", "")

      key = "ksn_tcp_empty_#{:rand.uniform(999_999)}"
      PubSub.subscribe("__keyspace@0__:#{key}", self())
      PubSub.subscribe("__keyevent@0__:set", self())

      port = Listener.port()
      sock = connect_and_hello(port)
      send_cmd(sock, ["SET", key, "v"])
      _resp = recv_response(sock)

      refute_received {:pubsub_message, _, _}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # TCP helpers (borrowed from commands_tcp_test.exs)
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
end
