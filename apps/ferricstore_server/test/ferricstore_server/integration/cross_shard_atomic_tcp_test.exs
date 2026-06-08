defmodule FerricstoreServer.Integration.CrossShardAtomicTcpTest do
  @moduledoc """
  TCP-level integration tests for cross-shard atomic MULTI/EXEC transactions.

  These tests verify the NEW behavior over real TCP sockets with RESP3:
  cross-shard MULTI/EXEC should succeed atomically (not return CROSSSLOT).
  The anchor shard approach submits a single Raft log entry containing
  commands for ALL involved shards. One Raft entry = one fsync = atomic.

  All key-to-shard mappings are discovered dynamically via ShardHelpers so
  tests work with any shard count (not just 4).

  RESP3 encoding note: simple strings like "OK" and "QUEUED" are returned
  as `{:simple, "OK"}` and `{:simple, "QUEUED"}` by the parser. Inside
  EXEC result arrays, SET responses are also `{:simple, "OK"}`.
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag :global_state
  @moduletag timeout: 60_000

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

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

  # RESP3 simple string helpers
  defp ok, do: {:simple, "OK"}
  defp queued, do: {:simple, "QUEUED"}

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup %{port: port} do
    [k0, k1, k2, k3] = ShardHelpers.keys_on_different_shards(4)

    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    recv_response(sock)

    on_exit(fn ->
      :gen_tcp.close(sock)
      ShardHelpers.flush_all_keys()
    end)

    %{sock: sock, port: port, k0: k0, k1: k1, k2: k2, k3: k3}
  end

  # ---------------------------------------------------------------------------
  # Basic TCP cross-shard
  # ---------------------------------------------------------------------------

  describe "basic cross-shard MULTI/EXEC succeeds atomically" do
    test "SET on 2 different shards — returns [OK, OK], both readable", %{
      sock: sock,
      k0: k0,
      k1: k1
    } do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", k0, "cross_val_0"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["SET", k1, "cross_val_1"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok()]

      # Both keys should be readable
      send_cmd(sock, ["GET", k0])
      assert recv_response(sock) == "cross_val_0"

      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == "cross_val_1"
    end

    test "MULTI with 4 keys across all shards, EXEC — all written", %{
      sock: sock,
      k0: k0,
      k1: k1,
      k2: k2,
      k3: k3
    } do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", k0, "s0"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", k1, "s1"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", k2, "s2"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", k3, "s3"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok(), ok(), ok()]

      send_cmd(sock, ["GET", k0])
      assert recv_response(sock) == "s0"
      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == "s1"
      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == "s2"
      send_cmd(sock, ["GET", k3])
      assert recv_response(sock) == "s3"
    end

    test "single MSET inside MULTI writes every key to its owning shard", %{
      sock: sock,
      k0: k0,
      k1: k1
    } do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MSET", k0, "mset_0", k1, "mset_1"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      assert recv_response(sock) == [ok()]

      send_cmd(sock, ["GET", k0])
      assert recv_response(sock) == "mset_0"

      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == "mset_1"
    end

    test "single MGET inside MULTI reads every key from its owning shard", %{
      sock: sock,
      k0: k0,
      k1: k1
    } do
      send_cmd(sock, ["SET", k0, "mget_0"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", k1, "mget_1"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MGET", k0, k1])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      assert recv_response(sock) == [["mget_0", "mget_1"]]
    end

    test "multi-key write is visible to later command on lower shard", %{
      sock: sock,
      k0: k0,
      k1: k1
    } do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MSET", k1, "ordered_1", k0, "ordered_0"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["GET", k0])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      assert recv_response(sock) == [ok(), "ordered_0"]
    end

    test "interleaved multi-key AST commands execute in original transaction order", %{
      sock: sock,
      k0: k0,
      k1: k1
    } do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MSET", k1, "first_1", k0, "first_0"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["SET", k0, "second_0"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["MGET", k1, k0])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      assert recv_response(sock) == [ok(), ok(), ["first_1", "second_0"]]
    end

    test "MULTI with GET + SET across shards, EXEC — results in order", %{
      sock: sock,
      k0: k0,
      k1: k1,
      k2: k2,
      k3: k3
    } do
      # Pre-set values on different shards
      send_cmd(sock, ["SET", k0, "existing_k0"])
      assert recv_response(sock) == ok()
      send_cmd(sock, ["SET", k2, "existing_k2"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", k0])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", k1, "new_k1"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", k3, "new_k3"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == ["existing_k0", ok(), "existing_k2", ok()]
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH over TCP with cross-shard tx
  # ---------------------------------------------------------------------------

  describe "WATCH with cross-shard tx over TCP" do
    test "WATCH conflict aborts cross-shard EXEC — NO writes on either shard", %{
      sock: sock,
      port: port,
      k0: k0,
      k1: k1
    } do
      # Pre-set values on two shards
      send_cmd(sock, ["SET", k0, "original_k0"])
      assert recv_response(sock) == ok()
      send_cmd(sock, ["SET", k1, "original_k1"])
      assert recv_response(sock) == ok()

      # WATCH key on first shard
      send_cmd(sock, ["WATCH", k0])
      assert recv_response(sock) == ok()

      # Queue cross-shard commands
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", k0, "from_tx_k0"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", k1, "from_tx_k1"])
      assert recv_response(sock) == queued()

      # Another connection modifies the watched key
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k0, "modified_by_other"])
      assert recv_response(sock2) == ok()
      :gen_tcp.close(sock2)

      # EXEC should return nil (aborted)
      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == nil

      # Neither shard should have transaction writes
      send_cmd(sock, ["GET", k0])
      assert recv_response(sock) == "modified_by_other"

      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == "original_k1"
    end
  end

  # ---------------------------------------------------------------------------
  # Recovery after cross-shard tx
  # ---------------------------------------------------------------------------

  describe "recovery after cross-shard tx" do
    test "after successful cross-shard EXEC, connection is back to normal mode", %{
      sock: sock,
      k0: k0,
      k1: k1,
      k2: k2
    } do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", k0, "tx_k0"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", k1, "tx_k1"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok()]

      # Connection should be back to normal — can issue regular commands
      send_cmd(sock, ["SET", k0, "after_tx"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", k0])
      assert recv_response(sock) == "after_tx"

      # Different shard also works
      send_cmd(sock, ["SET", k2, "normal_mode"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == "normal_mode"
    end
  end

  # ---------------------------------------------------------------------------
  # Hash tags still use single-shard fast path
  # ---------------------------------------------------------------------------

  describe "hash tags still use single-shard fast path" do
    test "keys with same hash tag execute as single-shard (fast path)", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", "{user:42}:name", "Alice"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", "{user:42}:email", "alice@example.com"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok()]

      send_cmd(sock, ["GET", "{user:42}:name"])
      assert recv_response(sock) == "Alice"
      send_cmd(sock, ["GET", "{user:42}:email"])
      assert recv_response(sock) == "alice@example.com"
    end
  end
end
