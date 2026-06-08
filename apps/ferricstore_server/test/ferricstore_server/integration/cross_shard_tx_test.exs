defmodule FerricstoreServer.Integration.CrossShardTxTest do
  @moduledoc """
  TCP-level integration tests for cross-shard MULTI/EXEC transactions.

  Cross-shard transactions now execute atomically via the anchor-shard Raft
  approach. A single Raft log entry is submitted to the anchor shard containing
  commands for ALL involved shards.

  All key-to-shard mappings are discovered dynamically via ShardHelpers so
  tests work with any shard count (not just 4).

  RESP3 encoding note: simple strings like "OK" and "QUEUED" are returned
  as `{:simple, "OK"}` and `{:simple, "QUEUED"}` by the parser. Inside
  EXEC result arrays, SET responses are also `{:simple, "OK"}`.
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag :global_state
  @moduletag :shard_kill

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

  defp recv_response(sock, buf, timeout \\ 15_000) do
    {:ok, data} = :gen_tcp.recv(sock, 0, timeout)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2, timeout)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # RESP3 simple string helper
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
    {same1, same2} = ShardHelpers.keys_on_same_shard()

    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    # FLUSHDB can take >15s in the full suite with 16 shards and many accumulated keys.
    recv_response(sock, "", 60_000)

    on_exit(fn ->
      :gen_tcp.close(sock)
      ShardHelpers.flush_all_keys()
    end)

    %{
      sock: sock,
      port: port,
      k0: k0,
      k1: k1,
      k2: k2,
      k3: k3,
      same1: same1,
      same2: same2
    }
  end

  # ---------------------------------------------------------------------------
  # Cross-shard MULTI/EXEC succeeds atomically
  # ---------------------------------------------------------------------------

  describe "cross-shard MULTI/EXEC succeeds atomically" do
    test "SET keys on different shards succeeds", %{sock: sock, k0: k0, k1: k1} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", k0, "cross_val_0"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["SET", k1, "cross_val_1"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok()]

      send_cmd(sock, ["GET", k0])
      assert recv_response(sock) == "cross_val_0"

      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == "cross_val_1"
    end

    test "SET keys across all four shards succeeds", %{
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

    test "mixed reads and writes across shards succeeds", %{
      sock: sock,
      k0: k0,
      k1: k1,
      k2: k2
    } do
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

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == ["existing_k0", ok(), "existing_k2"]
    end

    test "INCR on keys across different shards succeeds", %{sock: sock, k0: k0, k1: k1} do
      send_cmd(sock, ["SET", k0, "10"])
      assert recv_response(sock) == ok()
      send_cmd(sock, ["SET", k1, "20"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["INCR", k0])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["INCR", k1])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [11, 21]

      send_cmd(sock, ["GET", k0])
      assert recv_response(sock) == "11"
      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == "21"
    end

    test "connection returns to normal state after cross-shard tx", %{
      sock: sock,
      k0: k0,
      k1: k1
    } do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", k0, "v0"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", k1, "v1"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert result == [ok(), ok()]

      # Connection should be back to normal — can issue commands
      send_cmd(sock, ["SET", k0, "after_tx"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", k0])
      assert recv_response(sock) == "after_tx"
    end
  end

  # ---------------------------------------------------------------------------
  # Hash tags co-locate keys on same shard (fast path)
  # ---------------------------------------------------------------------------

  describe "hash tags use single-shard fast path" do
    test "keys with same hash tag execute atomically", %{sock: sock} do
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

  # ---------------------------------------------------------------------------
  # Single-shard MULTI/EXEC still works atomically
  # ---------------------------------------------------------------------------

  describe "single-shard transaction still works" do
    test "commands all on same shard use fast path", %{sock: sock, same1: s1, same2: s2} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", s1, "fast_s1"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", s2, "fast_s2"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["GET", s1])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == [ok(), ok(), "fast_s1"]
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH still works with single-shard transactions
  # ---------------------------------------------------------------------------

  describe "WATCH with single-shard transaction" do
    test "WATCH conflict aborts single-shard transaction", %{
      sock: sock,
      port: port,
      same1: s1,
      same2: s2
    } do
      send_cmd(sock, ["SET", s1, "original"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["WATCH", s1])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", s1, "from_tx"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", s2, "from_tx_s2"])
      assert recv_response(sock) == queued()

      # Another connection modifies watched key
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", s1, "modified_by_other"])
      assert recv_response(sock2) == ok()
      :gen_tcp.close(sock2)

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == nil

      send_cmd(sock, ["GET", s1])
      assert recv_response(sock) == "modified_by_other"

      # s2 should not be modified (transaction aborted)
      send_cmd(sock, ["GET", s2])
      assert recv_response(sock) == nil
    end
  end

  # ---------------------------------------------------------------------------
  # DISCARD works normally
  # ---------------------------------------------------------------------------

  describe "DISCARD works normally" do
    test "DISCARD clears queue", %{sock: sock, same1: s1, same2: s2} do
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["SET", s1, "will_discard"])
      assert recv_response(sock) == queued()
      send_cmd(sock, ["SET", s2, "will_discard"])
      assert recv_response(sock) == queued()

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == ok()

      send_cmd(sock, ["GET", s1])
      assert recv_response(sock) == nil
      send_cmd(sock, ["GET", s2])
      assert recv_response(sock) == nil
    end
  end
end
