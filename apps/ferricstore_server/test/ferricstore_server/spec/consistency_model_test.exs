defmodule FerricstoreServer.Spec.ConsistencyModelTest do
  @moduledoc """
  Spec section 5: Consistency Model tests.

  Verifies:
    - Section 5.1: Single-shard transactions are atomic (MULTI/EXEC within one shard)
    - Section 5.2: WATCH detects cross-connection modifications
    - Section 5.3: All shard-local operations are serialized via the GenServer

  Note: INCR/APPEND inside MULTI/EXEC is a known limitation (the Encoder does
  not yet handle {:ok, N} tuples returned by those commands). These tests use
  SET/GET/DEL within transactions and test INCR serialization via the Router
  API directly.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener
  alias Ferricstore.Store.Router

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

  # Hash tag ensures all generated keys co-locate on the same shard.
  defp ukey(name), do: "{spec5}:#{name}_#{:rand.uniform(999_999)}"

  # Route two keys to the same shard by brute-forcing suffixes.
  # Returns two keys guaranteed to hash to the same shard index.
  defp same_shard_keys(prefix1, prefix2) do
    base_key1 = ukey(prefix1)
    target_shard = Router.shard_for(FerricStore.Instance.get(:default), base_key1)

    base_key2 =
      Enum.find(0..10_000, fn i ->
        candidate = ukey("#{prefix2}_#{i}")
        Router.shard_for(FerricStore.Instance.get(:default), candidate) == target_shard
      end)

    {base_key1, ukey("#{prefix2}_#{base_key2}")}
  end

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Section 5.1: Single-shard transactions are atomic
  # ---------------------------------------------------------------------------

  describe "single-shard transaction atomicity (spec 5.1)" do
    test "MULTI/EXEC within one shard executes all commands atomically", %{port: port} do
      {k1, k2} = same_shard_keys("atom_a", "atom_b")

      sock = connect_and_hello(port)

      # Transaction: SET both keys atomically
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k1, "val_a"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["SET", k2, "val_b"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert is_list(result)
      assert length(result) == 4
      assert Enum.at(result, 0) == {:simple, "OK"}
      assert Enum.at(result, 1) == {:simple, "OK"}
      assert Enum.at(result, 2) == "val_a"
      assert Enum.at(result, 3) == "val_b"

      :gen_tcp.close(sock)
    end

    test "MULTI/EXEC results reflect sequential in-order execution", %{port: port} do
      k = ukey("order_exec")

      sock = connect_and_hello(port)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      # SET, overwrite SET, GET -- results must reflect sequential execution
      send_cmd(sock, ["SET", k, "first"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["SET", k, "second"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["SET", k, "third"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert is_list(result)
      assert length(result) == 5
      assert Enum.at(result, 0) == {:simple, "OK"}
      assert Enum.at(result, 1) == {:simple, "OK"}
      assert Enum.at(result, 2) == "second"
      assert Enum.at(result, 3) == {:simple, "OK"}
      assert Enum.at(result, 4) == "third"

      :gen_tcp.close(sock)
    end

    test "aborted transaction does not partially apply commands", %{port: port} do
      k1 = ukey("partial_a")
      k2 = ukey("partial_b")

      sock1 = connect_and_hello(port)

      # Set initial values
      send_cmd(sock1, ["SET", k1, "original_1"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["SET", k2, "original_2"])
      assert recv_response(sock1) == {:simple, "OK"}

      # WATCH k1
      send_cmd(sock1, ["WATCH", k1])
      assert recv_response(sock1) == {:simple, "OK"}

      # Modify k1 from another connection to cause abort
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k1, "conflict"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      # Queue changes to BOTH keys
      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["SET", k1, "txn_1"])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["SET", k2, "txn_2"])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      # EXEC should abort -- neither command should have applied
      send_cmd(sock1, ["EXEC"])
      assert recv_response(sock1) == nil

      # k1 should still have the conflict value, k2 should be unchanged
      send_cmd(sock1, ["GET", k1])
      assert recv_response(sock1) == "conflict"

      send_cmd(sock1, ["GET", k2])
      assert recv_response(sock1) == "original_2"

      :gen_tcp.close(sock1)
    end

    test "DEL within transaction is executed atomically", %{port: port} do
      k1 = ukey("txn_del_a")
      k2 = ukey("txn_del_b")

      sock = connect_and_hello(port)

      # Setup
      send_cmd(sock, ["SET", k1, "exists_1"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k2, "exists_2"])
      assert recv_response(sock) == {:simple, "OK"}

      # Transaction: DEL k1, check EXISTS, SET k2 to new value
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DEL", k1])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXISTS", k1])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["SET", k2, "updated"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert is_list(result)
      assert length(result) == 3
      # DEL returned 1
      assert Enum.at(result, 0) == 1
      # EXISTS returns 0 (deleted)
      assert Enum.at(result, 1) == 0
      assert Enum.at(result, 2) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Section 5.2: WATCH detects cross-connection modifications
  # ---------------------------------------------------------------------------

  describe "WATCH detects cross-connection modifications (spec 5.2)" do
    test "WATCH detects SET from another connection", %{port: port} do
      k = ukey("watch_set")

      sock1 = connect_and_hello(port)
      send_cmd(sock1, ["SET", k, "v1"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["WATCH", k])
      assert recv_response(sock1) == {:simple, "OK"}

      # Another connection modifies the key
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "v2"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["GET", k])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      assert recv_response(sock1) == nil

      :gen_tcp.close(sock1)
    end

    test "WATCH detects DEL from another connection", %{port: port} do
      k = ukey("watch_del")

      sock1 = connect_and_hello(port)
      send_cmd(sock1, ["SET", k, "exists"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["WATCH", k])
      assert recv_response(sock1) == {:simple, "OK"}

      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["DEL", k])
      assert recv_response(sock2) == 1
      :gen_tcp.close(sock2)

      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["SET", k, "should_not_apply"])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      assert recv_response(sock1) == nil

      :gen_tcp.close(sock1)
    end

    test "WATCH on non-existent key detects creation by another connection", %{port: port} do
      k = ukey("watch_create")

      sock1 = connect_and_hello(port)

      # WATCH a key that does not exist
      send_cmd(sock1, ["WATCH", k])
      assert recv_response(sock1) == {:simple, "OK"}

      # Another connection creates the key
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "created"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["GET", k])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      assert recv_response(sock1) == nil

      :gen_tcp.close(sock1)
    end

    test "WATCH with no modification allows EXEC to succeed", %{port: port} do
      k = ukey("watch_clean")

      sock = connect_and_hello(port)
      send_cmd(sock, ["SET", k, "stable"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)
      assert Enum.at(result, 0) == "stable"

      :gen_tcp.close(sock)
    end

    test "WATCH detects modification via write_version on the shard" do
      # This tests at the Router level that any write bumps the shard version
      k = ukey("watch_ver")

      Router.put(FerricStore.Instance.get(:default), k, "initial", 0)
      v_before = Router.get_version(FerricStore.Instance.get(:default), k)

      Router.put(FerricStore.Instance.get(:default), k, "modified", 0)
      v_after = Router.get_version(FerricStore.Instance.get(:default), k)

      assert v_after > v_before,
             "write_version should increase after a write"

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # Section 5.3: Shard-local operation serialization
  # ---------------------------------------------------------------------------

  describe "shard-local operation serialization (spec 5.3)" do
    test "concurrent INCR operations on the same key are serialized via Router" do
      k = ukey("serial_incr")

      # Set initial value directly via Router
      Router.put(FerricStore.Instance.get(:default), k, "0", 0)

      # Run concurrent INCRs via Router (bypasses TCP / Encoder issue)
      concurrency = 5
      incrs_per_task = 20

      tasks =
        for _ <- 1..concurrency do
          Task.async(fn ->
            for _ <- 1..incrs_per_task do
              Router.incr(FerricStore.Instance.get(:default), k, 1)
            end
          end)
        end

      Task.await_many(tasks, 15_000)

      result = Router.get(FerricStore.Instance.get(:default), k)
      assert result == Integer.to_string(concurrency * incrs_per_task)

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), k)
    end

    test "concurrent SET and GET are consistent (no torn reads)", %{port: port} do
      k = ukey("serial_rw")

      sock_setup = connect_and_hello(port)
      send_cmd(sock_setup, ["SET", k, "initial"])
      assert recv_response(sock_setup) == {:simple, "OK"}
      :gen_tcp.close(sock_setup)

      # Writer task: SET the key repeatedly
      writer =
        Task.async(fn ->
          sock = connect_and_hello(port)

          for i <- 1..50 do
            send_cmd(sock, ["SET", k, "value_#{i}"])
            recv_response(sock)
          end

          :gen_tcp.close(sock)
        end)

      # Reader task: GET the key repeatedly
      reader =
        Task.async(fn ->
          sock = connect_and_hello(port)

          results =
            for _ <- 1..50 do
              send_cmd(sock, ["GET", k])
              recv_response(sock)
            end

          :gen_tcp.close(sock)
          results
        end)

      Task.await(writer, 10_000)
      results = Task.await(reader, 10_000)

      # Every read should return either "initial" or a complete "value_N" string
      Enum.each(results, fn val ->
        assert is_binary(val),
               "Expected a binary value from GET, got: #{inspect(val)}"

        assert val == "initial" or String.starts_with?(val, "value_"),
               "Got unexpected value: #{inspect(val)}"
      end)
    end

    test "write_version increments on every write to the same shard" do
      k1 = ukey("ver_a")

      v1_before = Router.get_version(FerricStore.Instance.get(:default), k1)
      Router.put(FerricStore.Instance.get(:default), k1, "a", 0)
      v1_after = Router.get_version(FerricStore.Instance.get(:default), k1)

      assert v1_after > v1_before

      # Another write to the same shard (even a different key) bumps version
      target_shard = Router.shard_for(FerricStore.Instance.get(:default), k1)

      # Find a key that hashes to the same shard
      same_shard_idx =
        Enum.find(0..10_000, fn i ->
          candidate = "ver_check_#{i}"
          Router.shard_for(FerricStore.Instance.get(:default), candidate) == target_shard
        end)

      same_shard_key = "ver_check_#{same_shard_idx}"
      v_before = Router.get_version(FerricStore.Instance.get(:default), same_shard_key)
      Router.put(FerricStore.Instance.get(:default), same_shard_key, "b", 0)
      v_after = Router.get_version(FerricStore.Instance.get(:default), same_shard_key)

      assert v_after > v_before

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), k1)
      Router.delete(FerricStore.Instance.get(:default), same_shard_key)
    end

    test "delete also increments write_version" do
      k = ukey("ver_del")

      Router.put(FerricStore.Instance.get(:default), k, "value", 0)
      v_before = Router.get_version(FerricStore.Instance.get(:default), k)

      Router.delete(FerricStore.Instance.get(:default), k)
      v_after = Router.get_version(FerricStore.Instance.get(:default), k)

      assert v_after > v_before
    end
  end
end
