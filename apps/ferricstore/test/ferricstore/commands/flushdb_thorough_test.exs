defmodule Ferricstore.Commands.FlushdbThoroughTest do
  @moduledoc """
  Thorough tests for FLUSHDB/FLUSHALL verifying all storage layers are cleaned:
  ETS keydirs, prefix index, compound keys (H:, T:, S:, Z:), and Bitcask.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  defp shard_count, do: :persistent_term.get(:ferricstore_shard_count, 4)

  defp ets_total_keys do
    Enum.reduce(0..(shard_count() - 1), 0, fn i, acc ->
      try do acc + :ets.info(:"keydir_#{i}", :size) rescue _ -> acc end
    end)
  end

  defp wait_for_ets_count_at_least(count) do
    ShardHelpers.eventually(
      fn -> ets_total_keys() >= count end,
      "expected at least #{count} ETS keys",
      100,
      10
    )
  end

  defp wait_for_empty_ets do
    ShardHelpers.eventually(
      fn -> ets_total_keys() == 0 end,
      "expected empty ETS keydirs",
      100,
      10
    )
  end

  describe "FLUSHDB clears ETS completely" do
    test "string keys removed from ETS" do
      for i <- 1..100, do: Router.put(FerricStore.Instance.get(:default), "flush_str:#{i}", "val", 0)
      wait_for_ets_count_at_least(100)

      FerricStore.flushall()

      wait_for_empty_ets()
    end

    test "hash compound keys (H: and T:) removed from ETS" do
      FerricStore.hset("myhash", %{"f1" => "v1", "f2" => "v2", "f3" => "v3"})

      # Verify compound keys exist
      has_compound = Enum.any?(0..(shard_count() - 1), fn i ->
        try do
          :ets.foldl(fn {key, _, _, _, _, _, _}, acc ->
            if String.starts_with?(key, "H:") or String.starts_with?(key, "T:"), do: acc + 1, else: acc
          end, 0, :"keydir_#{i}") > 0
        rescue
          _ -> false
        end
      end)
      assert has_compound

      FerricStore.flushall()

      wait_for_empty_ets()
    end

    test "set compound keys (S: and T:) removed from ETS" do
      FerricStore.sadd("myset", ["a", "b", "c"])

      FerricStore.flushall()

      wait_for_empty_ets()
    end

    test "sorted set compound keys (Z: and T:) removed from ETS" do
      FerricStore.zadd("myzset", [{1.0, "a"}, {2.0, "b"}])

      FerricStore.flushall()

      wait_for_empty_ets()
    end

    test "list keys removed from ETS" do
      FerricStore.lpush("mylist", ["a", "b", "c"])

      FerricStore.flushall()

      wait_for_empty_ets()
    end

    test "mixed types all removed" do
      Router.put(FerricStore.Instance.get(:default), "str_key", "val", 0)
      FerricStore.hset("hash_key", %{"f" => "v"})
      FerricStore.sadd("set_key", ["m"])
      FerricStore.zadd("zset_key", [{1.0, "m"}])
      FerricStore.lpush("list_key", ["a"])

      assert ets_total_keys() > 5

      FerricStore.flushall()

      wait_for_empty_ets()
    end
  end

  describe "FLUSHDB keys don't resurrect" do
    test "GET after flush returns nil" do
      Router.put(FerricStore.Instance.get(:default), "resurrect:k1", "val", 0)
      Router.put(FerricStore.Instance.get(:default), "resurrect:k2", "val", 0)

      FerricStore.flushall()

      assert Router.get(FerricStore.Instance.get(:default), "resurrect:k1") == nil
      assert Router.get(FerricStore.Instance.get(:default), "resurrect:k2") == nil
    end

    test "HGET after flush returns nil" do
      FerricStore.hset("resurrect:hash", %{"f1" => "v1"})

      FerricStore.flushall()

      assert {:ok, nil} = FerricStore.hget("resurrect:hash", "f1")
    end

    test "DBSIZE after flush is 0" do
      for i <- 1..50, do: Router.put(FerricStore.Instance.get(:default), "dbsize:#{i}", "v", 0)

      FerricStore.flushall()

      assert Router.dbsize(FerricStore.Instance.get(:default)) == 0
    end

    test "KEYS after flush returns empty" do
      for i <- 1..20, do: Router.put(FerricStore.Instance.get(:default), "keys_test:#{i}", "v", 0)

      FerricStore.flushall()

      assert Router.keys(FerricStore.Instance.get(:default)) == []
    end
  end

  if Code.ensure_loaded?(FerricstoreServer.Listener) do
    describe "FLUSHDB via TCP" do
      @tag :skip
      # TCP listener (ranch) is not started in embedded mode
      test "FLUSHDB over TCP clears all keys" do
        port = FerricstoreServer.Listener.port()

        for i <- 1..50, do: Router.put(FerricStore.Instance.get(:default), "tcp_flush:#{i}", "val", 0)
        FerricStore.hset("tcp_flush:hash", %{"f" => "v"})

        assert ets_total_keys() > 50

        {:ok, sock} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])
        :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
        {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

        :gen_tcp.send(sock, "*1\r\n$7\r\nFLUSHDB\r\n")
        {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

        :gen_tcp.close(sock)

        wait_for_empty_ets()
        assert Router.dbsize(FerricStore.Instance.get(:default)) == 0
      end
    end
  end

  describe "double flush is idempotent" do
    test "flush twice doesn't crash" do
      Router.put(FerricStore.Instance.get(:default), "double:k", "v", 0)

      FerricStore.flushall()
      FerricStore.flushall()

      wait_for_empty_ets()
    end
  end
end
