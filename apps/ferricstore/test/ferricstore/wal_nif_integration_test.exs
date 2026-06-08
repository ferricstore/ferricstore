defmodule Ferricstore.WalNifIntegrationTest do
  @moduledoc """
  Integration tests for the Rust NIF WAL module used by WARaft segments.
  Tests that normal Router writes flow through the replicated storage path and
  remain readable.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  defp ukey(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"
  defp ctx, do: FerricStore.Instance.get(:default)

  # ---------------------------------------------------------------------------
  # Replicated write pipeline
  # ---------------------------------------------------------------------------

  describe "WARaft segment writes" do
    test "write and read back through Raft" do
      key = ukey("ttb_rw")
      :ok = Router.put(ctx(), key, "hello_ttb", 0)
      assert Router.get(ctx(), key) == "hello_ttb"
    end

    test "batch write through Batcher" do
      keys = for i <- 1..20, do: {ukey("ttb_batch_#{i}"), "val_#{i}"}

      for {key, val} <- keys do
        :ok = Router.put(ctx(), key, val, 0)
      end

      for {key, val} <- keys do
        assert Router.get(ctx(), key) == val
      end
    end

    test "delete through Raft with {ttb}" do
      key = ukey("ttb_del")
      :ok = Router.put(ctx(), key, "to_delete", 0)
      assert Router.get(ctx(), key) == "to_delete"

      :ok = Router.delete(ctx(), key)
      assert Router.get(ctx(), key) == nil
    end

    test "overwrite preserves latest value" do
      key = ukey("ttb_overwrite")
      :ok = Router.put(ctx(), key, "first", 0)
      :ok = Router.put(ctx(), key, "second", 0)
      :ok = Router.put(ctx(), key, "third", 0)
      assert Router.get(ctx(), key) == "third"
    end
  end

  # ---------------------------------------------------------------------------
  # NIF WAL durability
  # ---------------------------------------------------------------------------

  describe "NIF WAL durability" do
    test "many sequential writes are all durable" do
      keys =
        for i <- 1..100 do
          key = ukey("wal_seq_#{i}")
          val = "seq_#{i}"
          :ok = Router.put(ctx(), key, val, 0)
          {key, val}
        end

      for {key, expected} <- keys do
        assert Router.get(ctx(), key) == expected, "key #{key} mismatch"
      end
    end

    test "write with TTL is readable before expiry" do
      key = ukey("wal_ttl")
      expire_at = System.os_time(:millisecond) + 5_000
      :ok = Router.put(ctx(), key, "ttl_value", expire_at)
      assert Router.get(ctx(), key) == "ttl_value"
    end

    test "large value write and read" do
      key = ukey("wal_large")
      large_val = String.duplicate("X", 100_000)
      :ok = Router.put(ctx(), key, large_val, 0)
      assert Router.get(ctx(), key) == large_val
    end

    test "binary value with all byte values" do
      key = ukey("wal_binary")
      binary_val = :binary.list_to_bin(Enum.to_list(0..255))
      :ok = Router.put(ctx(), key, binary_val, 0)
      assert Router.get(ctx(), key) == binary_val
    end

    test "empty string value" do
      key = ukey("wal_empty")
      :ok = Router.put(ctx(), key, "", 0)
      assert Router.get(ctx(), key) == ""
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent writes
  # ---------------------------------------------------------------------------

  describe "concurrent writes" do
    test "parallel writes from multiple tasks" do
      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            key = "concurrent:#{i}:#{System.unique_integer([:positive])}"
            :ok = Router.put(ctx(), key, "task_#{i}", 0)
            {key, "task_#{i}"}
          end)
        end

      results = Task.await_many(tasks, 10_000)

      for {key, expected} <- results do
        assert Router.get(ctx(), key) == expected, "concurrent key #{key} missing"
      end
    end

    test "rapid writes to same key" do
      key = ukey("wal_race")

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            Router.put(ctx(), key, "writer_#{i}", 0)
          end)
        end

      Task.await_many(tasks, 10_000)

      val = Router.get(ctx(), key)
      assert val != nil
      assert String.starts_with?(val, "writer_")
    end
  end

  # ---------------------------------------------------------------------------
  # Hash tags
  # ---------------------------------------------------------------------------

  describe "hash tag routing through NIF WAL" do
    test "hash-tagged keys on same shard" do
      key1 = "{user:42}:session:#{System.unique_integer([:positive])}"
      key2 = "{user:42}:profile:#{System.unique_integer([:positive])}"

      :ok = Router.put(ctx(), key1, "session", 0)
      :ok = Router.put(ctx(), key2, "profile", 0)

      assert Router.get(ctx(), key1) == "session"
      assert Router.get(ctx(), key2) == "profile"
      assert Router.shard_for(ctx(), key1) == Router.shard_for(ctx(), key2)
    end
  end

  # ---------------------------------------------------------------------------
  # NIF module is active
  # ---------------------------------------------------------------------------

  describe "NIF WAL activation" do
    test "ferricstore_wal_nif module is loaded" do
      assert Code.ensure_loaded?(:ferricstore_wal_nif)
    end

    test "raw append handle is exported and usable for segment logs" do
      path = Path.join(System.tmp_dir!(), "ferricstore-wal-raw-#{System.unique_integer([:positive])}.log")
      File.rm(path)

      try do
        assert {:ok, handle} = :ferricstore_wal_nif.open_raw_append(path, 0, 1024 * 1024, 0)
        assert :ok = :ferricstore_wal_nif.write(handle, ["abc", "def"])

        ref = make_ref()
        assert :ok = :ferricstore_wal_nif.sync_with_delay(handle, self(), ref, 0)
        assert_receive {synced, ^ref, position} when synced in [:wal_sync_complete, :wal_sync_error], 5_000
        assert synced == :wal_sync_complete
        assert position >= 6
        assert {:ok, 6} = :ferricstore_wal_nif.position(handle)
        assert {:ok, "abcdef"} = :ferricstore_wal_nif.pread(handle, 0, 6)
        assert :ok = :ferricstore_wal_nif.close(handle)
      after
        File.rm(path)
      end
    end

    test "writes work (proves NIF is active)" do
      key = ukey("wal_active")
      :ok = Router.put(ctx(), key, "nif_active", 0)
      assert Router.get(ctx(), key) == "nif_active"
    end
  end

  # ---------------------------------------------------------------------------
  # Error handling
  # ---------------------------------------------------------------------------

  describe "error handling" do
    test "oversized key rejected" do
      big_key = String.duplicate("k", 65_537)
      assert {:error, _} = Router.put(ctx(), big_key, "val", 0)
    end

  end

  # ---------------------------------------------------------------------------
  # Mixed operations
  # ---------------------------------------------------------------------------

  describe "mixed operations" do
    test "interleaved writes and reads" do
      for i <- 1..50 do
        key = ukey("wal_mixed_#{i}")
        val = "val_#{i}"
        :ok = Router.put(ctx(), key, val, 0)
        assert Router.get(ctx(), key) == val
      end
    end

    test "write, delete, write cycle" do
      key = ukey("wal_cycle")

      :ok = Router.put(ctx(), key, "first", 0)
      assert Router.get(ctx(), key) == "first"

      :ok = Router.delete(ctx(), key)
      assert Router.get(ctx(), key) == nil

      :ok = Router.put(ctx(), key, "revived", 0)
      assert Router.get(ctx(), key) == "revived"
    end

    test "INCR through NIF WAL" do
      key = ukey("wal_incr")
      :ok = Router.put(ctx(), key, "0", 0)

      for _ <- 1..10 do
        Router.incr(ctx(), key, 1)
      end

      val = Router.get(ctx(), key)
      assert String.to_integer(val) == 10
    end
  end
end
