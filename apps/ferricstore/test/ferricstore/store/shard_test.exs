defmodule Ferricstore.Store.ShardTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.{CompoundKey, DiskPressure, TypeRegistry}
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    pid = Process.whereis(elem(ctx.shard_names, 0))
    keydir = elem(ctx.keydir_refs, 0)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    %{shard: pid, index: 0, keydir: keydir, ctx: ctx}
  end

  describe "put and get" do
    test "put and get returns value", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "key", "value", 0})
      assert "value" == GenServer.call(shard, {:get, "key"})
    end

    test "get on missing key returns nil", %{shard: shard} do
      assert nil == GenServer.call(shard, {:get, "missing"})
    end

    test "put overwrites previous value", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "key", "v1", 0})
      :ok = GenServer.call(shard, {:put, "key", "v2", 0})
      assert "v2" == GenServer.call(shard, {:get, "key"})
    end

    test "put with expire_at_ms 0 never expires", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "key", "value", 0})
      assert "value" == GenServer.call(shard, {:get, "key"})
    end

    test "put with past expire_at_ms returns nil on get", %{shard: shard} do
      past = System.os_time(:millisecond) - 1000
      :ok = GenServer.call(shard, {:put, "key", "value", past})
      assert nil == GenServer.call(shard, {:get, "key"})
    end

    test "put with future expire_at_ms returns value", %{shard: shard} do
      future = System.os_time(:millisecond) + 60_000
      :ok = GenServer.call(shard, {:put, "key", "value", future})
      assert "value" == GenServer.call(shard, {:get, "key"})
    end
  end

  describe "disk pressure" do
    test "frag_check does not clear pressure without a successful disk write", %{
      shard: shard,
      ctx: ctx
    } do
      DiskPressure.set(ctx, 0)

      send(shard, :frag_check)
      :sys.get_state(shard)

      assert DiskPressure.under_pressure?(ctx, 0)
    after
      DiskPressure.clear(ctx, 0)
    end
  end

  describe "flush" do
    test "returns an error when pending writes cannot be persisted", %{shard: shard} do
      missing_dir =
        Path.join(System.tmp_dir!(), "missing_flush_dir_#{System.unique_integer([:positive])}")

      missing_path = Path.join(missing_dir, "00000.log")

      :sys.replace_state(shard, fn state ->
        %{
          state
          | active_file_path: missing_path,
            pending: [{"flush_fail", "value", 0}],
            pending_count: 1
        }
      end)

      assert {:error, {:flush_failed, _reason}} = GenServer.call(shard, :flush)

      state = :sys.get_state(shard)
      assert state.pending_count == 1
      assert state.pending != []
    end
  end

  describe "delete" do
    test "delete existing key returns :ok", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "key", "value", 0})
      assert :ok == GenServer.call(shard, {:delete, "key"})
    end

    test "delete missing key returns :ok (idempotent)", %{shard: shard} do
      assert :ok == GenServer.call(shard, {:delete, "nonexistent"})
    end

    test "get after delete returns nil", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "key", "value", 0})
      :ok = GenServer.call(shard, {:delete, "key"})
      assert nil == GenServer.call(shard, {:get, "key"})
    end
  end

  describe "exists?" do
    test "true for live key", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "key", "value", 0})
      assert true == GenServer.call(shard, {:exists, "key"})
    end

    test "false for missing key", %{shard: shard} do
      assert false == GenServer.call(shard, {:exists, "nokey"})
    end

    test "false for expired key", %{shard: shard} do
      past = System.os_time(:millisecond) - 1000
      :ok = GenServer.call(shard, {:put, "key", "value", past})
      assert false == GenServer.call(shard, {:exists, "key"})
    end
  end

  describe "compound type claims" do
    test "concurrent incompatible first claims cannot replace each other", %{
      ctx: ctx,
      keydir: keydir
    } do
      key = "compound-type-race"

      results =
        [:hash, :set]
        |> Enum.map(fn type ->
          Task.async(fn -> TypeRegistry.check_or_set_status(key, type, ctx) end)
        end)
        |> Task.await_many(5_000)

      assert Enum.count(results, &(&1 == {:ok, :created})) == 1

      assert Enum.count(results, fn
               {:error, message} when is_binary(message) -> String.contains?(message, "WRONGTYPE")
               _other -> false
             end) == 1

      type_key = CompoundKey.type_key(key)
      assert [{^type_key, type, 0, _lfu, _fid, _offset, _size}] = :ets.lookup(keydir, type_key)
      assert type in ["hash", "set"]
    end
  end

  describe "keys" do
    test "returns all live keys", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "a", "1", 0})
      :ok = GenServer.call(shard, {:put, "b", "2", 0})
      keys = GenServer.call(shard, :keys)
      assert Enum.sort(keys) == ["a", "b"]
    end

    test "does not include deleted keys", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "a", "1", 0})
      :ok = GenServer.call(shard, {:put, "b", "2", 0})
      :ok = GenServer.call(shard, {:delete, "a"})
      keys = GenServer.call(shard, :keys)
      assert keys == ["b"]
    end

    test "does not include expired keys", %{shard: shard} do
      past = System.os_time(:millisecond) - 1000
      :ok = GenServer.call(shard, {:put, "expired", "v", past})
      :ok = GenServer.call(shard, {:put, "live", "v", 0})
      keys = GenServer.call(shard, :keys)
      assert keys == ["live"]
    end
  end

  describe "ETS hot cache" do
    test "put immediately populates ETS", %{shard: shard, keydir: keydir} do
      :ok = GenServer.call(shard, {:put, "cached_key", "val", 0})
      # Single-table format: {key, value, expire_at_ms, lfu_counter}
      assert [{"cached_key", "val", 0, _lfu, _fid, _off, _vsize}] =
               :ets.lookup(keydir, "cached_key")
    end

    @tag :shard_kill
    test "get warms ETS on cold key", %{shard: shard, keydir: keydir} do
      :ok = GenServer.call(shard, {:put, "warm_key", "warm_val", 0})
      :ok = GenServer.call(shard, :flush)

      [{_, _, _, _, fid, _, _}] = :ets.lookup(keydir, "warm_key")
      assert is_integer(fid) and fid >= 0, "file_id should be real, got #{inspect(fid)}"

      # Simulate cold key: set value to nil, preserve disk location
      [{_, _val, exp, lfu, fid, off, vsize}] = :ets.lookup(keydir, "warm_key")
      :ets.insert(keydir, {"warm_key", nil, exp, lfu, fid, off, vsize})
      # Now get — should fetch from Bitcask and warm ETS
      assert "warm_val" == GenServer.call(shard, {:get, "warm_key"})

      assert [{"warm_key", "warm_val", 0, _lfu, _fid, _off, _vsize}] =
               :ets.lookup(keydir, "warm_key")
    end

    test "delete evicts ETS entry", %{shard: shard, keydir: keydir} do
      :ok = GenServer.call(shard, {:put, "evict_key", "val", 0})
      :ok = GenServer.call(shard, {:delete, "evict_key"})
      assert [] == :ets.lookup(keydir, "evict_key")
    end

    test "expired key is evicted from ETS on read", %{shard: shard, keydir: keydir} do
      past = System.os_time(:millisecond) - 1000
      :ok = GenServer.call(shard, {:put, "exp_key", "val", past})
      # Confirm entry is in ETS (put always writes to ETS)
      # Single-table format: {key, value, expire_at_ms, lfu_counter}
      assert [{"exp_key", "val", ^past, _lfu, _fid, _off, _vsize}] =
               :ets.lookup(keydir, "exp_key")

      # get should detect expiry and evict
      assert nil == GenServer.call(shard, {:get, "exp_key"})
      assert [] == :ets.lookup(keydir, "exp_key")
    end
  end

  describe "get_meta" do
    test "returns {value, expire_at_ms} for live key", %{shard: shard} do
      future = System.os_time(:millisecond) + 60_000
      :ok = GenServer.call(shard, {:put, "meta_key", "meta_val", future})
      assert {"meta_val", ^future} = GenServer.call(shard, {:get_meta, "meta_key"})
    end

    test "returns {value, 0} for key with no expiry", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "noexp", "val", 0})
      assert {"val", 0} = GenServer.call(shard, {:get_meta, "noexp"})
    end

    test "returns nil for missing key", %{shard: shard} do
      assert nil == GenServer.call(shard, {:get_meta, "no_such_key"})
    end

    test "returns nil for expired key", %{shard: shard} do
      past = System.os_time(:millisecond) - 1000
      :ok = GenServer.call(shard, {:put, "exp", "val", past})
      assert nil == GenServer.call(shard, {:get_meta, "exp"})
    end

    test "returns {value, 0} for key with expire_at_ms of 0", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "zero_exp", "val", 0})
      assert {"val", 0} = GenServer.call(shard, {:get_meta, "zero_exp"})
    end
  end

  # ---------------------------------------------------------------------------
  # Additional edge cases
  # ---------------------------------------------------------------------------

  describe "binary key and value edge cases" do
    test "handles binary keys with null bytes", %{shard: shard} do
      key = <<0, 1, 2>>
      :ok = GenServer.call(shard, {:put, key, "value", 0})
      assert "value" == GenServer.call(shard, {:get, key})
    end

    test "handles large value (50KB)", %{shard: shard} do
      large_value = :crypto.strong_rand_bytes(50_000)
      :ok = GenServer.call(shard, {:put, "big", large_value, 0})
      assert large_value == GenServer.call(shard, {:get, "big"})
    end
  end

  describe "many keys" do
    test "handles 500 key-value pairs", %{shard: shard} do
      for i <- 1..500 do
        key = "key_#{i}"
        :ok = GenServer.call(shard, {:put, key, "val_#{i}", 0})
      end

      keys = GenServer.call(shard, :keys)
      assert length(keys) == 500
    end
  end

  describe "concurrent access" do
    test "concurrent gets from same shard do not crash", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "shared", "value", 0})

      tasks =
        for _ <- 1..30 do
          Task.async(fn ->
            GenServer.call(shard, {:get, "shared"})
          end)
        end

      results = Task.await_many(tasks, 5_000)
      assert Enum.all?(results, &(&1 == "value"))
    end
  end

  describe "consistency" do
    test "put then immediate exists? is consistent", %{shard: shard} do
      :ok = GenServer.call(shard, {:put, "fresh", "val", 0})
      assert true == GenServer.call(shard, {:exists, "fresh"})
    end

    test "exists? after expired put returns false", %{shard: shard} do
      past = System.os_time(:millisecond) - 1000
      :ok = GenServer.call(shard, {:put, "gone", "val", past})
      assert false == GenServer.call(shard, {:exists, "gone"})
    end
  end

  describe "keys edge cases" do
    test "keys returns empty list for fresh shard", %{shard: shard} do
      assert [] == GenServer.call(shard, :keys)
    end
  end
end
