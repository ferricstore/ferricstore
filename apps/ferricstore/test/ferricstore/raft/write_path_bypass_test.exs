defmodule Ferricstore.Raft.WritePathBypassTest do
  @moduledoc """
  Tests for the quorum write-path bypass optimization.

  When a key's namespace has `:quorum` durability (the default), write
  operations bypass the Shard GenServer entirely and send commands
  directly from Router to Batcher. The StateMachine writes ETS on
  Raft commit -- no optimistic ETS write, no dirty reads.

  These tests verify that every write command type works correctly
  through the bypass path, including read-modify-write operations,
  concurrency correctness, and read-your-own-writes semantics.
  """

  use ExUnit.Case, async: false
  @moduletag :raft
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    # Restart-heavy suites can leave Ra leaders elected but the public
    # Batcher/Ra reply path still settling. This suite asserts the bypass
    # write path itself, so first prove a public quorum write can round-trip.
    ShardHelpers.wait_default_quorum_writable(60_000)
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
    :ok
  end

  # Unique key generator to avoid cross-test collisions
  defp ukey(base), do: "bypass_#{base}_#{:rand.uniform(9_999_999)}"

  # ---------------------------------------------------------------------------
  # 1. Basic write commands (pure writes, no read needed)
  # ---------------------------------------------------------------------------

  describe "SET (pure write) via bypass" do
    test "SET then GET -- value is present after set returns :ok" do
      k = ukey("set_basic")
      assert :ok = Router.put(FerricStore.Instance.get(:default), k, "hello", 0)
      assert "hello" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SET with TTL -- expiry is correct" do
      k = ukey("set_ttl")
      future = System.os_time(:millisecond) + 60_000
      assert :ok = Router.put(FerricStore.Instance.get(:default), k, "ttl_val", future)

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "ttl_val"
      assert expire_at_ms == future
    end

    test "SET then SET again (overwrite)" do
      k = ukey("set_overwrite")
      assert :ok = Router.put(FerricStore.Instance.get(:default), k, "v1", 0)
      assert "v1" == Router.get(FerricStore.Instance.get(:default), k)

      assert :ok = Router.put(FerricStore.Instance.get(:default), k, "v2", 0)
      assert "v2" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SET empty value" do
      k = ukey("set_empty")
      assert :ok = Router.put(FerricStore.Instance.get(:default), k, "", 0)
      assert "" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SET binary value with null bytes" do
      k = ukey("set_null_bytes")
      value = <<0, 1, 2, 0, 3, 0>>
      assert :ok = Router.put(FerricStore.Instance.get(:default), k, value, 0)
      assert value == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  describe "DEL via bypass" do
    test "DEL then GET -- key is gone" do
      k = ukey("del_basic")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "to_delete", 0)
      assert "to_delete" == Router.get(FerricStore.Instance.get(:default), k)

      :ok = Router.delete(FerricStore.Instance.get(:default), k)
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "DEL non-existent key returns :ok" do
      k = ukey("del_missing")
      assert :ok = Router.delete(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # 2. Read-modify-write commands (via StateMachine)
  # ---------------------------------------------------------------------------

  describe "INCR via bypass" do
    test "INCR returns correct new value" do
      k = ukey("incr_basic")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
      assert {:ok, 11} = Router.incr(FerricStore.Instance.get(:default), k, 1)
      assert "11" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCR on non-existent key initializes to delta" do
      k = ukey("incr_new")
      assert {:ok, 5} = Router.incr(FerricStore.Instance.get(:default), k, 5)
      assert "5" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCR on non-integer returns error" do
      k = ukey("incr_bad")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "not_a_number", 0)
      assert {:error, _} = Router.incr(FerricStore.Instance.get(:default), k, 1)
    end

    test "DECR (negative delta)" do
      k = ukey("decr")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
      assert {:ok, 7} = Router.incr(FerricStore.Instance.get(:default), k, -3)
      assert "7" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCR then DEL then INCR starts from 0 again" do
      k = ukey("incr_del_incr")
      assert {:ok, 5} = Router.incr(FerricStore.Instance.get(:default), k, 5)
      :ok = Router.delete(FerricStore.Instance.get(:default), k)
      assert {:ok, 1} = Router.incr(FerricStore.Instance.get(:default), k, 1)
      assert "1" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  describe "INCRBYFLOAT via bypass" do
    test "returns correct float" do
      k = ukey("incrfloat")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "10.5", 0)
      assert {:ok, result_val} = Router.incr_float(FerricStore.Instance.get(:default), k, 1.5)
      assert_in_delta result_val, 12.0, 0.001
    end

    test "on non-existent key initializes to delta" do
      k = ukey("incrfloat_new")
      assert {:ok, _} = Router.incr_float(FerricStore.Instance.get(:default), k, 3.14)
    end
  end

  describe "APPEND via bypass" do
    test "returns correct length and value is concatenated" do
      k = ukey("append")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "hello", 0)
      assert {:ok, 10} = Router.append(FerricStore.Instance.get(:default), k, "world")
      assert "helloworld" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "on non-existent key creates with suffix" do
      k = ukey("append_new")
      assert {:ok, 5} = Router.append(FerricStore.Instance.get(:default), k, "fresh")
      assert "fresh" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  describe "GETSET via bypass" do
    test "returns old value and new value is set" do
      k = ukey("getset")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "old_val", 0)
      assert "old_val" == Router.getset(FerricStore.Instance.get(:default), k, "new_val")
      assert "new_val" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "returns nil when key does not exist, sets new value" do
      k = ukey("getset_new")
      assert nil == Router.getset(FerricStore.Instance.get(:default), k, "brand_new")
      assert "brand_new" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  describe "GETDEL via bypass" do
    test "returns value and key is deleted" do
      k = ukey("getdel")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "to_getdel", 0)
      assert "to_getdel" == Router.getdel(FerricStore.Instance.get(:default), k)
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "returns nil when key does not exist" do
      k = ukey("getdel_missing")
      assert nil == Router.getdel(FerricStore.Instance.get(:default), k)
    end
  end

  describe "GETEX via bypass" do
    test "returns value and TTL is updated" do
      k = ukey("getex")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "getex_val", 0)
      new_ttl = System.os_time(:millisecond) + 120_000
      assert "getex_val" == Router.getex(FerricStore.Instance.get(:default), k, new_ttl)

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "getex_val"
      assert expire_at_ms == new_ttl
    end

    test "returns nil when key does not exist" do
      k = ukey("getex_missing")

      assert nil ==
               Router.getex(
                 FerricStore.Instance.get(:default),
                 k,
                 System.os_time(:millisecond) + 60_000
               )
    end
  end

  describe "SETRANGE via bypass" do
    test "correct overwrite, returns new length" do
      k = ukey("setrange")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "Hello World", 0)
      assert {:ok, 11} = Router.setrange(FerricStore.Instance.get(:default), k, 6, "Redis")
      assert "Hello Redis" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "zero-pads when key does not exist" do
      k = ukey("setrange_new")
      assert {:ok, 8} = Router.setrange(FerricStore.Instance.get(:default), k, 5, "abc")
      value = Router.get(FerricStore.Instance.get(:default), k)
      assert byte_size(value) == 8
      assert binary_part(value, 5, 3) == "abc"
    end
  end

  # ---------------------------------------------------------------------------
  # 3. Atomic/conditional commands
  # ---------------------------------------------------------------------------

  describe "CAS via bypass" do
    test "success -- old value matches, swap happens" do
      k = ukey("cas_ok")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "old", 0)
      assert 1 == Router.cas(FerricStore.Instance.get(:default), k, "old", "new", nil)
      assert "new" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "failure -- old value doesn't match, no swap" do
      k = ukey("cas_fail")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "actual", 0)
      assert 0 == Router.cas(FerricStore.Instance.get(:default), k, "wrong", "new", nil)
      assert "actual" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "non-existent key returns nil" do
      k = ukey("cas_missing")
      assert nil == Router.cas(FerricStore.Instance.get(:default), k, "expected", "new", nil)
    end

    test "CAS with TTL" do
      k = ukey("cas_ttl")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "old", 0)
      assert 1 == Router.cas(FerricStore.Instance.get(:default), k, "old", "new", 60_000)
      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "new"
      assert expire_at_ms > System.os_time(:millisecond)
    end
  end

  describe "LOCK/UNLOCK/EXTEND via bypass" do
    test "acquires lock, second lock fails" do
      k = ukey("lock")
      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 10_000)
      assert {:error, _} = Router.lock(FerricStore.Instance.get(:default), k, "owner2", 10_000)
    end

    test "unlock releases lock, second lock succeeds" do
      k = ukey("unlock")
      :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 10_000)
      assert 1 == Router.unlock(FerricStore.Instance.get(:default), k, "owner1")
      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner2", 10_000)
    end

    test "extend updates TTL on held lock" do
      k = ukey("extend")
      :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 10_000)
      assert 1 == Router.extend(FerricStore.Instance.get(:default), k, "owner1", 60_000)
      {_value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert expire_at_ms > System.os_time(:millisecond) + 30_000
    end

    test "extend fails on non-existent lock" do
      k = ukey("extend_missing")
      assert {:error, _} = Router.extend(FerricStore.Instance.get(:default), k, "owner1", 10_000)
    end
  end

  describe "RATELIMIT via bypass" do
    test "allows within window, rejects over limit" do
      k = ukey("ratelimit")

      [status1, _count1, _rem1, _ttl1] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, 1_000, 2, 1)

      assert status1 == "allowed"

      [status2, _count2, _rem2, _ttl2] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, 1_000, 2, 1)

      assert status2 == "allowed"

      [status3, _count3, _rem3, _ttl3] =
        Router.ratelimit_add(FerricStore.Instance.get(:default), k, 1_000, 2, 1)

      assert status3 == "denied"
    end
  end

  # ---------------------------------------------------------------------------
  # 4. List operations via bypass
  # ---------------------------------------------------------------------------

  describe "list operations via bypass" do
    test "LPUSH then LRANGE -- elements are present" do
      k = ukey("lpush")
      assert 1 = Router.list_op(FerricStore.Instance.get(:default), k, {:lpush, ["a"]})
      assert 2 = Router.list_op(FerricStore.Instance.get(:default), k, {:lpush, ["b"]})
      assert ["b", "a"] = Router.list_op(FerricStore.Instance.get(:default), k, {:lrange, 0, -1})
    end

    test "RPUSH then LRANGE" do
      k = ukey("rpush")
      assert 1 = Router.list_op(FerricStore.Instance.get(:default), k, {:rpush, ["x"]})
      assert 2 = Router.list_op(FerricStore.Instance.get(:default), k, {:rpush, ["y"]})
      assert ["x", "y"] = Router.list_op(FerricStore.Instance.get(:default), k, {:lrange, 0, -1})
    end

    test "LPOP" do
      k = ukey("lpop")
      Router.list_op(FerricStore.Instance.get(:default), k, {:rpush, ["a", "b", "c"]})
      assert "a" = Router.list_op(FerricStore.Instance.get(:default), k, {:lpop, 1})
      assert ["b", "c"] = Router.list_op(FerricStore.Instance.get(:default), k, {:lrange, 0, -1})
    end

    test "LLEN" do
      k = ukey("llen")
      Router.list_op(FerricStore.Instance.get(:default), k, {:rpush, ["a", "b"]})
      assert 2 = Router.list_op(FerricStore.Instance.get(:default), k, :llen)
    end
  end

  # ---------------------------------------------------------------------------
  # 5. Concurrency correctness
  # ---------------------------------------------------------------------------

  describe "concurrency correctness" do
    test "50 concurrent INCRs on same key -- final value = 50" do
      k = ukey("conc_incr")

      tasks =
        for _ <- 1..50 do
          Task.async(fn -> Router.incr(FerricStore.Instance.get(:default), k, 1) end)
        end

      results = Task.await_many(tasks, 30_000)

      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)

      assert "50" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "50 concurrent CAS on same key -- exactly 1 succeeds" do
      k = ukey("conc_cas")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "original", 0)

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            Router.cas(FerricStore.Instance.get(:default), k, "original", "writer_#{i}", nil)
          end)
        end

      results = Task.await_many(tasks, 30_000)
      successes = Enum.count(results, &(&1 == 1))
      assert successes == 1, "expected exactly 1 CAS success, got #{successes}"
    end

    test "50 concurrent LOCKs on same key -- exactly 1 acquires" do
      k = ukey("conc_lock")

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            Router.lock(FerricStore.Instance.get(:default), k, "owner_#{i}", 30_000)
          end)
        end

      results = Task.await_many(tasks, 30_000)
      successes = Enum.count(results, &(&1 == :ok))
      assert successes == 1, "expected exactly 1 lock acquire, got #{successes}"
    end

    test "50 concurrent PUTs to different keys all succeed" do
      keys = for i <- 1..50, do: ukey("conc_put_#{i}")

      tasks =
        Enum.map(keys, fn k ->
          Task.async(fn -> Router.put(FerricStore.Instance.get(:default), k, "val_#{k}", 0) end)
        end)

      results = Task.await_many(tasks, 30_000)
      assert Enum.all?(results, &(&1 == :ok))

      for k <- keys do
        assert "val_#{k}" == Router.get(FerricStore.Instance.get(:default), k)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 6. Read-your-own-writes (quorum)
  # ---------------------------------------------------------------------------

  describe "read-your-own-writes (quorum)" do
    test "SET then GET in same process sees own write" do
      k = ukey("ryow")
      assert :ok = Router.put(FerricStore.Instance.get(:default), k, "my_write", 0)
      assert "my_write" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCR then GET sees correct value" do
      k = ukey("ryow_incr")
      assert {:ok, 42} = Router.incr(FerricStore.Instance.get(:default), k, 42)
      assert "42" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "GETSET returns old value correctly" do
      k = ukey("ryow_getset")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "first", 0)
      old = Router.getset(FerricStore.Instance.get(:default), k, "second")
      assert old == "first"
      assert "second" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # 7. Write version increments (WATCH support)
  # ---------------------------------------------------------------------------

  describe "write version increments via bypass" do
    test "SET changes write version" do
      k = ukey("wv_set")
      v_before = Router.get_version(FerricStore.Instance.get(:default), k)
      :ok = Router.put(FerricStore.Instance.get(:default), k, "ver_test", 0)
      v_after = Router.get_version(FerricStore.Instance.get(:default), k)
      assert v_after > v_before
    end

    test "INCR changes write version" do
      k = ukey("wv_incr")
      v_before = Router.get_version(FerricStore.Instance.get(:default), k)
      {:ok, _} = Router.incr(FerricStore.Instance.get(:default), k, 1)
      v_after = Router.get_version(FerricStore.Instance.get(:default), k)
      assert v_after > v_before
    end

    test "DEL changes write version" do
      k = ukey("wv_del")
      :ok = Router.put(FerricStore.Instance.get(:default), k, "to_delete", 0)
      v_before = Router.get_version(FerricStore.Instance.get(:default), k)
      :ok = Router.delete(FerricStore.Instance.get(:default), k)
      v_after = Router.get_version(FerricStore.Instance.get(:default), k)
      assert v_after > v_before
    end
  end

  # ---------------------------------------------------------------------------
  # 8. Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "very long key" do
      k = ukey("longkey_" <> String.duplicate("x", 10_000))
      assert :ok = Router.put(FerricStore.Instance.get(:default), k, "long_key_val", 0)
      assert "long_key_val" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "INCR interleaving with GET always returns valid integer" do
      k = ukey("incr_get_interleave")

      incr_tasks =
        for _ <- 1..20 do
          Task.async(fn -> Router.incr(FerricStore.Instance.get(:default), k, 1) end)
        end

      get_tasks =
        for _ <- 1..20 do
          Task.async(fn ->
            val = Router.get(FerricStore.Instance.get(:default), k)

            cond do
              val == nil ->
                :nil_ok

              is_integer(val) ->
                :valid

              is_binary(val) ->
                case Integer.parse(val) do
                  {_n, ""} -> :valid
                  _ -> {:invalid, val}
                end

              true ->
                {:invalid, val}
            end
          end)
        end

      Task.await_many(incr_tasks, 30_000)
      get_results = Task.await_many(get_tasks, 30_000)

      for r <- get_results do
        assert r in [:valid, :nil_ok],
               "GET during concurrent INCRs returned invalid value: #{inspect(r)}"
      end
    end
  end
end
