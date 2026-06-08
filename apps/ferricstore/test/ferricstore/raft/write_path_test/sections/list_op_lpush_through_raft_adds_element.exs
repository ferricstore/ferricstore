defmodule Ferricstore.Raft.WritePathTest.Sections.ListOpLpushThroughRaftAddsElement do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.WARaftSegmentReader
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Test.ShardHelpers
      alias Ferricstore.Raft.StateMachine, as: SM

      describe "list_op: LPUSH through Raft adds element" do
        test "LPUSH to a new key creates a list with the pushed elements" do
          ctx = fresh_sm_state()
          {state, _ets, _store, _dir} = ctx

          {new_state, result} =
            SM.apply(%{}, {:list_op, "mylist", {:lpush, ["a", "b", "c"]}}, state)

          assert result == 3
          assert new_state.applied_count == 1

          # Verify via LRANGE (compound key format)
          {_state2, elements} =
            SM.apply(%{}, {:list_op, "mylist", {:lrange, 0, -1}}, new_state)

          assert elements == ["c", "b", "a"]

          cleanup_sm(ctx)
        end

        test "LPUSH to existing list prepends elements" do
          ctx = fresh_sm_state()
          {state, _ets, _store, _dir} = ctx

          {state2, 2} =
            SM.apply(%{}, {:list_op, "mylist", {:lpush, ["a", "b"]}}, state)

          {state3, 4} =
            SM.apply(%{}, {:list_op, "mylist", {:lpush, ["c", "d"]}}, state2)

          {_state4, elements} =
            SM.apply(%{}, {:list_op, "mylist", {:lrange, 0, -1}}, state3)

          assert elements == ["d", "c", "b", "a"]

          cleanup_sm(ctx)
        end
      end

      describe "list_op: RPUSH through Raft adds element" do
        test "RPUSH to a new key creates a list with the pushed elements" do
          ctx = fresh_sm_state()
          {state, _ets, _store, _dir} = ctx

          {new_state, result} =
            SM.apply(%{}, {:list_op, "rlist", {:rpush, ["x", "y", "z"]}}, state)

          assert result == 3

          {_state2, elements} =
            SM.apply(%{}, {:list_op, "rlist", {:lrange, 0, -1}}, new_state)

          assert elements == ["x", "y", "z"]

          cleanup_sm(ctx)
        end

        test "RPUSH to existing list appends elements" do
          ctx = fresh_sm_state()
          {state, _ets, _store, _dir} = ctx

          {state2, 2} =
            SM.apply(%{}, {:list_op, "rlist", {:rpush, ["a", "b"]}}, state)

          {state3, 4} =
            SM.apply(%{}, {:list_op, "rlist", {:rpush, ["c", "d"]}}, state2)

          # Verify via LRANGE
          {_state4, elements} =
            SM.apply(%{}, {:list_op, "rlist", {:lrange, 0, -1}}, state3)

          assert elements == ["a", "b", "c", "d"]

          cleanup_sm(ctx)
        end
      end

      describe "list_op: LPOP through Raft removes element" do
        test "LPOP from a list returns and removes the leftmost element" do
          ctx = fresh_sm_state()
          {state, _ets, _store, _dir} = ctx

          {state2, 3} =
            SM.apply(%{}, {:list_op, "poplist", {:rpush, ["a", "b", "c"]}}, state)

          {state3, popped} =
            SM.apply(%{}, {:list_op, "poplist", {:lpop, 1}}, state2)

          assert popped == "a"
          assert state3.applied_count == 2

          {_state4, elements} =
            SM.apply(%{}, {:list_op, "poplist", {:lrange, 0, -1}}, state3)

          assert elements == ["b", "c"]

          cleanup_sm(ctx)
        end

        test "LPOP from empty / non-existent key returns nil" do
          ctx = fresh_sm_state()
          {state, _ets, _store, _dir} = ctx

          {_new_state, result} =
            SM.apply(%{}, {:list_op, "nokey", {:lpop, 1}}, state)

          assert result == nil

          cleanup_sm(ctx)
        end

        test "LPOP all elements empties the list" do
          ctx = fresh_sm_state()
          {state, _ets, _store, _dir} = ctx

          {state2, 1} =
            SM.apply(%{}, {:list_op, "single", {:rpush, ["only"]}}, state)

          {state3, "only"} =
            SM.apply(%{}, {:list_op, "single", {:lpop, 1}}, state2)

          # List should be empty — LLEN returns 0 or LRANGE returns []
          {_state4, elements} =
            SM.apply(%{}, {:list_op, "single", {:lrange, 0, -1}}, state3)

          assert elements == [] or elements == nil

          cleanup_sm(ctx)
        end
      end

      describe "list_op: RPOP through Raft removes element" do
        test "RPOP from a list returns and removes the rightmost element" do
          ctx = fresh_sm_state()
          {state, _ets, _store, _dir} = ctx

          {state2, 3} =
            SM.apply(%{}, {:list_op, "rpoplist", {:rpush, ["a", "b", "c"]}}, state)

          {state3, popped} =
            SM.apply(%{}, {:list_op, "rpoplist", {:rpop, 1}}, state2)

          assert popped == "c"

          {_state4, elements} =
            SM.apply(%{}, {:list_op, "rpoplist", {:lrange, 0, -1}}, state3)

          assert elements == ["a", "b"]

          cleanup_sm(ctx)
        end
      end

      describe "compound_put: HSET through Raft writes field" do
        test "compound_put inserts a hash field into ETS and Bitcask" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          compound_key = "myhash\x00field1"

          {new_state, result} =
            SM.apply(%{}, {:compound_put, compound_key, "value1", 0}, state)

          assert result == :ok
          assert new_state.applied_count == 1

          # Verify ETS
          assert [{^compound_key, "value1", _, _, _, _, _}] = :ets.lookup(ets, compound_key)

          # Verify Bitcask
          assert [{_, "value1", _, _, _, _, _}] = :ets.lookup(ets, compound_key)

          cleanup_sm(ctx)
        end

        test "compound_put overwrites existing field value" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          compound_key = "myhash\x00field1"

          {state2, :ok} =
            SM.apply(%{}, {:compound_put, compound_key, "v1", 0}, state)

          {_state3, :ok} =
            SM.apply(%{}, {:compound_put, compound_key, "v2", 0}, state2)

          assert [{^compound_key, "v2", _, _, _, _, _}] = :ets.lookup(ets, compound_key)

          cleanup_sm(ctx)
        end

        test "multiple compound_puts for different fields" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          {state2, :ok} =
            SM.apply(%{}, {:compound_put, "h\x00f1", "val1", 0}, state)

          {state3, :ok} =
            SM.apply(%{}, {:compound_put, "h\x00f2", "val2", 0}, state2)

          {_state4, :ok} =
            SM.apply(%{}, {:compound_put, "h\x00f3", "val3", 0}, state3)

          assert [{_, "val1", _, _, _, _, _}] = :ets.lookup(ets, "h\x00f1")
          assert [{_, "val2", _, _, _, _, _}] = :ets.lookup(ets, "h\x00f2")
          assert [{_, "val3", _, _, _, _, _}] = :ets.lookup(ets, "h\x00f3")

          cleanup_sm(ctx)
        end

        test "compound_put writes promoted hash fields to the dedicated Bitcask" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          key = "promoted_hash"
          compound_key = Ferricstore.Store.CompoundKey.hash_field(key, "field1")

          {:ok, dedicated_path} =
            Ferricstore.Store.Promotion.open_dedicated(state.data_dir, 0, :hash, key)

          dedicated_log = Ferricstore.Store.Promotion.find_active(dedicated_path)

          assert File.stat!(dedicated_log).size == 0

          {new_state, :ok} = SM.apply(%{}, {:compound_put, compound_key, "value1", 0}, state)

          assert new_state.applied_count == 1

          assert [{^compound_key, "value1", 0, _lfu, 0, off, _vsize}] =
                   :ets.lookup(ets, compound_key)

          assert off >= 0
          assert {:ok, "value1"} = NIF.v2_pread_at(dedicated_log, off)
          assert File.stat!(dedicated_log).size > 0

          cleanup_sm(ctx)
        end
      end

      describe "compound_delete: HDEL through Raft removes field" do
        test "compound_delete removes a hash field from ETS and Bitcask" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          compound_key = "myhash\x00field1"

          {state2, :ok} =
            SM.apply(%{}, {:compound_put, compound_key, "value1", 0}, state)

          {state3, :ok} =
            SM.apply(%{}, {:compound_delete, compound_key}, state2)

          assert state3.applied_count == 2
          assert [] == :ets.lookup(ets, compound_key)
          assert [] = :ets.lookup(ets, compound_key)

          cleanup_sm(ctx)
        end

        test "compound_delete on non-existent key returns :ok" do
          ctx = fresh_sm_state()
          {state, _ets, _store, _dir} = ctx

          {_new_state, result} =
            SM.apply(%{}, {:compound_delete, "nonexistent\x00field"}, state)

          assert result == :ok

          cleanup_sm(ctx)
        end

        test "compound_delete tombstones promoted hash fields in the dedicated Bitcask" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          key = "promoted_hash_delete"
          compound_key = Ferricstore.Store.CompoundKey.hash_field(key, "field1")

          {:ok, dedicated_path} =
            Ferricstore.Store.Promotion.open_dedicated(state.data_dir, 0, :hash, key)

          dedicated_log = Ferricstore.Store.Promotion.find_active(dedicated_path)

          {state2, :ok} = SM.apply(%{}, {:compound_put, compound_key, "value1", 0}, state)
          size_after_put = File.stat!(dedicated_log).size
          assert size_after_put > 0

          {state3, :ok} = SM.apply(%{}, {:compound_delete, compound_key}, state2)

          assert state3.applied_count == 2
          assert [] == :ets.lookup(ets, compound_key)
          assert File.stat!(dedicated_log).size > size_after_put

          cleanup_sm(ctx)
        end
      end

      describe "compound_put: SADD through Raft adds member" do
        test "compound_put adds a set member (presence marker)" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          # Sets use a presence marker as the value
          compound_key = "myset\x00member1"
          presence = "1"

          {new_state, :ok} =
            SM.apply(%{}, {:compound_put, compound_key, presence, 0}, state)

          assert new_state.applied_count == 1
          assert [{^compound_key, ^presence, _, _, _, _, _}] = :ets.lookup(ets, compound_key)
          assert [{_, ^presence, _, _, _, _, _}] = :ets.lookup(ets, compound_key)

          cleanup_sm(ctx)
        end

        test "multiple set members via compound_put" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          {state2, :ok} =
            SM.apply(%{}, {:compound_put, "myset\x00m1", "1", 0}, state)

          {state3, :ok} =
            SM.apply(%{}, {:compound_put, "myset\x00m2", "1", 0}, state2)

          {_state4, :ok} =
            SM.apply(%{}, {:compound_put, "myset\x00m3", "1", 0}, state3)

          assert [{_, "1", _, _, _, _, _}] = :ets.lookup(ets, "myset\x00m1")
          assert [{_, "1", _, _, _, _, _}] = :ets.lookup(ets, "myset\x00m2")
          assert [{_, "1", _, _, _, _, _}] = :ets.lookup(ets, "myset\x00m3")

          cleanup_sm(ctx)
        end
      end

      describe "compound_delete_prefix: DEL on hash through Raft cleans up all fields" do
        test "deletes all compound keys matching prefix" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          # Insert several fields for a hash "myhash"
          prefix = "myhash\x00"

          {state2, :ok} =
            SM.apply(%{}, {:compound_put, "myhash\x00f1", "v1", 0}, state)

          {state3, :ok} =
            SM.apply(%{}, {:compound_put, "myhash\x00f2", "v2", 0}, state2)

          {state4, :ok} =
            SM.apply(%{}, {:compound_put, "myhash\x00f3", "v3", 0}, state3)

          # Also insert a key with a different prefix to ensure it is NOT deleted
          {state5, :ok} =
            SM.apply(%{}, {:compound_put, "otherhash\x00x", "ox", 0}, state4)

          # Verify all 4 keys exist in ETS
          assert :ets.info(ets, :size) == 4

          # Now delete all keys with the "myhash\0" prefix
          {state6, :ok} =
            SM.apply(%{}, {:compound_delete_prefix, prefix}, state5)

          assert state6.applied_count == 5

          # All "myhash" fields should be gone
          assert [] == :ets.lookup(ets, "myhash\x00f1")
          assert [] == :ets.lookup(ets, "myhash\x00f2")
          assert [] == :ets.lookup(ets, "myhash\x00f3")

          # Bitcask should also have them deleted
          assert [] = :ets.lookup(ets, "myhash\x00f1")
          assert [] = :ets.lookup(ets, "myhash\x00f2")
          assert [] = :ets.lookup(ets, "myhash\x00f3")

          # The "otherhash" key should still exist
          assert [{_, "ox", _, _, _, _, _}] = :ets.lookup(ets, "otherhash\x00x")

          cleanup_sm(ctx)
        end

        test "deletes all set members when DEL is called on the set key" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          prefix = "myset\x00"

          {state2, :ok} =
            SM.apply(%{}, {:compound_put, "myset\x00m1", "1", 0}, state)

          {state3, :ok} =
            SM.apply(%{}, {:compound_put, "myset\x00m2", "1", 0}, state2)

          {state4, :ok} =
            SM.apply(%{}, {:compound_put, "myset\x00m3", "1", 0}, state3)

          {_state5, :ok} =
            SM.apply(%{}, {:compound_delete_prefix, prefix}, state4)

          assert [] == :ets.lookup(ets, "myset\x00m1")
          assert [] == :ets.lookup(ets, "myset\x00m2")
          assert [] == :ets.lookup(ets, "myset\x00m3")

          cleanup_sm(ctx)
        end

        test "compound_delete_prefix with no matching keys is a no-op" do
          ctx = fresh_sm_state()
          {state, _ets, _store, _dir} = ctx

          {new_state, :ok} =
            SM.apply(%{}, {:compound_delete_prefix, "nonexistent\x00"}, state)

          assert new_state.applied_count == 1

          cleanup_sm(ctx)
        end

        test "compound_delete_prefix does not affect unrelated keys" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          {state2, :ok} =
            SM.apply(%{}, {:compound_put, "hash_a\x00f1", "v1", 0}, state)

          {state3, :ok} =
            SM.apply(%{}, {:compound_put, "hash_b\x00f1", "v2", 0}, state2)

          # Only delete hash_a's fields
          {_state4, :ok} =
            SM.apply(%{}, {:compound_delete_prefix, "hash_a\x00"}, state3)

          assert [] == :ets.lookup(ets, "hash_a\x00f1")
          assert [{_, "v2", _, _, _, _, _}] = :ets.lookup(ets, "hash_b\x00f1")

          cleanup_sm(ctx)
        end
      end

      describe "batch containing new command types" do
        test "put_batch applies many string writes with one batched result" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          entries = [
            {"hotbatch:a", "va", 0},
            {"hotbatch:b", "vb", 12_345},
            {"hotbatch:c", "vc", 0}
          ]

          {new_state, {:ok, results}} = SM.apply(%{}, {:put_batch, entries}, state)

          assert results == [:ok, :ok, :ok]
          assert new_state.applied_count == 3
          assert [{_, "va", 0, _, _, _, _}] = :ets.lookup(ets, "hotbatch:a")
          assert [{_, "vb", 12_345, _, _, _, _}] = :ets.lookup(ets, "hotbatch:b")
          assert [{_, "vc", 0, _, _, _, _}] = :ets.lookup(ets, "hotbatch:c")

          cleanup_sm(ctx)
        end

        test "put_batch clears stale compound data before storing a string" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx
          key = "hotbatch:compound"
          type_key = Ferricstore.Store.CompoundKey.type_key(key)
          field_key = Ferricstore.Store.CompoundKey.hash_field(key, "field")

          {state, :ok} = SM.apply(%{}, {:compound_put, type_key, "hash", 0}, state)
          {state, :ok} = SM.apply(%{}, {:compound_put, field_key, "old", 0}, state)

          {new_state, {:ok, [:ok]}} = SM.apply(%{}, {:put_batch, [{key, "string", 0}]}, state)

          assert new_state.applied_count == 3
          assert [{^key, "string", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, key)
          assert [] == :ets.lookup(ets, type_key)
          assert [] == :ets.lookup(ets, field_key)

          cleanup_sm(ctx)
        end

        test "put_batch preserves per-key lock rejection" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx
          owner = make_ref()

          locked_state = %{
            state
            | cross_shard_locks: %{
                "hotbatch:locked" => {owner, Ferricstore.HLC.now_ms() + 60_000}
              }
          }

          entries = [
            {"hotbatch:open", "va", 0},
            {"hotbatch:locked", "vb", 0}
          ]

          {new_state, {:ok, results}} = SM.apply(%{}, {:put_batch, entries}, locked_state)

          assert results == [:ok, {:error, :key_locked}]
          assert new_state.applied_count == 2
          assert [{_, "va", 0, _, _, _, _}] = :ets.lookup(ets, "hotbatch:open")
          assert [] == :ets.lookup(ets, "hotbatch:locked")

          cleanup_sm(ctx)
        end

        test "delete_batch removes many keys and preserves per-key results" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          {state, {:ok, [:ok, :ok]}} =
            SM.apply(%{}, {:put_batch, [{"hotdel:a", "va", 0}, {"hotdel:b", "vb", 0}]}, state)

          {new_state, {:ok, results}} =
            SM.apply(%{}, {:delete_batch, ["hotdel:a", "hotdel:b"]}, state)

          assert results == [:ok, :ok]
          assert new_state.applied_count == 4
          assert [] == :ets.lookup(ets, "hotdel:a")
          assert [] == :ets.lookup(ets, "hotdel:b")

          cleanup_sm(ctx)
        end

        test "compound_batch_put applies many fields with one batched result" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx
          key = "hotcompound:hash"
          field_a = CompoundKey.hash_field(key, "a")
          field_b = CompoundKey.hash_field(key, "b")

          {new_state, {:ok, results}} =
            SM.apply(
              %{},
              {:compound_batch_put, key, [{field_a, "va", 0}, {field_b, "vb", 12_345}]},
              state
            )

          assert results == [:ok, :ok]
          assert new_state.applied_count == 2
          assert [{^field_a, "va", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, field_a)
          assert [{^field_b, "vb", 12_345, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, field_b)

          cleanup_sm(ctx)
        end

        test "compound_batch_delete removes many fields with one batched result" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx
          key = "hotcompound:hash"
          field_a = CompoundKey.hash_field(key, "a")
          field_b = CompoundKey.hash_field(key, "b")

          {state, {:ok, [:ok, :ok]}} =
            SM.apply(
              %{},
              {:compound_batch_put, key, [{field_a, "va", 0}, {field_b, "vb", 0}]},
              state
            )

          {new_state, {:ok, results}} =
            SM.apply(%{}, {:compound_batch_delete, key, [field_a, field_b]}, state)

          assert results == [:ok, :ok]
          assert new_state.applied_count == 4
          assert [] == :ets.lookup(ets, field_a)
          assert [] == :ets.lookup(ets, field_b)

          cleanup_sm(ctx)
        end

        test "batch with list_op, compound_put, and compound_delete" do
          ctx = fresh_sm_state()
          {state, ets, _store, _dir} = ctx

          commands = [
            {:list_op, "mylist", {:rpush, ["a", "b"]}},
            {:compound_put, "myhash\x00f1", "v1", 0},
            {:compound_put, "myset\x00m1", "1", 0}
          ]

          {new_state, {:ok, results}} =
            SM.apply(%{}, {:batch, commands}, state)

          # list_op returns the new length
          assert [2, :ok, :ok] = results
          assert new_state.applied_count == 3

          # Verify list via LRANGE
          {_state2, elements} =
            SM.apply(%{}, {:list_op, "mylist", {:lrange, 0, -1}}, new_state)

          assert elements == ["a", "b"]

          # Verify hash field
          assert [{_, "v1", _, _, _, _, _}] = :ets.lookup(ets, "myhash\x00f1")

          # Verify set member
          assert [{_, "1", _, _, _, _, _}] = :ets.lookup(ets, "myset\x00m1")

          cleanup_sm(ctx)
        end
      end

      describe "CAS through Raft" do
        test "match succeeds -- swaps value and returns 1" do
          k = ukey("cas_match")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "old_val", 0)
          assert "old_val" == Router.get(FerricStore.Instance.get(:default), k)

          assert 1 = Router.cas(FerricStore.Instance.get(:default), k, "old_val", "new_val", nil)
          assert "new_val" == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "mismatch fails -- returns 0 and does not change value" do
          k = ukey("cas_mismatch")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "current", 0)

          assert 0 =
                   Router.cas(FerricStore.Instance.get(:default), k, "wrong_expected", "new", nil)

          assert "current" == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "missing key returns nil" do
          k = ukey("cas_missing")

          assert nil == Router.cas(FerricStore.Instance.get(:default), k, "anything", "new", nil)
          assert nil == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "CAS with TTL sets expiry on swapped value" do
          k = ukey("cas_ttl")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "v1", 0)
          assert 1 = Router.cas(FerricStore.Instance.get(:default), k, "v1", "v2", 60_000)

          {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
          assert value == "v2"
          assert expire_at_ms > System.os_time(:millisecond)
          assert expire_at_ms <= System.os_time(:millisecond) + 60_000
        end

        test "CAS without TTL preserves original expiry" do
          k = ukey("cas_preserve_ttl")
          future = System.os_time(:millisecond) + 120_000

          :ok = Router.put(FerricStore.Instance.get(:default), k, "v1", future)
          assert 1 = Router.cas(FerricStore.Instance.get(:default), k, "v1", "v2", nil)

          {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
          assert value == "v2"
          assert expire_at_ms == future
        end

        test "CAS on expired key returns nil" do
          k = ukey("cas_expired")
          past = System.os_time(:millisecond) - 1_000

          :ok = Router.put(FerricStore.Instance.get(:default), k, "expired_val", past)

          assert nil ==
                   Router.cas(FerricStore.Instance.get(:default), k, "expired_val", "new", nil)
        end
      end

      describe "LOCK through Raft" do
        test "acquires lock on non-existent key" do
          k = ukey("lock_acquire")

          assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)
          assert "owner1" == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "acquires lock on expired key" do
          k = ukey("lock_expired")
          past = System.os_time(:millisecond) - 1_000

          :ok = Router.put(FerricStore.Instance.get(:default), k, "stale_owner", past)
          assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "new_owner", 30_000)
          assert "new_owner" == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "re-acquire by same owner succeeds" do
          k = ukey("lock_reacquire")

          assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)
          assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 60_000)
          assert "owner1" == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "returns error when locked by different owner" do
          k = ukey("lock_conflict")

          assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)

          assert {:error, "DISTLOCK lock is held by another owner"} =
                   Router.lock(FerricStore.Instance.get(:default), k, "owner2", 30_000)
        end

        test "lock sets TTL on the key" do
          k = ukey("lock_ttl")

          assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)
          {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
          assert value == "owner1"
          assert expire_at_ms > System.os_time(:millisecond)
          assert expire_at_ms <= System.os_time(:millisecond) + 30_000
        end
      end

      describe "UNLOCK through Raft" do
        test "releases lock when owner matches" do
          k = ukey("unlock_match")

          :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)
          assert "owner1" == Router.get(FerricStore.Instance.get(:default), k)

          assert 1 = Router.unlock(FerricStore.Instance.get(:default), k, "owner1")
          assert nil == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "returns error when caller is not the owner" do
          k = ukey("unlock_wrong_owner")

          :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)

          assert {:error, "DISTLOCK caller is not the lock owner"} =
                   Router.unlock(FerricStore.Instance.get(:default), k, "owner2")

          # Lock should still be held
          assert "owner1" == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "unlocking non-existent key returns 1" do
          k = ukey("unlock_missing")
          assert 1 = Router.unlock(FerricStore.Instance.get(:default), k, "anyone")
        end
      end

      describe "EXTEND through Raft" do
        test "extends TTL when owner matches" do
          k = ukey("extend_match")

          :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 10_000)
          {_, expire_before} = Router.get_meta(FerricStore.Instance.get(:default), k)

          # Extend with a longer TTL
          assert 1 = Router.extend(FerricStore.Instance.get(:default), k, "owner1", 60_000)

          {value, expire_after} = Router.get_meta(FerricStore.Instance.get(:default), k)
          assert value == "owner1"
          assert expire_after > expire_before
        end

        test "returns error when caller is not the owner" do
          k = ukey("extend_wrong_owner")

          :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner1", 30_000)

          assert {:error, "DISTLOCK caller is not the lock owner"} =
                   Router.extend(FerricStore.Instance.get(:default), k, "owner2", 60_000)
        end

        test "returns error when key does not exist" do
          k = ukey("extend_missing")

          assert {:error, "DISTLOCK lock does not exist or has expired"} =
                   Router.extend(FerricStore.Instance.get(:default), k, "owner1", 60_000)
        end

        test "returns error when lock has expired" do
          k = ukey("extend_expired")
          past = System.os_time(:millisecond) - 1_000

          :ok = Router.put(FerricStore.Instance.get(:default), k, "owner1", past)

          assert {:error, "DISTLOCK lock does not exist or has expired"} =
                   Router.extend(FerricStore.Instance.get(:default), k, "owner1", 60_000)
        end
      end
    end
  end
end
