defmodule Ferricstore.Raft.StateMachineTest.Sections.Part07 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.{BlobCommand, StateMachine}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

  describe "apply/3 with {:set, key, value, expire_at_ms, opts}" do
    test "SET NX treats a cold keydir entry as existing without warming it", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      key = "set_nx_cold_existing"
      {:ok, {offset, _record_size}} = NIF.v2_append_record(active_file_path, key, "old", 0)
      value_size = byte_size("old")
      :ets.insert(ets, {key, nil, 0, Ferricstore.Store.LFU.initial(), 0, offset, value_size})

      {_new_state, result} =
        StateMachine.apply(%{}, {:set, key, "new", 0, set_opts(%{nx: true})}, state)

      assert result == nil
      assert [{^key, nil, 0, _lfu, 0, ^offset, ^value_size}] = :ets.lookup(ets, key)
    end

    test "SET XX updates a cold key even when the old value is unreadable", %{
      state: state,
      ets: ets
    } do
      key = "set_xx_cold_unreadable"
      :ets.insert(ets, {key, nil, 0, Ferricstore.Store.LFU.initial(), 99, 123, 3})

      {_new_state, result} =
        StateMachine.apply(%{}, {:set, key, "new", 0, set_opts(%{xx: true})}, state)

      assert result == :ok
      assert [{^key, "new", 0, _lfu, _fid, _off, 3}] = :ets.lookup(ets, key)
    end

    test "SET XX updates a WARaft segment-backed cold key without reading it", %{
      state: state,
      ets: ets
    } do
      key = "set_xx_waraft_segment_cold"

      :ets.insert(
        ets,
        {key, nil, 0, Ferricstore.Store.LFU.initial(), {:waraft_segment, 42}, 123, 3}
      )

      {_new_state, result} =
        StateMachine.apply(%{}, {:set, key, "new", 0, set_opts(%{xx: true})}, state)

      assert result == :ok
      assert [{^key, "new", 0, _lfu, _fid, _off, 3}] = :ets.lookup(ets, key)
    end

    test "SET KEEPTTL preserves cold key TTL without reading the old value", %{
      state: state,
      ets: ets
    } do
      key = "set_keepttl_cold_unreadable"
      expire_at_ms = System.os_time(:millisecond) + 60_000
      :ets.insert(ets, {key, nil, expire_at_ms, Ferricstore.Store.LFU.initial(), 99, 123, 3})

      {_new_state, result} =
        StateMachine.apply(%{}, {:set, key, "new", 0, set_opts(%{keepttl: true})}, state)

      assert result == :ok
      assert [{^key, "new", ^expire_at_ms, _lfu, _fid, _off, 3}] = :ets.lookup(ets, key)
    end

    test "SET KEEPTTL preserves WARaft segment-backed cold key TTL without reading it", %{
      state: state,
      ets: ets
    } do
      key = "set_keepttl_waraft_segment_cold"
      expire_at_ms = System.os_time(:millisecond) + 60_000

      :ets.insert(
        ets,
        {key, nil, expire_at_ms, Ferricstore.Store.LFU.initial(), {:waraft_segment, 42}, 123, 3}
      )

      {_new_state, result} =
        StateMachine.apply(%{}, {:set, key, "new", 0, set_opts(%{keepttl: true})}, state)

      assert result == :ok
      assert [{^key, "new", ^expire_at_ms, _lfu, _fid, _off, 3}] = :ets.lookup(ets, key)
    end

    test "SET KEEPTTL does not preserve TTL from malformed cold rows", %{
      state: state,
      ets: ets
    } do
      key = "set_keepttl_bad_cold_ref"
      expire_at_ms = System.os_time(:millisecond) + 60_000

      :ets.insert(
        ets,
        {key, nil, expire_at_ms, Ferricstore.Store.LFU.initial(), 0, :pending_offset, 3}
      )

      {_new_state, result} =
        StateMachine.apply(%{}, {:set, key, "new", 0, set_opts(%{keepttl: true})}, state)

      assert result == :ok
      assert [{^key, "new", 0, _lfu, _fid, _off, 3}] = :ets.lookup(ets, key)
    end

    test "SET blob ref NX skips existing keys without writing the ref", %{
      state: state,
      ets: ets
    } do
      key = "set_blob_ref_nx_existing"
      payload = :binary.copy("blob-set-nx", 32)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:set_blob_ref, key, encoded_ref, 0, set_opts(%{nx: true})},
          state
        )

      assert result == nil
      assert [{^key, "old", 0, _lfu, 0, 0, 3}] = :ets.lookup(ets, key)
    end

    test "SET blob ref NX skip does not validate an unreadable ref", %{
      state: state,
      ets: ets
    } do
      key = "set_blob_ref_nx_skip_invalid_ref"
      missing_ref = BlobRef.encode!(BlobRef.from_payload("missing"))
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:set_blob_ref, key, missing_ref, 0, set_opts(%{nx: true})},
          state
        )

      assert result == nil
      assert [{^key, "old", 0, _lfu, 0, 0, 3}] = :ets.lookup(ets, key)
    end

    test "SET blob ref XX stores the validated ref without materializing it in ETS", %{
      state: state,
      ets: ets
    } do
      key = "set_blob_ref_xx_existing"
      payload = :binary.copy("blob-set-xx", 32)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)
      encoded_ref_size = byte_size(encoded_ref)
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:set_blob_ref, key, encoded_ref, 0, set_opts(%{xx: true})},
          state
        )

      assert result == :ok
      assert [{^key, nil, 0, _lfu, _fid, _off, ^encoded_ref_size}] = :ets.lookup(ets, key)
    end

    test "SET blob ref GET returns the old value while storing the new ref", %{
      state: state,
      ets: ets
    } do
      key = "set_blob_ref_get_existing"
      payload = :binary.copy("blob-set-get", 32)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)
      encoded_ref_size = byte_size(encoded_ref)
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:set_blob_ref, key, encoded_ref, 0, set_opts(%{get: true})},
          state
        )

      assert result == "old"
      assert [{^key, nil, 0, _lfu, _fid, _off, ^encoded_ref_size}] = :ets.lookup(ets, key)
    end

    test "GETSET blob ref returns the old value while storing the new ref", %{
      state: state,
      ets: ets
    } do
      key = "getset_blob_ref_existing"
      payload = :binary.copy("blob-getset", 32)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)
      encoded_ref_size = byte_size(encoded_ref)
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(%{}, {:getset_blob_ref, key, encoded_ref}, state)

      assert result == "old"
      assert [{^key, nil, 0, _lfu, _fid, _off, ^encoded_ref_size}] = :ets.lookup(ets, key)
    end

    test "GETSET blob ref preserves the old value when ref validation fails", %{
      state: state,
      ets: ets
    } do
      key = "getset_blob_ref_invalid"
      missing_ref = BlobRef.encode!(BlobRef.from_payload("missing"))
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(%{}, {:getset_blob_ref, key, missing_ref}, state)

      assert {:error, {:blob_ref_unavailable, :enoent}} = result
      assert [{^key, "old", 0, _lfu, 0, 0, 3}] = :ets.lookup(ets, key)
    end

    test "APPEND blob ref appends the materialized suffix", %{
      state: state,
      ets: ets
    } do
      key = "append_blob_ref_existing"
      suffix = :binary.copy("suffix", 8)
      expected = "old" <> suffix
      expected_size = byte_size(expected)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, suffix)
      encoded_ref = BlobRef.encode!(ref)
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(%{}, {:append_blob_ref, key, encoded_ref}, state)

      assert result == {:ok, expected_size}
      assert [{^key, ^expected, 0, _lfu, _fid, _off, ^expected_size}] = :ets.lookup(ets, key)
    end

    test "APPEND blob ref preserves the old value when ref materialization fails", %{
      state: state,
      ets: ets
    } do
      key = "append_blob_ref_invalid"
      missing_ref = BlobRef.encode!(BlobRef.from_payload("missing"))
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(%{}, {:append_blob_ref, key, missing_ref}, state)

      assert {:error, {:blob_ref_unavailable, :enoent}} = result
      assert [{^key, "old", 0, _lfu, 0, 0, 3}] = :ets.lookup(ets, key)
    end

    test "SETRANGE blob ref applies the materialized patch", %{
      state: state,
      ets: ets
    } do
      key = "setrange_blob_ref_existing"
      patch = :binary.copy("R", 8)
      expected = "he" <> patch
      expected_size = byte_size(expected)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, patch)
      encoded_ref = BlobRef.encode!(ref)

      :ets.insert(
        ets,
        {key, "hello", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("hello")}
      )

      {_new_state, result} =
        StateMachine.apply(%{}, {:setrange_blob_ref, key, 2, encoded_ref}, state)

      assert result == {:ok, expected_size}
      assert [{^key, ^expected, 0, _lfu, _fid, _off, ^expected_size}] = :ets.lookup(ets, key)
    end

    test "SETRANGE blob ref preserves the old value when ref materialization fails", %{
      state: state,
      ets: ets
    } do
      key = "setrange_blob_ref_invalid"
      missing_ref = BlobRef.encode!(BlobRef.from_payload("missing"))

      :ets.insert(
        ets,
        {key, "hello", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("hello")}
      )

      {_new_state, result} =
        StateMachine.apply(%{}, {:setrange_blob_ref, key, 2, missing_ref}, state)

      assert {:error, {:blob_ref_unavailable, :enoent}} = result
      assert [{^key, "hello", 0, _lfu, 0, 0, 5}] = :ets.lookup(ets, key)
    end

    test "mixed batch SET blob ref is visible to later RMW commands", %{
      state: state,
      ets: ets
    } do
      key = "batch_set_blob_ref_read_your_write"
      payload = "blob-value"
      expected = payload <> "!"
      expected_size = byte_size(expected)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:batch, [{:set_blob_ref, key, encoded_ref, 0, set_opts(%{})}, {:append, key, "!"}]},
          state
        )

      assert result == {:ok, [:ok, {:ok, expected_size}]}
      assert [{^key, ^expected, 0, _lfu, _fid, _off, ^expected_size}] = :ets.lookup(ets, key)
    end

    test "mixed batch GETSET blob ref is visible to later RMW commands", %{
      state: state,
      ets: ets
    } do
      key = "batch_getset_blob_ref_read_your_write"
      payload = "new"
      expected = payload <> "!"
      expected_size = byte_size(expected)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:batch, [{:getset_blob_ref, key, encoded_ref}, {:append, key, "!"}]},
          state
        )

      assert result == {:ok, ["old", {:ok, expected_size}]}
      assert [{^key, ^expected, 0, _lfu, _fid, _off, ^expected_size}] = :ets.lookup(ets, key)
    end

    test "CAS blob ref stores the validated ref when expected value matches", %{
      state: state,
      ets: ets
    } do
      key = "cas_blob_ref_match"
      payload = :binary.copy("blob-cas", 32)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)
      encoded_ref_size = byte_size(encoded_ref)
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(%{}, {:cas_blob_ref, key, "old", encoded_ref, nil}, state)

      assert result == 1
      assert [{^key, nil, 0, _lfu, _fid, _off, ^encoded_ref_size}] = :ets.lookup(ets, key)
    end

    test "CAS blob ref mismatch skips validation and preserves the old value", %{
      state: state,
      ets: ets
    } do
      key = "cas_blob_ref_mismatch"
      missing_ref = BlobRef.encode!(BlobRef.from_payload("missing"))
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(%{}, {:cas_blob_ref, key, "other", missing_ref, nil}, state)

      assert result == 0
      assert [{^key, "old", 0, _lfu, 0, 0, 3}] = :ets.lookup(ets, key)
    end

    test "CAS blob ref preserves the old value when matching ref validation fails", %{
      state: state,
      ets: ets
    } do
      key = "cas_blob_ref_invalid"
      missing_ref = BlobRef.encode!(BlobRef.from_payload("missing"))
      :ets.insert(ets, {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")})

      {_new_state, result} =
        StateMachine.apply(%{}, {:cas_blob_ref, key, "old", missing_ref, nil}, state)

      assert {:error, {:blob_ref_unavailable, :enoent}} = result
      assert [{^key, "old", 0, _lfu, 0, 0, 3}] = :ets.lookup(ets, key)
    end

    test "compound put blob ref stores the validated ref", %{
      state: state,
      ets: ets
    } do
      compound_key = CompoundKey.hash_field("blob_hash", "field")
      payload = :binary.copy("blob-hash", 32)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)
      encoded_ref_size = byte_size(encoded_ref)

      {_new_state, result} =
        StateMachine.apply(%{}, {:compound_put_blob_ref, compound_key, encoded_ref, 0}, state)

      assert result == :ok

      assert [{^compound_key, nil, 0, _lfu, _fid, _off, ^encoded_ref_size}] =
               :ets.lookup(ets, compound_key)
    end

    test "compound put blob ref preserves the old value when validation fails", %{
      state: state,
      ets: ets
    } do
      compound_key = CompoundKey.hash_field("blob_hash_invalid", "field")
      missing_ref = BlobRef.encode!(BlobRef.from_payload("missing"))

      :ets.insert(
        ets,
        {compound_key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")}
      )

      {_new_state, result} =
        StateMachine.apply(%{}, {:compound_put_blob_ref, compound_key, missing_ref, 0}, state)

      assert {:error, {:blob_ref_unavailable, :enoent}} = result
      assert [{^compound_key, "old", 0, _lfu, 0, 0, 3}] = :ets.lookup(ets, compound_key)
    end

    test "locked put blob ref stores the validated ref for the lock owner", %{
      state: state,
      ets: ets
    } do
      key = "locked_blob_ref"
      payload = :binary.copy("blob-locked", 32)
      owner_ref = make_ref()
      apply_now = System.os_time(:millisecond)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)
      encoded_ref_size = byte_size(encoded_ref)

      locked_state = %{
        state
        | cross_shard_locks: %{
            key => {owner_ref, apply_now + 30_000}
          }
      }

      {_new_state, result} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:locked_put_blob_ref, key, encoded_ref, 0, owner_ref},
          locked_state
        )

      assert result == :ok
      assert [{^key, nil, 0, _lfu, _fid, _off, ^encoded_ref_size}] = :ets.lookup(ets, key)
    end

    test "locked put blob ref rejects non-owner before validating the ref", %{
      state: state,
      ets: ets
    } do
      key = "locked_blob_ref_wrong_owner"
      owner_ref = make_ref()
      apply_now = System.os_time(:millisecond)
      missing_ref = BlobRef.encode!(BlobRef.from_payload("missing"))

      :ets.insert(
        ets,
        {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")}
      )

      locked_state = %{
        state
        | cross_shard_locks: %{
            key => {owner_ref, apply_now + 30_000}
          }
      }

      {_new_state, result} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:locked_put_blob_ref, key, missing_ref, 0, make_ref()},
          locked_state
        )

      assert {:error, :key_locked} = result
      assert [{^key, "old", 0, _lfu, 0, 0, 3}] = :ets.lookup(ets, key)
    end

    test "locked put blob ref preserves old value when validation fails for owner", %{
      state: state,
      ets: ets
    } do
      key = "locked_blob_ref_invalid"
      owner_ref = make_ref()
      apply_now = System.os_time(:millisecond)
      missing_ref = BlobRef.encode!(BlobRef.from_payload("missing"))

      :ets.insert(
        ets,
        {key, "old", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("old")}
      )

      locked_state = %{
        state
        | cross_shard_locks: %{
            key => {owner_ref, apply_now + 30_000}
          }
      }

      {_new_state, result} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:locked_put_blob_ref, key, missing_ref, 0, owner_ref},
          locked_state
        )

      assert {:error, {:blob_ref_unavailable, :enoent}} = result
      assert [{^key, "old", 0, _lfu, 0, 0, 3}] = :ets.lookup(ets, key)
    end

    test "mixed batch locked put is visible to later RMW commands", %{
      state: state,
      ets: ets
    } do
      key = "locked_put_batch"
      expected_size = byte_size("v!")
      owner_ref = make_ref()
      apply_now = System.os_time(:millisecond)

      locked_state = %{
        state
        | cross_shard_locks: %{
            key => {owner_ref, apply_now + 30_000}
          }
      }

      {_new_state, result} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:batch, [{:locked_put, key, "v", 0, owner_ref}, {:append, key, "!"}]},
          locked_state
        )

      assert result == {:ok, [:ok, {:ok, expected_size}]}
      assert [{^key, "v!", 0, _lfu, _fid, _off, ^expected_size}] = :ets.lookup(ets, key)
    end

    test "mixed batch locked put blob ref is visible to later RMW commands", %{
      state: state,
      ets: ets
    } do
      key = "locked_blob_ref_batch"
      payload = "locked"
      expected = payload <> "!"
      expected_size = byte_size(expected)
      owner_ref = make_ref()
      apply_now = System.os_time(:millisecond)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)

      locked_state = %{
        state
        | cross_shard_locks: %{
            key => {owner_ref, apply_now + 30_000}
          }
      }

      {_new_state, result} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:batch, [{:locked_put_blob_ref, key, encoded_ref, 0, owner_ref}, {:append, key, "!"}]},
          locked_state
        )

      assert result == {:ok, [:ok, {:ok, expected_size}]}
      assert [{^key, ^expected, 0, _lfu, _fid, _off, ^expected_size}] = :ets.lookup(ets, key)
    end

    test "mixed batch compound put blob ref is visible to later RMW commands", %{
      state: state,
      ets: ets
    } do
      key = "blob_hash_batch"
      field = "field"
      compound_key = CompoundKey.hash_field(key, field)
      payload = "1"
      expected_size = byte_size("2")
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:batch,
           [{:compound_put_blob_ref, compound_key, encoded_ref, 0}, {:hincrby, key, field, 1}]},
          state
        )

      assert result == {:ok, [:ok, 2]}

      assert [{^compound_key, "2", 0, _lfu, _fid, _off, ^expected_size}] =
               :ets.lookup(ets, compound_key)
    end

    test "mixed batch compound blob batch put is visible to later RMW commands", %{
      state: state,
      ets: ets
    } do
      key = "blob_hash_batch_many"
      field = "field"
      compound_key = CompoundKey.hash_field(key, field)
      payload = "1"
      expected_size = byte_size("2")
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:batch,
           [
             {:compound_blob_batch_put, key, [{compound_key, encoded_ref, 0, :blob_ref}]},
             {:hincrby, key, field, 1}
           ]},
          state
        )

      assert result == {:ok, [:ok, 2]}

      assert [{^compound_key, "2", 0, _lfu, _fid, _off, ^expected_size}] =
               :ets.lookup(ets, compound_key)
    end

    test "mixed batch with multiple compound puts publishes every field after append", %{
      state: state,
      ets: ets
    } do
      redis_key = "compound_batch_many_puts"
      field_a = CompoundKey.hash_field(redis_key, "a")
      field_b = CompoundKey.hash_field(redis_key, "b")

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:batch,
           [
             {:compound_batch_put, redis_key, [{field_a, "va", 0}]},
             {:compound_batch_put, redis_key, [{field_b, "vb", 0}]}
           ]},
          state
        )

      assert result == {:ok, [:ok, :ok]}
      assert [{^field_a, "va", 0, _lfu, _fid, _off, 2}] = :ets.lookup(ets, field_a)
      assert [{^field_b, "vb", 0, _lfu, _fid, _off, 2}] = :ets.lookup(ets, field_b)
    end

    test "compound blob batch put stores inline and blob ref entries", %{
      state: state,
      ets: ets
    } do
      redis_key = "blob_hash_batch_put"
      small_field = CompoundKey.hash_field(redis_key, "small")
      large_field = CompoundKey.hash_field(redis_key, "large")
      payload = :binary.copy("blob-batch", 32)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)
      encoded_ref_size = byte_size(encoded_ref)

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:compound_blob_batch_put, redis_key,
           [{small_field, "v", 0, :value}, {large_field, encoded_ref, 0, :blob_ref}]},
          state
        )

      assert result == {:ok, [:ok, :ok]}
      assert [{^small_field, "v", 0, _lfu, _fid, _off, 1}] = :ets.lookup(ets, small_field)

      assert [{^large_field, nil, 0, _lfu, _fid, _off, ^encoded_ref_size}] =
               :ets.lookup(ets, large_field)
    end

    test "compound blob batch put preserves old values when ref validation fails", %{
      state: state,
      ets: ets
    } do
      redis_key = "blob_hash_batch_invalid"
      existing = CompoundKey.hash_field(redis_key, "existing")
      new_field = CompoundKey.hash_field(redis_key, "new")
      missing_ref = BlobRef.encode!(BlobRef.from_payload("missing"))

      {state2, {:ok, [:ok]}} =
        StateMachine.apply(%{}, {:compound_batch_put, redis_key, [{existing, "old", 0}]}, state)

      old_entry = :ets.lookup(ets, existing)

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:compound_blob_batch_put, redis_key,
           [{existing, missing_ref, 0, :blob_ref}, {new_field, "new", 0, :value}]},
          state2
        )

      assert {:error, {:blob_ref_unavailable, :enoent}} = result
      assert old_entry == :ets.lookup(ets, existing)
      assert [] == :ets.lookup(ets, new_field)
    end

    test "compound blob batch put Bitcask append errors keep existing fields visible", %{
      state: state,
      ets: ets
    } do
      redis_key = "blob_hash_batch_append_invalid"
      existing = CompoundKey.hash_field(redis_key, "existing")
      new_field = CompoundKey.hash_field(redis_key, "new")
      payload = :binary.copy("blob-batch-append", 32)
      assert {:ok, ref} = BlobStore.put(state.data_dir, state.shard_index, payload)
      encoded_ref = BlobRef.encode!(ref)

      {state2, {:ok, [:ok]}} =
        StateMachine.apply(%{}, {:compound_batch_put, redis_key, [{existing, "old", 0}]}, state)

      old_entry = :ets.lookup(ets, existing)
      file_id = 9_600_000 + :erlang.unique_integer([:positive])
      bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
      File.mkdir_p!(bad_active_path)
      bad_state = %{state2 | active_file_id: file_id, active_file_path: bad_active_path}

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:compound_blob_batch_put, redis_key,
           [{existing, encoded_ref, 0, :blob_ref}, {new_field, "new", 0, :value}]},
          bad_state
        )

      assert {:error, {:bitcask_append_failed, _reason}} = result
      assert old_entry == :ets.lookup(ets, existing)
      assert [] == :ets.lookup(ets, new_field)
    end
  end

  describe "apply/3 with {:append, key, suffix}" do
    test "APPEND treats a mismatched cold offset as missing", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      key = "append_stale_cold_offset"
      other_key = "append_other_cold_offset"

      {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
        NIF.v2_append_batch(active_file_path, [
          {other_key, "wrong-value", 0},
          {key, "right-value", 0}
        ])

      :ets.insert(
        ets,
        {key, nil, 0, Ferricstore.Store.LFU.initial(), 0, other_offset, value_size}
      )

      {_new_state, {:ok, 1}} = StateMachine.apply(%{}, {:append, key, "!"}, state)

      assert [{^key, "!", 0, _lfu, _fid, _off, 1}] = :ets.lookup(ets, key)
    end
  end

  # ---------------------------------------------------------------------------
  # apply/3 with :delete
  # ---------------------------------------------------------------------------
    end
  end
end
