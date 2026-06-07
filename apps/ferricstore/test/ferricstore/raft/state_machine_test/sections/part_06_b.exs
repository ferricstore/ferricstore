defmodule Ferricstore.Raft.StateMachineTest.Sections.Part06B do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.{BlobCommand, StateMachine}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

  describe "apply/3 with {:put, key, value, expire_at_ms} part 2" do
    test "cross-shard GET rejects mismatched cold offsets", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path,
      shard_index: shard_index
    } do
      key = "cross_cold_stale_offset"
      other_key = "cross_cold_other_offset"

      {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
        NIF.v2_append_batch(active_file_path, [
          {other_key, "wrong-value", 0},
          {key, "right-value", 0}
        ])

      :ets.insert(
        ets,
        {key, nil, 0, Ferricstore.Store.LFU.initial(), 0, other_offset, value_size}
      )

      {_new_state, %{^shard_index => [nil]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"GET", [key]}], nil}]},
          state
        )

      assert [{^key, nil, 0, _lfu, 0, ^other_offset, ^value_size}] = :ets.lookup(ets, key)
    end

    test "cross-shard PTTL reads cold metadata from valid file id zero", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path,
      shard_index: shard_index
    } do
      now = Ferricstore.HLC.now_ms()
      expire_at_ms = now + 5_000

      {:ok, {offset, value_size}} =
        NIF.v2_append_record(active_file_path, "cross_cold_meta_fid0", "cold-meta", expire_at_ms)

      :ets.insert(
        ets,
        {"cross_cold_meta_fid0", nil, expire_at_ms, Ferricstore.Store.LFU.initial(), 0, offset,
         value_size}
      )

      {_new_state, %{^shard_index => [5_000]}} =
        StateMachine.apply(
          %{system_time: now},
          {:cross_shard_tx, [{shard_index, [{"PTTL", ["cross_cold_meta_fid0"]}], nil}]},
          state
        )
    end

    test "cross-shard PTTL reads WARaft apply projection cold metadata", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      key = "cross_waraft_projection_meta"
      now = Ferricstore.HLC.now_ms()
      expire_at_ms = now + 5_000
      projection_index = 79

      assert :ok =
               Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                 state.data_dir,
                 shard_index,
                 projection_index,
                 [{key, "value", expire_at_ms}]
               )

      :ets.insert(
        ets,
        {key, nil, expire_at_ms, Ferricstore.Store.LFU.initial(),
         {:waraft_apply_projection, projection_index}, 0, byte_size("value")}
      )

      {_new_state, %{^shard_index => [5_000]}} =
        StateMachine.apply(
          %{system_time: now},
          {:cross_shard_tx, [{shard_index, [{"PTTL", [key]}], nil}]},
          state
        )
    end

    test "cross-shard HGETALL reads WARaft apply projection cold fields", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      redis_key = "cross_waraft_projection_hash"
      field_key = CompoundKey.hash_field(redis_key, "field")
      projection_index = 80

      assert :ok =
               Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                 state.data_dir,
                 shard_index,
                 projection_index,
                 [{field_key, "hash-value", 0}]
               )

      :ets.insert(
        ets,
        {field_key, nil, 0, Ferricstore.Store.LFU.initial(),
         {:waraft_apply_projection, projection_index}, 0, byte_size("hash-value")}
      )

      {_new_state, %{^shard_index => [["field", "hash-value"]]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"HGETALL", [redis_key]}], nil}]},
          state
        )
    end

    test "cross-shard GET purges malformed cold location instead of retrying exception path", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      :ets.insert(
        ets,
        {"cross_bad_offset", nil, 0, Ferricstore.Store.LFU.initial(), 0, :pending_offset, 5}
      )

      {_new_state, %{^shard_index => [nil]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"GET", ["cross_bad_offset"]}], nil}]},
          state
        )

      assert [] == :ets.lookup(ets, "cross_bad_offset")
    end

    test "cross-shard PTTL purges malformed cold location instead of retrying exception path", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      :ets.insert(
        ets,
        {"cross_bad_meta_offset", nil, Ferricstore.HLC.now_ms() + 5_000,
         Ferricstore.Store.LFU.initial(), 0, :pending_offset, 5}
      )

      {_new_state, %{^shard_index => [-2]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"PTTL", ["cross_bad_meta_offset"]}], nil}]},
          state
        )

      assert [] == :ets.lookup(ets, "cross_bad_meta_offset")
    end

    test "cross-shard read fallbacks report unavailable keydirs", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      handler_id = {__MODULE__, self(), make_ref()}
      parent = self()

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :store, :shard_unavailable],
          fn event, measurements, metadata, _config ->
            send(parent, {:sm_keydir_unavailable, event, measurements, metadata})
          end,
          nil
        )

      try do
        :ets.delete(ets)

        {_new_state, %{^shard_index => [nil, -2, 0]}} =
          StateMachine.apply(
            %{system_time: Ferricstore.HLC.now_ms()},
            {:cross_shard_tx,
             [
               {shard_index,
                [
                  {"GET", ["missing_keydir_get"]},
                  {"PTTL", ["missing_keydir_pttl"]},
                  {"HLEN", ["missing_keydir_hash"]}
                ], nil}
             ]},
            state
          )

        assert_keydir_unavailable_event(:cross_shard_get)
        assert_keydir_unavailable_event(:cross_shard_get_meta)
        assert_keydir_unavailable_event(:cross_shard_prefix_count)
      after
        :telemetry.detach(handler_id)
      end
    end

    test "cross-shard prefix delete reports unavailable keydirs" do
      source =
        Ferricstore.Test.SourceFiles.state_machine_source()

      [_, body] = String.split(source, "defp cross_shard_delete_prefix", parts: 2)
      body = body |> String.split("defp sm_file_path_from_path", parts: 2) |> hd()

      assert body =~ "emit_cross_shard_keydir_unavailable(ctx, :cross_shard_delete_prefix)",
             "cross_shard_delete_prefix/3 must emit shard_unavailable before returning :ok on a missing ETS keydir"
    end

    test "stamped ratelimit ignores legacy embedded now_ms", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000
      embedded_now = local_now + 30_000
      window_ms = 10_000

      {_new_state, ["allowed", 1, 9, ^window_ms]} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:ratelimit_add, "stamped_ratelimit", window_ms, 10, 1, embedded_now},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )

      expected_expire_at_ms = stamped_now + window_ms * 2

      assert [{"stamped_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "stamped_ratelimit")

      assert {1, ^stamped_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
    end

    test "stamped batch ratelimit ignores legacy embedded now_ms", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000
      embedded_now = local_now + 30_000
      window_ms = 10_000

      {_new_state, {:ok, [["allowed", 1, 9, ^window_ms]]}} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:batch,
            [{:ratelimit_add, "batch_stamped_ratelimit", window_ms, 10, 1, embedded_now}]},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )

      expected_expire_at_ms = stamped_now + window_ms * 2

      assert [{"batch_stamped_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "batch_stamped_ratelimit")

      assert {1, ^stamped_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
    end

    test "stamped ratelimit repairs malformed state with stamped time", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000
      window_ms = 10_000

      :ets.insert(
        ets,
        {"malformed_stamped_ratelimit", "bad-state", 0, Ferricstore.Store.LFU.initial(), 0, 0, 0}
      )

      {_new_state, ["allowed", 1, 9, ^window_ms]} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:ratelimit_add, "malformed_stamped_ratelimit", window_ms, 10, 1},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )

      expected_expire_at_ms = stamped_now + window_ms * 2

      assert [{"malformed_stamped_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "malformed_stamped_ratelimit")

      assert {1, ^stamped_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
    end

    test "legacy unwrapped ratelimit keeps embedded now_ms for replay compatibility", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      embedded_now = local_now - 30_000
      window_ms = 10_000

      {_new_state, ["allowed", 1, 9, ^window_ms]} =
        StateMachine.apply(
          %{},
          {:ratelimit_add, "legacy_ratelimit", window_ms, 10, 1, embedded_now},
          state
        )

      expected_expire_at_ms = embedded_now + window_ms * 2

      assert [{"legacy_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "legacy_ratelimit")

      assert {1, ^embedded_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
    end

    test "uses raft meta system_time when acquiring cross-shard locks", %{state: state} do
      local_now = Ferricstore.HLC.now_ms()
      apply_now = local_now - 20_000
      existing_lock_expiry = apply_now + 10_000
      existing_owner = make_ref()

      locked_state = %{
        state
        | cross_shard_locks: %{
            "meta_time_lock_conflict" => {existing_owner, existing_lock_expiry}
          }
      }

      {new_state, result} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:lock_keys, ["meta_time_lock_conflict"], make_ref(), apply_now + 30_000},
          locked_state
        )

      assert {:error, :keys_locked} = result

      assert %{"meta_time_lock_conflict" => {^existing_owner, ^existing_lock_expiry}} =
               new_state.cross_shard_locks
    end

    test "cross-shard control commands tolerate legacy state without lock maps", %{
      state: state
    } do
      apply_now = Ferricstore.HLC.now_ms()
      owner = make_ref()
      legacy_state = Map.drop(state, [:cross_shard_locks, :cross_shard_intents])

      {locked_state, :ok} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:lock_keys, ["legacy_lock"], owner, apply_now + 30_000},
          legacy_state
        )

      assert %{"legacy_lock" => {^owner, _expires_at}} = locked_state.cross_shard_locks

      {intent_state, :ok} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:cross_shard_intent, owner, %{0 => ["legacy_lock"]}},
          Map.drop(locked_state, [:cross_shard_intents])
        )

      assert %{^owner => %{0 => ["legacy_lock"]}} = intent_state.cross_shard_intents

      {cleared_state, :ok} =
        StateMachine.apply(%{system_time: apply_now}, {:clear_locks}, legacy_state)

      assert cleared_state.cross_shard_locks == %{}
      assert cleared_state.cross_shard_intents == %{}
    end

    test "uses raft meta system_time for standalone read-modify-write TTL checks", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      apply_now = local_now - 20_000
      expires_after_apply_time = apply_now + 10_000

      :ets.insert(
        ets,
        {"meta_time_incr", "5", expires_after_apply_time, Ferricstore.Store.LFU.initial(), 0, 0,
         byte_size("5")}
      )

      {_new_state, {:ok, 6}} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:incr, "meta_time_incr", 1},
          state
        )

      assert [{"meta_time_incr", "6", ^expires_after_apply_time, _, _, _, _}] =
               :ets.lookup(ets, "meta_time_incr")
    end

    test "missing active file fails put and rolls back new key", %{
      state: state,
      ets: ets
    } do
      missing_state = state_with_missing_active_file(state)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "missing_active_new", "value", 0}, missing_state)

      assert {:error, :active_file_unavailable} = result
      assert [] == :ets.lookup(ets, "missing_active_new")
    end

    test "missing active file fails overwrite and restores old ETS entry", %{
      state: state,
      ets: ets
    } do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "missing_active_existing", "old", 0}, state)
      old_entry = :ets.lookup(ets, "missing_active_existing")
      missing_state = state_with_missing_active_file(state2)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "missing_active_existing", "new", 0}, missing_state)

      assert {:error, :active_file_unavailable} = result
      assert old_entry == :ets.lookup(ets, "missing_active_existing")
    end

    test "missing state active file falls back to live ActiveFile registry", %{
      state: state,
      ets: ets,
      dir: dir,
      shard_index: shard_index
    } do
      file_id = 8_000_000 + :erlang.unique_integer([:positive])
      live_path = Path.join(dir, "#{file_id}.log")
      File.touch!(live_path)
      Ferricstore.Store.ActiveFile.publish(shard_index, file_id, live_path, dir)

      missing_state = state_with_missing_active_file(state, shard_index: shard_index)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "missing_active_fallback", "value", 0}, missing_state)

      assert :ok = result

      assert [{"missing_active_fallback", "value", 0, _, ^file_id, offset, value_size}] =
               :ets.lookup(ets, "missing_active_fallback")

      assert is_integer(offset)
      assert value_size > 0
    end

    test "uses live ActiveFile registry when state active file is stale but still exists", %{
      state: state,
      ets: ets,
      dir: dir,
      shard_index: shard_index
    } do
      live_file_id = 8_100_000 + :erlang.unique_integer([:positive])
      live_path = Path.join(dir, "#{live_file_id}.log")
      File.touch!(live_path)
      Ferricstore.Store.ActiveFile.publish(shard_index, live_file_id, live_path, dir)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "stale_active_registry", "value", 0}, state)

      assert :ok = result

      assert [{"stale_active_registry", "value", 0, _, ^live_file_id, offset, value_size}] =
               :ets.lookup(ets, "stale_active_registry")

      assert is_integer(offset)
      assert value_size > 0
      assert {:ok, "value"} = NIF.v2_pread_at(live_path, offset)
    end

    test "Bitcask append errors fail quorum apply and roll back pending ETS", %{
      state: state,
      ets: ets
    } do
      file_id = 9_000_000 + :erlang.unique_integer([:positive])
      bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
      File.mkdir_p!(bad_active_path)

      bad_state = %{state | active_file_id: file_id, active_file_path: bad_active_path}

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "append_error_key", "value", 0}, bad_state)

      assert {:error, {:bitcask_append_failed, _reason}} = result
      assert [] == :ets.lookup(ets, "append_error_key")
    end

    test "batch Bitcask append errors restore deleted ETS entries and remove new puts", %{
      state: state,
      ets: ets
    } do
      {state2, :ok} =
        StateMachine.apply(%{}, {:put, "delete_failure_existing", "old_value", 0}, state)

      old_entry = :ets.lookup(ets, "delete_failure_existing")

      file_id = 9_100_000 + :erlang.unique_integer([:positive])
      bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
      File.mkdir_p!(bad_active_path)
      bad_state = %{state2 | active_file_id: file_id, active_file_path: bad_active_path}

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:batch,
           [
             {:delete, "delete_failure_existing"},
             {:put, "delete_failure_new", "new_value", 0}
           ]},
          bad_state
        )

      assert {:error, {:bitcask_append_failed, _reason}} = result
      assert old_entry == :ets.lookup(ets, "delete_failure_existing")
      assert [] == :ets.lookup(ets, "delete_failure_new")
    end

    test "put_batch Bitcask append errors restore originals and remove new puts", %{
      state: state,
      ets: ets
    } do
      {state2, :ok} =
        StateMachine.apply(%{}, {:put, "put_batch_failure_existing", "old_value", 0}, state)

      old_entry = :ets.lookup(ets, "put_batch_failure_existing")

      file_id = 9_200_000 + :erlang.unique_integer([:positive])
      bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
      File.mkdir_p!(bad_active_path)
      bad_state = %{state2 | active_file_id: file_id, active_file_path: bad_active_path}

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:put_batch,
           [
             {"put_batch_failure_existing", "new_value", 0},
             {"put_batch_failure_new", "new_value", 0}
           ]},
          bad_state
        )

      assert {:error, {:bitcask_append_failed, _reason}} = result
      assert old_entry == :ets.lookup(ets, "put_batch_failure_existing")
      assert [] == :ets.lookup(ets, "put_batch_failure_new")
    end

    test "delete_batch Bitcask append errors keep existing keys visible", %{
      state: state,
      ets: ets
    } do
      {state2, {:ok, [:ok, :ok]}} =
        StateMachine.apply(
          %{},
          {:put_batch,
           [
             {"delete_batch_failure_existing_a", "old_a", 0},
             {"delete_batch_failure_existing_b", "old_b", 0}
           ]},
          state
        )

      old_a = :ets.lookup(ets, "delete_batch_failure_existing_a")
      old_b = :ets.lookup(ets, "delete_batch_failure_existing_b")

      file_id = 9_300_000 + :erlang.unique_integer([:positive])
      bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
      File.mkdir_p!(bad_active_path)
      bad_state = %{state2 | active_file_id: file_id, active_file_path: bad_active_path}

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:delete_batch,
           [
             "delete_batch_failure_existing_a",
             "delete_batch_failure_existing_b",
             "delete_batch_failure_missing"
           ]},
          bad_state
        )

      assert {:error, {:bitcask_append_failed, _reason}} = result
      assert old_a == :ets.lookup(ets, "delete_batch_failure_existing_a")
      assert old_b == :ets.lookup(ets, "delete_batch_failure_existing_b")
      assert [] == :ets.lookup(ets, "delete_batch_failure_missing")
    end

    test "compound_batch_put Bitcask append errors keep existing fields visible", %{
      state: state,
      ets: ets
    } do
      redis_key = "compound_batch_failure_hash"
      existing = CompoundKey.hash_field(redis_key, "existing")
      new_field = CompoundKey.hash_field(redis_key, "new")

      {state2, {:ok, [:ok]}} =
        StateMachine.apply(%{}, {:compound_batch_put, redis_key, [{existing, "old", 0}]}, state)

      old_entry = :ets.lookup(ets, existing)

      file_id = 9_400_000 + :erlang.unique_integer([:positive])
      bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
      File.mkdir_p!(bad_active_path)
      bad_state = %{state2 | active_file_id: file_id, active_file_path: bad_active_path}

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:compound_batch_put, redis_key, [{existing, "new", 0}, {new_field, "new", 0}]},
          bad_state
        )

      assert {:error, {:bitcask_append_failed, _reason}} = result
      assert old_entry == :ets.lookup(ets, existing)
      assert [] == :ets.lookup(ets, new_field)
    end

  end
    end
  end
end
