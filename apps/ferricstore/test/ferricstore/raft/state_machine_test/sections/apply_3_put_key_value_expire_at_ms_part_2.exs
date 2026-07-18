defmodule Ferricstore.Raft.StateMachineTest.Sections.Apply3PutKeyValueExpireAtMsPart2 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      describe "apply/3 with {:put, key, value, expire_at_ms} part 2" do
        test "transaction GET rejects mismatched cold offsets", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
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

          {_new_state, result} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"GET", [key]}], nil},
              state
            )

          assert {:error, {:state_read_failed, _reason}} = result

          assert [{^key, nil, 0, _lfu, 0, ^other_offset, ^value_size}] = :ets.lookup(ets, key)
        end

        test "transaction PTTL reads cold metadata from valid file id zero", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          now = Ferricstore.HLC.now_ms()
          expire_at_ms = now + 5_000

          {:ok, {offset, value_size}} =
            NIF.v2_append_record(
              active_file_path,
              "cross_cold_meta_fid0",
              "cold-meta",
              expire_at_ms
            )

          :ets.insert(
            ets,
            {"cross_cold_meta_fid0", nil, expire_at_ms, Ferricstore.Store.LFU.initial(), 0,
             offset, value_size}
          )

          {_new_state, [5_000]} =
            StateMachine.apply(
              %{system_time: now},
              {:tx_execute, [{"PTTL", ["cross_cold_meta_fid0"]}], nil},
              state
            )
        end

        test "transaction PTTL reads WARaft apply projection cold metadata", %{
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

          {_new_state, [5_000]} =
            StateMachine.apply(
              %{system_time: now},
              {:tx_execute, [{"PTTL", [key]}], nil},
              state
            )
        end

        test "transaction HGETALL reads WARaft apply projection cold fields", %{
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

          Ferricstore.Store.Shard.CompoundMemberIndex.put(
            state.compound_member_index_name,
            field_key
          )

          {_new_state, [["field", "hash-value"]]} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"HGETALL", [redis_key]}], nil},
              state
            )
        end

        test "transaction GET fences malformed cold locations without deleting metadata",
             %{
               state: state,
               ets: ets
             } do
          entry =
            {"cross_bad_offset", nil, 0, Ferricstore.Store.LFU.initial(), 0, :pending_offset, 5}

          :ets.insert(ets, entry)

          {_new_state, result} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"GET", ["cross_bad_offset"]}], nil},
              state
            )

          assert {:error, {:state_read_failed, _reason}} = result
          assert [entry] == :ets.lookup(ets, "cross_bad_offset")
        end

        test "transaction PTTL fences malformed cold locations without deleting metadata",
             %{
               state: state,
               ets: ets
             } do
          entry =
            {"cross_bad_meta_offset", nil, Ferricstore.HLC.now_ms() + 5_000,
             Ferricstore.Store.LFU.initial(), 0, :pending_offset, 5}

          :ets.insert(ets, entry)

          {_new_state, result} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"PTTL", ["cross_bad_meta_offset"]}], nil},
              state
            )

          assert {:error, {:state_read_failed, _reason}} = result
          assert [entry] == :ets.lookup(ets, "cross_bad_meta_offset")
        end

        test "transaction read fallbacks report unavailable keydirs", %{
          state: state,
          ets: ets
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

            {_new_state, result} =
              StateMachine.apply(
                %{system_time: Ferricstore.HLC.now_ms()},
                {:tx_execute,
                 [
                   {"GET", ["missing_keydir_get"]},
                   {"PTTL", ["missing_keydir_pttl"]},
                   {"HLEN", ["missing_keydir_hash"]}
                 ], nil},
                state
              )

            assert {:error, {:state_read_failed, :keydir_unavailable}} = result
            assert_keydir_unavailable_event(:cross_shard_get)
            assert_keydir_unavailable_event(:cross_shard_get_meta)
            assert_keydir_unavailable_event(:cross_shard_exists)
          after
            :telemetry.detach(handler_id)
          end
        end

        test "transaction SETRANGE enforces the replicated size limit", %{
          state: state,
          ets: ets
        } do
          key = "tx_setrange_replicated_size_limit"
          context = Ferricstore.Raft.ApplyContext.new(max_value_size: 4)

          state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          {_new_state, result} =
            StateMachine.apply(
              %{system_time: Ferricstore.HLC.now_ms()},
              {:tx_execute, [{"SETRANGE", [key, "4", "x"]}], nil},
              state
            )

          assert {:error, "ERR value too large (5 bytes, max 4 bytes)"} = result
          assert [] == :ets.lookup(ets, key)
        end

        test "stamped rate-limit rejects an embedded apply timestamp", %{
          state: state,
          ets: ets
        } do
          local_now = Ferricstore.HLC.now_ms()
          stamped_now = local_now - 30_000
          embedded_now = local_now + 30_000
          window_ms = 10_000
          command = {:ratelimit_add, "stamped_ratelimit", window_ms, 10, 1, embedded_now}

          assert {^state, {:error, {:unknown_command, ^command}}} =
                   StateMachine.apply(
                     %{system_time: local_now},
                     {command, %{hlc_ts: {stamped_now, 0}, wall_time_ms: stamped_now}},
                     state
                   )

          assert [] == :ets.lookup(ets, "stamped_ratelimit")
        end

        test "stamped batch rate-limit rejects an embedded apply timestamp", %{
          state: state,
          ets: ets
        } do
          local_now = Ferricstore.HLC.now_ms()
          stamped_now = local_now - 30_000
          embedded_now = local_now + 30_000
          window_ms = 10_000

          command =
            {:ratelimit_add, "batch_stamped_ratelimit", window_ms, 10, 1, embedded_now}

          {_new_state, {:ok, [{:error, {:unknown_command, ^command}}]}} =
            StateMachine.apply(
              %{system_time: local_now},
              {{:batch, [command]}, %{hlc_ts: {stamped_now, 0}, wall_time_ms: stamped_now}},
              state
            )

          assert [] == :ets.lookup(ets, "batch_stamped_ratelimit")
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
            {"malformed_stamped_ratelimit", "bad-state", 0, Ferricstore.Store.LFU.initial(), 0, 0,
             0}
          )

          {_new_state, ["allowed", 1, 9, ^window_ms]} =
            StateMachine.apply(
              %{system_time: local_now},
              {{:ratelimit_add, "malformed_stamped_ratelimit", window_ms, 10, 1},
               %{hlc_ts: {stamped_now, 0}, wall_time_ms: stamped_now}},
              state
            )

          expected_expire_at_ms = stamped_now + window_ms * 2

          assert [{"malformed_stamped_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
                   :ets.lookup(ets, "malformed_stamped_ratelimit")

          assert {1, ^stamped_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
        end

        @tag :ratelimit_i64_precision
        test "stamped ratelimit preserves counters above float integer precision", %{
          state: state,
          ets: ets
        } do
          key = "stamped_ratelimit_i64_precision"
          apply_now = Ferricstore.HLC.now_ms() - 30_000
          window_ms = 60_000
          previous_count = 9_007_199_254_740_993

          encoded =
            Ferricstore.Store.ValueCodec.encode_ratelimit(
              previous_count,
              apply_now - window_ms,
              0
            )

          :ets.insert(
            ets,
            {key, encoded, 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(encoded)}
          )

          {_new_state, ["denied", ^previous_count, 0, ^window_ms]} =
            StateMachine.apply(
              %{system_time: apply_now},
              {{:ratelimit_add, key, window_ms, previous_count, 1},
               %{hlc_ts: {apply_now, 0}, wall_time_ms: apply_now}},
              state
            )
        end

        @tag :ratelimit_denied_noop
        test "non-rotating ratelimit denials avoid replicated storage writes", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path
        } do
          key = "ratelimit_denied_noop"
          apply_now = Ferricstore.HLC.now_ms()
          window_ms = 60_000
          expire_at_ms = apply_now + window_ms * 2
          encoded = Ferricstore.Store.ValueCodec.encode_ratelimit(10, apply_now, 0)

          :ets.insert(
            ets,
            {key, encoded, expire_at_ms, Ferricstore.Store.LFU.initial(), 0, 0,
             byte_size(encoded)}
          )

          {new_state, ["denied", 10, 0, ^window_ms]} =
            StateMachine.apply(
              %{system_time: apply_now},
              {{:ratelimit_add, key, window_ms, 10, 1},
               %{hlc_ts: {apply_now, 0}, wall_time_ms: apply_now}},
              state
            )

          assert new_state.active_file_size == state.active_file_size
          assert {:ok, %{size: 0}} = File.stat(active_file_path)
        end

        test "rejects rate-limit commands that embed an apply timestamp", %{
          state: state,
          ets: ets
        } do
          window_ms = 10_000
          embedded_now = Ferricstore.HLC.now_ms() - 30_000
          command = {:ratelimit_add, "embedded_time_ratelimit", window_ms, 10, 1, embedded_now}

          assert {^state, {:error, {:unknown_command, ^command}}} =
                   StateMachine.apply(%{}, command, state)

          assert [] == :ets.lookup(ets, "embedded_time_ratelimit")
        end

        @tag :fetch_or_compute_state_machine
        test "publishing a fetch-or-compute value consumes the lease atomically", %{
          state: state,
          ets: ets
        } do
          apply_now = Ferricstore.HLC.now_ms()
          key = "fetch-or-compute-publish-once"
          owner = "fetch-or-compute-publisher"
          outcome_key = Ferricstore.FetchOrCompute.Outcome.key(key)

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:fetch_or_compute_lock, key, outcome_key, owner, apply_now + 30_000},
              state
            )

          {published_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now + 1},
              {:fetch_or_compute_publish, key, "first", 0, owner},
              locked_state
            )

          refute Map.has_key?(published_state.fetch_or_compute_locks, key)
          assert [{^key, "first", 0, _, _, _, _}] = :ets.lookup(ets, key)

          {replayed_state, {:error, :key_not_locked}} =
            StateMachine.apply(
              %{system_time: apply_now + 2},
              {:fetch_or_compute_publish, key, "replayed", 0, owner},
              published_state
            )

          assert replayed_state.fetch_or_compute_locks == published_state.fetch_or_compute_locks
          assert [{^key, "first", 0, _, _, _, _}] = :ets.lookup(ets, key)
        end

        @tag :fetch_or_compute_state_machine
        test "fetch-or-compute release removes the exact lease and expiry entry", %{state: state} do
          apply_now = Ferricstore.HLC.now_ms()
          key = "fetch-or-compute-release"
          owner = "fetch-or-compute-release-owner"
          expire_at = apply_now + 30_000
          outcome_key = Ferricstore.FetchOrCompute.Outcome.key(key)

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:fetch_or_compute_lock, key, outcome_key, owner, expire_at},
              state
            )

          assert {:value, ^owner} =
                   :gb_trees.lookup(
                     {expire_at, key},
                     locked_state.fetch_or_compute_lock_expiries
                   )

          {unchanged_state, {:error, :not_lock_owner}} =
            StateMachine.apply(
              %{system_time: apply_now + 1},
              {:fetch_or_compute_release, key, "other-owner"},
              locked_state
            )

          assert unchanged_state.fetch_or_compute_locks == locked_state.fetch_or_compute_locks

          {released_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now + 2},
              {:fetch_or_compute_release, key, owner},
              locked_state
            )

          refute Map.has_key?(released_state.fetch_or_compute_locks, key)

          assert :none =
                   :gb_trees.lookup(
                     {expire_at, key},
                     released_state.fetch_or_compute_lock_expiries
                   )
        end

        @tag :fetch_or_compute_state_machine
        test "a fetch-or-compute lease does not disable unrelated put batches", %{
          state: state,
          ets: ets
        } do
          apply_now = Ferricstore.HLC.now_ms()
          locked_key = "fetch-or-compute-locked"
          owner = "fetch-or-compute-batch-owner"
          outcome_key = Ferricstore.FetchOrCompute.Outcome.key(locked_key)

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:fetch_or_compute_lock, locked_key, outcome_key, owner, apply_now + 30_000},
              state
            )

          {written_state, {:ok, [:ok, :ok]}} =
            StateMachine.apply(
              %{system_time: apply_now + 1},
              {:put_batch, [{"unrelated-a", "a", 0}, {"unrelated-b", "b", 0}]},
              locked_state
            )

          assert [{_, "a", 0, _, _, _, _}] = :ets.lookup(ets, "unrelated-a")
          assert [{_, "b", 0, _, _, _, _}] = :ets.lookup(ets, "unrelated-b")
          assert {^owner, _expire_at} = written_state.fetch_or_compute_locks[locked_key]
        end

        @tag :fetch_or_compute_state_machine
        test "publishes a fetch-or-compute failure and releases its lease atomically", %{
          state: state,
          ets: ets
        } do
          apply_now = Ferricstore.HLC.now_ms()
          key = "fetch-or-compute-failure"
          owner = "fetch-or-compute-owner"
          outcome_key = Ferricstore.FetchOrCompute.Outcome.key(key)
          {:ok, encoded_error} = Ferricstore.FetchOrCompute.Outcome.encode_error("failed")

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:fetch_or_compute_lock, key, outcome_key, owner, apply_now + 30_000},
              state
            )

          {failed_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now + 1},
              {:fetch_or_compute_fail, key, outcome_key, encoded_error, apply_now + 5_000, owner},
              locked_state
            )

          refute Map.has_key?(failed_state.fetch_or_compute_locks, key)

          assert [{^outcome_key, ^encoded_error, outcome_expire_at, _, _, _, _}] =
                   :ets.lookup(ets, outcome_key)

          assert outcome_expire_at == apply_now + 5_000
        end

        @tag :fetch_or_compute_state_machine
        test "a new fetch-or-compute lease clears the previous failure outcome", %{
          state: state,
          ets: ets
        } do
          apply_now = Ferricstore.HLC.now_ms()
          key = "fetch-or-compute-retry"
          first_owner = "first-owner"
          next_owner = "next-owner"
          outcome_key = Ferricstore.FetchOrCompute.Outcome.key(key)
          {:ok, encoded_error} = Ferricstore.FetchOrCompute.Outcome.encode_error("retry")

          {first_locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:fetch_or_compute_lock, key, outcome_key, first_owner, apply_now + 30_000},
              state
            )

          {failed_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now + 1},
              {:fetch_or_compute_fail, key, outcome_key, encoded_error, apply_now + 5_000,
               first_owner},
              first_locked_state
            )

          assert :ets.lookup(ets, outcome_key) != []

          {next_locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now + 2},
              {:fetch_or_compute_lock, key, outcome_key, next_owner, apply_now + 30_002},
              failed_state
            )

          assert [] = :ets.lookup(ets, outcome_key)
          assert {^next_owner, _expire_at_ms} = next_locked_state.fetch_or_compute_locks[key]
        end

        @tag :fetch_or_compute_state_machine
        test "fetch-or-compute lock rejects an outcome key not derived from the cache key", %{
          state: state,
          ets: ets
        } do
          apply_now = Ferricstore.HLC.now_ms()
          key = "fetch-or-compute-invalid-lock-outcome"
          unrelated_key = "unrelated-lock-outcome"
          owner = "invalid-lock-outcome-owner"

          :ets.insert(
            ets,
            {unrelated_key, "preserve-me", 0, Ferricstore.Store.LFU.initial(), 0, 0, 11}
          )

          {unchanged_state, {:error, "ERR invalid fetch_or_compute outcome key"}} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:fetch_or_compute_lock, key, unrelated_key, owner, apply_now + 30_000},
              state
            )

          refute Map.has_key?(unchanged_state.fetch_or_compute_locks, key)

          assert [{^unrelated_key, "preserve-me", 0, _, _, _, 11}] =
                   :ets.lookup(ets, unrelated_key)
        end

        @tag :fetch_or_compute_state_machine
        test "fetch-or-compute failure rejects an outcome key not derived from the cache key", %{
          state: state,
          ets: ets
        } do
          apply_now = Ferricstore.HLC.now_ms()
          key = "fetch-or-compute-invalid-failure-outcome"
          outcome_key = Ferricstore.FetchOrCompute.Outcome.key(key)
          unrelated_key = "unrelated-failure-outcome"
          owner = "invalid-failure-outcome-owner"
          {:ok, encoded_error} = Ferricstore.FetchOrCompute.Outcome.encode_error("failed")

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:fetch_or_compute_lock, key, outcome_key, owner, apply_now + 30_000},
              state
            )

          {unchanged_state, {:error, "ERR invalid fetch_or_compute outcome key"}} =
            StateMachine.apply(
              %{system_time: apply_now + 1},
              {:fetch_or_compute_fail, key, unrelated_key, encoded_error, apply_now + 5_000,
               owner},
              locked_state
            )

          assert {^owner, _expire_at_ms} = unchanged_state.fetch_or_compute_locks[key]
          assert [] = :ets.lookup(ets, unrelated_key)
        end

        @tag :fetch_or_compute_state_machine
        test "fetch-or-compute failure rejects a malformed replicated outcome", %{
          state: state,
          ets: ets
        } do
          apply_now = Ferricstore.HLC.now_ms()
          key = "fetch-or-compute-invalid-failure-payload"
          outcome_key = Ferricstore.FetchOrCompute.Outcome.key(key)
          owner = "invalid-failure-payload-owner"

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:fetch_or_compute_lock, key, outcome_key, owner, apply_now + 30_000},
              state
            )

          {unchanged_state, {:error, "ERR invalid fetch_or_compute outcome"}} =
            StateMachine.apply(
              %{system_time: apply_now + 1},
              {:fetch_or_compute_fail, key, outcome_key, "malformed", apply_now + 5_000, owner},
              locked_state
            )

          assert {^owner, _expire_at_ms} = unchanged_state.fetch_or_compute_locks[key]
          assert [] = :ets.lookup(ets, outcome_key)
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
            {"meta_time_incr", "5", expires_after_apply_time, Ferricstore.Store.LFU.initial(), 0,
             0, byte_size("5")}
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
          {state2, :ok} =
            StateMachine.apply(%{}, {:put, "missing_active_existing", "old", 0}, state)

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

        test "ignores an ActiveFile row owned by another shard data directory", %{
          state: state,
          ets: ets,
          active_file_path: active_file_path,
          shard_index: shard_index
        } do
          unrelated_dir =
            Path.join(
              System.tmp_dir!(),
              "sm_unrelated_active_#{System.unique_integer([:positive])}"
            )

          unrelated_path = Path.join(unrelated_dir, "00099.log")
          File.mkdir_p!(unrelated_dir)
          File.touch!(unrelated_path)

          Ferricstore.Store.ActiveFile.publish(
            shard_index,
            99,
            unrelated_path,
            unrelated_dir
          )

          on_exit(fn ->
            Ferricstore.Store.ActiveFile.delete(shard_index)
            File.rm_rf!(unrelated_dir)
          end)

          {_new_state, result} =
            StateMachine.apply(%{}, {:put, "isolated_active_file", "value", 0}, state)

          assert :ok = result

          assert [{"isolated_active_file", "value", 0, _, 0, offset, value_size}] =
                   :ets.lookup(ets, "isolated_active_file")

          assert value_size > 0
          assert {:ok, "value"} = NIF.v2_pread_at(active_file_path, offset)
          assert {:ok, %{size: 0}} = File.stat(unrelated_path)
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
            StateMachine.apply(
              %{},
              {:compound_batch_put, redis_key, [{existing, "old", 0}]},
              state
            )

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

        test "promoted batches reject shared marker and dedicated member writes atomically", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          redis_key = "compound_batch_mixed_target_hash"
          marker = Promotion.marker_key(redis_key)
          member = CompoundKey.hash_field(redis_key, "field")
          dedicated_path = Promotion.dedicated_path(state.data_dir, shard_index, :hash, redis_key)
          File.mkdir_p!(dedicated_path)
          File.touch!(Path.join(dedicated_path, "00000.log"))

          :ets.insert(ets, {marker, "hash", 0, LFU.initial(), 0, 0, 4})

          promoted_state = %{
            state
            | promoted_instances: %{redis_key => %{path: dedicated_path}}
          }

          {_new_state, result} =
            StateMachine.apply(
              %{},
              {:compound_batch_put, redis_key, [{marker, "hash", 0}, {member, "value", 0}]},
              promoted_state
            )

          assert {:error, :mixed_compound_batch_targets} = result
          assert [{^marker, "hash", _, _, _, _, _}] = :ets.lookup(ets, marker)
          assert [] == :ets.lookup(ets, member)
        end
      end
    end
  end
end
