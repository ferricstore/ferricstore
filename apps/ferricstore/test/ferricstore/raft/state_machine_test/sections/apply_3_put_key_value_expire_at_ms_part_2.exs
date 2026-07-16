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
            assert_keydir_unavailable_event(:cross_shard_prefix_count)
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

          assert [{:error, "ERR value too large (5 bytes, max 4 bytes)"}] = result
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
                     {command, %{hlc_ts: {stamped_now, 0}}},
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
              {{:batch, [command]}, %{hlc_ts: {stamped_now, 0}}},
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
               %{hlc_ts: {stamped_now, 0}}},
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
              {{:ratelimit_add, key, window_ms, previous_count, 1}, %{hlc_ts: {apply_now, 0}}},
              state
            )
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

        test "rejects cross-shard intents without the current watch-token contract", %{
          state: state
        } do
          owner = make_ref()

          malformed_intent = %{
            command: :rename,
            keys: %{source: "source", dest: "destination"},
            status: :executing,
            created_at: Ferricstore.HLC.now_ms()
          }

          assert {unchanged_state, {:error, :invalid_cross_shard_intent}} =
                   StateMachine.apply(
                     %{system_time: Ferricstore.HLC.now_ms()},
                     {:cross_shard_intent, owner, malformed_intent},
                     state
                   )

          assert unchanged_state.cross_shard_intents == %{}
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

        @tag :cross_shard_expiry_index
        test "maintains an ordered expiry index for cross-shard locks", %{state: state} do
          apply_now = Ferricstore.HLC.now_ms()
          expire_at = apply_now + 30_000
          owner = "ordered-expiry-owner"

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:lock_keys, ["indexed-a", "indexed-b"], owner, expire_at},
              state
            )

          assert {:value, ^owner} =
                   :gb_trees.lookup(
                     {expire_at, "indexed-a"},
                     locked_state.cross_shard_lock_expiries
                   )

          {unlocked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:unlock_keys, ["indexed-a"], owner},
              locked_state
            )

          assert :none =
                   :gb_trees.lookup(
                     {expire_at, "indexed-a"},
                     unlocked_state.cross_shard_lock_expiries
                   )

          assert {:value, ^owner} =
                   :gb_trees.lookup(
                     {expire_at, "indexed-b"},
                     unlocked_state.cross_shard_lock_expiries
                   )
        end

        @tag :strict_key_lock_renewal
        test "renews every live owned lock and replaces its expiry index entry", %{state: state} do
          apply_now = Ferricstore.HLC.now_ms()
          old_expiry = apply_now + 1_000
          new_expiry = apply_now + 30_000
          owner = "renew-owner"

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:lock_keys, ["renew-a", "renew-b"], owner, old_expiry},
              state
            )

          {renewed_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now + 1},
              {:renew_key_locks, ["renew-a", "renew-b"], owner, new_expiry},
              locked_state
            )

          assert renewed_state.cross_shard_locks["renew-a"] == {owner, new_expiry}
          assert renewed_state.cross_shard_locks["renew-b"] == {owner, new_expiry}

          for key <- ["renew-a", "renew-b"] do
            assert :none =
                     :gb_trees.lookup({old_expiry, key}, renewed_state.cross_shard_lock_expiries)

            assert {:value, ^owner} =
                     :gb_trees.lookup({new_expiry, key}, renewed_state.cross_shard_lock_expiries)
          end
        end

        @tag :strict_key_lock_renewal
        test "rejects an expired owned lock without changing any requested lock", %{state: state} do
          apply_now = Ferricstore.HLC.now_ms()
          owner = "expired-renew-owner"
          old_expiry = apply_now + 10

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:lock_keys, ["expired-renew-a", "expired-renew-b"], owner, old_expiry},
              state
            )

          {rejected_state, {:error, :not_lock_owner}} =
            StateMachine.apply(
              %{system_time: old_expiry},
              {:renew_key_locks, ["expired-renew-a", "expired-renew-b"], owner,
               old_expiry + 30_000},
              locked_state
            )

          assert rejected_state.cross_shard_locks == locked_state.cross_shard_locks

          assert rejected_state.cross_shard_lock_expiries ==
                   locked_state.cross_shard_lock_expiries
        end

        @tag :strict_key_lock_renewal
        test "rejects mixed ownership atomically", %{state: state} do
          apply_now = Ferricstore.HLC.now_ms()
          expiry = apply_now + 30_000

          {first_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:lock_keys, ["mixed-renew-a"], "owner-a", expiry},
              state
            )

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:lock_keys, ["mixed-renew-b"], "owner-b", expiry},
              first_state
            )

          {rejected_state, {:error, :not_lock_owner}} =
            StateMachine.apply(
              %{system_time: apply_now + 1},
              {:renew_key_locks, ["mixed-renew-a", "mixed-renew-b"], "owner-a", expiry + 1_000},
              locked_state
            )

          assert rejected_state.cross_shard_locks == locked_state.cross_shard_locks

          assert rejected_state.cross_shard_lock_expiries ==
                   locked_state.cross_shard_lock_expiries
        end

        @tag :strict_key_lock_renewal
        test "rejects a missing requested lock atomically", %{state: state} do
          apply_now = Ferricstore.HLC.now_ms()
          expiry = apply_now + 30_000
          owner = "missing-renew-owner"

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:lock_keys, ["present-renew"], owner, expiry},
              state
            )

          {rejected_state, {:error, :not_lock_owner}} =
            StateMachine.apply(
              %{system_time: apply_now + 1},
              {:renew_key_locks, ["present-renew", "missing-renew"], owner, expiry + 1_000},
              locked_state
            )

          assert rejected_state.cross_shard_locks == locked_state.cross_shard_locks

          assert rejected_state.cross_shard_lock_expiries ==
                   locked_state.cross_shard_lock_expiries
        end

        @tag :cross_shard_expiry_index
        test "reacquires an expired requested lock after a bounded expiry prune", %{state: state} do
          apply_now = Ferricstore.HLC.now_ms()

          expired =
            Map.new(1..300, fn index ->
              {"expired-#{index}", {"old-owner-#{index}", apply_now - 2}}
            end)
            |> Map.put("target", {"stale-target-owner", apply_now - 1})

          expiry_index =
            Enum.reduce(expired, :gb_trees.empty(), fn {key, {owner, expire_at}}, index ->
              :gb_trees.enter({expire_at, key}, owner, index)
            end)

          indexed_state = %{
            state
            | cross_shard_locks: expired,
              cross_shard_lock_expiries: expiry_index
          }

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:lock_keys, ["target"], "new-owner", apply_now + 30_000},
              indexed_state
            )

          assert {"new-owner", new_expiry} = locked_state.cross_shard_locks["target"]
          assert new_expiry > apply_now

          assert {:value, "new-owner"} =
                   :gb_trees.lookup(
                     {new_expiry, "target"},
                     locked_state.cross_shard_lock_expiries
                   )

          assert map_size(locked_state.cross_shard_locks) > 1
        end

        @tag :cross_shard_expiry_index
        test "cross-shard lock acquisition does not rebuild the full lock map" do
          source =
            File.read!(
              Path.expand(
                "lib/ferricstore/raft/state_machine/sections/data_mutations.ex",
                File.cwd!()
              )
            )

          refute source =~ "Map.reject(locks",
                 "lock acquisition must prune through the ordered expiry index, not scan every lock"
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

          refute Map.has_key?(published_state.cross_shard_locks, key)
          assert [{^key, "first", 0, _, _, _, _}] = :ets.lookup(ets, key)

          {replayed_state, {:error, :key_not_locked}} =
            StateMachine.apply(
              %{system_time: apply_now + 2},
              {:fetch_or_compute_publish, key, "replayed", 0, owner},
              published_state
            )

          assert replayed_state.cross_shard_locks == published_state.cross_shard_locks
          assert [{^key, "first", 0, _, _, _, _}] = :ets.lookup(ets, key)
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

          refute Map.has_key?(failed_state.cross_shard_locks, key)

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
          assert {^next_owner, _expire_at_ms} = next_locked_state.cross_shard_locks[key]
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

          refute Map.has_key?(unchanged_state.cross_shard_locks, key)

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

          assert {^owner, _expire_at_ms} = unchanged_state.cross_shard_locks[key]
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

          assert {^owner, _expire_at_ms} = unchanged_state.cross_shard_locks[key]
          assert [] = :ets.lookup(ets, outcome_key)
        end

        test "cross-shard control commands maintain current lock and intent state", %{
          state: state
        } do
          apply_now = Ferricstore.HLC.now_ms()
          owner = make_ref()
          key = "current_lock"

          {locked_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:lock_keys, [key], owner, apply_now + 30_000},
              state
            )

          assert %{^key => {^owner, _expires_at}} = locked_state.cross_shard_locks

          intent = %{
            command: :test,
            keys: %{target: key},
            value_hashes: %{key => nil},
            status: :executing,
            created_at: apply_now
          }

          {intent_state, :ok} =
            StateMachine.apply(
              %{system_time: apply_now},
              {:cross_shard_intent, owner, intent},
              locked_state
            )

          assert %{^owner => ^intent} = intent_state.cross_shard_intents

          {cleared_state, :ok} =
            StateMachine.apply(%{system_time: apply_now}, {:clear_locks}, intent_state)

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
