defmodule Ferricstore.Raft.StateMachineTest.Sections.BatchValueLimits do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, ListOps}
      alias Ferricstore.Store.Shard.CompoundMemberIndex

      describe "replicated batch value limits" do
        test "put_batch preserves per-entry results on the fast path", %{
          state: state,
          ets: ets
        } do
          state = batch_value_limit_state(state, 4)
          valid_key = "batch-limit:valid"
          oversized_key = "batch-limit:oversized"
          error = {:error, "ERR value too large (5 bytes, max 4 bytes)"}

          {_new_state, result} =
            StateMachine.apply(
              %{},
              {:put_batch, [{valid_key, "1234", 0}, {oversized_key, "12345", 0}]},
              state
            )

          assert {:ok, [:ok, ^error]} = result

          assert [{^valid_key, "1234", 0, _lfu, _file_id, _offset, 4}] =
                   :ets.lookup(ets, valid_key)

          assert [] == :ets.lookup(ets, oversized_key)
        end

        test "put_batch fallback enforces the same value limit", %{
          state: state,
          ets: ets
        } do
          key = "batch-limit:fallback"
          type_key = CompoundKey.type_key(key)

          {state, :ok} =
            StateMachine.apply(%{}, {:compound_put, type_key, "hash", 0}, state)

          original_type = :ets.lookup(ets, type_key)
          state = batch_value_limit_state(state, 4)

          {_new_state, result} =
            StateMachine.apply(%{}, {:put_batch, [{key, "12345", 0}]}, state)

          assert {:ok, [{:error, "ERR value too large (5 bytes, max 4 bytes)"}]} = result
          assert [] == :ets.lookup(ets, key)
          assert original_type == :ets.lookup(ets, type_key)
        end

        test "put_batch filters oversized values before blob preparation", %{
          state: state,
          ets: ets
        } do
          state =
            state
            |> batch_value_limit_state(4)
            |> Map.put(:blob_side_channel_threshold_bytes, 1)

          valid_key = "batch-limit:blob-derived:valid"
          oversized_key = "batch-limit:blob-derived:oversized"
          error = {:error, "ERR value too large (5 bytes, max 4 bytes)"}

          {_new_state, result} =
            StateMachine.apply(
              %{},
              {:put_batch, [{valid_key, "1234", 0}, {oversized_key, "12345", 0}]},
              state
            )

          assert {:ok, [:ok, ^error]} = result

          assert [{^valid_key, nil, 0, _lfu, _file_id, _offset, encoded_size}] =
                   :ets.lookup(ets, valid_key)

          assert BlobRef.encoded_size?(encoded_size)
          assert [] == :ets.lookup(ets, oversized_key)
        end

        test "mixed blob preparation preserves inline member lock rejection", %{
          state: state,
          ets: ets
        } do
          locked_key = "batch-lock:mixed:inline"
          blob_key = "batch-lock:mixed:blob"

          state = %{
            state
            | blob_side_channel_threshold_bytes: 4,
              fetch_or_compute_locks: %{
                locked_key => {make_ref(), Ferricstore.HLC.now_ms() + 60_000}
              }
          }

          {_new_state, result} =
            StateMachine.apply(
              %{},
              {:put_batch, [{locked_key, "v", 0}, {blob_key, "12345", 0}]},
              state
            )

          assert {:ok, [{:error, :key_locked}, :ok]} = result
          assert [] == :ets.lookup(ets, locked_key)

          assert [{^blob_key, nil, 0, _lfu, _file_id, _offset, encoded_size}] =
                   :ets.lookup(ets, blob_key)

          assert BlobRef.encoded_size?(encoded_size)
        end

        test "MSET and MSETNX reject an oversized member atomically", %{
          state: state,
          ets: ets
        } do
          state = batch_value_limit_state(state, 4)
          error = {:error, "ERR value too large (5 bytes, max 4 bytes)"}

          for {operation, prefix} <- [{:mset, "mset"}, {:msetnx, "msetnx"}] do
            valid_key = "batch-limit:#{prefix}:valid"
            oversized_key = "batch-limit:#{prefix}:oversized"

            {_new_state, result} =
              StateMachine.apply(
                %{},
                {operation, [{valid_key, "1234", 0}, {oversized_key, "12345", 0}]},
                state
              )

            assert ^error = result
            assert [] == :ets.lookup(ets, valid_key)
            assert [] == :ets.lookup(ets, oversized_key)
          end
        end

        test "plain atomic batches accept the exact replicated value limit", %{
          state: state,
          ets: ets
        } do
          state = batch_value_limit_state(state, 4)

          assert {_state, :ok} =
                   StateMachine.apply(
                     %{},
                     {:mset, [{"batch-limit:mset:exact", "1234", 0}]},
                     state
                   )

          assert {_state, 1} =
                   StateMachine.apply(
                     %{},
                     {:msetnx, [{"batch-limit:msetnx:exact", "1234", 0}]},
                     state
                   )

          assert [{"batch-limit:mset:exact", "1234", 0, _lfu, _file_id, _offset, 4}] =
                   :ets.lookup(ets, "batch-limit:mset:exact")

          assert [{"batch-limit:msetnx:exact", "1234", 0, _lfu, _file_id, _offset, 4}] =
                   :ets.lookup(ets, "batch-limit:msetnx:exact")
        end

        test "MSETNX validates every value before its existence precondition", %{
          state: state,
          ets: ets
        } do
          existing_key = "batch-limit:msetnx:existing"

          assert {_state, :ok} =
                   StateMachine.apply(%{}, {:put, existing_key, "old", 0}, state)

          state = batch_value_limit_state(state, 4)

          {_new_state, result} =
            StateMachine.apply(
              %{},
              {:msetnx,
               [
                 {existing_key, "1234", 0},
                 {"batch-limit:msetnx:precondition-oversized", "12345", 0}
               ]},
              state
            )

          assert {:error, "ERR value too large (5 bytes, max 4 bytes)"} = result

          assert [{^existing_key, "old", 0, _lfu, _file_id, _offset, 3}] =
                   :ets.lookup(ets, existing_key)

          assert [] == :ets.lookup(ets, "batch-limit:msetnx:precondition-oversized")
        end

        test "blob batches validate materialized ref size before publication", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          state = batch_value_limit_state(state, 4)
          assert {:ok, ref} = BlobStore.put(state.data_dir, shard_index, "12345")
          encoded_ref = BlobRef.encode!(ref)
          error = {:error, "ERR value too large (5 bytes, max 4 bytes)"}

          for {command, key} <- [
                {{:put_blob_batch, [{"batch-limit:blob:put", encoded_ref, 0, :blob_ref}]},
                 "batch-limit:blob:put"},
                {{:mset_blob_batch, [{"batch-limit:blob:mset", encoded_ref, 0, :blob_ref}]},
                 "batch-limit:blob:mset"},
                {{:msetnx_blob_batch, [{"batch-limit:blob:msetnx", encoded_ref, 0, :blob_ref}]},
                 "batch-limit:blob:msetnx"}
              ] do
            {_new_state, result} = StateMachine.apply(%{}, command, state)
            assert ^error = result
            assert [] == :ets.lookup(ets, key)
          end
        end

        test "blob batches accept refs at the exact replicated value limit", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          state = batch_value_limit_state(state, 4)
          assert {:ok, ref} = BlobStore.put(state.data_dir, shard_index, "1234")
          encoded_ref = BlobRef.encode!(ref)

          assert {_state, {:ok, [:ok]}} =
                   StateMachine.apply(
                     %{},
                     {:put_blob_batch,
                      [{"batch-limit:blob:exact:put", encoded_ref, 0, :blob_ref}]},
                     state
                   )

          assert {_state, :ok} =
                   StateMachine.apply(
                     %{},
                     {:mset_blob_batch,
                      [{"batch-limit:blob:exact:mset", encoded_ref, 0, :blob_ref}]},
                     state
                   )

          assert {_state, 1} =
                   StateMachine.apply(
                     %{},
                     {:msetnx_blob_batch,
                      [{"batch-limit:blob:exact:msetnx", encoded_ref, 0, :blob_ref}]},
                     state
                   )

          for key <- [
                "batch-limit:blob:exact:put",
                "batch-limit:blob:exact:mset",
                "batch-limit:blob:exact:msetnx"
              ] do
            assert [{^key, nil, 0, _lfu, _file_id, _offset, encoded_size}] =
                     :ets.lookup(ets, key)

            assert BlobRef.encoded_size?(encoded_size)
          end
        end

        test "MSETNX validates blob ref size before its existence precondition", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          existing_key = "batch-limit:blob:msetnx:existing"

          assert {_state, :ok} =
                   StateMachine.apply(%{}, {:put, existing_key, "old", 0}, state)

          state = batch_value_limit_state(state, 4)
          assert {:ok, ref} = BlobStore.put(state.data_dir, shard_index, "12345")
          encoded_ref = BlobRef.encode!(ref)

          {_new_state, result} =
            StateMachine.apply(
              %{},
              {:msetnx_blob_batch,
               [
                 {existing_key, "1234", 0, :value},
                 {"batch-limit:blob:msetnx:precondition-oversized", encoded_ref, 0, :blob_ref}
               ]},
              state
            )

          assert {:error, "ERR value too large (5 bytes, max 4 bytes)"} = result

          assert [{^existing_key, "old", 0, _lfu, _file_id, _offset, 3}] =
                   :ets.lookup(ets, existing_key)

          assert [] == :ets.lookup(ets, "batch-limit:blob:msetnx:precondition-oversized")
        end

        test "MSETNX validates blob ref encoding before its existence precondition", %{
          state: state,
          ets: ets
        } do
          existing_key = "batch-limit:blob:msetnx:invalid-ref-existing"
          invalid_ref = String.duplicate("x", BlobRef.encoded_size())

          assert {_state, :ok} =
                   StateMachine.apply(%{}, {:put, existing_key, "old", 0}, state)

          {_new_state, result} =
            StateMachine.apply(
              %{},
              {:msetnx_blob_batch,
               [
                 {existing_key, "value", 0, :value},
                 {"batch-limit:blob:msetnx:invalid-ref", invalid_ref, 0, :blob_ref}
               ]},
              state
            )

          assert {:error, {:blob_ref_unavailable, :invalid_blob_ref}} = result

          assert [{^existing_key, "old", 0, _lfu, _file_id, _offset, 3}] =
                   :ets.lookup(ets, existing_key)

          assert [] == :ets.lookup(ets, "batch-limit:blob:msetnx:invalid-ref")
        end

        @tag :compound_batch_value_limits
        test "compound batches reject an oversized member atomically", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          state = batch_value_limit_state(state, 4)
          error = {:error, "ERR value too large (5 bytes, max 4 bytes)"}

          for {command_tag, representation, prefix} <- [
                {:compound_batch_put, :plain, "plain"},
                {:compound_blob_batch_put, :value, "blob-inline"}
              ] do
            redis_key = "batch-limit:compound:#{prefix}"
            valid = CompoundKey.hash_field(redis_key, "valid")
            oversized = CompoundKey.hash_field(redis_key, "oversized")

            command =
              case representation do
                :plain ->
                  {command_tag, redis_key, [{valid, "1234", 0}, {oversized, "12345", 0}]}

                :value ->
                  {command_tag, redis_key,
                   [{valid, "1234", 0, :value}, {oversized, "12345", 0, :value}]}
              end

            assert {_state, ^error} = StateMachine.apply(%{}, command, state)
            assert [] == :ets.lookup(ets, valid)
            assert [] == :ets.lookup(ets, oversized)
          end

          redis_key = "batch-limit:compound:blob-ref"
          ref_key = CompoundKey.hash_field(redis_key, "oversized")
          assert {:ok, ref} = BlobStore.put(state.data_dir, shard_index, "12345")

          assert {_state, ^error} =
                   StateMachine.apply(
                     %{},
                     {:compound_blob_batch_put, redis_key,
                      [{ref_key, BlobRef.encode!(ref), 0, :blob_ref}]},
                     state
                   )

          assert [] == :ets.lookup(ets, ref_key)

          exact_key = CompoundKey.hash_field("batch-limit:compound:exact", "field")

          assert {_state, {:ok, [:ok]}} =
                   StateMachine.apply(
                     %{},
                     {:compound_batch_put, "batch-limit:compound:exact",
                      [{exact_key, "1234", 0}]},
                     state
                   )

          assert [{^exact_key, "1234", 0, _lfu, _fid, _offset, 4}] =
                   :ets.lookup(ets, exact_key)
        end

        @tag :compound_batch_apply_budget
        test "compound batch put rejects member fanout before mutation", %{
          state: state,
          ets: ets
        } do
          redis_key = "batch-budget:compound-put"
          first = CompoundKey.hash_field(redis_key, "first")
          second = CompoundKey.hash_field(redis_key, "second")

          context = Ferricstore.Raft.ApplyContext.new(compound_member_apply_budget: 1)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          assert {_state, {:error, :compound_member_apply_budget_exceeded}} =
                   StateMachine.apply(
                     %{},
                     {:compound_batch_put, redis_key,
                      [{first, "one", 0}, {second, "two", 0}]},
                     limited_state
                   )

          assert [] == :ets.lookup(ets, first)
          assert [] == :ets.lookup(ets, second)

          assert {:ok, []} =
                   CompoundMemberIndex.keys_for_prefix(
                     limited_state.compound_member_index_name,
                     CompoundKey.hash_prefix(redis_key)
                   )
        end

        @tag :compound_batch_apply_budget
        test "compound batch delete rejects member fanout before mutation", %{
          state: state,
          ets: ets
        } do
          redis_key = "batch-budget:compound-delete"
          first = CompoundKey.hash_field(redis_key, "first")
          second = CompoundKey.hash_field(redis_key, "second")
          entries = [{first, "one", 0}, {second, "two", 0}]

          assert {seeded_state, {:ok, [:ok, :ok]}} =
                   StateMachine.apply(
                     %{},
                     {:compound_batch_put, redis_key, entries},
                     state
                   )

          rows_before = Map.new([first, second], &{&1, :ets.lookup(ets, &1)})
          prefix = CompoundKey.hash_prefix(redis_key)

          assert {:ok, keys_before} =
                   CompoundMemberIndex.keys_for_prefix(
                     seeded_state.compound_member_index_name,
                     prefix
                   )

          context = Ferricstore.Raft.ApplyContext.new(compound_member_apply_budget: 1)

          limited_state = %{
            seeded_state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          assert {_state, {:error, :compound_member_apply_budget_exceeded}} =
                   StateMachine.apply(
                     %{},
                     {:compound_batch_delete, redis_key, [first, second]},
                     limited_state
                   )

          assert rows_before == Map.new([first, second], &{&1, :ets.lookup(ets, &1)})

          assert {:ok, ^keys_before} =
                   CompoundMemberIndex.keys_for_prefix(
                     limited_state.compound_member_index_name,
                     prefix
                   )
        end

        @tag :compound_batch_apply_budget
        test "compound blob batch put rejects member fanout before materialization", %{
          state: state,
          ets: ets
        } do
          redis_key = "batch-budget:compound-blob-put"
          first = CompoundKey.hash_field(redis_key, "first")
          second = CompoundKey.hash_field(redis_key, "second")

          context = Ferricstore.Raft.ApplyContext.new(compound_member_apply_budget: 1)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          assert {_state, {:error, :compound_member_apply_budget_exceeded}} =
                   StateMachine.apply(
                     %{},
                     {:compound_blob_batch_put, redis_key,
                      [{first, "one", 0, :value}, {second, "two", 0, :value}]},
                     limited_state
                   )

          assert [] == :ets.lookup(ets, first)
          assert [] == :ets.lookup(ets, second)

          assert {:ok, []} =
                   CompoundMemberIndex.keys_for_prefix(
                     limited_state.compound_member_index_name,
                     CompoundKey.hash_prefix(redis_key)
                   )
        end

        @tag :compound_batch_apply_budget
        test "list rebalance admits its cumulative delete and put footprint", %{
          state: state,
          ets: ets
        } do
          redis_key = "batch-budget:list-rebalance"
          type_key = CompoundKey.type_key(redis_key)
          meta_key = CompoundKey.list_meta_key(redis_key)
          first = CompoundKey.list_element(redis_key, 0)
          second = CompoundKey.list_element(redis_key, 1)

          entries = [
            {type_key, "list", 0},
            {meta_key, ListOps.encode_meta({2, -1, 2}), 0},
            {first, "a", 0},
            {second, "b", 0}
          ]

          assert {seeded_state, {:ok, [:ok, :ok, :ok, :ok]}} =
                   StateMachine.apply(
                     %{},
                     {:compound_batch_put, redis_key, entries},
                     state
                   )

          keys = Enum.map(entries, &elem(&1, 0))
          rows_before = Map.new(keys, &{&1, :ets.lookup(ets, &1)})
          prefix = CompoundKey.list_prefix(redis_key)

          assert {:ok, members_before} =
                   CompoundMemberIndex.keys_for_prefix(
                     seeded_state.compound_member_index_name,
                     prefix
                   )

          context = Ferricstore.Raft.ApplyContext.new(compound_member_apply_budget: 5)

          limited_state = %{
            seeded_state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          assert {_state, {:error, :compound_member_apply_budget_exceeded}} =
                   StateMachine.apply(
                     %{},
                     {:list_op, redis_key, {:linsert, :after, "a", "x"}},
                     limited_state
                   )

          assert rows_before == Map.new(keys, &{&1, :ets.lookup(ets, &1)})

          assert {:ok, ^members_before} =
                   CompoundMemberIndex.keys_for_prefix(
                     limited_state.compound_member_index_name,
                     prefix
                   )
        end

        @tag :compound_batch_apply_budget
        test "rejected compound batches still advance release cursor accounting", %{
          state: state
        } do
          redis_key = "batch-budget:release-cursor"
          first = CompoundKey.hash_field(redis_key, "first")
          second = CompoundKey.hash_field(redis_key, "second")
          context = Ferricstore.Raft.ApplyContext.new(compound_member_apply_budget: 1)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context),
              release_cursor_interval: 1
          }

          meta = %{index: 42, term: 1, system_time: Ferricstore.HLC.now_ms()}

          assert {new_state,
                  {:applied_at, 42, {:error, :compound_member_apply_budget_exceeded}}, effects} =
                   StateMachine.apply(
                     meta,
                     {:compound_batch_put, redis_key,
                      [{first, "one", 0}, {second, "two", 0}]},
                     limited_state
                   )

          assert new_state.applied_count == limited_state.applied_count + 1
          assert new_state.pending_release_cursor_index == 42
          assert Enum.any?(effects, &match?({:send_msg, _, {:locally_applied, 42}, _}, &1))
        end

        @tag :compound_batch_apply_budget
        test "compound batch admission rejects improper tails deterministically", %{
          state: state,
          ets: ets
        } do
          redis_key = "batch-budget:improper"
          compound_key = CompoundKey.hash_field(redis_key, "field")
          context = Ferricstore.Raft.ApplyContext.new(compound_member_apply_budget: 2)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          commands = [
            {:compound_batch_put, redis_key, [{compound_key, "value", 0} | :invalid_tail]},
            {:compound_blob_batch_put, redis_key,
             [{compound_key, "value", 0, :value} | :invalid_tail]},
            {:compound_batch_delete, redis_key, [compound_key | :invalid_tail]}
          ]

          Enum.each(commands, fn command ->
            assert {_state, {:error, :invalid_compound_batch_entry}} =
                     StateMachine.apply(%{}, command, limited_state)

            assert [] == :ets.lookup(ets, compound_key)
          end)
        end

        @tag :compound_batch_apply_budget
        test "compound batch member budget is inclusive", %{state: state, ets: ets} do
          redis_key = "batch-budget:inclusive"
          first = CompoundKey.hash_field(redis_key, "first")
          second = CompoundKey.hash_field(redis_key, "second")
          context = Ferricstore.Raft.ApplyContext.new(compound_member_apply_budget: 2)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          assert {_state, {:ok, [:ok, :ok]}} =
                   StateMachine.apply(
                     %{},
                     {:compound_batch_put, redis_key,
                      [{first, "one", 0}, {second, "two", 0}]},
                     limited_state
                   )

          assert [{^first, "one", 0, _lfu, _file_id, _offset, 3}] = :ets.lookup(ets, first)
          assert [{^second, "two", 0, _lfu, _file_id, _offset, 3}] = :ets.lookup(ets, second)
        end

        @tag :compound_batch_value_limits
        test "promoted compound batches validate all values before append", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          redis_key = "batch-limit:promoted-compound"
          existing = CompoundKey.hash_field(redis_key, "existing")
          new_field = CompoundKey.hash_field(redis_key, "new")

          {state, _log_path} =
            promoted_single_fixture(state, ets, shard_index, redis_key, :hash, [
              {existing, "old", 0}
            ])

          state = batch_value_limit_state(state, 4)
          original = :ets.lookup(ets, existing)

          assert {_state, {:error, "ERR value too large (5 bytes, max 4 bytes)"}} =
                   StateMachine.apply(
                     %{},
                     {:compound_batch_put, redis_key,
                      [{existing, "12345", 0}, {new_field, "1234", 0}]},
                     state
                   )

          assert original == :ets.lookup(ets, existing)
          assert [] == :ets.lookup(ets, new_field)

          assert {_state, {:error, "ERR value too large (5 bytes, max 4 bytes)"}} =
                   StateMachine.apply(
                     %{},
                     {:compound_put, existing, "12345", 0},
                     state
                   )

          assert original == :ets.lookup(ets, existing)
        end

        @tag :segment_value_limit_projection
        test "segment projection preflights value limits before any prefix mutation", %{
          state: state,
          ets: ets,
          shard_index: shard_index
        } do
          state = batch_value_limit_state(state, 4)
          position = {:raft_log_pos, 42, 1}
          error = {:error, "ERR value too large (5 bytes, max 4 bytes)"}
          batch_valid = "segment-unit:batch-valid"
          batch_oversized = "segment-unit:batch-oversized"

          assert :unsupported =
                   WARaftStorage.__segment_project_command_for_test__(
                     {:put_batch, [{batch_valid, "1234", 0}, {batch_oversized, "12345", 0}]},
                     position,
                     state
                   )

          assert [] == :ets.lookup(ets, batch_valid)
          assert [] == :ets.lookup(ets, batch_oversized)

          generic_valid = "segment-unit:generic-valid"
          generic_oversized = "segment-unit:generic-oversized"

          assert :unsupported =
                   WARaftStorage.__segment_project_command_for_test__(
                     {:batch,
                      [
                        {:put, generic_valid, "1234", 0},
                        {:put, generic_oversized, "12345", 0}
                      ]},
                     position,
                     state
                   )

          assert [] == :ets.lookup(ets, generic_valid)
          assert [] == :ets.lookup(ets, generic_oversized)

          redis_key = "segment-unit:compound"
          compound_valid = CompoundKey.hash_field(redis_key, "valid")
          compound_oversized = CompoundKey.hash_field(redis_key, "oversized")

          assert :unsupported =
                   WARaftStorage.__segment_project_command_for_test__(
                     {:compound_batch_put, redis_key,
                      [{compound_valid, "1234", 0}, {compound_oversized, "12345", 0}]},
                     position,
                     state
                   )

          assert [] == :ets.lookup(ets, compound_valid)
          assert [] == :ets.lookup(ets, compound_oversized)

          assert {:ok, ref} = BlobStore.put(state.data_dir, shard_index, "12345")
          encoded_ref = BlobRef.encode!(ref)

          assert {:ok, _same_state, ^error, 0} =
                   WARaftStorage.__segment_project_command_for_test__(
                     {:put_blob_batch,
                      [{"segment-unit:blob-oversized", encoded_ref, 0, :blob_ref}]},
                     position,
                     state
                   )

          assert [] == :ets.lookup(ets, "segment-unit:blob-oversized")

          exact_key = "segment-unit:exact"

          assert {:ok, _new_state, :ok, 1} =
                   WARaftStorage.__segment_project_command_for_test__(
                     {:put, exact_key, "1234", 0},
                     position,
                     state
                   )

          assert [{^exact_key, "1234", 0, _lfu, {:waraft_segment, 42}, _offset, 4}] =
                   :ets.lookup(ets, exact_key)
        end

        @tag :segment_compound_count_failure
        test "segment projection falls back when exact compound count cleanup is bounded", %{
          state: state,
          ets: ets
        } do
          redis_key = "segment-count-bounded"
          prefix = CompoundKey.hash_prefix(redis_key)
          projected_key = CompoundKey.hash_field(redis_key, "new")
          index = state.compound_member_index_name
          expired_at_ms = Ferricstore.HLC.now_ms() - 1

          context =
            Ferricstore.Raft.ApplyContext.new(
              compound_member_apply_budget: 1,
              promotion_threshold: 1
            )

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          CompoundMemberIndex.ensure_table!(index)
          CompoundMemberIndex.reset(index)
          on_exit(fn -> safe_delete_ets(index) end)

          Enum.each(["a", "b"], fn field ->
            compound_key = CompoundKey.hash_field(redis_key, field)
            :ets.insert(ets, {compound_key, field, expired_at_ms, LFU.initial(), 0, 0, 1})
            CompoundMemberIndex.put(index, compound_key, expired_at_ms)
          end)

          assert :unsupported =
                   WARaftStorage.__segment_project_command_for_test__(
                     {:compound_put, projected_key, "value", 0},
                     {:raft_log_pos, 43, 1},
                     limited_state
                   )

          assert [] == :ets.lookup(ets, projected_key)
        end
      end

      defp batch_value_limit_state(state, max_value_size) do
        context = Ferricstore.Raft.ApplyContext.new(max_value_size: max_value_size)

        %{
          state
          | apply_context: context,
            apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
        }
      end
    end
  end
end
