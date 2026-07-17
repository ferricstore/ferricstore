defmodule Ferricstore.Raft.StateMachineTest.Sections.BatchValueLimits do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, ListOps}
      alias Ferricstore.Store.Shard.{CompoundMemberIndex, ZSetIndex}

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
                     {:compound_batch_put, redis_key, [{first, "one", 0}, {second, "two", 0}]},
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

          assert {new_state, {:applied_at, 42, {:error, :compound_member_apply_budget_exceeded}},
                  effects} =
                   StateMachine.apply(
                     meta,
                     {:compound_batch_put, redis_key, [{first, "one", 0}, {second, "two", 0}]},
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
            assert {_state, {:error, :invalid_batch_command_list}} =
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
                     {:compound_batch_put, redis_key, [{first, "one", 0}, {second, "two", 0}]},
                     limited_state
                   )

          assert [{^first, "one", 0, _lfu, _file_id, _offset, 3}] = :ets.lookup(ets, first)
          assert [{^second, "two", 0, _lfu, _file_id, _offset, 3}] = :ets.lookup(ets, second)
        end

        @tag :generic_batch_apply_budget
        test "generic batches share one compound member budget", %{state: state, ets: ets} do
          context = Ferricstore.Raft.ApplyContext.new(compound_member_apply_budget: 1)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          for {suffix, wrap} <- [
                {"flat", & &1},
                {"nested", &[{:batch, &1}]}
              ] do
            redis_key = "batch-budget:generic:#{suffix}"
            first = CompoundKey.hash_field(redis_key, "first")
            second = CompoundKey.hash_field(redis_key, "second")

            commands = [
              {:compound_batch_put, redis_key, [{first, "one", 0}]},
              {:compound_batch_put, redis_key, [{second, "two", 0}]}
            ]

            assert {_state, {:error, :compound_member_apply_budget_exceeded}} =
                     StateMachine.apply(%{}, {:batch, wrap.(commands)}, limited_state)

            assert [] == :ets.lookup(ets, first)
            assert [] == :ets.lookup(ets, second)
          end
        end

        @tag :generic_batch_apply_budget
        test "generic batches count singleton compound mutations cumulatively", %{
          state: state,
          ets: ets
        } do
          redis_key = "batch-budget:generic-singletons"
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
                     {:batch,
                      [
                        {:compound_put, first, "one", 0},
                        {:compound_put, second, "two", 0}
                      ]},
                     limited_state
                   )

          assert [] == :ets.lookup(ets, first)
          assert [] == :ets.lookup(ets, second)
        end

        @tag :generic_batch_apply_budget
        test "generic batches admit inner item footprints before dispatch", %{
          state: state,
          ets: ets
        } do
          context =
            Ferricstore.Raft.ApplyContext.new(
              batch_command_apply_budget: 1,
              compound_member_apply_budget: 10
            )

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          commands = [
            {:mset,
             [
               {"batch-budget:generic-mset:first", "one", 0},
               {"batch-budget:generic-mset:second", "two", 0}
             ]},
            {:pfadd, "batch-budget:generic-pfadd", ["first", "second"]},
            {:list_op, "batch-budget:generic-lpush", {:lpush, ["first", "second"]}}
          ]

          Enum.each(commands, fn command ->
            assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                     StateMachine.apply(%{}, {:batch, [command]}, limited_state)
          end)

          for key <- [
                "batch-budget:generic-mset:first",
                "batch-budget:generic-mset:second",
                "batch-budget:generic-pfadd"
              ] do
            assert [] == :ets.lookup(ets, key)
          end

          assert [] ==
                   :ets.lookup(
                     ets,
                     CompoundKey.type_key("batch-budget:generic-lpush")
                   )
        end

        @tag :generic_batch_apply_budget
        test "apply work admission follows async and trace wrappers", %{
          state: state,
          ets: ets
        } do
          context = Ferricstore.Raft.ApplyContext.new(batch_command_apply_budget: 1)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          for {suffix, command} <- [
                {"async",
                 {:async, :remote_budget_origin,
                  {:mset,
                   [
                     {"batch-budget:wrapped:async:first", "one", 0},
                     {"batch-budget:wrapped:async:second", "two", 0}
                   ]}}},
                {"trace",
                 {:ferricstore_latency_trace,
                  {:mset,
                   [
                     {"batch-budget:wrapped:trace:first", "one", 0},
                     {"batch-budget:wrapped:trace:second", "two", 0}
                   ]}}}
              ] do
            assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                     StateMachine.apply(%{}, {:batch, [command]}, limited_state)

            assert [] == :ets.lookup(ets, "batch-budget:wrapped:#{suffix}:first")
            assert [] == :ets.lookup(ets, "batch-budget:wrapped:#{suffix}:second")
          end

          direct_async =
            {:async, :remote_budget_origin,
             {:mset,
              [
                {"batch-budget:wrapped:direct:first", "one", 0},
                {"batch-budget:wrapped:direct:second", "two", 0}
              ]}}

          assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                   StateMachine.apply(%{}, direct_async, limited_state)

          assert [] == :ets.lookup(ets, "batch-budget:wrapped:direct:first")
          assert [] == :ets.lookup(ets, "batch-budget:wrapped:direct:second")
        end

        @tag :generic_batch_apply_budget
        test "generic batch normalization bounds leaves and structural visits", %{
          state: state,
          ets: ets
        } do
          context = Ferricstore.Raft.ApplyContext.new(batch_command_apply_budget: 2)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          exact_entries = [
            {"batch-budget:command:exact:first", "one", 0},
            {"batch-budget:command:exact:second", "two", 0}
          ]

          assert {_state, {:ok, [:ok, :ok]}} =
                   StateMachine.apply(
                     %{},
                     {:batch, [{:put_batch, exact_entries}]},
                     limited_state
                   )

          oversized_entries = [
            {"batch-budget:command:oversized:first", "one", 0},
            {"batch-budget:command:oversized:second", "two", 0},
            {"batch-budget:command:oversized:third", "three", 0}
          ]

          assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                   StateMachine.apply(
                     %{},
                     {:batch, [{:put_batch, oversized_entries}]},
                     limited_state
                   )

          Enum.each(oversized_entries, fn {key, _value, _expire_at_ms} ->
            assert [] == :ets.lookup(ets, key)
          end)

          too_deep =
            Enum.reduce(1..5, [], fn _depth, nested ->
              [{:batch, nested}]
            end)

          assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                   StateMachine.apply(%{}, {:batch, too_deep}, limited_state)
        end

        @tag :generic_batch_apply_budget
        test "generic batch normalization rejects improper command tails", %{
          state: state,
          ets: ets
        } do
          key = "batch-budget:command:improper"
          commands = [{:put, key, "value", 0} | :invalid_tail]

          assert {_state, {:error, :invalid_batch_command_list}} =
                   Ferricstore.Raft.StateMachine.apply(%{}, {:batch, commands}, state)

          assert [] == :ets.lookup(ets, key)
        end

        @tag :direct_batch_apply_budget
        test "direct put batches reject command fanout before mutation", %{
          state: state,
          ets: ets
        } do
          entries = [
            {"batch-budget:direct-put:first", "one", 0},
            {"batch-budget:direct-put:second", "two", 0}
          ]

          context = Ferricstore.Raft.ApplyContext.new(batch_command_apply_budget: 1)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                   StateMachine.apply(%{}, {:put_batch, entries}, limited_state)

          Enum.each(entries, fn {key, _value, _expire_at_ms} ->
            assert [] == :ets.lookup(ets, key)
          end)
        end

        @tag :direct_batch_apply_budget
        test "direct blob put batches reject command fanout before materialization", %{
          state: state,
          ets: ets
        } do
          entries = [
            {"batch-budget:direct-blob:first", "one", 0, :value},
            {"batch-budget:direct-blob:second", "two", 0, :value}
          ]

          context = Ferricstore.Raft.ApplyContext.new(batch_command_apply_budget: 1)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                   StateMachine.apply(%{}, {:put_blob_batch, entries}, limited_state)

          Enum.each(entries, fn {key, _value, _expire_at_ms, _kind} ->
            assert [] == :ets.lookup(ets, key)
          end)
        end

        @tag :direct_batch_apply_budget
        test "direct delete batches reject command fanout before mutation", %{
          state: state,
          ets: ets
        } do
          entries = [
            {"batch-budget:direct-delete:first", "one", 0},
            {"batch-budget:direct-delete:second", "two", 0}
          ]

          assert {seeded_state, {:ok, [:ok, :ok]}} =
                   StateMachine.apply(%{}, {:put_batch, entries}, state)

          rows_before =
            Map.new(entries, fn {key, _value, _expire_at_ms} ->
              {key, :ets.lookup(ets, key)}
            end)

          context = Ferricstore.Raft.ApplyContext.new(batch_command_apply_budget: 1)

          limited_state = %{
            seeded_state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          keys = Enum.map(entries, &elem(&1, 0))

          assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                   StateMachine.apply(%{}, {:delete_batch, keys}, limited_state)

          assert rows_before == Map.new(keys, &{&1, :ets.lookup(ets, &1)})
        end

        @tag :direct_batch_apply_budget
        test "delete batches reject malformed keys before mutation", %{
          state: state,
          ets: ets
        } do
          key = "batch-budget:direct-delete:malformed"

          assert {seeded_state, :ok} =
                   StateMachine.apply(%{}, {:put, key, "value", 0}, state)

          row_before = :ets.lookup(ets, key)
          command = {:delete_batch, [key, :malformed]}

          assert {_state, {:error, :invalid_delete_batch_key}} =
                   StateMachine.apply(%{}, command, seeded_state)

          assert row_before == :ets.lookup(ets, key)

          assert {_state, {:error, :invalid_delete_batch_key}} =
                   StateMachine.apply(%{}, {:batch, [command]}, seeded_state)

          assert row_before == :ets.lookup(ets, key)
        end

        @tag :direct_batch_apply_budget
        test "direct expiry batches reject command fanout before scanning", %{
          state: state,
          ets: ets
        } do
          expired_at_ms = Ferricstore.HLC.now_ms() - 1_000

          entries = [
            {"batch-budget:direct-expire:first", "one", expired_at_ms},
            {"batch-budget:direct-expire:second", "two", expired_at_ms}
          ]

          assert {seeded_state, {:ok, [:ok, :ok]}} =
                   StateMachine.apply(%{}, {:put_batch, entries}, state)

          rows_before =
            Map.new(entries, fn {key, _value, _expire_at_ms} ->
              {key, :ets.lookup(ets, key)}
            end)

          context = Ferricstore.Raft.ApplyContext.new(batch_command_apply_budget: 1)

          limited_state = %{
            seeded_state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          expiry_entries =
            Enum.map(entries, fn {key, _value, expire_at_ms} ->
              {key, expire_at_ms}
            end)

          assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                   StateMachine.apply(
                     %{system_time: expired_at_ms + 1_000},
                     {:expire_if_batch, expiry_entries},
                     limited_state
                   )

          assert rows_before ==
                   Map.new(expiry_entries, fn {key, _expire_at_ms} ->
                     {key, :ets.lookup(ets, key)}
                   end)
        end

        @tag :direct_batch_apply_budget
        test "direct expiry batches reject malformed entries before mutation", %{
          state: state,
          ets: ets
        } do
          expired_at_ms = Ferricstore.HLC.now_ms() - 1_000
          key = "batch-budget:direct-expire:malformed"

          assert {seeded_state, :ok} =
                   StateMachine.apply(
                     %{},
                     {:put, key, "value", expired_at_ms},
                     state
                   )

          row_before = :ets.lookup(ets, key)

          assert {_state, {:error, :invalid_expire_if_batch_entry}} =
                   StateMachine.apply(
                     %{system_time: expired_at_ms + 1_000},
                     {:expire_if_batch, [{key, expired_at_ms}, :malformed]},
                     seeded_state
                   )

          assert row_before == :ets.lookup(ets, key)
        end

        @tag :direct_batch_apply_budget
        test "direct expiry batches share one compound member budget across keys", %{
          state: state,
          ets: ets
        } do
          expired_at_ms = Ferricstore.HLC.now_ms() - 1_000
          first = CompoundKey.hash_field("batch-budget:expire-compound:first", "field")
          second = CompoundKey.hash_field("batch-budget:expire-compound:second", "field")

          assert {first_state, :ok} =
                   StateMachine.apply(
                     %{},
                     {:compound_put, first, "one", expired_at_ms},
                     state
                   )

          assert {seeded_state, :ok} =
                   StateMachine.apply(
                     %{},
                     {:compound_put, second, "two", expired_at_ms},
                     first_state
                   )

          rows_before = %{first => :ets.lookup(ets, first), second => :ets.lookup(ets, second)}

          context =
            Ferricstore.Raft.ApplyContext.new(
              batch_command_apply_budget: 10,
              compound_member_apply_budget: 1
            )

          limited_state = %{
            seeded_state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          assert {_state, {:error, :compound_member_apply_budget_exceeded}} =
                   StateMachine.apply(
                     %{system_time: expired_at_ms + 1_000},
                     {:expire_if_batch, [{first, expired_at_ms}, {second, expired_at_ms}]},
                     limited_state
                   )

          assert rows_before == %{
                   first => :ets.lookup(ets, first),
                   second => :ets.lookup(ets, second)
                 }
        end

        @tag :direct_batch_apply_budget
        test "direct atomic string batches reject command fanout before mutation", %{
          state: state,
          ets: ets
        } do
          context = Ferricstore.Raft.ApplyContext.new(batch_command_apply_budget: 1)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          for {tag, blob?} <- [
                {:mset, false},
                {:msetnx, false},
                {:mset_blob_batch, true},
                {:msetnx_blob_batch, true}
              ] do
            first = "batch-budget:#{tag}:first"
            second = "batch-budget:#{tag}:second"

            entries =
              if blob? do
                [{first, "one", 0, :value}, {second, "two", 0, :value}]
              else
                [{first, "one", 0}, {second, "two", 0}]
              end

            assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                     StateMachine.apply(%{}, {tag, entries}, limited_state)

            assert [] == :ets.lookup(ets, first)
            assert [] == :ets.lookup(ets, second)
          end
        end

        @tag :direct_batch_apply_budget
        test "direct zset batches enforce command and compound member budgets", %{
          state: state,
          ets: ets
        } do
          Enum.each(
            [
              {state.zset_score_index_name, :ordered_set},
              {state.zset_score_lookup_name, :set}
            ],
            fn {table, type} ->
              if :ets.whereis(table) == :undefined do
                :ets.new(table, [type, :public, :named_table])
              else
                :ets.delete_all_objects(table)
              end
            end
          )

          on_exit(fn ->
            safe_delete_ets(state.zset_score_index_name)
            safe_delete_ets(state.zset_score_lookup_name)
          end)

          for {suffix, options, expected_error} <- [
                {"commands", [batch_command_apply_budget: 1, compound_member_apply_budget: 10],
                 :batch_command_apply_budget_exceeded},
                {"members", [batch_command_apply_budget: 10, compound_member_apply_budget: 1],
                 :compound_member_apply_budget_exceeded}
              ] do
            redis_key = "batch-budget:direct-zadd:#{suffix}"
            first_member = CompoundKey.zset_member(redis_key, "first")
            second_member = CompoundKey.zset_member(redis_key, "second")
            context = Ferricstore.Raft.ApplyContext.new(options)

            limited_state = %{
              state
              | apply_context: context,
                apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
            }

            assert {_state, {:error, ^expected_error}} =
                     StateMachine.apply(
                       %{},
                       {:zadd_many_single,
                        [{redis_key, 1.0, "first"}, {redis_key, 2.0, "second"}]},
                       limited_state
                     )

            assert [] == :ets.lookup(ets, CompoundKey.type_key(redis_key))
            assert [] == :ets.lookup(ets, first_member)
            assert [] == :ets.lookup(ets, second_member)

            assert [] ==
                     ZSetIndex.range(
                       state.zset_score_index_name,
                       redis_key,
                       :neg_inf,
                       :inf,
                       false
                     )
          end
        end

        @tag :direct_batch_apply_budget
        test "direct zset batches reject compound fanout before score preparation", %{
          state: state,
          ets: ets
        } do
          redis_key = "batch-budget:direct-zadd:preflight-order"
          huge_score = :erlang.bsl(1, 20_000)

          context =
            Ferricstore.Raft.ApplyContext.new(
              batch_command_apply_budget: 10,
              compound_member_apply_budget: 1
            )

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          assert {_state, {:error, :compound_member_apply_budget_exceeded}} =
                   StateMachine.apply(
                     %{},
                     {:zadd_many_single,
                      [{redis_key, 1.0, "first"}, {redis_key, huge_score, "second"}]},
                     limited_state
                   )

          assert [] == :ets.lookup(ets, CompoundKey.type_key(redis_key))
          assert [] == :ets.lookup(ets, CompoundKey.zset_member(redis_key, "first"))
          assert [] == :ets.lookup(ets, CompoundKey.zset_member(redis_key, "second"))
        end

        @tag :direct_batch_apply_budget
        test "direct zset batches validate all entries before mutation", %{
          state: state,
          ets: ets
        } do
          redis_key = "batch-budget:direct-zadd:malformed"
          first_member = CompoundKey.zset_member(redis_key, "first")

          assert {_state, {:error, :invalid_zadd_many_single_entry}} =
                   StateMachine.apply(
                     %{},
                     {:zadd_many_single, [{redis_key, 1.0, "first"}, :malformed]},
                     state
                   )

          assert [] == :ets.lookup(ets, CompoundKey.type_key(redis_key))
          assert [] == :ets.lookup(ets, first_member)
        end

        @tag :direct_batch_apply_budget
        test "zset single and batch score preparation rejects overflowing numbers", %{
          state: state,
          ets: ets
        } do
          huge_score = :erlang.bsl(1, 20_000)
          single_key = "batch-budget:zadd-score:single"

          assert {_state, {:error, :invalid_zadd_score}} =
                   StateMachine.apply(
                     %{},
                     {:zadd_single, single_key, huge_score, "member"},
                     state
                   )

          assert [] == :ets.lookup(ets, CompoundKey.type_key(single_key))
          assert [] == :ets.lookup(ets, CompoundKey.zset_member(single_key, "member"))

          batch_key = "batch-budget:zadd-score:batch"

          assert {_state, {:error, :invalid_zadd_many_single_entry}} =
                   StateMachine.apply(
                     %{},
                     {:zadd_many_single,
                      [{batch_key, 1.0, "first"}, {batch_key, huge_score, "second"}]},
                     state
                   )

          assert [] == :ets.lookup(ets, CompoundKey.type_key(batch_key))
          assert [] == :ets.lookup(ets, CompoundKey.zset_member(batch_key, "first"))
          assert [] == :ets.lookup(ets, CompoundKey.zset_member(batch_key, "second"))
        end

        @tag :direct_batch_apply_budget
        test "direct list pushes reject item fanout before type claiming", %{
          state: state,
          ets: ets
        } do
          context =
            Ferricstore.Raft.ApplyContext.new(
              batch_command_apply_budget: 1,
              compound_member_apply_budget: 10
            )

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          for tag <- [:lpush, :lpushx, :rpush, :rpushx] do
            redis_key = "batch-budget:direct-list:#{tag}"

            assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                     StateMachine.apply(
                       %{},
                       {:list_op, redis_key, {tag, ["first", "second"]}},
                       limited_state
                     )

            assert [] == :ets.lookup(ets, CompoundKey.type_key(redis_key))
            assert [] == :ets.lookup(ets, CompoundKey.list_meta_key(redis_key))
          end
        end

        @tag :direct_batch_apply_budget
        test "direct list-bearing commands reject item fanout before execution", %{
          state: state,
          ets: ets
        } do
          context = Ferricstore.Raft.ApplyContext.new(batch_command_apply_budget: 1)

          limited_state = %{
            state
            | apply_context: context,
              apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(context)
          }

          commands = [
            {:pfadd, "batch-budget:direct-pfadd", ["first", "second"]},
            {:pfmerge, "batch-budget:direct-pfmerge", ["source-a", "source-b"], ["sketch"]},
            {:bloom_madd, "batch-budget:direct-bloom", ["first", "second"], nil},
            {:cms_incrby, "batch-budget:direct-cms", [{"first", 1}, {"second", 1}]},
            {:cms_merge, "batch-budget:direct-cms-merge", ["source"], [1, 2], nil},
            {:topk_add, "batch-budget:direct-topk-add", ["first", "second"]},
            {:topk_incrby, "batch-budget:direct-topk-incr", [{"first", 1}, {"second", 1}]},
            {:watch_tokens,
             ["batch-budget:direct-watch:first", "batch-budget:direct-watch:second"]}
          ]

          Enum.each(commands, fn command ->
            assert {_state, {:error, :batch_command_apply_budget_exceeded}} =
                     StateMachine.apply(%{}, command, limited_state)
          end)

          for key <- [
                "batch-budget:direct-pfadd",
                "batch-budget:direct-pfmerge",
                "batch-budget:direct-bloom",
                "batch-budget:direct-cms",
                "batch-budget:direct-cms-merge",
                "batch-budget:direct-topk-add",
                "batch-budget:direct-topk-incr"
              ] do
            assert [] == :ets.lookup(ets, key)
          end
        end

        @tag :empty_batch_release_cursor
        test "empty replicated batches advance release cursor accounting", %{state: state} do
          commands = [
            {:batch, []},
            {:put_batch, []},
            {:put_blob_batch, []},
            {:delete_batch, []},
            {:zadd_many_single, []},
            {:compound_batch_put, "batch-budget:empty:put", []},
            {:compound_blob_batch_put, "batch-budget:empty:blob", []},
            {:compound_batch_delete, "batch-budget:empty:delete", []}
          ]

          initial_state = %{state | release_cursor_interval: 1}

          Enum.reduce(Enum.with_index(commands, 1), initial_state, fn {command, index}, current ->
            meta = %{index: index, term: 1, system_time: Ferricstore.HLC.now_ms()}

            assert {next_state, {:applied_at, ^index, {:ok, []}}, effects} =
                     StateMachine.apply(meta, command, current)

            assert next_state.applied_count == current.applied_count + 1
            assert next_state.pending_release_cursor_index == index

            assert Enum.any?(
                     effects,
                     &match?({:send_msg, _, {:locally_applied, ^index}, _}, &1)
                   )

            next_state
          end)
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
