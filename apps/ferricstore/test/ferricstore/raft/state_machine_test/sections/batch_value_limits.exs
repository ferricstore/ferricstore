defmodule Ferricstore.Raft.StateMachineTest.Sections.BatchValueLimits do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey}

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
