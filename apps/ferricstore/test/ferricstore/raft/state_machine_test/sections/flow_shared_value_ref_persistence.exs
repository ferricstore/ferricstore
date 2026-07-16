defmodule Ferricstore.Raft.StateMachineTest.Sections.FlowSharedValueRefPersistence do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine

      describe "Flow shared value ref persistence" do
        @tag :flow_shared_ref_codec
        test "rejects noncanonical shared ref registries without mutating the flow", %{
          state: state,
          ets: ets
        } do
          setup_flow_indexes(state)

          partition_key = "shared-ref-registry-tenant"
          id = "shared-ref-registry-flow"
          type = "shared-ref-registry-type"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
          shared_ref = Ferricstore.Flow.Keys.value_key("owner", :shared, 1, partition_key)

          assert {_state, :ok} =
                   StateMachine.apply(
                     %{system_time: 1_000},
                     {:flow_create, state_key,
                      %{
                        id: id,
                        type: type,
                        state: "queued",
                        partition_key: partition_key,
                        payload_ref: shared_ref
                      }},
                     state
                   )

          registry_key =
            Ferricstore.Flow.Keys.shared_value_ref_registry_key(id, partition_key)

          corrupt = Ferricstore.TermCodec.encode([shared_ref]) <> <<0>>

          true =
            :ets.insert(
              ets,
              {registry_key, corrupt, 0, Ferricstore.Store.LFU.initial(), 0, 0,
               byte_size(corrupt)}
            )

          created = flow_record!(state, state_key)
          assert created.state == "queued"
          assert created.payload_ref == shared_ref

          assert [{^registry_key, ^corrupt, 0, _lfu, 0, 0, _size}] =
                   :ets.lookup(ets, registry_key)

          assert {_state, {:error, {:invalid_flow_shared_value_ref_registry, ^registry_key}}} =
                   StateMachine.apply(
                     %{system_time: 2_000},
                     {:flow_named_value_put, state_key,
                      %{
                        id: id,
                        name: "result",
                        value: "value",
                        partition_key: partition_key,
                        now_ms: 2_000
                      }},
                     state
                   )

          assert flow_record!(state, state_key) == created
        end

        @tag :flow_shared_ref_codec
        test "rejects corrupt shared ref counts instead of resetting them", %{
          state: state,
          ets: ets
        } do
          setup_flow_indexes(state)

          partition_key = "shared-ref-count-tenant"
          type = "shared-ref-count-type"
          shared_ref = Ferricstore.Flow.Keys.value_key("owner", :shared, 1, partition_key)

          first_id = "shared-ref-count-first"
          first_key = Ferricstore.Flow.Keys.state_key(first_id, partition_key)

          assert {_state, :ok} =
                   StateMachine.apply(
                     %{system_time: 1_000},
                     {:flow_create, first_key,
                      %{
                        id: first_id,
                        type: type,
                        state: "queued",
                        partition_key: partition_key,
                        payload_ref: shared_ref
                      }},
                     state
                   )

          count_key =
            Ferricstore.Flow.Keys.shared_value_ref_count_key(shared_ref, state.shard_index)

          corrupt = <<131, 255>>

          true =
            :ets.insert(
              ets,
              {count_key, corrupt, 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(corrupt)}
            )

          second_id = "shared-ref-count-second"
          second_key = Ferricstore.Flow.Keys.state_key(second_id, partition_key)

          assert {_state, {:error, {:invalid_flow_shared_value_ref_count, ^count_key}}} =
                   StateMachine.apply(
                     %{system_time: 2_000},
                     {:flow_create, second_key,
                      %{
                        id: second_id,
                        type: type,
                        state: "queued",
                        partition_key: partition_key,
                        payload_ref: shared_ref
                      }},
                     state
                   )

          assert [] = :ets.lookup(ets, second_key)
          assert [{^count_key, ^corrupt, 0, _lfu, 0, 0, _size}] = :ets.lookup(ets, count_key)
        end
      end
    end
  end
end
