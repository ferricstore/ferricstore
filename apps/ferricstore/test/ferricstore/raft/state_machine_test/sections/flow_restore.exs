defmodule Ferricstore.Raft.StateMachineTest.Sections.FlowRestore do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine

      # With a Raft index in meta, apply wraps the reply and returns effects.
      defp restore_apply(meta, command, state) do
        result = StateMachine.apply(meta, command, state)
        {:applied_at, _index, reply} = elem(result, 1)
        {elem(result, 0), reply}
      end

      describe "Flow restore" do
        test "a restored run takes its sequence numbers from this shard's Raft position", %{
          state: state
        } do
          setup_flow_indexes(state)
          span = 4_294_967_296
          partition_key = "tenant-restore"
          source_key = Ferricstore.Flow.Keys.state_key("restore-source", partition_key)

          {state, :ok} =
            restore_apply(
              %{index: 10, system_time: 1_000},
              {:flow_create, source_key,
               %{
                 id: "restore-source",
                 type: "restore-type",
                 state: "queued",
                 partition_key: partition_key,
                 now_ms: 1_000
               }},
              state
            )

          source = flow_record!(state, source_key)
          assert source.state_enter_seq == 10 * span

          # As exported from another store, whose shard was far ahead.
          exported = %{
            source
            | id: "restored",
              root_flow_id: "restored",
              incarnation: 999_999 * span,
              state_enter_seq: 999_999 * span + 7
          }

          restored_key = Ferricstore.Flow.Keys.state_key("restored", partition_key)

          {state, :ok} =
            restore_apply(
              %{index: 20, system_time: 2_000},
              {:flow_restore, restored_key,
               %{
                 restore: :flow,
                 type: "restore-type",
                 partition_key: partition_key,
                 record: exported,
                 values: [],
                 history: [],
                 aux: []
               }},
              state
            )

          restored = flow_record!(state, restored_key)
          assert restored.state_enter_seq == 20 * span
          assert restored.incarnation == 20 * span

          ignored = [:id, :root_flow_id, :incarnation, :state_enter_seq]
          assert Map.drop(restored, ignored) == Map.drop(source, ignored)
        end

        test "restore refuses a run that already exists", %{state: state} do
          setup_flow_indexes(state)
          partition_key = "tenant-restore-existing"
          key = Ferricstore.Flow.Keys.state_key("existing", partition_key)

          {state, :ok} =
            restore_apply(
              %{index: 5, system_time: 1_000},
              {:flow_create, key,
               %{
                 id: "existing",
                 type: "restore-type",
                 state: "queued",
                 partition_key: partition_key,
                 now_ms: 1_000
               }},
              state
            )

          existing = flow_record!(state, key)

          {state, {:error, "ERR flow already exists"}} =
            restore_apply(
              %{index: 6, system_time: 2_000},
              {:flow_restore, key,
               %{
                 restore: :flow,
                 type: "restore-type",
                 partition_key: partition_key,
                 record: %{existing | version: 9},
                 values: [],
                 history: [],
                 aux: []
               }},
              state
            )

          assert flow_record!(state, key) == existing
        end
      end
    end
  end
end
