defmodule Ferricstore.Raft.StateMachineTest.Sections.FlowGovernanceReleaseOutbox do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      @tag :flow_governance_release_outbox
      test "malformed release outbox acknowledgement is rejected as an applied command", %{
        state: state,
        shard_index: shard_index
      } do
        key = Ferricstore.Flow.Keys.governance_release_outbox_meta_key(shard_index)

        command =
          {:flow_governance_release_outbox_ack, key, shard_index, 0, 1}

        assert {next_state,
                {:error, "ERR invalid flow governance release outbox acknowledgement"}} =
                 Ferricstore.Raft.StateMachine.apply(%{}, command, state)

        assert next_state.applied_count == state.applied_count + 1
      end

      @tag :flow_governance_release_outbox
      test "oversized release completion is rejected as an applied command", %{
        state: state,
        shard_index: shard_index
      } do
        key = Ferricstore.Flow.Keys.governance_release_outbox_meta_key(shard_index)

        command =
          {:flow_governance_release_outbox_mark_completed, key, shard_index, Enum.to_list(1..257)}

        assert {next_state, {:error, "ERR invalid flow governance release outbox completion"}} =
                 Ferricstore.Raft.StateMachine.apply(%{}, command, state)

        assert next_state.applied_count == state.applied_count + 1
      end
    end
  end
end
