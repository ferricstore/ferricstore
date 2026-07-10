defmodule Ferricstore.Raft.StateMachineTest.Sections.FlowGovernanceLimit do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      @tag :flow_governance_limit
      test "malformed limit mutation is rejected as an applied command", %{state: state} do
        command = {:flow_governance_limit_mutate, "owner-key"}

        assert {next_state, {:error, "ERR invalid flow limit mutation"}} =
                 Ferricstore.Raft.StateMachine.apply(%{}, command, state)

        assert next_state.applied_count == state.applied_count + 1
      end
    end
  end
end
