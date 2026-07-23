defmodule Ferricstore.Commands.FlowCommandBoundaryTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, Flow}

  test "Flow dispatch preserves the supplied instance context" do
    supplied_ctx = %FerricStore.Instance{name: :command_boundary_test}
    parent = self()

    trace_loop = fn loop ->
      receive do
        message ->
          send(parent, {:flow_trace, message})
          loop.(loop)
      end
    end

    tracer = spawn(fn -> trace_loop.(trace_loop) end)

    Code.ensure_loaded!(Ferricstore.Flow)
    :erlang.trace(self(), true, [:call, {:tracer, tracer}])
    :erlang.trace_pattern({Ferricstore.Flow, :create, 3}, true, [])

    on_exit(fn ->
      :erlang.trace_pattern({Ferricstore.Flow, :create, 3}, false, [])
      :erlang.trace(self(), false, [:call])
    end)

    assert {:error, "ERR flow id must be a non-empty string"} =
             Flow.handle_ast({:flow_create, "", []}, supplied_ctx)

    assert_receive {:flow_trace,
                    {:trace, _pid, :call, {Ferricstore.Flow, :create, [^supplied_ctx, "", []]}}}
  end

  test "structured Flow opcodes are rejected explicitly by scalar COMMAND_EXEC preparation" do
    structured_commands = ~w(
      FLOW.STEP_CONTINUE FLOW.START_AND_CLAIM FLOW.RUN_STEPS_MANY
      FLOW.SCHEDULE.CREATE FLOW.SCHEDULE.GET FLOW.SCHEDULE.DELETE FLOW.SCHEDULE.FIRE_DUE
      FLOW.SCHEDULE.LIST FLOW.SCHEDULE.FIRE FLOW.SCHEDULE.PAUSE FLOW.SCHEDULE.RESUME
      FLOW.EFFECT.RESERVE FLOW.EFFECT.CONFIRM FLOW.EFFECT.FAIL FLOW.EFFECT.COMPENSATE
      FLOW.EFFECT.GET FLOW.GOVERNANCE.LEDGER FLOW.GOVERNANCE.OVERVIEW
      FLOW.APPROVAL.REQUEST FLOW.APPROVAL.APPROVE FLOW.APPROVAL.REJECT FLOW.APPROVAL.GET
      FLOW.APPROVAL.LIST FLOW.CIRCUIT.OPEN FLOW.CIRCUIT.CLOSE FLOW.CIRCUIT.GET
      FLOW.BUDGET.RESERVE FLOW.BUDGET.COMMIT FLOW.BUDGET.RELEASE FLOW.BUDGET.GET
      FLOW.BUDGET.LIST FLOW.LIMIT.LEASE FLOW.LIMIT.SPEND FLOW.LIMIT.RELEASE
      FLOW.LIMIT.GET FLOW.LIMIT.LIST
    )

    for command <- structured_commands do
      expected =
        "ERR command '#{String.downcase(command)}' requires its structured native opcode"

      assert {:ok, prepared} = Dispatcher.prepare_raw(command, [])
      assert {:error, ^expected} = Dispatcher.dispatch_prepared(prepared, %{})
    end
  end

  test "Flow response normalization converts keys recursively inside lists" do
    response = %{
      decision: %{reason: "lowest_cost"},
      alternatives: [
        %{index: %{logical_id: "runs_by_state"}, comparison: %{cost_delta: 5}}
      ]
    }

    assert %{
             "decision" => %{"reason" => "lowest_cost"},
             "alternatives" => [
               %{
                 "index" => %{"logical_id" => "runs_by_state"},
                 "comparison" => %{"cost_delta" => 5}
               }
             ]
           } = Flow.normalize_result({:ok, response})
  end
end
