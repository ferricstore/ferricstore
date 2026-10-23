defmodule FerricstoreServer.Native.ApplyBudgetErrorTest do
  @moduledoc """
  One replicated entry applies at most the instance's compound member budget
  (4,096 by default). A single command past it is refused, and the client
  must learn why, not get the internal reason atom.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias FerricstoreServer.Native.Commands

  @op_command_exec 0x0100
  @op_rpush 0x0121

  setup do
    ctx = FerricStore.Instance.get(:default)

    state = %{
      acl_cache: :full_access,
      require_auth: false,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      compression: :none,
      compact_flow_responses: false
    }

    %{ctx: ctx, state: state, limit: ctx.apply_context.compound_member_apply_budget}
  end

  test "RPUSH past the member budget explains the limit", %{state: state, limit: limit} do
    key = "{native-budget}list:#{System.unique_integer([:positive])}"
    values = for i <- 0..limit, do: "v#{i}"

    assert {:error, message, _state} =
             Commands.execute(@op_rpush, %{"key" => key, "values" => values}, state)

    assert message == "ERR too many members in one command (the limit is #{limit})"
  end

  test "HDEL past the member budget explains the limit", %{
    ctx: ctx,
    state: state,
    limit: limit
  } do
    key = "{native-budget}hash:#{System.unique_integer([:positive])}"
    fields = for i <- 0..limit, do: "f#{i}"

    assert limit + 1 ==
             Dispatcher.dispatch("HSET", [key | Enum.flat_map(fields, &[&1, "v"])], ctx)

    assert {:error, message, _state} =
             Commands.execute(
               @op_command_exec,
               %{"command" => "HDEL", "args" => [key | fields]},
               state
             )

    assert message == "ERR too many members in one command (the limit is #{limit})"
  end
end
