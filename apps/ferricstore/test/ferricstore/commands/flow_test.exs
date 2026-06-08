Code.require_file("flow_test/sections/dispatches_flow_value_put_payload_refs_through_rust_ast.exs", __DIR__)
Code.require_file("flow_test/sections/dispatches_flow_create_many_through_rust_ast.exs", __DIR__)
Code.require_file("flow_test/sections/dispatches_flow_rewind_through_rust_ast.exs", __DIR__)

defmodule Ferricstore.Commands.FlowTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.{MockStore, ShardHelpers}

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp uid(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"

  use Ferricstore.Commands.FlowTest.Sections.DispatchesFlowValuePutPayloadRefsThroughRustAst
  use Ferricstore.Commands.FlowTest.Sections.DispatchesFlowCreateManyThroughRustAst
  use Ferricstore.Commands.FlowTest.Sections.DispatchesFlowRewindThroughRustAst
end
