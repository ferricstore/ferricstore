defmodule Ferricstore.Raft.StateMachineColdScanGuardTest do
  use ExUnit.Case, async: true

  @state_machine_path Path.expand(
                        "../../../lib/ferricstore/raft/state_machine.ex",
                        __DIR__
                      )

  test "cross-shard prefix scans batch cold Bitcask reads" do
    source = File.read!(@state_machine_path)
    ast = Code.string_to_quoted!(source)

    assert function_calls?(ast, Ferricstore.Store.ColdRead, :pread_batch, 2),
           "cross_shard_prefix_scan_from_path/3 should use ColdRead.pread_batch/2; " <>
             "per-entry pread_at/3 creates one async waiter per cold collection member"
  end

  defp function_calls?(ast, module, function, arity) do
    {_ast, found?} =
      Macro.prewalk(ast, false, fn
        {{:., _, [mod_ast, ^function]}, _, args} = node, _found?
        when length(args) == arity ->
          {node, module_ast?(mod_ast, module)}

        node, found? ->
          {node, found?}
      end)

    found?
  end

  defp module_ast?(ast, module) do
    case Macro.expand(ast, __ENV__) do
      ^module -> true
      _ -> false
    end
  end
end
