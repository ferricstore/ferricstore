defmodule Ferricstore.Store.ShardLifecycleGuardTest do
  use ExUnit.Case, async: true

  @root Path.expand("../../..", __DIR__)
  @lifecycle_path Path.join(@root, "lib/ferricstore/store/shard/lifecycle.ex")

  test "recovery logging does not copy the full keydir" do
    # recover_keydir/3 can run with millions of entries during startup. A full
    # :ets.tab2list/1 copy just to log a small sample creates avoidable latency
    # and memory pressure, so the lifecycle path must use bounded ETS reads.
    assert tab2list_calls(@lifecycle_path) == []
  end

  defp tab2list_calls(path) do
    {:ok, ast} =
      path
      |> File.read!()
      |> Code.string_to_quoted(columns: true)

    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., meta, [:ets, :tab2list]}, _call_meta, _args} = node, acc ->
          {node, [{path, meta[:line], meta[:column]} | acc]}

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(calls)
  end
end
