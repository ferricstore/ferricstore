defmodule Ferricstore.Commands.StreamPrefixScanGuardTest do
  @moduledoc false
  use ExUnit.Case, async: true

  @stream_path Path.expand("../../../lib/ferricstore/commands/stream.ex", __DIR__)

  test "stream commands do not enumerate the whole keyspace" do
    {:ok, ast} =
      @stream_path
      |> File.read!()
      |> Code.string_to_quoted()

    raw_keyspace_calls =
      ast
      |> find_ops_keys_calls()
      |> Enum.map(fn {_node, meta} -> meta[:line] end)
      |> Enum.sort()

    # Stream entries are stored under a compound-key prefix, so read/trim/count
    # paths must use compound_scan/compound_count. A raw KEYS-style scan turns
    # every XRANGE/XTRIM/XLEN fallback into O(database keys), including keys
    # unrelated to the target stream.
    assert raw_keyspace_calls == []
  end

  defp find_ops_keys_calls(ast) do
    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., meta, [{:__aliases__, _, [:Ops]}, :keys]} = node, _call_meta, _args} = expr, acc ->
          {expr, [{node, meta} | acc]}

        expr, acc ->
          {expr, acc}
      end)

    Enum.reverse(calls)
  end
end
