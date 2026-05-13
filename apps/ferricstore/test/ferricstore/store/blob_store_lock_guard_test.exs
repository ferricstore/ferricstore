defmodule Ferricstore.Store.BlobStoreLockGuardTest do
  use ExUnit.Case, async: true

  @source Path.expand("../../../lib/ferricstore/store/blob_store.ex", __DIR__)

  test "blob store uses a local shard latch instead of global locking" do
    # Blob files are shard-local and the lock key is scoped to this BEAM node.
    # Keep this path off :global.trans/3; it adds distributed-lock machinery
    # without improving correctness for blob append segments.
    ast = @source |> File.read!() |> Code.string_to_quoted!()

    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., dot_meta, [:global, :trans]} = node, call_meta, args}, acc ->
          line = dot_meta[:line] || call_meta[:line]
          {node, [{line, length(args)} | acc]}

        node, acc ->
          {node, acc}
      end)

    assert Enum.reverse(calls) == [],
           "BlobStore should not call :global.trans/3 on the large-value hot path; " <>
             "found calls at #{inspect(Enum.reverse(calls))}"
  end
end
