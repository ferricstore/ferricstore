defmodule Ferricstore.Store.StorePathStatGuardTest do
  use ExUnit.Case, async: true

  @store_root Path.expand("../../../lib/ferricstore/store", __DIR__)

  test "managed storage paths never use symlink-following File metadata predicates" do
    calls =
      @store_root
      |> Path.join("**/*.ex")
      |> Path.wildcard()
      |> Enum.flat_map(&symlink_following_file_calls/1)

    assert calls == [],
           "storage metadata checks must use File.lstat so path replacement cannot redirect " <>
             "accounting or recovery outside the data root; found #{inspect(calls)}"
  end

  defp symlink_following_file_calls(path) do
    ast = path |> File.read!() |> Code.string_to_quoted!()

    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., dot_meta, [{:__aliases__, _, [:File]}, function]} = node, call_meta, _args}, acc
        when function in [:stat, :stat!, :regular?, :dir?] ->
          line = dot_meta[:line] || call_meta[:line]
          {node, [{Path.relative_to(path, @store_root), line, function} | acc]}

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(calls)
  end
end
