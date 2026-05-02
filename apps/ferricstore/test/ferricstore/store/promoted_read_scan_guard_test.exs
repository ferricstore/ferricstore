defmodule Ferricstore.Store.PromotedReadScanGuardTest do
  use ExUnit.Case, async: true

  @compound_path Path.expand("../../../lib/ferricstore/store/shard/compound.ex", __DIR__)

  test "promoted request reads do not scan dedicated files on ETS miss" do
    {:ok, ast} =
      @compound_path
      |> File.read!()
      |> Code.string_to_quoted()

    promoted_read_body = function_body(ast, :promoted_read, 3)

    # Promoted collection reads are request path. A missing ETS row should be a
    # miss; scanning the whole dedicated file makes HGET O(file_size) and can
    # surprise latency after eviction or corruption.
    refute contains_call?(promoted_read_body, :NIF, :v2_scan_file, 1),
           "promoted_read/3 must not scan the dedicated file on an ETS miss"
  end

  defp function_body(ast, name, arity) do
    {_ast, body} =
      Macro.prewalk(ast, nil, fn
        {:def, _meta, [{^name, _fun_meta, args}, [do: body]]} = node, nil
        when length(args) == arity ->
          {node, body}

        node, acc ->
          {node, acc}
      end)

    assert body != nil, "expected to find #{name}/#{arity}"
    body
  end

  defp contains_call?(ast, module_alias, function, arity) do
    {_ast, found?} =
      Macro.prewalk(ast, false, fn
        {{:., _, [{:__aliases__, _, [^module_alias]}, ^function]}, _, args} = node, _found
        when length(args) == arity ->
          {node, true}

        node, found ->
          {node, found}
      end)

    found?
  end
end
