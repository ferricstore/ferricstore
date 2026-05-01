defmodule Ferricstore.Commands.CrossShardPublicApiGuardTest do
  @moduledoc false

  use ExUnit.Case, async: true

  @guarded_fns [
    copy: 3,
    rename: 2,
    renamenx: 2,
    sdiffstore: 2,
    sinterstore: 2,
    sunionstore: 2
  ]

  test "public multi-key APIs do not pass routing stores that bypass CrossShardOp locks" do
    source_path = Path.join([__DIR__, "..", "..", "..", "lib", "ferricstore.ex"])
    {:ok, ast} = source_path |> File.read!() |> Code.string_to_quoted(columns: true)

    offenders =
      for {name, arity} <- @guarded_fns,
          def_ast <- defs_for(ast, name, arity),
          calls_build_compound_store?(def_ast) do
        "#{name}/#{arity}"
      end

    # CrossShardOp intentionally trusts fully-capable stores for command-unit tests.
    # Public embedded APIs must therefore pass no caller store (or an inert one)
    # when they need quorum locks, otherwise the handler executes without the
    # lock/intent protocol even though the operation can span shards.
    assert offenders == []
  end

  defp defs_for(ast, name, arity) do
    {_ast, defs} =
      Macro.prewalk(ast, [], fn
        {:def, _meta, [head, _body]} = def_ast, acc ->
          if function_head?(head, name, arity) do
            {def_ast, [def_ast | acc]}
          else
            {def_ast, acc}
          end

        node, acc ->
          {node, acc}
      end)

    defs
  end

  defp function_head?({:when, _meta, [head | _guards]}, name, arity),
    do: function_head?(head, name, arity)

  defp function_head?({name, _meta, args}, name, arity),
    do: is_list(args) and length(args) == arity

  defp function_head?(_head, _name, _arity), do: false

  defp calls_build_compound_store?(ast) do
    {_ast, found?} =
      Macro.prewalk(ast, false, fn
        {:build_compound_store, _meta, _args} = node, _found? -> {node, true}
        node, found? -> {node, found?}
      end)

    found?
  end
end
