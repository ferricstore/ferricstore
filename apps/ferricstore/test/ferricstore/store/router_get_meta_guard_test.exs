defmodule Ferricstore.Store.RouterGetMetaGuardTest do
  use ExUnit.Case, async: true

  @router_path Path.expand(
                 "../../../lib/ferricstore/store/router.ex",
                 __DIR__
               )

  test "get_meta reads value and expiry from the same ETS row" do
    {:ok, ast} =
      @router_path
      |> File.read!()
      |> Code.string_to_quoted()

    get_meta_body = function_body(ast, :get_meta, 2)

    assert count_ets_lookup_calls(get_meta_body) == 0,
           "get_meta/2 must not call :ets.lookup directly after reading the value; " <>
             "value and expire_at_ms must come from one ETS row to avoid impossible pairs"
  end

  test "batch_get cold reads use the async batch pread NIF" do
    source = File.read!(@router_path)

    # Cold values are the main product path for large values. batch_get/2 must
    # submit all cold disk reads together so one request does not serialize N
    # blocking preads on the caller process.
    assert source =~ "v2_pread_batch_async",
           "expected Router.batch_get/2 cold path to use v2_pread_batch_async/3"
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

  defp count_ets_lookup_calls(ast) do
    {_ast, count} =
      Macro.prewalk(ast, 0, fn
        {{:., _, [:ets, :lookup]}, _, _args} = node, count ->
          {node, count + 1}

        node, count ->
          {node, count}
      end)

    count
  end
end
