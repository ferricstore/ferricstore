defmodule Ferricstore.Store.RouterGetMetaGuardTest do
  use ExUnit.Case, async: true

  test "get_meta reads value and expiry from the same ETS row" do
    {:ok, ast} =
      Ferricstore.Test.SourceFiles.router_source()
      |> Code.string_to_quoted()

    get_meta_body = function_body(ast, :get_meta, 2)

    assert count_ets_lookup_calls(get_meta_body) == 0,
           "get_meta/2 must not call :ets.lookup directly after reading the value; " <>
             "value and expire_at_ms must come from one ETS row to avoid impossible pairs"
  end

  test "batch_get cold reads use the shared async batch cold reader" do
    source = Ferricstore.Test.SourceFiles.router_source()

    # Cold values are the main product path for large values. batch_get/2 must
    # submit all cold disk reads together so one request does not serialize N
    # blocking preads on the caller process.
    assert source =~ "ColdRead.pread_batch",
           "expected Router.batch_get/2 cold path to use ColdRead.pread_batch/2"
  end

  test "batch_get cold reads dedupe duplicate locations with one pass" do
    {:ok, ast} =
      Ferricstore.Test.SourceFiles.router_source()
      |> Code.string_to_quoted()

    bodies = function_bodies(ast, :read_cold_batch_async, 2)

    # Duplicate keys in MGET-style reads should collapse to one physical
    # pread, but the dedupe map walk itself must not run twice on the hot
    # cold-batch path.
    assert count_local_calls(bodies, :dedupe_cold_batch_entries, 1) == 1,
           "expected Router cold batch reads to dedupe once, not repeat the O(N) pass"
  end

  test "direct cold get and get_meta use the shared async cold reader" do
    source = Ferricstore.Test.SourceFiles.router_source()

    # High-volume cold reads must not call blocking pread on a Normal scheduler.
    assert source =~ "ColdRead.pread_keyed",
           "expected direct Router cold reads to use keyed ColdRead async wrapper"

    refute Regex.match?(~r/(?<!_)v2_pread_at\(/, source),
           "expected Router cold reads to avoid blocking v2_pread_at/2"
  end

  defp function_body(ast, name, arity) do
    [body] = function_bodies(ast, name, arity)
    body
  end

  defp function_bodies(ast, name, arity) do
    {_ast, bodies} =
      Macro.prewalk(ast, nil, fn
        {kind, _meta, [{^name, _fun_meta, args}, [do: body]]} = node, acc
        when kind in [:def, :defp] and length(args) == arity ->
          {node, [body | List.wrap(acc)]}

        node, acc ->
          {node, acc}
      end)

    bodies = bodies || []
    assert bodies != [], "expected to find #{name}/#{arity}"
    Enum.reverse(bodies)
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

  defp count_local_calls(ast, name, arity) do
    {_ast, count} =
      Macro.prewalk(ast, 0, fn
        {^name, _meta, args} = node, count when length(args) == arity ->
          {node, count + 1}

        node, count ->
          {node, count}
      end)

    count
  end
end
