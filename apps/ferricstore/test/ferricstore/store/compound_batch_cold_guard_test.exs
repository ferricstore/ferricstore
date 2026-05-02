defmodule Ferricstore.Store.CompoundBatchColdGuardTest do
  use ExUnit.Case, async: true

  @compound_path Path.expand(
                   "../../../lib/ferricstore/store/shard/compound.ex",
                   __DIR__
                 )

  test "compound batch cold reads use the async batch pread path" do
    ast = compound_ast()
    batch_reader_body = function_body(ast, :read_compound_cold_batch_async, 1)

    # HMGET/ZMSCORE/SMISMEMBER-style commands can read many cold large fields.
    # The shared compound batch path must submit those cold reads together
    # instead of serializing one blocking pread per field on the shard process.
    assert contains_remote_call?(
             batch_reader_body,
             [:Ferricstore, :Store, :ColdRead],
             :pread_batch,
             2
           ),
           "expected Shard.Compound batch get path to use ColdRead.pread_batch/2"
  end

  test "promoted compound batch reads use a dedicated batch cold path" do
    ast = compound_ast()
    batch_get_body = function_body(ast, :handle_compound_batch_get, 3)

    # Promoted hashes/sets/zsets store fields in dedicated Bitcask files.
    # Multi-key reads such as HMGET, SMISMEMBER, and ZMSCORE must keep using a
    # single batched cold-read submission instead of looping through the
    # single-field helper and waiting once per field on the shard process.
    assert contains_local_call?(batch_get_body, :compound_batch_get_dedicated, 3),
           "expected promoted compound batch get to call compound_batch_get_dedicated/3"

    refute contains_local_call?(batch_get_body, :compound_get_value, 3),
           "promoted compound batch get must not serialize through compound_get_value/3"

    dedicated_body = function_body(ast, :compound_batch_get_dedicated, 3)

    assert contains_local_call?(dedicated_body, :read_compound_cold_batch_async, 1),
           "expected promoted compound batch get to use the shared batched cold reader"
  end

  test "promoted compound batch metadata reads use a dedicated batch cold path" do
    ast = compound_ast()
    batch_get_meta_body = function_body(ast, :handle_compound_batch_get_meta, 3)

    # HGETEX/HSETEX-style commands can fetch many value+TTL pairs. Keep their
    # promoted cold path batched for the same reason as value-only batch reads.
    assert contains_local_call?(batch_get_meta_body, :compound_batch_get_meta_dedicated, 3),
           "expected promoted compound batch metadata get to call compound_batch_get_meta_dedicated/3"

    refute contains_local_call?(batch_get_meta_body, :compound_get_meta_value, 3),
           "promoted compound batch metadata get must not serialize through compound_get_meta_value/3"

    dedicated_body = function_body(ast, :compound_batch_get_meta_dedicated, 3)

    assert contains_local_call?(dedicated_body, :read_compound_cold_batch_async, 1),
           "expected promoted compound batch metadata get to use the shared batched cold reader"
  end

  defp compound_ast do
    @compound_path
    |> File.read!()
    |> Code.string_to_quoted!()
  end

  defp function_body(ast, name, arity) do
    {_ast, body} =
      Macro.prewalk(ast, nil, fn
        {kind, _meta, [{^name, _fun_meta, args}, [do: body]]} = node, nil
        when kind in [:def, :defp] and length(args) == arity ->
          {node, body}

        node, acc ->
          {node, acc}
      end)

    assert body != nil, "expected to find #{name}/#{arity}"
    body
  end

  defp contains_local_call?(ast, function, arity) do
    {_ast, found?} =
      Macro.prewalk(ast, false, fn
        {^function, _meta, args} = node, _found
        when is_list(args) and length(args) in [arity, max(arity - 1, 0)] ->
          {node, true}

        node, found ->
          {node, found}
      end)

    found?
  end

  defp contains_remote_call?(ast, module_alias, function, arity) do
    {_ast, found?} =
      Macro.prewalk(ast, false, fn
        {{:., _, [{:__aliases__, _, ^module_alias}, ^function]}, _, args} = node, _found
        when is_list(args) and length(args) == arity ->
          {node, true}

        node, found ->
          {node, found}
      end)

    found?
  end
end
