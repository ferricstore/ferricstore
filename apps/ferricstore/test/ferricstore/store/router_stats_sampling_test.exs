defmodule Ferricstore.Store.RouterStatsSamplingTest do
  use ExUnit.Case, async: false

  @router_source Path.expand("../../../lib/ferricstore/store/router.ex", __DIR__)

  alias Ferricstore.Stats
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  test "normal keyspace misses are sampled by read_sample_rate" do
    ctx = IsolatedInstance.checkout(shard_count: 1, read_sample_rate: 10)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    before = Stats.keyspace_misses(ctx)

    for i <- 1..9 do
      assert Router.get(ctx, "sampled-miss-#{i}") == nil
    end

    assert Stats.keyspace_misses(ctx) - before == 0

    assert Router.get(ctx, "sampled-miss-10") == nil
    assert Stats.keyspace_misses(ctx) - before == 1
  end

  test "read_sample_rate one keeps keyspace misses exact" do
    ctx = IsolatedInstance.checkout(shard_count: 1, read_sample_rate: 1)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    before = Stats.keyspace_misses(ctx)

    for i <- 1..3 do
      assert Router.get(ctx, "exact-miss-#{i}") == nil
    end

    assert Stats.keyspace_misses(ctx) - before == 3
  end

  test "normal keyspace hits are batched by read_sample_rate" do
    ctx = IsolatedInstance.checkout(shard_count: 1, read_sample_rate: 3)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    key = "sampled-hit"
    assert :ok = Router.put(ctx, key, "value")

    before = Stats.keyspace_hits(ctx)

    :rand.seed(:exsss, {1, 2, 3})

    assert Router.get(ctx, key) == "value"
    assert Router.get(ctx, key) == "value"
    assert Stats.keyspace_hits(ctx) - before == 0

    assert Router.get(ctx, key) == "value"
    assert Stats.keyspace_hits(ctx) - before == 1
  end

  test "batch_get samples hot hits once per batch" do
    ctx = IsolatedInstance.checkout(shard_count: 1, read_sample_rate: 3)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    keys = ["hitbatch-first:1", "hitbatch-second:1", "hitbatch-third:1"]
    Enum.each(keys, fn key -> assert :ok = Router.put(ctx, key, "value:" <> key) end)

    Stats.reset_hotness()
    before = Stats.keyspace_hits(ctx)

    assert Router.batch_get(ctx, keys) == Enum.map(keys, &("value:" <> &1))
    assert Stats.keyspace_hits(ctx) - before == 1

    assert {"hitbatch-third", 1, 0, 0.0} in Stats.hotness_top(10)

    refute Enum.any?(Stats.hotness_top(10), fn {prefix, hot, _cold, _pct} ->
             prefix == "hitbatch-first" and hot > 0
           end)
  end

  test "batch_get_with_file_refs samples hot hits once per batch" do
    ctx = IsolatedInstance.checkout(shard_count: 1, read_sample_rate: 3)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    keys = ["fileref-hit-first:1", "fileref-hit-second:1", "fileref-hit-third:1"]
    Enum.each(keys, fn key -> assert :ok = Router.put(ctx, key, "value:" <> key) end)

    Stats.reset_hotness()
    before = Stats.keyspace_hits(ctx)

    assert Router.batch_get_with_file_refs(ctx, keys, 1024) == Enum.map(keys, &("value:" <> &1))
    assert Stats.keyspace_hits(ctx) - before == 1

    assert {"fileref-hit-third", 1, 0, 0.0} in Stats.hotness_top(10)

    refute Enum.any?(Stats.hotness_top(10), fn {prefix, hot, _cold, _pct} ->
             prefix == "fileref-hit-first" and hot > 0
           end)
  end

  test "deferred batch_get with presence reports no file refs for hot hits" do
    ctx = IsolatedInstance.checkout(shard_count: 1, read_sample_rate: 3)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    keys = ["fileref-presence-hot-first:1", "fileref-presence-hot-second:1"]
    Enum.each(keys, fn key -> assert :ok = Router.put(ctx, key, "value:" <> key) end)

    expected = Enum.map(keys, &("value:" <> &1))

    assert {^expected, false} =
             Router.batch_get_with_deferred_blob_file_refs_and_presence(ctx, keys, 1024)
  end

  test "batch read functions do not advance hit sampling one key at a time" do
    ast =
      @router_source
      |> File.read!()
      |> Code.string_to_quoted!()

    for {name, arity} <- [
          {:batch_get, 2},
          {:batch_get_planned, 2},
          {:do_batch_get_lookup_keys_with_file_refs, 4},
          {:do_batch_get_with_file_refs, 4},
          {:compound_batch_get, 3}
        ] do
      body = find_function_body!(ast, name, arity)

      refute contains_call?(body, :sampled_read_bookkeeping_fast, 4),
             "#{name}/#{arity} must batch hot-read bookkeeping instead of sampling per hit"

      assert contains_call?(body, :sampled_read_bookkeeping_batch, 3),
             "#{name}/#{arity} must use sampled_read_bookkeeping_batch/3"
    end
  end

  test "read_sample_rate one keeps keyspace hits exact" do
    ctx = IsolatedInstance.checkout(shard_count: 1, read_sample_rate: 1)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    key = "exact-hit"
    assert :ok = Router.put(ctx, key, "value")

    before = Stats.keyspace_hits(ctx)

    for _ <- 1..3 do
      assert Router.get(ctx, key) == "value"
    end

    assert Stats.keyspace_hits(ctx) - before == 3
  end

  defp find_function_body!(ast, name, arity) do
    {_ast, body} =
      Macro.prewalk(ast, nil, fn
        {:def, _meta, [head, [do: body]]} = node, nil ->
          {node, maybe_function_body(head, name, arity, body)}

        {:defp, _meta, [head, [do: body]]} = node, nil ->
          {node, maybe_function_body(head, name, arity, body)}

        node, acc ->
          {node, acc}
      end)

    body || flunk("could not find #{name}/#{arity}")
  end

  defp maybe_function_body(head, name, arity, body) do
    if function_head?(head, name, arity), do: body
  end

  defp function_head?({:when, _meta, [head | _guards]}, name, arity),
    do: function_head?(head, name, arity)

  defp function_head?({fun, _meta, args}, name, arity)
       when fun == name and is_list(args) and length(args) == arity,
       do: true

  defp function_head?(_head, _name, _arity), do: false

  defp contains_call?(ast, name, arity) do
    {_ast, found?} =
      Macro.prewalk(ast, false, fn
        {fun, _meta, args} = node, _found?
        when fun == name and is_list(args) and length(args) == arity ->
          {node, true}

        node, found? ->
          {node, found?}
      end)

    found?
  end
end
