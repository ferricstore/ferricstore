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

  test "raft startup errors are not downgraded to direct-write shards" do
    # If a Batcher exists, the shard is expected to run with Raft. Swallowing a
    # Ra start error and returning false would silently enable local direct writes.
    refute File.read!(@lifecycle_path) =~ "_, _ -> false"
  end

  test "keydir recovery scans Bitcask logs in bounded pages" do
    # Startup can recover tens of millions of Bitcask records. Calling
    # v2_scan_file/1 materializes the full file scan result in BEAM before
    # recovery can insert the first key, so recovery must use the paged NIF.
    calls = nif_calls(@lifecycle_path)

    refute {:NIF, :v2_scan_file, 1} in calls
    refute {:NIF, :v2_scan_file_from_offset, 2} in calls
    assert {:NIF, :v2_scan_file_page, 3} in calls
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

  defp nif_calls(path) do
    {:ok, ast} =
      path
      |> File.read!()
      |> Code.string_to_quoted(columns: true)

    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., _meta, [{:__aliases__, _, [:NIF]}, fun]}, _call_meta, args} = node, acc
        when is_atom(fun) and is_list(args) ->
          {node, [{:NIF, fun, length(args)} | acc]}

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(calls)
  end
end
