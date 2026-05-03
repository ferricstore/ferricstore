defmodule Ferricstore.FsUsageGuardTest do
  use ExUnit.Case, async: true

  @moduletag :guard

  @blocked_file_fns MapSet.new([
                      :dir?,
                      :exists?,
                      :ls,
                      :ls!,
                      :mkdir_p,
                      :mkdir_p!,
                      :rename,
                      :rename!,
                      :rm,
                      :rm!,
                      :rm_rf,
                      :rm_rf!,
                      :touch,
                      :touch!
                    ])

  @allowed_paths MapSet.new([
                   "lib/ferricstore/fs.ex",
                   "lib/ferricstore/bitcask/nif.ex",
                   "lib/mix/tasks/compile/patched_wal.ex"
                 ])

  test "storage production code uses Ferricstore.FS for filesystem metadata" do
    offenders =
      ["lib/ferricstore/**/*.ex", "../ferricstore_server/lib/ferricstore_server/health/**/*.ex"]
      |> Enum.flat_map(&Path.wildcard/1)
      |> Enum.reject(&MapSet.member?(@allowed_paths, &1))
      |> Enum.flat_map(&raw_file_metadata_calls/1)

    assert offenders == []
  end

  defp raw_file_metadata_calls(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted(columns: true)

    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., meta, [{:__aliases__, _, [:File]}, fun]}, _call_meta, _args} = node, acc
        when is_atom(fun) ->
          if MapSet.member?(@blocked_file_fns, fun) do
            {node, [{path, meta[:line], fun} | acc]}
          else
            {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(calls)
  end
end
