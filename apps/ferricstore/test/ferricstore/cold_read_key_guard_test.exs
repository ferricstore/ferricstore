defmodule Ferricstore.ColdReadKeyGuardTest do
  use ExUnit.Case, async: true

  @lib_root Path.expand("../../lib/ferricstore", __DIR__)
  @cold_read_module Path.join(@lib_root, "store/cold_read.ex")

  test "production cold reads validate the expected key" do
    offenders =
      @lib_root
      |> Path.join("**/*.ex")
      |> Path.wildcard()
      |> Enum.reject(&(&1 == @cold_read_module))
      |> Enum.flat_map(&offset_only_cold_read_calls/1)

    # Bitcask offsets can become stale after compaction, rollback repair, or
    # recovery. Production callers that read through ETS locations must pass the
    # expected key so the NIF can reject another record at the same offset.
    assert offenders == [],
           "expected production cold reads to use keyed pread APIs, got:\n" <>
             Enum.map_join(offenders, "\n", fn {path, line, call} ->
               "#{Path.relative_to_cwd(path)}:#{line}: #{call}"
             end)
  end

  defp offset_only_cold_read_calls(path) do
    ast =
      path
      |> File.read!()
      |> Code.string_to_quoted!(columns: true)

    {_ast, offenders} =
      Macro.prewalk(ast, [], fn
        {{:., meta, [module_ast, function]}, _call_meta, args} = node, acc
        when is_list(args) and function in [:pread_at, :pread_batch] ->
          if cold_read_module?(module_ast) and offset_only_call?(function, length(args)) do
            line = Keyword.get(meta, :line, 1)
            {node, [{path, line, "#{function}/#{length(args)}"} | acc]}
          else
            {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(offenders)
  end

  defp cold_read_module?({:__aliases__, _meta, [:Ferricstore, :Store, :ColdRead]}), do: true
  defp cold_read_module?({:__aliases__, _meta, [:ColdRead]}), do: true
  defp cold_read_module?(_module_ast), do: false

  defp offset_only_call?(:pread_batch, 2), do: true
  defp offset_only_call?(:pread_at, 3), do: true
  defp offset_only_call?(_function, _arity), do: false
end
