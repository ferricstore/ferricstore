defmodule Ferricstore.ArchTest do
  use ExUnit.Case, async: false
  use ArchTest, app: :ferricstore

  test "core library source does not reference the server application" do
    assert module_reference_violations(core_production_files(), :FerricstoreServer) == []
  end

  test "public API layer does not depend on durability internals" do
    api_modules = modules_matching("FerricStore.API.**")

    api_modules
    |> should_not_depend_on(modules_matching("Ferricstore.Raft.**"))

    api_modules
    |> should_not_depend_on(modules_matching("Ferricstore.Bitcask.**"))
  end

  test "bitcask wrapper does not depend on higher core layers" do
    bitcask_modules = modules_matching("Ferricstore.Bitcask.**")

    bitcask_modules
    |> should_not_depend_on(modules_matching("Ferricstore.Store.**"))

    bitcask_modules
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))
  end

  test "ordinary command modules do not depend on Raft internals" do
    modules_matching("Ferricstore.Commands.**")
    |> excluding("Ferricstore.Commands.Cluster")
    |> excluding("Ferricstore.Commands.Server")
    |> excluding("Ferricstore.Commands.Server.Info")
    |> should_not_depend_on(modules_matching("Ferricstore.Raft.**"))
  end

  defp core_production_files do
    Path.wildcard(core_path("lib/**/*.ex"))
  end

  defp core_path(path), do: Path.expand("../../#{path}", __DIR__)

  defp module_reference_violations(paths, root_module) do
    paths
    |> Enum.filter(fn path ->
      {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()

      {_, referenced?} =
        Macro.prewalk(ast, false, fn
          {:__aliases__, _, [^root_module | _]} = node, _acc -> {node, true}
          node, acc -> {node, acc}
        end)

      referenced?
    end)
    |> Enum.map(&Path.relative_to_cwd/1)
  end
end
