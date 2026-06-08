defmodule FerricstoreServer.ArchTest do
  use ExUnit.Case, async: false
  use ArchTest, app: :ferricstore_server

  @max_production_file_lines 1_000

  test "server production files stay below the agreed readability budget" do
    assert files_over_line_budget(server_production_files()) == []
  end

  test "server implementation does not use anonymous part section files" do
    assert Path.wildcard(server_path("lib/**/sections/part_*.ex")) == []
  end

  test "dashboard render modules stay presentation-only" do
    render_modules = modules_matching("FerricstoreServer.Health.Dashboard.Render.**")

    render_modules
    |> should_not_depend_on(modules_matching("Ferricstore.Store.**"))

    render_modules
    |> should_not_depend_on(modules_matching("Ferricstore.Raft.**"))

    render_modules
    |> should_not_depend_on(modules_matching("Ferricstore.Commands.**"))

    render_modules
    |> should_not_depend_on(modules_matching("Ferricstore.Bitcask.**"))
  end

  test "connection hot path does not depend on health dashboard modules" do
    modules_matching("FerricstoreServer.Connection.**")
    |> excluding("FerricstoreServer.Connection.Dashboard")
    |> should_not_depend_on(modules_matching("FerricstoreServer.Health.**"))
  end

  defp server_production_files do
    server_path("lib/**/*.ex")
    |> Path.wildcard()
    |> Enum.reject(&String.contains?(&1, "/test/"))
  end

  defp server_path(path), do: Path.expand("../../#{path}", __DIR__)

  defp files_over_line_budget(paths) do
    paths
    |> Enum.map(fn path -> {Path.relative_to_cwd(path), line_count(path)} end)
    |> Enum.filter(fn {_path, count} -> count > @max_production_file_lines end)
  end

  defp line_count(path) do
    path
    |> File.stream!()
    |> Enum.count()
  end
end
