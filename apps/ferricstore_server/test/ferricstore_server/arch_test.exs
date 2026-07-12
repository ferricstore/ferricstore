defmodule FerricstoreServer.ArchTest do
  use ExUnit.Case, async: false
  use ArchTest, app: :ferricstore_server

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

  test "native connection path does not depend on health or dashboard modules" do
    modules_matching("FerricstoreServer.Native.Connection.**")
    |> should_not_depend_on(modules_matching("FerricstoreServer.Health.**"))
  end
end
