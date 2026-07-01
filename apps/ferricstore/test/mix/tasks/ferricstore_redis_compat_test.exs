defmodule Mix.Tasks.Ferricstore.RedisCompatTest do
  use ExUnit.Case, async: false

  setup do
    Mix.shell(Mix.Shell.Process)
    Mix.Task.reenable("ferricstore.redis_compat")

    on_exit(fn ->
      Mix.shell(Mix.Shell.IO)
      Mix.Task.reenable("ferricstore.redis_compat")
    end)
  end

  test "prints a JSON matrix" do
    Mix.Tasks.Ferricstore.RedisCompat.run(["matrix", "--format", "json"])

    assert_receive {:mix_shell, :info, [json]}
    assert %{"matrix" => matrix} = Jason.decode!(json)
    assert %{"status" => "compatible"} = Enum.find(matrix, &(&1["command"] == "set"))
  end
end
