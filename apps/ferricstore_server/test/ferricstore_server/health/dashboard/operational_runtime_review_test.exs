defmodule FerricstoreServer.Health.Dashboard.OperationalRuntimeReviewTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Health.Dashboard.Data.Operational

  test "effective runtime values reflect CONFIG SET and never reveal configured secrets" do
    previous = Ferricstore.Config.get_value("slowlog-max-len")
    previous_secret = Ferricstore.Config.get_value("requirepass")

    on_exit(fn ->
      Ferricstore.Config.set("slowlog-max-len", previous)
      Ferricstore.Config.set("requirepass", previous_secret)
    end)

    assert :ok = Ferricstore.Config.set("slowlog-max-len", "257")
    assert :ok = Ferricstore.Config.set("requirepass", "operational-review-secret")

    parameters = Operational.collect_config_page().config_parameters

    assert %{value: "257", source: "CONFIG GET"} =
             Enum.find(parameters, &(&1.parameter == "slowlog-max-len"))

    assert %{value: level, source: "Logger.level"} =
             Enum.find(parameters, &(&1.parameter == "log_level"))

    assert level == to_string(Logger.level())
    assert %{value: secret} = Enum.find(parameters, &(&1.parameter == "requirepass"))
    assert secret == Ferricstore.Config.get("requirepass") |> Map.new() |> Map.get("requirepass")
    refute inspect(parameters) =~ "operational-review-secret"
  end

  test "the memory collector keeps the existing guard snapshot's RSS and budget fields" do
    memory = Operational.collect_memory()
    assert Map.has_key?(memory, :rss_bytes)
    assert Map.has_key?(memory, :rss_ratio)
    assert Map.has_key?(memory, :memory_limit)
    assert Map.has_key?(memory, :keydir_max_ram)
  end
end
