defmodule Ferricstore.ConfigIntegrityTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Config

  test "only the config owner may alter authentication settings in ETS" do
    assert :ets.info(:ferricstore_config, :owner) == Process.whereis(Config)
    assert :ets.info(:ferricstore_config, :protection) == :protected

    original = Config.get_value("requirepass") || ""
    on_exit(fn -> Config.set("requirepass", original) end)

    assert :ok = Config.set("requirepass", "owner-secret")

    result =
      Task.async(fn ->
        try do
          :ets.insert(:ferricstore_config, {"requirepass", ""})
          :wrote
        rescue
          ArgumentError -> :protected
        end
      end)
      |> Task.await()

    assert result == :protected
    assert Config.get_value("requirepass") == "owner-secret"
  end

  test "rejects unbounded config values without replacing the current value" do
    original = Config.get_value("requirepass") || ""
    on_exit(fn -> Config.set("requirepass", original) end)

    assert {:error, message} = Config.set("requirepass", :binary.copy("x", 65_537))
    assert message =~ "too large"
    assert Config.get_value("requirepass") == original
  end

  test "rejects unsupported keyspace notification flags" do
    original = Config.get_value("notify-keyspace-events") || ""
    on_exit(fn -> Config.set("notify-keyspace-events", original) end)

    assert {:error, message} = Config.set("notify-keyspace-events", "KEZ")
    assert message =~ "Invalid argument"
    assert Config.get_value("notify-keyspace-events") == original
  end

  test "rewrite rejects invalid UTF-8 without crashing the config server" do
    original_data_dir = Application.get_env(:ferricstore, :data_dir)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_config_invalid_utf8_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(data_dir)
    Application.put_env(:ferricstore, :data_dir, data_dir)
    File.write!(Config.config_file_path(), <<255, 10>>)

    on_exit(fn ->
      case original_data_dir do
        nil -> Application.delete_env(:ferricstore, :data_dir)
        value -> Application.put_env(:ferricstore, :data_dir, value)
      end

      File.rm_rf!(data_dir)
    end)

    assert {:error, message} = Config.rewrite()
    assert message =~ "valid UTF-8"
    assert Process.alive?(Process.whereis(Config))
  end
end
