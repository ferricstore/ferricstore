defmodule Ferricstore.TestConfigGuardTest do
  use ExUnit.Case, async: false

  @repo_root Path.expand("../../../..", __DIR__)
  @test_config Path.join(@repo_root, "config/test.exs")

  setup do
    original_data_dir = System.get_env("FERRICSTORE_DATA_DIR")
    System.delete_env("FERRICSTORE_DATA_DIR")

    on_exit(fn -> restore_env("FERRICSTORE_DATA_DIR", original_data_dir) end)
    :ok
  end

  test "independent test config evaluations use isolated data directories" do
    first_config = read_test_config()
    second_config = read_test_config()
    first = Keyword.fetch!(first_config, :data_dir)
    second = Keyword.fetch!(second_config, :data_dir)

    refute first == second
    assert Path.dirname(first) == Path.expand(System.tmp_dir!())
    assert Path.dirname(second) == Path.expand(System.tmp_dir!())
    assert Keyword.fetch!(first_config, :test_data_dir_auto_cleanup)
    assert Keyword.fetch!(second_config, :test_data_dir_auto_cleanup)
  end

  test "explicit test data directory is honored and never marked for automatic cleanup" do
    override = Path.join(System.tmp_dir!(), "ferricstore-explicit-test-root")
    System.put_env("FERRICSTORE_DATA_DIR", override)

    config = read_test_config()

    assert Keyword.fetch!(config, :data_dir) == Path.expand(override)
    refute Keyword.fetch!(config, :test_data_dir_auto_cleanup)
  end

  defp read_test_config do
    @test_config
    |> Config.Reader.read!(env: :test, target: :host)
    |> Keyword.fetch!(:ferricstore)
  end

  defp restore_env(name, nil), do: System.delete_env(name)
  defp restore_env(name, value), do: System.put_env(name, value)
end
