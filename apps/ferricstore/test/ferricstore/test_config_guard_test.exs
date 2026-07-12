defmodule Ferricstore.TestConfigGuardTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../..", __DIR__)
  @test_config Path.join(@repo_root, "config/test.exs")

  test "independent test config evaluations use isolated data directories" do
    first = read_test_data_dir()
    second = read_test_data_dir()

    refute first == second
    assert Path.dirname(first) == Path.expand(System.tmp_dir!())
    assert Path.dirname(second) == Path.expand(System.tmp_dir!())
  end

  defp read_test_data_dir do
    @test_config
    |> Config.Reader.read!(env: :test, target: :host)
    |> Keyword.fetch!(:ferricstore)
    |> Keyword.fetch!(:data_dir)
  end
end
