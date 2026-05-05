defmodule Ferricstore.Commands.ProbTypeTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.ProbType

  describe "large cold string classification" do
    test "check_expected returns WRONGTYPE without loading a large cold value" do
      store = large_cold_string_store(self(), "cold_string", 1_000_000)

      assert {:error, msg} = ProbType.check_expected("cold_string", :bloom, store)
      assert msg =~ "WRONGTYPE"
      refute_received {:loaded_cold_value, "cold_string"}
    end

    test "check_create returns WRONGTYPE without loading a large cold value" do
      store = large_cold_string_store(self(), "cold_string", 1_000_000)

      assert {:error, msg} = ProbType.check_create("cold_string", :cms, store)
      assert msg =~ "WRONGTYPE"
      refute_received {:loaded_cold_value, "cold_string"}
    end
  end

  defp large_cold_string_store(test_pid, key, value_size) do
    %{
      value_size: fn ^key -> value_size end,
      get: fn ^key ->
        send(test_pid, {:loaded_cold_value, key})
        :binary.copy("x", value_size)
      end,
      compound_get: fn _redis_key, _compound_key -> nil end,
      exists?: fn ^key -> true end
    }
  end
end
