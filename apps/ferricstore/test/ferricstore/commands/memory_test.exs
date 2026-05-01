defmodule Ferricstore.Commands.MemoryTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Dispatcher, Memory}
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # MEMORY USAGE
  # ---------------------------------------------------------------------------

  describe "MEMORY USAGE" do
    test "returns positive integer for existing key" do
      store = MockStore.make(%{"mykey" => {"hello", 0}})
      result = Memory.handle("USAGE", ["mykey"], store)

      assert is_integer(result)
      assert result > 0
      # Should be at least overhead (96) + key bytes (5) + value bytes (5)
      assert result >= 96 + 5 + 5
    end

    test "returns nil for missing key" do
      store = MockStore.make()
      assert nil == Memory.handle("USAGE", ["nonexistent"], store)
    end

    test "accounts for key and value sizes" do
      short_store = MockStore.make(%{"k" => {"v", 0}})
      long_store = MockStore.make(%{"k" => {String.duplicate("x", 1000), 0}})

      short_usage = Memory.handle("USAGE", ["k"], short_store)
      long_usage = Memory.handle("USAGE", ["k"], long_store)

      assert long_usage > short_usage
      assert long_usage - short_usage >= 999
    end

    test "with SAMPLES argument still works" do
      store = MockStore.make(%{"mykey" => {"value", 0}})
      result = Memory.handle("USAGE", ["mykey", "SAMPLES", "5"], store)

      assert is_integer(result)
      assert result > 0
    end

    test "uses stored value size without loading cold value" do
      store = %{
        value_size: fn "cold" -> 10_000 end,
        get: fn _key -> flunk("MEMORY USAGE should not load a cold string value") end
      }

      assert 96 + byte_size("cold") + 10_000 == Memory.handle("USAGE", ["cold"], store)
    end

    test "with no args returns error" do
      store = MockStore.make()
      assert {:error, _} = Memory.handle("USAGE", [], store)
    end
  end

  # ---------------------------------------------------------------------------
  # MEMORY DOCTOR
  # ---------------------------------------------------------------------------

  describe "MEMORY DOCTOR" do
    test "returns diagnostic string" do
      store = MockStore.make()
      result = Memory.handle("DOCTOR", [], store)

      assert is_binary(result)
      assert result =~ "no memory problems"
    end

    test "with args returns error" do
      store = MockStore.make()
      assert {:error, _} = Memory.handle("DOCTOR", ["extra"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # MEMORY STATS
  # ---------------------------------------------------------------------------

  describe "MEMORY STATS" do
    test "returns flat list of key-value pairs" do
      store = MockStore.make()
      result = Memory.handle("STATS", [], store)

      assert is_list(result)
      # Result is a flat list [key, value, key, value, ...]
      assert rem(length(result), 2) == 0
    end

    test "includes expected stat keys" do
      store = MockStore.make()
      result = Memory.handle("STATS", [], store)

      keys = every_other(result, 0)
      assert "peak.allocated" in keys
      assert "total.allocated" in keys
      assert "dataset.bytes" in keys
      assert "process.memory" in keys
      assert "ets.memory" in keys
    end

    test "total.allocated is a positive integer" do
      store = MockStore.make()
      result = Memory.handle("STATS", [], store)

      idx = Enum.find_index(result, &(&1 == "total.allocated"))
      total = Enum.at(result, idx + 1)

      assert is_integer(total)
      assert total > 0
    end

    test "with args returns error" do
      store = MockStore.make()
      assert {:error, _} = Memory.handle("STATS", ["extra"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # MEMORY HELP
  # ---------------------------------------------------------------------------

  describe "MEMORY HELP" do
    test "returns list of help strings" do
      store = MockStore.make()
      result = Memory.handle("HELP", [], store)

      assert [_ | _] = result
      assert Enum.all?(result, &is_binary/1)
    end

    test "help mentions all subcommands" do
      store = MockStore.make()
      result = Memory.handle("HELP", [], store)
      text = Enum.join(result, " ")

      assert text =~ "DOCTOR"
      assert text =~ "HELP"
      assert text =~ "MALLOC-STATS"
      assert text =~ "PURGE"
      assert text =~ "STATS"
      assert text =~ "USAGE"
    end

    test "with args returns error" do
      store = MockStore.make()
      assert {:error, _} = Memory.handle("HELP", ["extra"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # MEMORY PURGE
  # ---------------------------------------------------------------------------

  describe "MEMORY PURGE" do
    test "returns OK" do
      store = MockStore.make()
      assert :ok = Memory.handle("PURGE", [], store)
    end

    test "with args returns error" do
      store = MockStore.make()
      assert {:error, _} = Memory.handle("PURGE", ["extra"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # MEMORY MALLOC-STATS
  # ---------------------------------------------------------------------------

  describe "MEMORY MALLOC-STATS" do
    test "returns string" do
      store = MockStore.make()
      result = Memory.handle("MALLOC-STATS", [], store)

      assert is_binary(result)
      assert result =~ "not available"
    end

    test "with args returns error" do
      store = MockStore.make()
      assert {:error, _} = Memory.handle("MALLOC-STATS", ["extra"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # MEMORY unknown subcommand
  # ---------------------------------------------------------------------------

  describe "MEMORY unknown" do
    test "unknown subcommand returns error" do
      store = MockStore.make()
      result = Memory.handle("BADSUBCMD", [], store)

      assert {:error, msg} = result
      assert msg =~ "unknown subcommand"
    end
  end

  # ---------------------------------------------------------------------------
  # Dispatcher integration
  # ---------------------------------------------------------------------------

  describe "MEMORY via Dispatcher" do
    test "MEMORY USAGE dispatches correctly" do
      store = MockStore.make(%{"k" => {"v", 0}})
      result = Dispatcher.dispatch("MEMORY", ["USAGE", "k"], store)
      assert is_integer(result)
      assert result > 0
    end

    test "MEMORY DOCTOR dispatches correctly" do
      store = MockStore.make()
      result = Dispatcher.dispatch("MEMORY", ["DOCTOR"], store)
      assert is_binary(result)
    end

    test "MEMORY with no subcommand returns error" do
      store = MockStore.make()
      result = Dispatcher.dispatch("MEMORY", [], store)
      assert {:error, _} = result
    end

    test "MEMORY is case-insensitive" do
      store = MockStore.make(%{"k" => {"v", 0}})
      result = Dispatcher.dispatch("memory", ["usage", "k"], store)
      assert is_integer(result)
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp every_other(list, offset) do
    list
    |> Enum.drop(offset)
    |> Enum.take_every(2)
  end
end
