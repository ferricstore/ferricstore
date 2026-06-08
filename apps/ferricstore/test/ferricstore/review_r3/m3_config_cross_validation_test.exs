defmodule Ferricstore.ReviewR3.M3ConfigCrossValidationTest do
  @moduledoc """
  Verifies CONFIG SET rejects hot-cache-min-ram > hot-cache-max-ram
  and vice versa.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Config

  setup do
    # Save originals
    orig_min = Application.get_env(:ferricstore, :hot_cache_min_ram)
    orig_max = Application.get_env(:ferricstore, :hot_cache_max_ram)

    # Reset to safe defaults before each test so leftover state from other tests
    # (e.g. a high hot-cache-min-ram) doesn't cause cross-validation failures.
    Ferricstore.Config.set("hot-cache-min-ram", "1048576")
    Ferricstore.Config.set("hot-cache-max-ram", "1073741824")

    on_exit(fn ->
      if orig_min, do: Application.put_env(:ferricstore, :hot_cache_min_ram, orig_min)
      if orig_max, do: Application.put_env(:ferricstore, :hot_cache_max_ram, orig_max)
    end)

    :ok
  end

  describe "hot-cache min/max cross-validation" do
    test "setting max below current min is rejected" do
      # Reset to known state: min=100MB, max=1GB
      Application.put_env(:ferricstore, :hot_cache_min_ram, 1_073_741_824)
      Application.put_env(:ferricstore, :hot_cache_max_ram, 2_000_000_000)

      # Try to set max to 100MB (below min of 1GB) — should fail
      result = Config.set("hot-cache-max-ram", "104857600")
      assert {:error, msg} = result
      assert msg =~ "must be >= hot-cache-min-ram"
    end

    test "setting min above current max is rejected" do
      # Reset to known state: min=50MB, max=100MB
      Application.put_env(:ferricstore, :hot_cache_min_ram, 52_428_800)
      Application.put_env(:ferricstore, :hot_cache_max_ram, 104_857_600)

      # Try to set min to 1GB (above max of 100MB) — should fail
      result = Config.set("hot-cache-min-ram", "1073741824")
      assert {:error, msg} = result
      assert msg =~ "must be <= hot-cache-max-ram"
    end

    test "setting min below max succeeds" do
      assert :ok = Config.set("hot-cache-max-ram", "1073741824")
      assert :ok = Config.set("hot-cache-min-ram", "104857600")
    end

    test "setting max above min succeeds" do
      assert :ok = Config.set("hot-cache-min-ram", "104857600")
      assert :ok = Config.set("hot-cache-max-ram", "1073741824")
    end

    test "setting min equal to max succeeds" do
      assert :ok = Config.set("hot-cache-max-ram", "536870912")
      assert :ok = Config.set("hot-cache-min-ram", "536870912")
    end
  end
end
