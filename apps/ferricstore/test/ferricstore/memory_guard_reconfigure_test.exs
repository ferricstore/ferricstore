defmodule Ferricstore.MemoryGuardReconfigureTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.MemoryGuard

  setup do
    original = :sys.get_state(MemoryGuard)

    on_exit(fn ->
      MemoryGuard.reconfigure(%{
        max_memory_bytes: original.max_memory_bytes,
        keydir_max_ram: original.keydir_max_ram,
        hot_cache_min_ram: original.hot_cache_min_ram,
        hot_cache_max_ram:
          if(Map.get(original, :hot_cache_max_mode, :auto) == :auto,
            do: :auto,
            else: original.hot_cache_max_ram
          ),
        eviction_policy: original.eviction_policy
      })
    end)

    {:ok, original: original}
  end

  test "rejects malformed updates without mutating or crashing", %{original: original} do
    invalid_updates = [
      %{unknown_budget: 1},
      %{max_memory_bytes: 0},
      %{max_memory_bytes: "1 GB"},
      %{keydir_max_ram: -1},
      %{hot_cache_min_ram: -1},
      %{hot_cache_max_ram: -1},
      %{eviction_policy: :random},
      %{hot_cache_min_ram: 2, hot_cache_max_ram: 1}
    ]

    Enum.each(invalid_updates, fn params ->
      assert {:error, message} = MemoryGuard.reconfigure(params)
      assert String.starts_with?(message, "ERR ")
      assert Process.alive?(Process.whereis(MemoryGuard))
      assert comparable_state(:sys.get_state(MemoryGuard)) == comparable_state(original)
    end)
  end

  test "derives a non-negative automatic hot-cache budget" do
    assert :ok =
             MemoryGuard.reconfigure(%{
               max_memory_bytes: 1_024,
               keydir_max_ram: 1_024,
               hot_cache_min_ram: 0,
               hot_cache_max_ram: :auto
             })

    assert %{hot_cache_max_ram: 0} = :sys.get_state(MemoryGuard)
  end

  test "unrelated updates preserve an explicit hot-cache budget" do
    assert :ok =
             MemoryGuard.reconfigure(%{
               hot_cache_min_ram: 0,
               hot_cache_max_ram: 1_024
             })

    assert :ok = MemoryGuard.reconfigure(%{eviction_policy: :noeviction})
    assert %{hot_cache_max_ram: 1_024} = :sys.get_state(MemoryGuard)
  end

  defp comparable_state(state) do
    Map.take(state, [
      :max_memory_bytes,
      :keydir_max_ram,
      :hot_cache_min_ram,
      :hot_cache_max_ram,
      :eviction_policy
    ])
  end
end
