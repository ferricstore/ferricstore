defmodule Ferricstore.MemoryGuardDefaultInstanceTest do
  use ExUnit.Case, async: false

  alias Ferricstore.MemoryGuard

  @default_instance_key {FerricStore.Instance, :default}

  test "pressure flag readers fail open when the default instance is not registered" do
    original = :persistent_term.get(@default_instance_key, :missing)

    if original != :missing do
      :persistent_term.erase(@default_instance_key)
    end

    try do
      refute MemoryGuard.reject_writes?()
      refute MemoryGuard.keydir_full?()
      refute MemoryGuard.skip_promotion?()
    after
      if original != :missing do
        :persistent_term.put(@default_instance_key, original)
      end
    end
  end

  test "stats omit RSS pressure instead of crashing when the default instance is not registered" do
    original = :persistent_term.get(@default_instance_key, :missing)

    if original != :missing do
      :persistent_term.erase(@default_instance_key)
    end

    if Process.whereis(MemoryGuard) == nil do
      start_supervised!({MemoryGuard, interval_ms: 60_000})
    end

    try do
      stats = MemoryGuard.stats()

      assert stats.rss_bytes == 0
      assert stats.rss_pressure_level == :ok
    after
      if original != :missing do
        :persistent_term.put(@default_instance_key, original)
      end
    end
  end

  test "periodic checks survive missing default instance during startup or teardown" do
    original = :persistent_term.get(@default_instance_key, :missing)

    if original != :missing do
      :persistent_term.erase(@default_instance_key)
    end

    pid =
      case Process.whereis(MemoryGuard) do
        nil -> start_supervised!({MemoryGuard, interval_ms: 60_000})
        pid -> pid
      end

    try do
      send(pid, :check)
      Process.sleep(20)

      assert Process.alive?(pid)
    after
      if original != :missing do
        :persistent_term.put(@default_instance_key, original)
      end
    end
  end
end
