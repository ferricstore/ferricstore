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
end
