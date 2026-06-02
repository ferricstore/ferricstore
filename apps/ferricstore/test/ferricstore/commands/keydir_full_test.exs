defmodule Ferricstore.Commands.KeydirFullTest do
  @moduledoc """
  Tests for the KEYDIR_FULL error when the keydir (ETS) approaches its
  memory limit under the `:noeviction` policy (spec section 2.4).

  New-key writes are rejected when MemoryGuard reports reject-level pressure
  and the eviction policy is `:noeviction`. Updates to existing keys always
  succeed regardless of pressure level.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  # ---------------------------------------------------------------------------
  # Setup: flush keys and save/restore MemoryGuard state
  # ---------------------------------------------------------------------------

  setup do
    ShardHelpers.flush_all_keys()

    # This module intentionally mutates the global MemoryGuard. Start every test
    # from a writable baseline so leaked pressure from earlier suites cannot make
    # the seed writes fail before the scenario under test begins.
    force_ok_mode()

    on_exit(fn ->
      # Leave the shared application MemoryGuard writable for later synchronous
      # command suites; pressure scenarios are local to this module.
      force_ok_mode()
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Forces MemoryGuard into reject+noeviction state so reject_writes?() is true.
  defp force_reject_mode do
    :sys.replace_state(MemoryGuard, fn state ->
      %{state | last_pressure_level: :reject, eviction_policy: :noeviction}
    end)
    # Also update persistent_term so the hot-path reads see the change
    MemoryGuard.set_reject_writes(true)
    MemoryGuard.set_keydir_full(true)
  end

  # Forces MemoryGuard into ok pressure (no rejection).
  defp force_ok_mode do
    :sys.replace_state(MemoryGuard, fn state ->
      %{state | last_pressure_level: :ok, keydir_pressure_level: :ok, eviction_policy: :noeviction}
    end)
    MemoryGuard.set_reject_writes(false)
    MemoryGuard.set_keydir_full(false)
  end

  # Forces MemoryGuard into reject pressure but with a non-noeviction policy.
  # With volatile_lru, eviction handles pressure -- new keys are NOT blocked.
  defp force_reject_with_eviction do
    :sys.replace_state(MemoryGuard, fn state ->
      %{state | last_pressure_level: :reject, eviction_policy: :volatile_lru}
    end)
    # volatile_lru + reject = don't reject writes (eviction handles it)
    MemoryGuard.set_reject_writes(false)
    MemoryGuard.set_keydir_full(false)
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "KEYDIR_FULL rejection (spec 2.4)" do
    test "new key is rejected when pressure is at reject level with noeviction" do
      # Write one key before enabling reject mode.
      assert :ok = Router.put(FerricStore.Instance.get(:default), "seed_key", "value", 0)

      # Switch MemoryGuard to reject+noeviction mode.
      force_reject_mode()
      assert MemoryGuard.reject_writes?() == true

      # Try writing a NEW key -- should be rejected.
      result = Router.put(FerricStore.Instance.get(:default), "brand_new_key", "value", 0)
      assert {:error, msg} = result
      assert msg =~ "KEYDIR_FULL"
    end

    test "update to existing key succeeds even under reject pressure" do
      # Write a key before pressure kicks in.
      assert :ok = Router.put(FerricStore.Instance.get(:default), "existing_key", "old_value", 0)

      # Switch to reject mode.
      force_reject_mode()
      assert MemoryGuard.reject_writes?() == true

      # Update the EXISTING key -- should succeed despite pressure.
      assert :ok = Router.put(FerricStore.Instance.get(:default), "existing_key", "new_value", 0)

      # Verify the update took effect.
      assert Router.get(FerricStore.Instance.get(:default), "existing_key") == "new_value"
    end

    test "new key succeeds when pressure is below reject threshold" do
      force_ok_mode()
      assert MemoryGuard.reject_writes?() == false

      # New key should succeed.
      assert :ok = Router.put(FerricStore.Instance.get(:default), "allowed_key", "value", 0)
      assert Router.get(FerricStore.Instance.get(:default), "allowed_key") == "value"
    end

    test "new key succeeds when eviction policy is not :noeviction" do
      # Reject-level pressure but with volatile_lru policy.
      force_reject_with_eviction()

      # reject_writes? should be false because policy is volatile_lru, not noeviction.
      assert MemoryGuard.reject_writes?() == false

      # New key should succeed.
      assert :ok = Router.put(FerricStore.Instance.get(:default), "new_volatile_key", "value", 0)
      assert Router.get(FerricStore.Instance.get(:default), "new_volatile_key") == "value"
    end

    test "KEYDIR_FULL error message is correct" do
      assert :ok = Router.put(FerricStore.Instance.get(:default), "seed_msg", "v", 0)
      force_reject_mode()
      assert MemoryGuard.reject_writes?() == true

      result = Router.put(FerricStore.Instance.get(:default), "fail_key", "v", 0)

      assert {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"} = result
    end

    test "multiple new key attempts are all rejected under pressure" do
      force_reject_mode()

      for i <- 1..5 do
        result = Router.put(FerricStore.Instance.get(:default), "new_key_#{i}", "v", 0)
        assert {:error, msg} = result
        assert msg =~ "KEYDIR_FULL"
      end
    end

    test "existing key with TTL update succeeds under pressure" do
      # Write key with no expiry.
      assert :ok = Router.put(FerricStore.Instance.get(:default), "ttl_key", "value", 0)

      force_reject_mode()

      # Update with new TTL -- should succeed (it's an existing key).
      expire_at = System.os_time(:millisecond) + 60_000
      assert :ok = Router.put(FerricStore.Instance.get(:default), "ttl_key", "value", expire_at)
    end
  end
end
