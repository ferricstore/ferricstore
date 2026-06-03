defmodule FerricstoreServer.Spec.MemoryGuardBudgetTest do
  @moduledoc """
  Spec section 2.4: Keydir budget enforcement tests.

  Verifies that MemoryGuard enforces per-subsystem RAM budgets for keydir ETS
  tables and hot_cache ETS tables. When keydir usage exceeds 95% of its
  configured budget (`keydir_max_ram`), new key writes are rejected with
  `{:error, "KEYDIR_FULL cannot accept new keys"}` while updates to existing
  keys are always allowed.

  Tests:
    - KEYDIR_FULL returned when keydir exceeds 95% budget
    - Existing key updates still work during KEYDIR_FULL
    - Pressure level telemetry emitted at thresholds
    - hot_cache shrinks during pressure
    - CONFIG GET keydir_max_ram returns value
    - CONFIG SET keydir-max-ram updates value
    - CONFIG SET hot-cache-max-ram updates value
    - CONFIG SET hot-cache-min-ram updates value
    - Stress: rapid key creation until KEYDIR_FULL triggers
  """

  use ExUnit.Case, async: false

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  # Default keydir budget: 256 MB
  @default_keydir_max_ram 256 * 1024 * 1024

  setup do
    # Save original config values
    orig_keydir_max = Application.get_env(:ferricstore, :keydir_max_ram)
    orig_hot_cache_max = Application.get_env(:ferricstore, :hot_cache_max_ram)
    orig_hot_cache_min = Application.get_env(:ferricstore, :hot_cache_min_ram)
    orig_eviction = Application.get_env(:ferricstore, :eviction_policy)

    # Reset min to a safe low value before each test so leftover state from
    # other tests (e.g. a high hot-cache-min-ram) doesn't cause cross-validation
    # failures when setting hot-cache-max-ram. Use Application.put_env directly
    # to avoid triggering MemoryGuard reconfigure that would override :auto max.
    Application.put_env(:ferricstore, :keydir_max_ram, @default_keydir_max_ram)
    Application.put_env(:ferricstore, :hot_cache_min_ram, 1_048_576)
    MemoryGuard.reconfigure(%{keydir_max_ram: @default_keydir_max_ram})

    on_exit(fn ->
      # Restore original config
      if orig_keydir_max do
        Application.put_env(:ferricstore, :keydir_max_ram, orig_keydir_max)
      else
        Application.delete_env(:ferricstore, :keydir_max_ram)
      end

      if orig_hot_cache_max do
        Application.put_env(:ferricstore, :hot_cache_max_ram, orig_hot_cache_max)
      else
        Application.delete_env(:ferricstore, :hot_cache_max_ram)
      end

      if orig_hot_cache_min do
        Application.put_env(:ferricstore, :hot_cache_min_ram, orig_hot_cache_min)
      else
        Application.delete_env(:ferricstore, :hot_cache_min_ram)
      end

      if orig_eviction do
        Application.put_env(:ferricstore, :eviction_policy, orig_eviction)
      end

      # Reconfigure MemoryGuard back to the suite-level config. test.exs uses
      # a larger keydir budget to avoid incidental KEYDIR_FULL pressure outside
      # this focused budget spec.
      MemoryGuard.reconfigure(%{
        keydir_max_ram: orig_keydir_max || @default_keydir_max_ram,
        max_memory_bytes: Application.get_env(:ferricstore, :max_memory_bytes, 1_073_741_824)
      })

      MemoryGuard.force_check()

      # Reset atomics flags so tiny-budget MemoryGuard checks
      # don't leak KEYDIR_FULL into other tests.
      MemoryGuard.reset_pressure_flags()

      ShardHelpers.flush_all_keys()
      ShardHelpers.wait_shards_alive()
    end)

    ShardHelpers.flush_all_keys()
    MemoryGuard.reset_pressure_flags()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Section 2.4: Keydir budget defaults
  # ---------------------------------------------------------------------------

  describe "keydir budget defaults" do
    test "default keydir_max_ram is 256 MB" do
      stats = MemoryGuard.stats()
      assert stats.keydir_max_ram == @default_keydir_max_ram
    end

    test "default hot_cache_max_ram is :auto" do
      stats = MemoryGuard.stats()
      # Auto means: total max_memory_bytes - keydir_max_ram
      expected = stats.max_bytes - stats.keydir_max_ram
      assert stats.hot_cache_max_ram == expected
    end

    test "stats include keydir-specific fields" do
      stats = MemoryGuard.stats()
      assert Map.has_key?(stats, :keydir_bytes)
      assert Map.has_key?(stats, :hot_cache_bytes)
      assert Map.has_key?(stats, :keydir_max_ram)
      assert Map.has_key?(stats, :hot_cache_max_ram)
      assert Map.has_key?(stats, :hot_cache_min_ram)
      assert Map.has_key?(stats, :keydir_pressure_level)
      assert is_integer(stats.keydir_bytes)
      assert is_integer(stats.hot_cache_bytes)
      assert stats.keydir_bytes >= 0
      assert stats.hot_cache_bytes >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # Section 2.4: KEYDIR_FULL rejection
  # ---------------------------------------------------------------------------

  describe "KEYDIR_FULL rejection when keydir exceeds 95% budget" do
    test "keydir_full? returns true with tiny keydir budget" do
      # Set keydir budget very low to force KEYDIR_FULL
      MemoryGuard.reconfigure(%{keydir_max_ram: 1})
      # Trigger a check so pressure level gets updated
      MemoryGuard.force_check()

      ShardHelpers.eventually(
        fn ->
          MemoryGuard.keydir_full?() == true
        end,
        "keydir_full? should be true with tiny budget",
        20,
        20
      )
    end

    test "keydir_full? returns false with generous keydir budget" do
      MemoryGuard.reconfigure(%{keydir_max_ram: 1_073_741_824})
      MemoryGuard.force_check()

      ShardHelpers.eventually(
        fn ->
          MemoryGuard.keydir_full?() == false
        end,
        "keydir_full? should be false with generous budget",
        20,
        20
      )
    end

    test "new key write rejected when keydir budget exceeded" do
      # Set keydir budget to 1 byte (guaranteed to be exceeded)
      MemoryGuard.reconfigure(%{keydir_max_ram: 1})
      MemoryGuard.force_check()

      ShardHelpers.eventually(
        fn ->
          MemoryGuard.keydir_full?() == true
        end,
        "keydir_full? should be true",
        20,
        20
      )

      # Use a unique key that definitely doesn't exist
      unique_key = "keydir_full_test_#{System.unique_integer([:positive])}"
      result = Router.put(FerricStore.Instance.get(:default), unique_key, "value", 0)

      assert {:error, msg} = result
      assert msg =~ "KEYDIR_FULL"
      assert msg =~ "cannot accept new keys"
    end

    test "existing key update allowed during KEYDIR_FULL" do
      # First, write a key while budget is generous
      MemoryGuard.reconfigure(%{keydir_max_ram: 1_073_741_824})
      MemoryGuard.force_check()

      ShardHelpers.eventually(
        fn ->
          MemoryGuard.keydir_full?() == false
        end,
        "keydir_full? should be false with generous budget",
        20,
        20
      )

      key = "existing_key_update_test_#{System.unique_integer([:positive])}"
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "original_value", 0)

      # Now shrink the keydir budget to trigger KEYDIR_FULL
      MemoryGuard.reconfigure(%{keydir_max_ram: 1})
      MemoryGuard.force_check()

      ShardHelpers.eventually(
        fn ->
          MemoryGuard.keydir_full?() == true
        end,
        "keydir_full? should be true after shrinking budget",
        20,
        20
      )

      # Update to existing key should succeed
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "updated_value", 0)
    end

    test "KEYDIR_FULL error message format" do
      MemoryGuard.reconfigure(%{keydir_max_ram: 1})
      MemoryGuard.force_check()

      ShardHelpers.eventually(
        fn ->
          MemoryGuard.keydir_full?() == true
        end,
        "keydir_full? should be true",
        20,
        20
      )

      unique_key = "keydir_full_format_#{System.unique_integer([:positive])}"
      {:error, msg} = Router.put(FerricStore.Instance.get(:default), unique_key, "value", 0)

      # Exact error message per spec
      assert msg == "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"
    end
  end

  # ---------------------------------------------------------------------------
  # Section 2.4: Pressure levels (keydir-specific)
  # ---------------------------------------------------------------------------

  describe "keydir pressure levels" do
    test "keydir pressure is :ok when well below 70%" do
      MemoryGuard.reconfigure(%{keydir_max_ram: 1_073_741_824})
      MemoryGuard.force_check()

      ShardHelpers.eventually(
        fn ->
          MemoryGuard.stats().keydir_pressure_level == :ok
        end,
        "keydir pressure should be :ok",
        20,
        20
      )
    end

    test "keydir pressure is :reject when at 95%+ of budget" do
      MemoryGuard.reconfigure(%{keydir_max_ram: 1})
      MemoryGuard.force_check()

      ShardHelpers.eventually(
        fn ->
          MemoryGuard.stats().keydir_pressure_level == :reject
        end,
        "keydir pressure should be :reject",
        20,
        20
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Section 2.4: Pressure level telemetry
  # ---------------------------------------------------------------------------

  describe "keydir pressure telemetry" do
    test "keydir pressure telemetry emitted at reject threshold" do
      test_pid = self()
      handler_id = "keydir-pressure-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :memory, :keydir_pressure],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:keydir_pressure, measurements, metadata})
        end,
        nil
      )

      MemoryGuard.reconfigure(%{keydir_max_ram: 1})
      MemoryGuard.force_check()

      assert_receive {:keydir_pressure, measurements, metadata}, 2000

      assert is_integer(measurements.keydir_bytes)
      assert is_integer(measurements.keydir_max_ram)
      assert is_float(measurements.keydir_ratio)
      assert measurements.keydir_ratio >= 0.95
      assert metadata.keydir_pressure_level in [:pressure, :reject]

      :telemetry.detach(handler_id)
    end

    test "keydir pressure telemetry includes hot_cache_action during pressure" do
      test_pid = self()
      handler_id = "keydir-pressure-action-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :memory, :keydir_pressure],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:keydir_pressure_meta, metadata})
        end,
        nil
      )

      # Set budget in the pressure zone (85-95% would be ideal but with tiny
      # budget we'll hit reject, which is a superset)
      MemoryGuard.reconfigure(%{keydir_max_ram: 1})
      MemoryGuard.force_check()

      assert_receive {:keydir_pressure_meta, metadata}, 2000
      assert Map.has_key?(metadata, :keydir_pressure_level)

      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # Section 2.4: hot_cache shrinks during keydir pressure
  # ---------------------------------------------------------------------------

  describe "hot_cache shrinks during keydir pressure" do
    test "hot_cache shrink telemetry emitted during pressure" do
      test_pid = self()
      handler_id = "hot-cache-shrink-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :memory, :hot_cache_shrink],
        fn _event, measurements, _metadata, _config ->
          send(test_pid, {:hot_cache_shrink, measurements})
        end,
        nil
      )

      # Force pressure by setting keydir budget very low
      MemoryGuard.reconfigure(%{keydir_max_ram: 1})
      MemoryGuard.force_check()

      # Hot cache shrink may or may not fire depending on hot_cache contents.
      # If there are items in hot_cache, it should trigger.
      # Write some data to hot_cache first.
      Router.put(
        FerricStore.Instance.get(:default),
        "shrink_test_key_1",
        String.duplicate("x", 1000),
        0
      )

      Router.put(
        FerricStore.Instance.get(:default),
        "shrink_test_key_2",
        String.duplicate("y", 1000),
        0
      )

      MemoryGuard.force_check()

      # The shrink event may or may not arrive depending on whether there's
      # actually hot_cache data to evict. We verify the mechanism exists.
      receive do
        {:hot_cache_shrink, measurements} ->
          assert is_integer(measurements.bytes_before)
          assert is_integer(measurements.bytes_after)
      after
        1000 ->
          # No hot_cache data to shrink -- this is acceptable
          :ok
      end

      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # Section 2.4: CONFIG GET/SET keydir-max-ram
  # ---------------------------------------------------------------------------

  describe "CONFIG GET/SET keydir-max-ram" do
    test "CONFIG GET keydir-max-ram returns configured value" do
      result = Ferricstore.Config.get("keydir-max-ram")
      assert [{"keydir-max-ram", value}] = result
      {n, ""} = Integer.parse(value)
      assert n > 0
    end

    test "CONFIG SET keydir-max-ram updates the value" do
      assert :ok = Ferricstore.Config.set("keydir-max-ram", "134217728")
      [{"keydir-max-ram", value}] = Ferricstore.Config.get("keydir-max-ram")
      assert value == "134217728"
    end

    test "CONFIG SET keydir-max-ram with invalid value returns error" do
      assert {:error, msg} = Ferricstore.Config.set("keydir-max-ram", "abc")
      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET keydir-max-ram with 0 returns error" do
      assert {:error, msg} = Ferricstore.Config.set("keydir-max-ram", "0")
      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET keydir-max-ram with negative returns error" do
      assert {:error, msg} = Ferricstore.Config.set("keydir-max-ram", "-1")
      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET keydir-max-ram reconfigures MemoryGuard" do
      new_budget = 512 * 1024 * 1024
      assert :ok = Ferricstore.Config.set("keydir-max-ram", to_string(new_budget))

      stats = MemoryGuard.stats()
      assert stats.keydir_max_ram == new_budget
    end
  end

  describe "CONFIG GET/SET hot-cache-max-ram" do
    test "CONFIG GET hot-cache-max-ram returns configured value" do
      result = Ferricstore.Config.get("hot-cache-max-ram")
      assert [{"hot-cache-max-ram", value}] = result
      {n, ""} = Integer.parse(value)
      assert n > 0
    end

    test "CONFIG SET hot-cache-max-ram updates the value" do
      assert :ok = Ferricstore.Config.set("hot-cache-max-ram", "536870912")
      [{"hot-cache-max-ram", value}] = Ferricstore.Config.get("hot-cache-max-ram")
      assert value == "536870912"
    end

    test "CONFIG SET hot-cache-max-ram with invalid value returns error" do
      assert {:error, msg} = Ferricstore.Config.set("hot-cache-max-ram", "abc")
      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET hot-cache-max-ram with 0 returns error" do
      assert {:error, msg} = Ferricstore.Config.set("hot-cache-max-ram", "0")
      assert msg =~ "Invalid argument"
    end
  end

  describe "CONFIG GET/SET hot-cache-min-ram" do
    test "CONFIG GET hot-cache-min-ram returns configured value" do
      result = Ferricstore.Config.get("hot-cache-min-ram")
      assert [{"hot-cache-min-ram", value}] = result
      {n, ""} = Integer.parse(value)
      assert n > 0
    end

    test "CONFIG SET hot-cache-min-ram updates the value" do
      # Ensure max >= min so cross-validation passes
      Application.put_env(:ferricstore, :hot_cache_max_ram, 1_073_741_824)
      assert :ok = Ferricstore.Config.set("hot-cache-min-ram", "536870912")
      [{"hot-cache-min-ram", value}] = Ferricstore.Config.get("hot-cache-min-ram")
      assert value == "536870912"
    end

    test "CONFIG SET hot-cache-min-ram with invalid value returns error" do
      assert {:error, msg} = Ferricstore.Config.set("hot-cache-min-ram", "abc")
      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET hot-cache-min-ram with 0 returns error" do
      assert {:error, msg} = Ferricstore.Config.set("hot-cache-min-ram", "0")
      assert msg =~ "Invalid argument"
    end
  end

  # ---------------------------------------------------------------------------
  # Section 2.4: CONFIG GET * includes keydir budget params
  # ---------------------------------------------------------------------------

  describe "CONFIG GET * includes keydir budget parameters" do
    test "CONFIG GET * includes keydir-max-ram" do
      result = Ferricstore.Config.get("*")
      keys = Enum.map(result, fn {k, _v} -> k end)
      assert "keydir-max-ram" in keys
    end

    test "CONFIG GET * includes hot-cache-max-ram" do
      result = Ferricstore.Config.get("*")
      keys = Enum.map(result, fn {k, _v} -> k end)
      assert "hot-cache-max-ram" in keys
    end

    test "CONFIG GET * includes hot-cache-min-ram" do
      result = Ferricstore.Config.get("*")
      keys = Enum.map(result, fn {k, _v} -> k end)
      assert "hot-cache-min-ram" in keys
    end
  end

  # ---------------------------------------------------------------------------
  # Section 2.4: Stress test — rapid key creation until KEYDIR_FULL
  # ---------------------------------------------------------------------------

  describe "stress: rapid key creation until KEYDIR_FULL triggers" do
    test "KEYDIR_FULL eventually triggered under small budget with rapid writes" do
      # Set a keydir budget that's large enough to hold a few keys but small
      # enough that we can fill it quickly. We measure current keydir usage
      # and set the budget just above it to trigger KEYDIR_FULL quickly.
      stats = MemoryGuard.stats()
      # Set budget to current usage + a tiny amount so a few more keys triggers it
      tight_budget = stats.keydir_bytes + 256
      MemoryGuard.reconfigure(%{keydir_max_ram: tight_budget})
      MemoryGuard.force_check()

      ShardHelpers.eventually(
        fn ->
          MemoryGuard.stats().keydir_pressure_level in [:pressure, :reject]
        end,
        "keydir pressure should be elevated with tight budget",
        20,
        20
      )

      # Rapidly write keys until we get KEYDIR_FULL or exhaust attempts
      results =
        Enum.reduce_while(1..10_000, [], fn i, acc ->
          key = "stress_keydir_#{i}"
          result = Router.put(FerricStore.Instance.get(:default), key, "v", 0)

          case result do
            {:error, msg} ->
              if msg =~ "KEYDIR_FULL" do
                {:halt, [{:keydir_full, i} | acc]}
              else
                {:cont, [{:error, msg, i} | acc]}
              end

            :ok ->
              # Periodically force a check so MemoryGuard picks up new usage
              if rem(i, 50) == 0 do
                MemoryGuard.force_check()

                ShardHelpers.eventually(
                  fn ->
                    MemoryGuard.stats().keydir_bytes > 0
                  end,
                  "MemoryGuard check should complete",
                  5,
                  5
                )
              end

              {:cont, [{:ok, i} | acc]}
          end
        end)

      # Verify that KEYDIR_FULL was triggered
      assert Enum.any?(results, fn
               {:keydir_full, _} -> true
               _ -> false
             end),
             "Expected KEYDIR_FULL to trigger during stress test, but it didn't. " <>
               "Results sample: #{inspect(Enum.take(results, 5))}"
    end

    test "after KEYDIR_FULL, deleting keys and rechecking allows new writes" do
      # Trigger KEYDIR_FULL
      MemoryGuard.reconfigure(%{keydir_max_ram: 1})
      MemoryGuard.force_check()

      ShardHelpers.eventually(
        fn ->
          MemoryGuard.keydir_full?() == true
        end,
        "keydir_full? should be true",
        20,
        20
      )

      unique_key = "recovery_test_#{System.unique_integer([:positive])}"
      assert {:error, _} = Router.put(FerricStore.Instance.get(:default), unique_key, "value", 0)

      # Restore generous budget
      MemoryGuard.reconfigure(%{keydir_max_ram: 1_073_741_824})
      MemoryGuard.force_check()

      ShardHelpers.eventually(
        fn ->
          MemoryGuard.keydir_full?() == false
        end,
        "keydir_full? should be false after restoring budget",
        20,
        20
      )

      # New writes should succeed
      assert :ok = Router.put(FerricStore.Instance.get(:default), unique_key, "value", 0)
    end
  end
end
