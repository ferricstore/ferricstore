defmodule FerricstoreServer.Spec.MemoryPressureTelemetryTest do
  @moduledoc """
  Spec section 2.4: Memory pressure level telemetry tests.

  Verifies that MemoryGuard emits the correct telemetry events with the
  spec-mandated pressure level names:

    * `[:ferricstore, :memory, :pressure]` with `%{level: :full}`     when keydir > 95%
    * `[:ferricstore, :memory, :pressure]` with `%{level: :pressure}` when keydir 85-95%
    * `[:ferricstore, :memory, :pressure]` with `%{level: :warn}`     when keydir 70-85%
    * `[:ferricstore, :memory, :pressure]` with `%{level: :ok}`       when keydir < 70%
    * `[:ferricstore, :hot_cache, :limit_reduced]`  when hot_cache budget shrinks under pressure
    * `[:ferricstore, :hot_cache, :limit_restored]`  when hot_cache budget recovers
  """

  use ExUnit.Case, async: false

  alias Ferricstore.MemoryGuard

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp attach(event_name) do
    test_pid = self()
    handler_id = "mem-pressure-test-#{inspect(event_name)}-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      event_name,
      fn event, measurements, metadata, config ->
        send(config.test_pid, {:telemetry, event, measurements, metadata})
      end,
      %{test_pid: test_pid}
    )

    handler_id
  end

  # Reset atomics flags after each test so that tiny-budget
  # MemoryGuard checks don't leak KEYDIR_FULL into other tests.
  setup do
    original_ctx = FerricStore.Instance.get(:default)
    global_guard = Process.whereis(MemoryGuard)

    # These tests exercise MemoryGuard's keydir/NIF accounting thresholds.
    # Disable host RSS in this module so macOS/Linux runner process size does
    # not force a higher overall pressure level than the synthetic budget.
    FerricStore.Instance.inject_callbacks(:default, process_rss_fn: fn -> 0 end)

    # The application-level guard also emits the same telemetry event. Suspend it
    # while this module tests standalone guards so background pressure telemetry
    # cannot race with the synthetic threshold assertions.
    if is_pid(global_guard) do
      try do
        :sys.suspend(global_guard)
      catch
        :exit, _ -> :ok
      end
    end

    on_exit(fn ->
      :persistent_term.put({FerricStore.Instance, :default}, original_ctx)

      if is_pid(global_guard) and Process.alive?(global_guard) do
        try do
          :sys.resume(global_guard)
        catch
          :exit, _ -> :ok
        end
      end

      MemoryGuard.reset_pressure_flags()
    end)

    MemoryGuard.reset_pressure_flags()
  end

  defp start_guard(opts) do
    defaults = [interval_ms: 60_000, shard_count: 4, eviction_policy: :volatile_lru]
    merged = Keyword.merge(defaults, opts)
    GenServer.start_link(MemoryGuard, merged)
  end

  defp trigger_check(pid) do
    send(pid, :check)
    # Allow the GenServer to process the :check message and emit telemetry
    Process.sleep(100)
  end

  # Trigger check repeatedly until we receive a telemetry event with the
  # expected level. ETS memory fluctuates between measurement and check,
  # so the ratio may land in a different band on the first try.
  defp trigger_until_level(pid, expected_level, retries \\ 15) do
    drain_pressure_messages()
    trigger_check(pid)

    receive do
      {:telemetry, [:ferricstore, :memory, :pressure], measurements, %{level: ^expected_level}} ->
        measurements
    after
      500 ->
        if retries > 0 do
          trigger_until_level(pid, expected_level, retries - 1)
        else
          nil
        end
    end
  end

  # Drains all messages matching the given pattern from the mailbox.
  # Used to discard events from the application-wide MemoryGuard or leftover
  # per-shard pressure events before asserting on the next expected event.
  defp drain_pressure_messages do
    receive do
      {:telemetry, [:ferricstore, :memory, :pressure], _, _} ->
        drain_pressure_messages()
    after
      0 -> :ok
    end
  end

  # ---------------------------------------------------------------------------
  # 1. :full level emitted at > 95% (ratio >= 0.95)
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :memory, :pressure] level: :full at >95%" do
    test "emits level: :full when memory ratio exceeds 95%" do
      handler_id = attach([:ferricstore, :memory, :pressure])

      # Drain any stale events from the application-wide guard
      drain_pressure_messages()

      # max_memory_bytes=1 forces ratio >> 0.95 since ETS tables always use some memory
      {:ok, pid} = start_guard(max_memory_bytes: 1)

      trigger_check(pid)

      # Match specifically on spec-level events (contain :level key, not :pressure_level)
      assert_receive {:telemetry, [:ferricstore, :memory, :pressure], measurements,
                      %{level: :full}},
                     2000

      assert is_float(measurements.ratio)
      assert measurements.ratio >= 0.95

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end

    test ":full event includes total_bytes and max_bytes measurements" do
      handler_id = attach([:ferricstore, :memory, :pressure])
      drain_pressure_messages()

      {:ok, pid} = start_guard(max_memory_bytes: 1)

      trigger_check(pid)

      assert_receive {:telemetry, [:ferricstore, :memory, :pressure], measurements,
                      %{level: :full}},
                     2000

      assert is_integer(measurements.total_bytes)
      assert measurements.total_bytes > 0
      assert is_integer(measurements.max_bytes)
      assert measurements.max_bytes == 1

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # 2. :pressure level emitted at 85-95%
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :memory, :pressure] level: :pressure at 85-95%" do
    test "emits level: :pressure when memory ratio is between 85% and 95%" do
      handler_id = attach([:ferricstore, :memory, :pressure])
      drain_pressure_messages()

      {:ok, pid} = start_guard(max_memory_bytes: 1_073_741_824)
      budget = budget_for_ratio(pid, 0.90)

      :sys.replace_state(pid, fn state ->
        %{state | max_memory_bytes: budget}
      end)

      measurements = trigger_until_level(pid, :pressure)
      assert measurements != nil, "expected :pressure level telemetry"
      assert measurements.ratio >= 0.85
      assert measurements.ratio < 0.95

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # 3. :warn level emitted at 70-85%
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :memory, :pressure] level: :warn at 70-85%" do
    test "emits level: :warn when memory ratio is between 70% and 85%" do
      handler_id = attach([:ferricstore, :memory, :pressure])
      drain_pressure_messages()

      {:ok, pid} = start_guard(max_memory_bytes: 1_073_741_824)
      budget = budget_for_ratio(pid, 0.77)

      :sys.replace_state(pid, fn state ->
        %{state | max_memory_bytes: budget}
      end)

      measurements = trigger_until_level(pid, :warn)
      assert measurements != nil, "expected :warn level telemetry"
      assert measurements.ratio >= 0.70
      assert measurements.ratio < 0.85

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # 4. :ok level emitted below 70%
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :memory, :pressure] level: :ok below 70%" do
    test "emits level: :ok when memory ratio is below 70%" do
      handler_id = attach([:ferricstore, :memory, :pressure])
      drain_pressure_messages()

      # 1 GB budget -- ratio will be very low
      {:ok, pid} = start_guard(max_memory_bytes: 1_073_741_824)

      trigger_check(pid)

      assert_receive {:telemetry, [:ferricstore, :memory, :pressure], measurements,
                      %{level: :ok}},
                     2000

      assert measurements.ratio < 0.70

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end

    test ":ok event includes all expected measurement fields" do
      handler_id = attach([:ferricstore, :memory, :pressure])
      drain_pressure_messages()

      {:ok, pid} = start_guard(max_memory_bytes: 1_073_741_824)

      trigger_check(pid)

      assert_receive {:telemetry, [:ferricstore, :memory, :pressure], measurements,
                      %{level: :ok}},
                     2000

      assert is_integer(measurements.total_bytes)
      assert is_integer(measurements.max_bytes)
      assert is_float(measurements.ratio)

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # 5. hot_cache limit_reduced fires during pressure
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :hot_cache, :limit_reduced]" do
    test "fires when transitioning from ok to a pressure state" do
      handler_id = attach([:ferricstore, :hot_cache, :limit_reduced])

      # Start with a generous budget (level :ok)
      {:ok, pid} = start_guard(max_memory_bytes: 1_073_741_824)

      trigger_check(pid)

      # Now shrink budget to force pressure, which should reduce hot_cache budget
      :sys.replace_state(pid, fn state ->
        %{state | max_memory_bytes: 1}
      end)

      trigger_check(pid)

      assert_receive {:telemetry, [:ferricstore, :hot_cache, :limit_reduced], measurements,
                      metadata},
                     2000

      assert is_integer(measurements.new_budget_bytes)
      assert is_integer(measurements.old_budget_bytes)
      assert measurements.new_budget_bytes < measurements.old_budget_bytes
      assert metadata.level in [:warn, :pressure, :full]

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end

    test "includes shard_count in metadata" do
      handler_id = attach([:ferricstore, :hot_cache, :limit_reduced])

      {:ok, pid} = start_guard(max_memory_bytes: 1_073_741_824)

      trigger_check(pid)

      :sys.replace_state(pid, fn state ->
        %{state | max_memory_bytes: 1}
      end)

      trigger_check(pid)

      assert_receive {:telemetry, [:ferricstore, :hot_cache, :limit_reduced], _measurements,
                      metadata},
                     2000

      assert metadata.shard_count == 4

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # 6. hot_cache limit_restored fires on recovery
  # ---------------------------------------------------------------------------

  describe "[:ferricstore, :hot_cache, :limit_restored]" do
    test "fires when transitioning from pressure back to ok" do
      handler_id = attach([:ferricstore, :hot_cache, :limit_restored])

      # Start under pressure
      {:ok, pid} = start_guard(max_memory_bytes: 1)

      trigger_check(pid)

      # Now restore to a generous budget
      :sys.replace_state(pid, fn state ->
        %{state | max_memory_bytes: 1_073_741_824}
      end)

      trigger_check(pid)

      assert_receive {:telemetry, [:ferricstore, :hot_cache, :limit_restored], measurements,
                      metadata},
                     2000

      assert is_integer(measurements.new_budget_bytes)
      assert is_integer(measurements.old_budget_bytes)
      assert measurements.new_budget_bytes > measurements.old_budget_bytes
      assert metadata.level == :ok

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end

    test "does not fire when staying at the same pressure level" do
      handler_id = attach([:ferricstore, :hot_cache, :limit_restored])

      # Start and stay with generous budget
      {:ok, pid} = start_guard(max_memory_bytes: 1_073_741_824)

      trigger_check(pid)
      trigger_check(pid)

      refute_received {:telemetry, [:ferricstore, :hot_cache, :limit_restored], _, _}

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # 7. Stress: rapid memory changes, correct level transitions
  # ---------------------------------------------------------------------------

  describe "stress: rapid memory changes produce correct level transitions" do
    test "cycles through all pressure levels with correct telemetry" do
      handler_id = attach([:ferricstore, :memory, :pressure])
      drain_pressure_messages()

      {:ok, pid} = start_guard(max_memory_bytes: 1_073_741_824)

      total_bytes = measure_guard_total_bytes(pid)

      # Cycle through levels: :ok -> :warn -> :pressure -> :full -> :ok
      budgets = [
        # :ok -- generous budget
        {1_073_741_824, :ok},
        # :warn -- ~77% usage
        {budget_from_total(total_bytes, 0.77), :warn},
        # :pressure -- ~90% usage
        {budget_from_total(total_bytes, 0.90), :pressure},
        # :full -- tiny budget
        {1, :full},
        # back to :ok
        {1_073_741_824, :ok}
      ]

      for {budget, expected_level} <- budgets do
        :sys.replace_state(pid, fn state ->
          %{state | max_memory_bytes: budget}
        end)

        measurements = trigger_until_level(pid, expected_level)
        assert measurements != nil, "expected #{inspect(expected_level)} level telemetry"
      end

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end

    test "rapid transitions emit both limit_reduced and limit_restored" do
      reduced_id = attach([:ferricstore, :hot_cache, :limit_reduced])
      restored_id = attach([:ferricstore, :hot_cache, :limit_restored])

      {:ok, pid} = start_guard(max_memory_bytes: 1_073_741_824)

      # :ok check
      trigger_check(pid)

      # Transition to :full (should emit limit_reduced)
      :sys.replace_state(pid, fn state ->
        %{state | max_memory_bytes: 1}
      end)

      trigger_check(pid)

      assert_receive {:telemetry, [:ferricstore, :hot_cache, :limit_reduced], _, _}, 2000

      # Transition back to :ok (should emit limit_restored)
      :sys.replace_state(pid, fn state ->
        %{state | max_memory_bytes: 1_073_741_824}
      end)

      trigger_check(pid)

      assert_receive {:telemetry, [:ferricstore, :hot_cache, :limit_restored], _, _}, 2000

      GenServer.stop(pid)
      :telemetry.detach(reduced_id)
      :telemetry.detach(restored_id)
    end

    test "multiple rapid checks at the same level emit pressure event each time" do
      handler_id = attach([:ferricstore, :memory, :pressure])
      drain_pressure_messages()

      {:ok, pid} = start_guard(max_memory_bytes: 1_073_741_824)

      # Run 3 checks, all at :ok level
      for _ <- 1..3 do
        trigger_check(pid)
      end

      # Should have received 3 :ok events (match specifically on spec-level events)
      for _ <- 1..3 do
        assert_receive {:telemetry, [:ferricstore, :memory, :pressure], _, %{level: :ok}}, 2000
      end

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp budget_for_ratio(pid, target_ratio) do
    pid
    |> measure_guard_total_bytes()
    |> budget_from_total(target_ratio)
  end

  defp budget_from_total(total_bytes, target_ratio) do
    if total_bytes > 0 do
      max(:erlang.ceil(total_bytes / target_ratio), 1)
    else
      100
    end
  end

  defp measure_guard_total_bytes(pid) do
    stats = GenServer.call(pid, :stats)
    stats.total_bytes
  end
end
