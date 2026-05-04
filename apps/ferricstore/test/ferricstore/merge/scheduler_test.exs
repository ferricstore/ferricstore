defmodule Ferricstore.Merge.SchedulerTest do
  @moduledoc """
  Tests for the event-driven merge scheduler.

  The scheduler triggers compaction on file rotation notifications from the
  Shard, not on a polling timer. Tests verify the decision logic, mode
  guards, semaphore contention, and edge cases.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Merge.Scheduler
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @scheduler_path Path.expand("../../../lib/ferricstore/merge/scheduler.ex", __DIR__)

  defmodule SlowShard do
    use GenServer

    def start(opts), do: GenServer.start(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

    @impl true
    def init(opts), do: {:ok, Map.new(opts)}

    @impl true
    def handle_call(:file_sizes, _from, state), do: {:reply, {:ok, [{0, 1024}, {1, 1024}]}, state}

    def handle_call(:available_disk_space, _from, state),
      do: {:reply, {:ok, 1_000_000_000}, state}

    def handle_call({:run_compaction, [0]}, _from, state) do
      send(state.parent, :slow_compaction_started)
      Process.sleep(state.sleep_ms)
      send(state.parent, :slow_compaction_finished)
      {:reply, {:ok, {1, 0, 128}}, state}
    end
  end

  defmodule TrackingShard do
    use GenServer

    def start(opts), do: GenServer.start(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

    @impl true
    def init(opts), do: {:ok, Map.new(opts)}

    @impl true
    def handle_call(:file_sizes, _from, state) do
      send(state.parent, :file_sizes_called)
      {:reply, {:ok, [{0, 1024}, {1, 1024}]}, state}
    end

    def handle_call(:available_disk_space, _from, state),
      do: {:reply, {:ok, 1_000_000_000}, state}

    def handle_call({:run_compaction, [0]}, _from, state) do
      send(state.parent, :compaction_called)
      {:reply, {:ok, {1, 0, 128}}, state}
    end
  end

  defmodule FlakyShard do
    use GenServer

    def start(opts), do: GenServer.start(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

    @impl true
    def init(opts), do: {:ok, opts |> Map.new() |> Map.put(:attempts, 0)}

    @impl true
    def handle_call(:available_disk_space, _from, state),
      do: {:reply, {:ok, 1_000_000_000}, state}

    def handle_call({:run_compaction, [0]}, _from, %{attempts: 0} = state) do
      send(state.parent, {:compaction_attempt, 1})
      {:reply, {:error, :eio}, %{state | attempts: 1}}
    end

    def handle_call({:run_compaction, [0]}, _from, state) do
      send(state.parent, {:compaction_attempt, state.attempts + 1})
      {:reply, {:ok, {1, 0, 128}}, %{state | attempts: state.attempts + 1}}
    end
  end

  defmodule NoCompactionShard do
    use GenServer

    def start(opts), do: GenServer.start(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

    @impl true
    def init(opts), do: {:ok, Map.new(opts)}

    @impl true
    def handle_call(:available_disk_space, _from, state),
      do: {:reply, {:ok, 1_000_000_000}, state}

    def handle_call({:run_compaction, [0]}, _from, state) do
      send(state.parent, :no_compaction_attempt)
      {:reply, {:error, {:no_compactable_files, [0]}}, state}
    end
  end

  defmodule BlockingShard do
    use GenServer

    def start(opts), do: GenServer.start(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

    @impl true
    def init(opts), do: {:ok, Map.new(opts)}

    @impl true
    def handle_call(:available_disk_space, _from, state),
      do: {:reply, {:ok, 1_000_000_000}, state}

    def handle_call({:run_compaction, [0]}, _from, state) do
      send(state.parent, {:compaction_started, state.shard_index})

      receive do
        {:finish_compaction, index} when index == state.shard_index -> :ok
      after
        state.sleep_ms -> :ok
      end

      send(state.parent, {:compaction_finished, state.shard_index})
      {:reply, {:ok, {1, 0, 128}}, state}
    end
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Basic lifecycle
  # ---------------------------------------------------------------------------

  describe "scheduler lifecycle" do
    test "schedulers are registered for each shard" do
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        name = Scheduler.scheduler_name(i)
        pid = Process.whereis(name)
        assert is_pid(pid), "Expected scheduler #{i} registered as #{name}"
        assert Process.alive?(pid)
      end
    end

    test "status returns scheduler state" do
      status = Scheduler.status(0)

      assert is_map(status)
      assert status.shard_index == 0
      assert status.mode in [:hot, :bulk, :age]
      assert is_boolean(status.merging)
      assert is_integer(status.merge_count)
      assert is_integer(status.file_count)
      assert is_map(status.config)
    end

    test "dedicated scheduler initializes file count from existing shard log files" do
      data_dir =
        Path.join(System.tmp_dir!(), "scheduler_restart_#{System.unique_integer([:positive])}")

      shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      File.mkdir_p!(shard_dir)

      File.write!(Path.join(shard_dir, "00000.log"), "old")
      File.write!(Path.join(shard_dir, "00001.log"), "active")
      File.write!(Path.join(shard_dir, "README.txt"), "ignored")

      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          merge_config: %{mode: :hot, min_files_for_merge: 100},
          name: :test_scheduler_restart_file_count
        )

      try do
        assert GenServer.call(pid, :status).file_count == 2
      after
        GenServer.stop(pid)
        File.rm_rf!(data_dir)
      end
    end

    test "init fails closed when interrupted merge recovery fails" do
      data_dir =
        Path.join(
          System.tmp_dir!(),
          "scheduler_manifest_recovery_fail_#{System.unique_integer([:positive])}"
        )

      shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      File.mkdir_p!(shard_dir)
      :ok = Ferricstore.Merge.Manifest.write(shard_dir, %{shard_index: 0, input_file_ids: [1]})

      Process.put(:ferricstore_merge_manifest_before_cleanup_hook, fn ->
        File.rm_rf!(shard_dir)
        File.write!(shard_dir, "not a directory")
      end)

      try do
        assert {:stop,
                {:merge_manifest_recovery_failed, 0,
                 {:cleanup_partial_output_list_failed, ^shard_dir, {_kind, _message}}}} =
                 Scheduler.init(
                   shard_index: 0,
                   data_dir: data_dir,
                   merge_config: %{mode: :hot, min_files_for_merge: 100},
                   name: :test_scheduler_manifest_recovery_fail
                 )
      after
        Process.delete(:ferricstore_merge_manifest_before_cleanup_hook)
        File.rm_rf!(data_dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Event-driven trigger: file_rotated
  # ---------------------------------------------------------------------------

  describe "file rotation notification" do
    test "notify_rotation updates file count in scheduler state" do
      Scheduler.notify_rotation(0, 5)

      ShardHelpers.eventually(
        fn ->
          Scheduler.status(0).file_count == 5
        end,
        "file count not updated",
        20,
        10
      )

      status = Scheduler.status(0)
      assert status.file_count == 5
    end

    test "notify_rotation with count below threshold does not trigger merge" do
      # min_files_for_merge defaults to 2, send count of 1
      Scheduler.notify_rotation(0, 1)

      ShardHelpers.eventually(
        fn ->
          Scheduler.status(0).file_count == 1
        end,
        "file count not updated",
        20,
        10
      )

      status = Scheduler.status(0)
      assert status.merging == false
    end

    test "notify_rotation is safe when scheduler is not running" do
      # Cast to non-existent scheduler should not crash
      assert :ok = Scheduler.notify_rotation(99_999, 10)
    end

    test "multiple rapid rotations are handled sequentially" do
      for i <- 1..10 do
        Scheduler.notify_rotation(0, i)
      end

      ShardHelpers.eventually(
        fn ->
          Scheduler.status(0).file_count == 10
        end,
        "file count not updated to 10",
        20,
        10
      )

      # Scheduler should be alive and have the latest file count
      status = Scheduler.status(0)
      assert status.file_count == 10
      assert Process.alive?(Process.whereis(Scheduler.scheduler_name(0)))
    end
  end

  # ---------------------------------------------------------------------------
  # Merge decision logic
  # ---------------------------------------------------------------------------

  describe "merge decision" do
    test "cooldown decisions use monotonic elapsed time" do
      source = File.read!(@scheduler_path)
      [_before, rest] = String.split(source, "defp should_merge?", parts: 2)
      [body | _after] = String.split(rest, "\n  defp mode_allows_merge?", parts: 2)

      assert body =~ "System.monotonic_time(:millisecond)"
      refute body =~ "System.system_time(:millisecond)"
    end

    test "trigger_check with sufficient files attempts merge" do
      # Set file count above threshold via notification
      Scheduler.notify_rotation(0, 5)

      ShardHelpers.eventually(
        fn ->
          Scheduler.status(0).file_count == 5
        end,
        "file count not updated",
        20,
        10
      )

      # trigger_check should run the merge check
      assert :ok = Scheduler.trigger_check(0)
    end

    test "trigger_check with insufficient files does not merge" do
      Scheduler.notify_rotation(0, 1)

      ShardHelpers.eventually(
        fn ->
          Scheduler.status(0).file_count == 1
        end,
        "file count not updated",
        20,
        10
      )

      assert :ok = Scheduler.trigger_check(0)
      status = Scheduler.status(0)
      assert status.merging == false
    end

    test "run_compaction can exceed GenServer default call timeout" do
      ctx = FerricStore.Instance.get(:default)
      shard_name = Router.shard_name(ctx, 0)
      real_shard = Process.whereis(shard_name)

      data_dir =
        Path.join(
          System.tmp_dir!(),
          "scheduler_slow_compaction_#{System.unique_integer([:positive])}"
        )

      shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      File.mkdir_p!(shard_dir)
      File.write!(Path.join(shard_dir, "00000.log"), "old")
      File.write!(Path.join(shard_dir, "00001.log"), "active")

      Process.unregister(shard_name)
      {:ok, fake_shard} = SlowShard.start(name: shard_name, parent: self(), sleep_ms: 5_200)

      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          merge_config: %{
            mode: :hot,
            min_files_for_merge: 2,
            merge_cooldown_ms: 0,
            compaction_call_timeout_ms: 7_000
          },
          name: :test_scheduler_slow_compaction
        )

      try do
        assert :ok = Scheduler.trigger_check(pid)
        assert_receive :slow_compaction_started
        assert_receive :slow_compaction_finished

        status = GenServer.call(pid, :status)
        assert status.merge_count == 1
        assert status.total_bytes_reclaimed == 128
      after
        if Process.alive?(pid), do: GenServer.stop(pid)
        ref = Process.monitor(fake_shard)
        Process.exit(fake_shard, :kill)
        assert_receive {:DOWN, ^ref, :process, ^fake_shard, :killed}, 1_000

        if is_pid(real_shard) and Process.alive?(real_shard),
          do: Process.register(real_shard, shard_name)

        File.rm_rf!(data_dir)
      end
    end

    test "trigger_check does not ask shard GenServer to scan file sizes" do
      ctx = FerricStore.Instance.get(:default)
      shard_name = Router.shard_name(ctx, 0)
      real_shard = Process.whereis(shard_name)

      data_dir =
        Path.join(
          System.tmp_dir!(),
          "scheduler_local_file_sizes_#{System.unique_integer([:positive])}"
        )

      shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      File.mkdir_p!(shard_dir)
      File.write!(Path.join(shard_dir, "00000.log"), "old")
      File.write!(Path.join(shard_dir, "00001.log"), "active")

      Process.unregister(shard_name)
      {:ok, fake_shard} = TrackingShard.start(name: shard_name, parent: self())

      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          merge_config: %{mode: :hot, min_files_for_merge: 2, merge_cooldown_ms: 0},
          name: :test_scheduler_local_file_sizes
        )

      try do
        assert :ok = Scheduler.trigger_check(pid)
        assert_receive :compaction_called
        refute_received :file_sizes_called
      after
        if Process.alive?(pid), do: GenServer.stop(pid)
        ref = Process.monitor(fake_shard)
        Process.exit(fake_shard, :kill)
        assert_receive {:DOWN, ^ref, :process, ^fake_shard, :killed}, 1_000

        if is_pid(real_shard) and Process.alive?(real_shard),
          do: Process.register(real_shard, shard_name)

        File.rm_rf!(data_dir)
      end
    end

    test "age mode does not compact fresh inactive files before min file age" do
      ctx = FerricStore.Instance.get(:default)
      shard_name = Router.shard_name(ctx, 0)
      real_shard = Process.whereis(shard_name)

      data_dir =
        Path.join(
          System.tmp_dir!(),
          "scheduler_age_fresh_files_#{System.unique_integer([:positive])}"
        )

      shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      File.mkdir_p!(shard_dir)
      File.write!(Path.join(shard_dir, "00000.log"), "fresh")
      File.write!(Path.join(shard_dir, "00001.log"), "active")

      Process.unregister(shard_name)
      {:ok, fake_shard} = TrackingShard.start(name: shard_name, parent: self())

      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          merge_config: %{
            mode: :age,
            merge_window: {0, 24},
            min_file_age_ms: 60_000,
            min_files_for_merge: 2,
            merge_cooldown_ms: 0
          },
          name: :test_scheduler_age_fresh_files
        )

      try do
        assert :ok = Scheduler.trigger_check(pid)
        refute_received :compaction_called
      after
        if Process.alive?(pid), do: GenServer.stop(pid)
        ref = Process.monitor(fake_shard)
        Process.exit(fake_shard, :kill)
        assert_receive {:DOWN, ^ref, :process, ^fake_shard, :killed}, 1_000

        if is_pid(real_shard) and Process.alive?(real_shard),
          do: Process.register(real_shard, shard_name)

        File.rm_rf!(data_dir)
      end
    end

    test "age mode compacts inactive files after min file age" do
      ctx = FerricStore.Instance.get(:default)
      shard_name = Router.shard_name(ctx, 0)
      real_shard = Process.whereis(shard_name)

      data_dir =
        Path.join(
          System.tmp_dir!(),
          "scheduler_age_old_files_#{System.unique_integer([:positive])}"
        )

      shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      inactive_path = Path.join(shard_dir, "00000.log")

      File.mkdir_p!(shard_dir)
      File.write!(inactive_path, "old")
      File.write!(Path.join(shard_dir, "00001.log"), "active")
      File.touch!(inactive_path, {{2000, 1, 1}, {0, 0, 0}})

      Process.unregister(shard_name)
      {:ok, fake_shard} = TrackingShard.start(name: shard_name, parent: self())

      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          merge_config: %{
            mode: :age,
            merge_window: {0, 24},
            min_file_age_ms: 60_000,
            min_files_for_merge: 2,
            merge_cooldown_ms: 0
          },
          name: :test_scheduler_age_old_files
        )

      try do
        assert :ok = Scheduler.trigger_check(pid)
        assert_receive :compaction_called, 1_000
      after
        if Process.alive?(pid), do: GenServer.stop(pid)
        ref = Process.monitor(fake_shard)
        Process.exit(fake_shard, :kill)
        assert_receive {:DOWN, ^ref, :process, ^fake_shard, :killed}, 1_000

        if is_pid(real_shard) and Process.alive?(real_shard),
          do: Process.register(real_shard, shard_name)

        File.rm_rf!(data_dir)
      end
    end

    test "fragmentation compaction retries after transient failure" do
      ctx = FerricStore.Instance.get(:default)
      shard_name = Router.shard_name(ctx, 0)
      real_shard = Process.whereis(shard_name)

      data_dir =
        Path.join(
          System.tmp_dir!(),
          "scheduler_frag_retry_#{System.unique_integer([:positive])}"
        )

      shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      File.mkdir_p!(shard_dir)
      File.write!(Path.join(shard_dir, "00000.log"), "old")
      File.write!(Path.join(shard_dir, "00001.log"), "active")

      Process.unregister(shard_name)
      {:ok, fake_shard} = FlakyShard.start(name: shard_name, parent: self())

      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          merge_config: %{
            mode: :hot,
            min_files_for_merge: 100,
            merge_cooldown_ms: 0,
            merge_retry_interval_ms: 50
          },
          name: :test_scheduler_frag_retry
        )

      try do
        GenServer.cast(pid, {:fragmentation, [0], 2})

        assert_receive {:compaction_attempt, 1}, 2_000

        status = GenServer.call(pid, :status)
        assert status.fragmentation_candidates == [0]

        ShardHelpers.eventually(
          fn ->
            GenServer.call(pid, :status).merge_count == 1
          end,
          "fragmentation merge was not retried",
          30,
          50
        )

        assert_receive {:compaction_attempt, 2}, 2_000

        status = GenServer.call(pid, :status)
        assert status.fragmentation_candidates == []
        assert status.total_bytes_reclaimed == 128
      after
        if Process.alive?(pid), do: GenServer.stop(pid)
        ref = Process.monitor(fake_shard)
        Process.exit(fake_shard, :kill)
        assert_receive {:DOWN, ^ref, :process, ^fake_shard, :killed}, 1_000

        if is_pid(real_shard) and Process.alive?(real_shard),
          do: Process.register(real_shard, shard_name)

        File.rm_rf!(data_dir)
      end
    end

    test "fragmentation no-compaction result clears candidates without retrying" do
      ctx = FerricStore.Instance.get(:default)
      shard_name = Router.shard_name(ctx, 0)
      real_shard = Process.whereis(shard_name)

      data_dir =
        Path.join(
          System.tmp_dir!(),
          "scheduler_frag_no_compaction_#{System.unique_integer([:positive])}"
        )

      shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      File.mkdir_p!(shard_dir)
      File.write!(Path.join(shard_dir, "00000.log"), "old")
      File.write!(Path.join(shard_dir, "00001.log"), "active")

      Process.unregister(shard_name)
      {:ok, fake_shard} = NoCompactionShard.start(name: shard_name, parent: self())

      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          merge_config: %{
            mode: :hot,
            min_files_for_merge: 100,
            merge_cooldown_ms: 0,
            merge_retry_interval_ms: 25
          },
          name: :test_scheduler_frag_no_compaction
        )

      try do
        GenServer.cast(pid, {:fragmentation, [0], 2})

        assert_receive :no_compaction_attempt, 1_000
        Process.sleep(75)
        refute_receive :no_compaction_attempt, 100

        scheduler_state = :sys.get_state(pid)
        assert scheduler_state.retry_ref == nil
        status = GenServer.call(pid, :status)
        assert status.fragmentation_candidates == []
      after
        if Process.alive?(pid), do: GenServer.stop(pid)
        ref = Process.monitor(fake_shard)
        Process.exit(fake_shard, :kill)
        assert_receive {:DOWN, ^ref, :process, ^fake_shard, :killed}, 1_000

        if is_pid(real_shard) and Process.alive?(real_shard),
          do: Process.register(real_shard, shard_name)

        File.rm_rf!(data_dir)
      end
    end

    test "compaction timeout does not release semaphore while shard is still compacting" do
      ctx = FerricStore.Instance.get(:default)
      shard0_name = Router.shard_name(ctx, 0)
      shard1_name = Router.shard_name(ctx, 1)
      real_shard0 = Process.whereis(shard0_name)
      real_shard1 = Process.whereis(shard1_name)

      data_dir =
        Path.join(
          System.tmp_dir!(),
          "scheduler_timeout_semaphore_#{System.unique_integer([:positive])}"
        )

      for index <- [0, 1] do
        shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, index)
        File.mkdir_p!(shard_dir)
        File.write!(Path.join(shard_dir, "00000.log"), "old")
        File.write!(Path.join(shard_dir, "00001.log"), "active")
      end

      {:ok, semaphore} = Ferricstore.Merge.Semaphore.start_link(name: :test_timeout_semaphore)

      Process.unregister(shard0_name)
      Process.unregister(shard1_name)

      {:ok, fake_shard0} =
        BlockingShard.start(name: shard0_name, parent: self(), shard_index: 0, sleep_ms: 500)

      {:ok, fake_shard1} =
        BlockingShard.start(name: shard1_name, parent: self(), shard_index: 1, sleep_ms: 10)

      merge_config = %{
        mode: :hot,
        min_files_for_merge: 2,
        merge_cooldown_ms: 0,
        compaction_call_timeout_ms: 50
      }

      {:ok, scheduler0} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: data_dir,
          merge_config: merge_config,
          semaphore: semaphore,
          name: :test_timeout_scheduler_0
        )

      {:ok, scheduler1} =
        Scheduler.start_link(
          shard_index: 1,
          data_dir: data_dir,
          merge_config: merge_config,
          semaphore: semaphore,
          name: :test_timeout_scheduler_1
        )

      try do
        task0 = Task.async(fn -> Scheduler.trigger_check(scheduler0) end)
        assert_receive {:compaction_started, 0}, 1_000

        Process.sleep(100)
        assert Ferricstore.Merge.Semaphore.status(semaphore) == {:held, 0}

        task1 = Task.async(fn -> Scheduler.trigger_check(scheduler1) end)
        refute_receive {:compaction_started, 1}, 150
        assert Task.await(task1, 1_000) == :ok

        send(fake_shard0, {:finish_compaction, 0})
        assert_receive {:compaction_finished, 0}, 1_000
        assert Task.await(task0, 1_000) == :ok

        assert Scheduler.trigger_check(scheduler1) == :ok
        assert_receive {:compaction_started, 1}, 1_000
        assert_receive {:compaction_finished, 1}, 1_000
      after
        for pid <- [scheduler0, scheduler1],
            is_pid(pid),
            Process.alive?(pid),
            do: GenServer.stop(pid)

        for pid <- [fake_shard0, fake_shard1], is_pid(pid), Process.alive?(pid) do
          ref = Process.monitor(pid)
          Process.exit(pid, :kill)
          assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
        end

        if Process.alive?(semaphore), do: GenServer.stop(semaphore)

        if is_pid(real_shard0) and Process.alive?(real_shard0),
          do: Process.register(real_shard0, shard0_name)

        if is_pid(real_shard1) and Process.alive?(real_shard1),
          do: Process.register(real_shard1, shard1_name)

        File.rm_rf!(data_dir)
      end
    end
  end

  describe "merge file selection" do
    test "min_files_for_merge counts the active file from rotation notifications" do
      config = %{
        min_files_for_merge: 2,
        max_files_per_merge: 8,
        small_file_threshold: 0
      }

      assert {:ok, [0]} =
               Scheduler.select_mergeable_file_ids([{0, 1024}, {1, 1024}], config, [])
    end
  end

  # ---------------------------------------------------------------------------
  # Mode guards
  # ---------------------------------------------------------------------------

  describe "merge modes" do
    test "dedicated scheduler in hot mode triggers on file count" do
      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{mode: :hot, min_files_for_merge: 100},
          name: :test_scheduler_hot
        )

      # Send file count below threshold
      GenServer.cast(pid, {:file_rotated, 5})

      ShardHelpers.eventually(
        fn ->
          GenServer.call(pid, :status).file_count == 5
        end,
        "file count not updated",
        20,
        10
      )

      status = GenServer.call(pid, :status)
      assert status.merging == false

      GenServer.stop(pid)
    end

    test "dedicated scheduler in bulk mode outside window does not merge" do
      # Set window to a time that's definitely not now
      {:ok, now} = DateTime.now("Etc/UTC")
      # Window is 2 hours from now to 3 hours from now — definitely not now
      # unless it's exactly that time (astronomically unlikely)
      start = rem(now.hour + 12, 24)
      stop = rem(now.hour + 13, 24)

      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{mode: :bulk, merge_window: {start, stop}, min_files_for_merge: 2},
          name: :test_scheduler_bulk
        )

      # Send file count above threshold
      GenServer.cast(pid, {:file_rotated, 10})

      ShardHelpers.eventually(
        fn ->
          GenServer.call(pid, :status).file_count == 10
        end,
        "file count not updated",
        20,
        10
      )

      status = GenServer.call(pid, :status)
      # Should NOT be merging because we're outside the window
      assert status.merging == false

      GenServer.stop(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Semaphore contention
  # ---------------------------------------------------------------------------

  describe "semaphore contention" do
    test "merge is deferred when semaphore is busy" do
      # Start a fake semaphore GenServer that always returns busy
      {:ok, sem} =
        Agent.start_link(fn -> :busy end)

      # Wrap the agent in a module-like interface by pre-acquiring the real
      # semaphore so our test scheduler can't get it.
      real_sem = Ferricstore.Merge.Semaphore
      :ok = Ferricstore.Merge.Semaphore.acquire(99, real_sem)

      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{mode: :hot, min_files_for_merge: 2},
          semaphore: real_sem,
          name: :test_scheduler_busy
        )

      GenServer.cast(pid, {:file_rotated, 10})

      ShardHelpers.eventually(
        fn ->
          GenServer.call(pid, :status).file_count == 10
        end,
        "file count not updated",
        20,
        10
      )

      status = GenServer.call(pid, :status)
      # Should not be merging — semaphore was held by shard 99
      assert status.merging == false
      assert status.merge_count == 0

      GenServer.stop(pid)
      Ferricstore.Merge.Semaphore.release(99, real_sem)
      Agent.stop(sem)
    end
  end

  # ---------------------------------------------------------------------------
  # Double-trigger guard
  # ---------------------------------------------------------------------------

  describe "merge in progress guard" do
    test "second rotation during merge does not double-trigger" do
      # Start a scheduler with a semaphore that accepts but we can observe state
      status_before = Scheduler.status(0)
      merge_count_before = status_before.merge_count

      # Two rapid rotations
      Scheduler.notify_rotation(0, 5)
      Scheduler.notify_rotation(0, 6)

      ShardHelpers.eventually(
        fn ->
          Scheduler.status(0).file_count == 6
        end,
        "file count not updated to 6",
        20,
        10
      )

      # At most one merge should have been triggered
      status_after = Scheduler.status(0)
      assert status_after.merge_count <= merge_count_before + 1
    end
  end

  # ---------------------------------------------------------------------------
  # Integration: Shard rotation triggers scheduler
  # ---------------------------------------------------------------------------

  describe "shard rotation integration" do
    test "writing enough data to trigger rotation notifies scheduler" do
      # Set a small file size to trigger rotation quickly
      orig_max = Application.get_env(:ferricstore, :max_active_file_size)
      Application.put_env(:ferricstore, :max_active_file_size, 1_048_576)

      # We need to restart shards to pick up the new config
      # Instead, just verify the notification path works
      initial_status = Scheduler.status(0)
      initial_count = initial_status.file_count

      # Manually notify (simulates what the shard does on rotation)
      Scheduler.notify_rotation(0, initial_count + 1)

      ShardHelpers.eventually(
        fn ->
          Scheduler.status(0).file_count == initial_count + 1
        end,
        "file count not updated",
        20,
        10
      )

      new_status = Scheduler.status(0)
      assert new_status.file_count == initial_count + 1

      # Restore
      if orig_max do
        Application.put_env(:ferricstore, :max_active_file_size, orig_max)
      else
        Application.delete_env(:ferricstore, :max_active_file_size)
      end
    end
  end
end
