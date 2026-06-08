defmodule Ferricstore.OperationalGuardTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Flow.Admission
  alias Ferricstore.OperationalGuard
  alias Ferricstore.Test.Utils

  setup do
    original_env =
      Map.new(
        [
          :operational_guard_enabled,
          :flow_create_pause_rss_ratio,
          :flow_create_resume_rss_ratio,
          :flow_create_resume_on_drained_active_memory,
          :flow_create_sticky_rss_resume_max_ratio,
          :flow_create_resume_active_memory_ratio,
          :flow_create_resume_keydir_ratio,
          :flow_create_resume_keydir_bytes,
          :flow_lmdb_mmap_reclaim_interval_ms,
          :flow_lmdb_mmap_reclaim_rss_ratio,
          :flow_lmdb_mmap_reclaim_enabled
        ],
        fn key -> {key, Application.get_env(:ferricstore, key)} end
      )

    Application.put_env(:ferricstore, :operational_guard_enabled, true)
    OperationalGuard.reset_for_test()

    on_exit(fn ->
      Enum.each(original_env, fn
        {key, nil} -> Application.delete_env(:ferricstore, key)
        {key, value} -> Application.put_env(:ferricstore, key, value)
      end)

      OperationalGuard.reset_for_test()
    end)
  end

  test "sets pressure and reject flags from the capacity snapshot" do
    parent = self()
    name = :"operational_guard_#{System.unique_integer([:positive])}"

    snapshot = %{
      data_dir: "/data",
      shard_count: 2,
      memory: %{
        level: :pressure,
        rss_bytes: 80,
        limit_bytes: 100,
        rss_ratio: 0.80
      },
      disk: %{
        level: :reject,
        used_bytes: 91,
        total_bytes: 100,
        used_ratio: 0.91
      }
    }

    start_supervised!(
      {OperationalGuard,
       name: name,
       shard_count: 2,
       data_dir: "/data",
       interval_ms: 60_000,
       limits_fun: fn opts ->
         send(parent, {:limits_opts, opts})
         snapshot
       end,
       apply_disk_pressure_fun: fn ctx, shard_count, level ->
         send(parent, {:disk_pressure, ctx, shard_count, level})
       end,
       apply_memory_pressure_fun: fn level ->
         send(parent, {:memory_pressure, level})
       end,
       telemetry_fun: fn event, measurements, metadata ->
         send(parent, {:telemetry, event, measurements, metadata})
       end}
    )

    assert_receive {:limits_opts, [data_dir: "/data", shard_count: 2]}
    assert_receive {:disk_pressure, nil, 2, :reject}
    assert_receive {:memory_pressure, :pressure}

    assert_receive {:telemetry, [:ferricstore, :operational, :guard], measurements, metadata}

    assert measurements.disk_used_bytes == 91
    assert measurements.disk_total_bytes == 100
    assert metadata.disk_level == :reject
    assert metadata.rss_level == :pressure

    assert OperationalGuard.pressure?()
    assert OperationalGuard.reject_writes?()
  end

  test "rss reject alone does not keep Flow creates paused below the hard create threshold" do
    parent = self()
    name = :"operational_guard_#{System.unique_integer([:positive])}"

    Application.put_env(:ferricstore, :flow_create_pause_rss_ratio, 0.92)
    Application.put_env(:ferricstore, :flow_create_resume_rss_ratio, 0.88)
    Admission.pause_creates(:rss_pressure, 2_000)

    snapshot = %{
      data_dir: "/data",
      shard_count: 1,
      memory: %{
        level: :reject,
        rss_bytes: 87,
        limit_bytes: 100,
        rss_ratio: 0.87
      },
      disk: %{
        level: :ok,
        used_bytes: 10,
        total_bytes: 100,
        used_ratio: 0.10
      }
    }

    start_supervised!(
      {OperationalGuard,
       name: name,
       shard_count: 1,
       data_dir: "/data",
       interval_ms: 60_000,
       limits_fun: fn _opts -> snapshot end,
       apply_disk_pressure_fun: fn _ctx, _shard_count, _level -> :ok end,
       apply_memory_pressure_fun: fn level -> send(parent, {:memory_pressure, level}) end,
       lmdb_reclaim_fun: fn -> {:ok, 0} end,
       telemetry_fun: fn _event, _measurements, _metadata -> :ok end}
    )

    assert_receive {:memory_pressure, :reject}
    Utils.eventually(fn -> refute Admission.reject_new_creates?() end, 1_000, 10)
    assert OperationalGuard.reject_writes?()
    refute OperationalGuard.reject_flow_creates?()
  end

  test "default rss pause clears once RSS is back below the practical resume threshold" do
    parent = self()
    name = :"operational_guard_#{System.unique_integer([:positive])}"

    Application.delete_env(:ferricstore, :flow_create_pause_rss_ratio)
    Application.delete_env(:ferricstore, :flow_create_resume_rss_ratio)
    Admission.pause_creates(:rss_pressure, 2_000)

    snapshot = %{
      data_dir: "/data",
      shard_count: 1,
      memory: %{
        level: :pressure,
        rss_bytes: 75,
        limit_bytes: 100,
        rss_ratio: 0.75
      },
      disk: %{
        level: :ok,
        used_bytes: 10,
        total_bytes: 100,
        used_ratio: 0.10
      }
    }

    start_supervised!(
      {OperationalGuard,
       name: name,
       shard_count: 1,
       data_dir: "/data",
       interval_ms: 60_000,
       limits_fun: fn _opts -> snapshot end,
       apply_disk_pressure_fun: fn _ctx, _shard_count, _level -> :ok end,
       apply_memory_pressure_fun: fn level -> send(parent, {:memory_pressure, level}) end,
       lmdb_reclaim_fun: fn -> {:ok, 0} end,
       telemetry_fun: fn _event, _measurements, _metadata -> :ok end}
    )

    assert_receive {:memory_pressure, :pressure}
    Utils.eventually(fn -> refute Admission.reject_new_creates?() end, 1_000, 10)
  end

  test "default rss threshold pauses Flow creates before hard RSS stop" do
    parent = self()
    name = :"operational_guard_#{System.unique_integer([:positive])}"

    Application.delete_env(:ferricstore, :flow_create_pause_rss_ratio)
    Application.delete_env(:ferricstore, :flow_create_resume_rss_ratio)

    snapshot = %{
      data_dir: "/data",
      shard_count: 1,
      memory: %{
        level: :pressure,
        rss_bytes: 89,
        limit_bytes: 100,
        rss_ratio: 0.89
      },
      disk: %{
        level: :ok,
        used_bytes: 10,
        total_bytes: 100,
        used_ratio: 0.10
      }
    }

    start_supervised!(
      {OperationalGuard,
       name: name,
       shard_count: 1,
       data_dir: "/data",
       interval_ms: 60_000,
       limits_fun: fn _opts -> snapshot end,
       apply_disk_pressure_fun: fn _ctx, _shard_count, _level -> :ok end,
       apply_memory_pressure_fun: fn level -> send(parent, {:memory_pressure, level}) end,
       memory_stats_fun: fn ->
         %{total_bytes: 60, ratio: 0.60, keydir_bytes: 60, keydir_ratio: 0.60}
       end,
       lmdb_reclaim_fun: fn -> {:ok, 0} end,
       telemetry_fun: fn _event, _measurements, _metadata -> :ok end}
    )

    assert_receive {:memory_pressure, :pressure}

    Utils.eventually(
      fn ->
        assert Admission.reject_new_creates?()
        assert Admission.status().reason == :rss_pressure
      end,
      1_000,
      10
    )

    refute OperationalGuard.reject_flow_creates?()
  end

  test "sticky RSS pause clears after active Flow memory is drained" do
    name = :"operational_guard_#{System.unique_integer([:positive])}"

    Application.delete_env(:ferricstore, :flow_create_pause_rss_ratio)
    Application.delete_env(:ferricstore, :flow_create_resume_rss_ratio)
    Admission.pause_creates(:rss_pressure, 5_000)

    snapshot = %{
      data_dir: "/data",
      shard_count: 1,
      memory: %{
        level: :panic,
        rss_bytes: 98,
        limit_bytes: 100,
        rss_ratio: 0.98
      },
      disk: %{
        level: :ok,
        used_bytes: 10,
        total_bytes: 100,
        used_ratio: 0.10
      }
    }

    start_supervised!(
      {OperationalGuard,
       name: name,
       shard_count: 1,
       data_dir: "/data",
       interval_ms: 60_000,
       limits_fun: fn _opts -> snapshot end,
       apply_disk_pressure_fun: fn _ctx, _shard_count, _level -> :ok end,
       apply_memory_pressure_fun: fn _level -> :ok end,
       memory_stats_fun: fn ->
         %{total_bytes: 0, ratio: 0.0, keydir_bytes: 0, keydir_ratio: 0.0}
       end,
       lmdb_reclaim_fun: fn -> {:ok, 0} end,
       telemetry_fun: fn _event, _measurements, _metadata -> :ok end}
    )

    Utils.eventually(
      fn ->
        refute Admission.reject_new_creates?()
        refute OperationalGuard.reject_flow_creates?()
      end,
      1_000,
      10
    )
  end

  test "sticky RSS pause stays closed while active Flow memory is still high" do
    name = :"operational_guard_#{System.unique_integer([:positive])}"

    Application.delete_env(:ferricstore, :flow_create_pause_rss_ratio)
    Application.delete_env(:ferricstore, :flow_create_resume_rss_ratio)
    Admission.pause_creates(:rss_pressure, 5_000)

    snapshot = %{
      data_dir: "/data",
      shard_count: 1,
      memory: %{
        level: :panic,
        rss_bytes: 98,
        limit_bytes: 100,
        rss_ratio: 0.98
      },
      disk: %{
        level: :ok,
        used_bytes: 10,
        total_bytes: 100,
        used_ratio: 0.10
      }
    }

    start_supervised!(
      {OperationalGuard,
       name: name,
       shard_count: 1,
       data_dir: "/data",
       interval_ms: 60_000,
       limits_fun: fn _opts -> snapshot end,
       apply_disk_pressure_fun: fn _ctx, _shard_count, _level -> :ok end,
       apply_memory_pressure_fun: fn _level -> :ok end,
       memory_stats_fun: fn ->
         %{total_bytes: 60, ratio: 0.60, keydir_bytes: 60, keydir_ratio: 0.60}
       end,
       lmdb_reclaim_fun: fn -> {:ok, 0} end,
       telemetry_fun: fn _event, _measurements, _metadata -> :ok end}
    )

    assert Admission.reject_new_creates?()
    assert Admission.status().reason == :rss_pressure
    assert OperationalGuard.reject_flow_creates?()
  end

  test "rss panic still pauses Flow creates" do
    parent = self()
    name = :"operational_guard_#{System.unique_integer([:positive])}"

    snapshot = %{
      data_dir: "/data",
      shard_count: 1,
      memory: %{
        level: :panic,
        rss_bytes: 95,
        limit_bytes: 100,
        rss_ratio: 0.95
      },
      disk: %{
        level: :ok,
        used_bytes: 10,
        total_bytes: 100,
        used_ratio: 0.10
      }
    }

    start_supervised!(
      {OperationalGuard,
       name: name,
       shard_count: 1,
       data_dir: "/data",
       interval_ms: 60_000,
       limits_fun: fn _opts -> snapshot end,
       apply_disk_pressure_fun: fn _ctx, _shard_count, _level -> :ok end,
       apply_memory_pressure_fun: fn level -> send(parent, {:memory_pressure, level}) end,
       memory_stats_fun: fn ->
         %{total_bytes: 60, ratio: 0.60, keydir_bytes: 60, keydir_ratio: 0.60}
       end,
       lmdb_reclaim_fun: fn -> {:ok, 0} end,
       telemetry_fun: fn _event, _measurements, _metadata -> :ok end}
    )

    assert_receive {:memory_pressure, :panic}
    assert Admission.reject_new_creates?()
    assert Admission.status().reason == :rss_pressure
    assert OperationalGuard.reject_flow_creates?()
  end

  test "rss pressure reclaims idle LMDB mmap cache" do
    parent = self()
    name = :"operational_guard_#{System.unique_integer([:positive])}"

    Application.put_env(:ferricstore, :flow_lmdb_mmap_reclaim_interval_ms, 1)
    Application.put_env(:ferricstore, :flow_lmdb_mmap_reclaim_rss_ratio, 0.80)

    ctx = %{
      flow_lmdb_writer_pending_ops: :atomics.new(1, signed: false),
      flow_lmdb_replay_safe_requested_index: :atomics.new(1, signed: false),
      flow_lmdb_replay_safe_index: :atomics.new(1, signed: false)
    }

    snapshot = %{
      data_dir: "/data",
      shard_count: 1,
      memory: %{
        level: :pressure,
        rss_bytes: 81,
        limit_bytes: 100,
        rss_ratio: 0.81
      },
      disk: %{
        level: :ok,
        used_bytes: 10,
        total_bytes: 100,
        used_ratio: 0.10
      }
    }

    start_supervised!(
      {OperationalGuard,
       name: name,
       instance_ctx: ctx,
       shard_count: 1,
       data_dir: "/data",
       interval_ms: 60_000,
       limits_fun: fn _opts -> snapshot end,
       apply_disk_pressure_fun: fn _ctx, _shard_count, _level -> :ok end,
       apply_memory_pressure_fun: fn _level -> :ok end,
       lmdb_reclaim_fun: fn ->
         send(parent, :lmdb_reclaimed)
         {:ok, 2}
       end,
       telemetry_fun: fn event, measurements, metadata ->
         send(parent, {:telemetry, event, measurements, metadata})
       end}
    )

    assert_receive :lmdb_reclaimed

    assert_receive {:telemetry, [:ferricstore, :flow, :lmdb, :mmap_reclaim],
                    %{released: 2, busy: 0}, %{status: :ok}}
  end

  test "default rss pressure reclaims idle LMDB mmap before Flow create pause zone" do
    parent = self()
    name = :"operational_guard_#{System.unique_integer([:positive])}"

    Application.delete_env(:ferricstore, :flow_lmdb_mmap_reclaim_interval_ms)
    Application.delete_env(:ferricstore, :flow_lmdb_mmap_reclaim_rss_ratio)
    Application.delete_env(:ferricstore, :flow_lmdb_mmap_reclaim_enabled)

    ctx = %{
      flow_lmdb_writer_pending_ops: :atomics.new(1, signed: false),
      flow_lmdb_replay_safe_requested_index: :atomics.new(1, signed: false),
      flow_lmdb_replay_safe_index: :atomics.new(1, signed: false)
    }

    snapshot = %{
      data_dir: "/data",
      shard_count: 1,
      memory: %{
        level: :pressure,
        rss_bytes: 71,
        limit_bytes: 100,
        rss_ratio: 0.71
      },
      disk: %{
        level: :ok,
        used_bytes: 10,
        total_bytes: 100,
        used_ratio: 0.10
      }
    }

    start_supervised!(
      {OperationalGuard,
       name: name,
       instance_ctx: ctx,
       shard_count: 1,
       data_dir: "/data",
       interval_ms: 60_000,
       limits_fun: fn _opts -> snapshot end,
       apply_disk_pressure_fun: fn _ctx, _shard_count, _level -> :ok end,
       apply_memory_pressure_fun: fn _level -> :ok end,
       lmdb_reclaim_fun: fn ->
         send(parent, :lmdb_reclaimed)
         {:ok, 1}
       end,
       telemetry_fun: fn _event, _measurements, _metadata -> :ok end}
    )

    assert_receive :lmdb_reclaimed
  end
end
