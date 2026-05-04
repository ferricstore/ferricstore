defmodule Ferricstore.Store.ShardLifecycleInstanceContextTest do
  @moduledoc false

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Shard
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle

  test "recover_keydir replays log files by numeric file id" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lifecycle_order_#{System.unique_integer([:positive])}"
      )

    shard_path = Path.join(tmp, "shard_0")
    File.mkdir_p!(shard_path)

    keydir = :ets.new(:"lifecycle_order_#{System.unique_integer([:positive])}", [:set, :public])

    on_exit(fn ->
      try do
        :ets.delete(keydir)
      rescue
        _ -> :ok
      end

      File.rm_rf!(tmp)
    end)

    key = "recover_numeric_order_key"

    assert {:ok, _} = NIF.v2_append_record(Path.join(shard_path, "99999.log"), key, "old", 0)
    assert {:ok, _} = NIF.v2_append_record(Path.join(shard_path, "100000.log"), key, "new", 0)

    ShardLifecycle.recover_keydir(shard_path, keydir, 0)

    assert [{^key, nil, 0, _lfu, 100_000, offset, value_size}] = :ets.lookup(keydir, key)
    assert value_size == byte_size("new")
    assert {:ok, "new"} = NIF.v2_pread_at(Path.join(shard_path, "100000.log"), offset)
  end

  test "recover_keydir ignores leftover compact temp logs" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lifecycle_compact_tmp_#{System.unique_integer([:positive])}"
      )

    shard_path = Path.join(tmp, "shard_0")
    File.mkdir_p!(shard_path)

    keydir =
      :ets.new(:"lifecycle_compact_tmp_#{System.unique_integer([:positive])}", [:set, :public])

    on_exit(fn ->
      try do
        :ets.delete(keydir)
      rescue
        _ -> :ok
      end

      File.rm_rf!(tmp)
    end)

    File.write!(Path.join(shard_path, "compact_1.log"), "partial compaction")
    key = "recover_ignores_compact_tmp"
    assert {:ok, _} = NIF.v2_append_record(Path.join(shard_path, "00000.log"), key, "value", 0)

    ShardLifecycle.recover_keydir(shard_path, keydir, 0)

    assert [{^key, nil, 0, _lfu, 0, offset, value_size}] = :ets.lookup(keydir, key)
    assert value_size == byte_size("value")
    assert {:ok, "value"} = NIF.v2_pread_at(Path.join(shard_path, "00000.log"), offset)
  end

  test "shard startup ignores non-numeric log-shaped files" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lifecycle_stray_logs_#{System.unique_integer([:positive])}"
      )

    shard_path = Path.join(tmp, "shard_0")
    File.mkdir_p!(shard_path)

    keydir =
      :ets.new(:"lifecycle_stray_logs_#{System.unique_integer([:positive])}", [:set, :public])

    on_exit(fn ->
      try do
        :ets.delete(keydir)
      rescue
        _ -> :ok
      end

      File.rm_rf!(tmp)
    end)

    File.write!(Path.join(shard_path, "notes.log"), "not a bitcask log")
    File.write!(Path.join(shard_path, "notes.hint"), "not a bitcask hint")

    key = "recover_ignores_stray_log_names"

    assert {:ok, {_offset, _size}} =
             NIF.v2_append_record(Path.join(shard_path, "00000.log"), key, "value", 0)

    assert {0, active_size} = ShardLifecycle.discover_active_file(shard_path)
    assert active_size > 0

    ShardLifecycle.recover_keydir(shard_path, keydir, 0)
    assert [{^key, nil, 0, _lfu, 0, offset, value_size}] = :ets.lookup(keydir, key)
    assert value_size == byte_size("value")
    assert {:ok, "value"} = NIF.v2_pread_at(Path.join(shard_path, "00000.log"), offset)

    assert %{0 => {^active_size, 0}} =
             Ferricstore.Store.Shard.Flush.compute_file_stats(shard_path, keydir)
  end

  test "recover_from_log fails closed and emits telemetry when scan target is missing" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lifecycle_missing_log_#{System.unique_integer([:positive])}"
      )

    shard_path = Path.join(tmp, "shard_0")
    File.mkdir_p!(shard_path)

    keydir =
      :ets.new(:"lifecycle_missing_log_#{System.unique_integer([:positive])}", [:set, :public])

    handler_id = {__MODULE__, self(), make_ref()}
    parent = self()

    :telemetry.attach(
      handler_id,
      [:ferricstore, :bitcask, :recovery_scan_failed],
      fn event, measurements, metadata, _config ->
        send(parent, {:recovery_scan_failed, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)

      try do
        :ets.delete(keydir)
      rescue
        _ -> :ok
      end

      File.rm_rf!(tmp)
    end)

    assert capture_log(fn ->
             assert_raise RuntimeError, ~r/recover_from_log failed to scan/, fn ->
               ShardLifecycle.recover_from_log(shard_path, "00000.log", keydir, 0)
             end
           end) =~ "recover_from_log failed to scan"

    assert_receive {:recovery_scan_failed, [:ferricstore, :bitcask, :recovery_scan_failed],
                    %{count: 1},
                    %{
                      operation: :recover_from_log,
                      path: missing_path,
                      shard_index: 0,
                      reason: reason
                    }}

    assert missing_path == Path.join(shard_path, "00000.log")
    assert is_binary(reason)
  end

  test "discover_active_file reports leftover compact temp cleanup failures" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lifecycle_compact_tmp_fail_#{System.unique_integer([:positive])}"
      )

    shard_path = Path.join(tmp, "shard_0")
    File.mkdir_p!(shard_path)

    on_exit(fn ->
      File.rm_rf!(tmp)
    end)

    File.write!(Path.join(shard_path, "00000.log"), "active")
    compact_dir = Path.join(shard_path, "compact_1.log")
    File.mkdir!(compact_dir)
    parent = self()
    handler_id = {:shard_compact_temp_cleanup_failed, parent, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :shard, :compact_temp_cleanup_failed],
      fn event, measurements, metadata, _config ->
        send(parent, {:compact_temp_cleanup_failed, event, measurements, metadata})
      end,
      nil
    )

    try do
      log =
        capture_log(fn ->
          assert {0, 6} = ShardLifecycle.discover_active_file(shard_path)
        end)

      assert log =~ "failed to remove leftover compaction temp file compact_1.log"

      assert_receive {:compact_temp_cleanup_failed,
                      [:ferricstore, :shard, :compact_temp_cleanup_failed], %{count: 1},
                      %{path: ^compact_dir, name: "compact_1.log", reason: {_kind, _message}}},
                     1_000

      assert File.dir?(compact_dir)
    after
      :telemetry.detach(handler_id)
    end
  end

  test "discover_active_file fails closed when shard path cannot be listed" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lifecycle_discover_ls_fail_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    shard_path = Path.join(tmp, "shard_0")
    File.write!(shard_path, "not a directory")

    on_exit(fn -> File.rm_rf!(tmp) end)

    assert_raise RuntimeError, ~r/discover_active_file failed to list/, fn ->
      ShardLifecycle.discover_active_file(shard_path)
    end
  end

  test "recover_keydir fails closed when shard path cannot be listed" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lifecycle_recover_ls_fail_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    shard_path = Path.join(tmp, "shard_0")
    File.write!(shard_path, "not a directory")

    keydir =
      :ets.new(:"lifecycle_recover_ls_fail_#{System.unique_integer([:positive])}", [
        :set,
        :public
      ])

    on_exit(fn ->
      try do
        :ets.delete(keydir)
      rescue
        _ -> :ok
      end

      File.rm_rf!(tmp)
    end)

    assert_raise RuntimeError, ~r/recover_keydir failed to list/, fn ->
      ShardLifecycle.recover_keydir(shard_path, keydir, 0)
    end
  end

  test "probabilistic migration fails closed when prob directory cannot be listed" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lifecycle_prob_ls_fail_#{System.unique_integer([:positive])}"
      )

    shard_path = Path.join(tmp, "shard_0")
    File.mkdir_p!(shard_path)
    File.write!(Path.join(shard_path, "prob"), "not a directory")

    keydir =
      :ets.new(:"lifecycle_prob_ls_fail_#{System.unique_integer([:positive])}", [
        :set,
        :public
      ])

    on_exit(fn ->
      try do
        :ets.delete(keydir)
      rescue
        _ -> :ok
      end

      File.rm_rf!(tmp)
    end)

    assert_raise RuntimeError, ~r/migrate_prob_files failed to list/, fn ->
      ShardLifecycle.migrate_prob_files(shard_path, keydir, 0)
    end
  end

  test "recover_keydir during custom shard startup does not mutate default accounting" do
    default_ctx = FerricStore.Instance.get(:default)
    default_before = keydir_binary_total(default_ctx)

    ctx = build_instance()
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    log_path = Path.join(shard_path, "00000.log")
    key = "recover_custom_instance_" <> String.duplicate("k", 80)

    assert {:ok, {_offset, _value_size}} = NIF.v2_append_record(log_path, key, "value", 0)

    custom_before = keydir_binary_total(ctx)

    {:ok, pid} =
      Ferricstore.Store.Shard.start_link(
        index: 0,
        data_dir: ctx.data_dir,
        instance_ctx: ctx
      )

    on_exit(fn -> cleanup_instance(ctx, pid) end)

    assert keydir_binary_total(default_ctx) == default_before
    assert keydir_binary_total(ctx) > custom_before
  end

  test "shard startup emits per-phase recovery profiling telemetry" do
    ctx = build_instance()
    test_pid = self()
    handler_id = "shard-startup-profile-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:ferricstore, :shard, :startup_phase],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:startup_profile, event, measurements, metadata})
      end,
      nil
    )

    {:ok, pid} =
      Ferricstore.Store.Shard.start_link(
        index: 0,
        data_dir: ctx.data_dir,
        instance_ctx: ctx
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      cleanup_instance(ctx, pid)
    end)

    assert_receive {:startup_profile, [:ferricstore, :shard, :startup_phase],
                    %{duration_us: duration}, %{shard_index: 0, phase: :recover_keydir}},
                   1_000

    assert is_integer(duration)
    assert duration >= 0

    assert_receive {:startup_profile, [:ferricstore, :shard, :startup_phase], %{duration_us: _},
                    %{shard_index: 0, phase: :compute_file_stats}},
                   1_000
  end

  test "shard startup reports directory fsync failure instead of starting dirty storage" do
    ctx = build_instance(ensure_layout?: false)
    parent = self()
    previous_trap_exit = Process.flag(:trap_exit, true)

    fsync_dir_fun = fn path ->
      send(parent, {:startup_fsync_dir, path})
      {:error, :eio}
    end

    result =
      Shard.start_link(
        index: 0,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        fsync_dir_fun: fsync_dir_fun
      )

    started_pid =
      case result do
        {:ok, pid} -> pid
        _ -> nil
      end

    on_exit(fn ->
      Process.flag(:trap_exit, previous_trap_exit)
      cleanup_instance(ctx, started_pid)
    end)

    assert {:error, {:fsync_dir_failed, :create_shard_dir, :eio}} = result

    expected_parent = Path.join(ctx.data_dir, "data")
    assert_received {:startup_fsync_dir, ^expected_parent}
  end

  test "shard startup reports active file directory fsync failure" do
    ctx = build_instance()
    parent = self()
    previous_trap_exit = Process.flag(:trap_exit, true)

    fsync_dir_fun = fn path ->
      send(parent, {:startup_fsync_dir, path})
      {:error, :enospc}
    end

    result =
      Shard.start_link(
        index: 0,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        fsync_dir_fun: fsync_dir_fun
      )

    started_pid =
      case result do
        {:ok, pid} -> pid
        _ -> nil
      end

    on_exit(fn ->
      Process.flag(:trap_exit, previous_trap_exit)
      cleanup_instance(ctx, started_pid)
    end)

    assert {:error, {:fsync_dir_failed, :create_active_file, :enospc}} = result

    expected_shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    assert_received {:startup_fsync_dir, ^expected_shard_path}
  end

  defp build_instance(opts \\ []) do
    name = :"lifecycle_instance_#{System.unique_integer([:positive])}"

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lifecycle_instance_#{System.unique_integer([:positive])}"
      )

    ctx =
      FerricStore.Instance.build(name,
        data_dir: data_dir,
        shard_count: 1,
        max_memory_bytes: 256 * 1024 * 1024,
        keydir_max_ram: 64 * 1024 * 1024
      )

    if Keyword.get(opts, :ensure_layout?, true) do
      Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    end

    ctx
  end

  defp cleanup_instance(ctx, pid) do
    if is_pid(pid) and Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    end

    try do
      :ets.delete(elem(ctx.keydir_refs, 0))
    rescue
      _ -> :ok
    end

    try do
      :ets.delete(ctx.hotness_table)
    rescue
      _ -> :ok
    end

    try do
      :ets.delete(ctx.config_table)
    rescue
      _ -> :ok
    end

    FerricStore.Instance.cleanup(ctx.name)
    File.rm_rf!(ctx.data_dir)
  end

  defp keydir_binary_total(ctx) do
    1..ctx.shard_count
    |> Enum.reduce(0, fn idx, acc -> acc + :atomics.get(ctx.keydir_binary_bytes, idx) end)
  end
end
