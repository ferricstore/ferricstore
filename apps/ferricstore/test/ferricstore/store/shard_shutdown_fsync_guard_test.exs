defmodule Ferricstore.Store.ShardShutdownFsyncGuardTest do
  use ExUnit.Case, async: true

  @shutdown_path "lib/ferricstore/store/shard/lifecycle/shutdown.ex"

  alias Ferricstore.Store.Shard.Lifecycle.Shutdown

  test "shutdown fsyncs the shard directory after writing the active hint" do
    source = File.read!(@shutdown_path)
    [_before, section] = String.split(source, "def do_terminate", parts: 2)
    [terminate_body | _after] = String.split(section, "\n  def shutdown_errors", parts: 2)

    assert terminate_body =~ "hint_dir_fsync_result",
           "terminate should track the hint directory fsync result in shutdown telemetry"

    assert terminate_body =~ "NIF.v2_fsync_dir(state.shard_data_path)",
           "terminate must fsync the shard dir after hint rename so the hint file survives crash"

    assert source =~ ":hint_dir_fsync",
           "shutdown warnings should name hint directory fsync failures separately"
  end

  test "shutdown telemetry reports an unflushed pending batch as a durability failure" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "shard_shutdown_pending_failure_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    keydir = :ets.new(:shutdown_pending_failure_keydir, [:set, :public])
    parent = self()
    handler_id = {:shutdown_pending_failure, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :shard, :shutdown],
      fn _event, _measurements, metadata, _config ->
        send(parent, {:shutdown_metadata, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      File.rm_rf!(dir)
    end)

    instance_ctx = %{
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      hot_cache_max_value_size: 65_536,
      keydir_binary_bytes: :atomics.new(1, signed: true)
    }

    state = %{
      active_file_id: 0,
      active_file_path: Path.join([dir, "missing", "00000.log"]),
      active_file_size: 0,
      file_stats: %{0 => {0, 0}},
      flush_in_flight: nil,
      index: 0,
      instance_ctx: instance_ctx,
      keydir: keydir,
      max_active_file_size: 64 * 1024 * 1024,
      pending: [{"unflushed", "value", 0}],
      pending_count: 1,
      shard_data_path: dir
    }

    try do
      assert :ok = Shutdown.do_terminate(:shutdown, state)

      assert_receive {:shutdown_metadata,
                      %{status: :warning, errors: errors, pending_write_count: 1}},
                     1_000

      assert {:pending_flush, _reason} = List.keyfind(errors, :pending_flush, 0)
    after
      :ets.delete(keydir)
    end
  end

  test "shutdown skips keydir hints when promotion recovery is required" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "shard_shutdown_promotion_recovery_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    active_path = Path.join(dir, "00000.log")
    File.write!(active_path, "active")
    keydir = :ets.new(:shutdown_promotion_recovery_keydir, [:set, :public])

    state = %{
      active_file_id: 0,
      active_file_path: active_path,
      flush_in_flight: nil,
      index: 0,
      instance_ctx: nil,
      keydir: keydir,
      pending: [],
      promotion_recovery_required: true,
      shard_data_path: dir
    }

    try do
      assert :ok = Shutdown.do_terminate({:compound_promotion_failed, :eio}, state)
      refute File.exists?(Path.join(dir, "00000.hint"))
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end
end
