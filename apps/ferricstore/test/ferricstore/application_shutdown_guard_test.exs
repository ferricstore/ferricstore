defmodule Ferricstore.ApplicationShutdownGuardTest do
  use ExUnit.Case, async: true

  @application_path Path.expand("../../lib/ferricstore/application.ex", __DIR__)

  test "shutdown fsync uses the active file registry before scanning old logs" do
    source = File.read!(@application_path)

    assert source =~ "ActiveFile.get",
           "shutdown_fsync_bitcask/2 must use ActiveFile.get/1 for the current active log"

    refute source =~ ":ferricstore_active_file_path",
           "shutdown_fsync_bitcask/2 must not use the retired persistent_term active-file key"
  end

  test "zero-shard shutdown does not probe descending shard indices" do
    unexpected = fn _ -> flunk("zero shards must not perform filesystem work") end

    assert :ok =
             Ferricstore.Application.fsync_bitcask_for_shutdown(0, "/unused",
               active_file_path: unexpected,
               exists?: unexpected,
               fsync: unexpected,
               list_log_files: unexpected
             )
  end

  test "shutdown fallback fsyncs only canonical Bitcask segments" do
    parent = self()
    data_dir = "/tmp/ferricstore-shutdown-canonical"
    shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)

    assert :ok =
             Ferricstore.Application.fsync_bitcask_for_shutdown(1, data_dir,
               active_file_path: fn 0 -> nil end,
               exists?: fn _path -> false end,
               list_log_files: fn ^shard_dir ->
                 {:ok, ["notes.log", "0.log", "compact_0.log", "00000.hint", "00000.log"]}
               end,
               fsync: fn path ->
                 send(parent, {:fsynced, path})
                 :ok
               end
             )

    assert_receive {:fsynced, path}
    assert path == Path.join(shard_dir, "00000.log")
    refute_receive {:fsynced, _other}
  end

  test "flow projection shutdown reports one suspend exit without retrying" do
    calls = :atomics.new(1, signed: false)

    suspend = fn _shard_count, _opts ->
      :atomics.add(calls, 1, 1)
      exit(:writer_unavailable)
    end

    assert {:error, :writer_unavailable} =
             Ferricstore.Application.__shutdown_flow_lmdb_for_test__(4, suspend)

    assert :atomics.get(calls, 1) == 1
  end

  test "lifecycle telemetry uses elapsed monotonic time" do
    assert Ferricstore.Application.__elapsed_ms_for_test__(100, 145) == 45
    assert Ferricstore.Application.__elapsed_ms_for_test__(145, 100) == 0
  end

  test "large-value diagnostics do not block application startup" do
    parent = self()

    check = fn _shard_count ->
      send(parent, {:large_value_check_started, self()})

      receive do
        :release -> :ok
      end
    end

    assert :ok = Ferricstore.Application.schedule_large_value_check(4, check)
    assert_receive {:large_value_check_started, task}
    send(task, :release)
  end
end
