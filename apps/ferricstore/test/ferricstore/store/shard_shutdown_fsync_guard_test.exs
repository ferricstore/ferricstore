defmodule Ferricstore.Store.ShardShutdownFsyncGuardTest do
  use ExUnit.Case, async: true

  @lifecycle_path "lib/ferricstore/store/shard/lifecycle.ex"

  test "shutdown fsyncs the shard directory after writing the active hint" do
    source = File.read!(@lifecycle_path)
    [_before, section] = String.split(source, "def do_terminate", parts: 2)
    [terminate_body | _after] = String.split(section, "\n  defp shutdown_errors", parts: 2)

    assert terminate_body =~ "hint_dir_fsync_result",
           "terminate should track the hint directory fsync result in shutdown telemetry"

    assert terminate_body =~ "NIF.v2_fsync_dir(state.shard_data_path)",
           "terminate must fsync the shard dir after hint rename so the hint file survives crash"

    assert source =~ ":hint_dir_fsync",
           "shutdown warnings should name hint directory fsync failures separately"
  end
end
