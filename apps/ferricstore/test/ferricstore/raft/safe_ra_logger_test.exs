defmodule Ferricstore.Raft.SafeRaLoggerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Ferricstore.Raft.SafeRaLogger

  @snapshot_written_format ~c"~ts: ra_log: ~s with ~b bytes written at index ~b with ~b live indexes in ~bms"

  test "snapshot debug log with undefined size does not crash the formatter" do
    log =
      capture_log([level: :debug], fn ->
        assert :ok =
                 SafeRaLogger.log(
                   :debug,
                   @snapshot_written_format,
                   [~c"ferricstore_shard_0", :snapshot, :undefined, 12, 3, 4],
                   %{domain: [:ra], mfa: {:ra_log, :handle_event, 2}}
                 )

        Logger.flush()
      end)

    refute log =~ "FORMATTER CRASH"
    assert log =~ "ra_log: snapshot"
    assert log =~ "undefined bytes written"
  end

  test "primary filter sanitizes raw ra logger events" do
    assert :ok = SafeRaLogger.install_filter()

    log =
      capture_log([level: :debug], fn ->
        :logger.log(
          :debug,
          @snapshot_written_format,
          [~c"ferricstore_shard_0", :snapshot, :undefined, 12, 3, 4],
          %{domain: [:ra], mfa: {:ra_log, :handle_event, 2}}
        )

        Logger.flush()
      end)

    refute log =~ "FORMATTER CRASH"
    assert log =~ "undefined bytes written"
  end

  test "non-buggy ra log messages pass through unchanged" do
    log =
      capture_log([level: :debug], fn ->
        assert :ok =
                 SafeRaLogger.log(
                   :debug,
                   ~c"~ts: ra_log:init recovered last_index_term ~w",
                   [~c"ferricstore_shard_0", {10, 2}],
                   %{domain: [:ra], mfa: {:ra_log, :init, 1}}
                 )

        Logger.flush()
      end)

    assert log =~ "ra_log:init recovered last_index_term {10, 2}"
  end

  test "ra is configured to use the safe logger" do
    assert Application.get_env(:ra, :logger_module) == SafeRaLogger
  end
end
