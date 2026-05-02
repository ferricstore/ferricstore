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
end
