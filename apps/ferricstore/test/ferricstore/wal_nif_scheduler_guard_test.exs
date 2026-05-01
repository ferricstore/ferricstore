defmodule Ferricstore.WalNifSchedulerGuardTest do
  use ExUnit.Case, async: true

  @moduletag :guard

  @source Path.expand(
            "../../native/ferricstore_wal_nif/src/lib.rs",
            __DIR__
          )

  test "hot WAL NIF calls stay on normal schedulers" do
    source = File.read!(@source)

    assert_nif_schedule(source, "write", "Normal")
    assert_nif_schedule(source, "sync", "Normal")
    assert_nif_schedule(source, "position", "Normal")
  end

  test "blocking WAL NIF calls run on dirty IO schedulers" do
    source = File.read!(@source)

    # open can touch filesystem metadata and pre-allocation, close waits for
    # drain+fdatasync, and pread performs recovery reads. Keep these off normal
    # schedulers so startup/recovery/shutdown stalls do not steal request CPU.
    assert_nif_schedule(source, "open", "DirtyIo")
    assert_nif_schedule(source, "close", "DirtyIo")
    assert_nif_schedule(source, "pread", "DirtyIo")
  end

  defp assert_nif_schedule(source, function, schedule) do
    pattern =
      ~r/#\[rustler::nif\(schedule = "#{schedule}"\)\]\s*(?:#\[allow\([^\]]+\)\]\s*)?fn #{function}\b/

    assert source =~ pattern,
           "expected #{function}/N to use #[rustler::nif(schedule = \"#{schedule}\")]"
  end
end
