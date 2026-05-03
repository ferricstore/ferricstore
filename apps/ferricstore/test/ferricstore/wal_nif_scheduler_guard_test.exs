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

  test "blocking WAL NIF calls stay on normal schedulers" do
    source = File.read!(@source)

    assert_nif_schedule(source, "open", "Normal")
    assert_nif_schedule(source, "close", "Normal")
    assert_nif_schedule(source, "pread", "Normal")
  end

  test "WAL Rust NIFs do not use dirty schedulers" do
    source = File.read!(@source)

    refute source =~ ~r/^\s*#\[rustler::nif\(schedule = "Dirty(?:Io|Cpu)"\)\]/m,
           "WAL NIFs must stay on Normal schedulers; move long I/O to Tokio async instead"
  end

  defp assert_nif_schedule(source, function, schedule) do
    pattern =
      ~r/#\[rustler::nif\(schedule = "#{schedule}"\)\]\s*(?:#\[allow\([^\]]+\)\]\s*)?fn #{function}\b/

    assert source =~ pattern,
           "expected #{function}/N to use #[rustler::nif(schedule = \"#{schedule}\")]"
  end
end
