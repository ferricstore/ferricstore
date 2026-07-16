defmodule Ferricstore.WalNifSchedulerGuardTest do
  use ExUnit.Case, async: true

  @moduletag :guard

  @source Path.expand(
            "../../native/ferricstore_wal_nif/src/lib.rs",
            __DIR__
          )

  @handle_source Path.expand(
                   "../../native/ferricstore_wal_nif/src/wal_handle.rs",
                   __DIR__
                 )

  test "constant-time WAL NIF calls stay on normal schedulers" do
    source = File.read!(@source)

    assert_nif_schedule(source, "sync", "Normal")
    assert_nif_schedule(source, "sync_with_delay", "Normal")
    assert_nif_schedule(source, "position", "Normal")
  end

  test "WAL writes copy caller-sized iodata on a dirty CPU scheduler" do
    source = File.read!(@source)

    assert_nif_schedule(source, "write", "DirtyCpu")
  end

  test "blocking WAL NIF calls use dirty IO schedulers" do
    source = File.read!(@source)

    assert_nif_schedule(source, "open", "DirtyIo")
    assert_nif_schedule(source, "open_raw_append", "DirtyIo")
    assert_nif_schedule(source, "close", "DirtyIo")
    assert_nif_schedule(source, "pread", "DirtyIo")
    assert_nif_schedule(source, "preallocate_keep_size", "DirtyIo")
  end

  test "sync submission never blocks a normal scheduler on a full queue" do
    source = File.read!(@handle_source)

    assert source =~ ".try_send(ThreadMsg::Flush(caller))"
    refute source =~ ~r/\.send\(ThreadMsg::Flush\(caller\)\)/
  end

  test "sync submission never waits for close or panic admission mutexes" do
    source = File.read!(@handle_source)
    body = function_body(source, "request_sync")

    assert body =~ ".close_gate"
    assert body =~ ".sync_admission"
    assert length(Regex.scan(~r/\.try_lock\(\)/, body)) == 2
    refute body =~ ".lock()"
  end

  test "WAL close does not detach a still-running durability worker on timeout" do
    source = File.read!(@handle_source)

    refute source =~ "close_thread_with_timeout",
           "WAL close must not return while its final drain/sync thread is still running"

    refute source =~ "wal_close_timeout",
           "a timeout cannot make a destructive WAL close outcome definitive"
  end

  defp assert_nif_schedule(source, function, schedule) do
    pattern =
      ~r/#\[rustler::nif\(schedule = "#{schedule}"\)\]\s*(?:#\[allow\([^\]]+\)\]\s*)?fn #{function}\b/

    assert source =~ pattern,
           "expected #{function}/N to use #[rustler::nif(schedule = \"#{schedule}\")]"
  end

  defp function_body(source, function) do
    [_before, rest] = String.split(source, "pub fn #{function}", parts: 2)
    [body, _after] = String.split(rest, "\n    }\n", parts: 2)
    body
  end
end
