defmodule Ferricstore.BitcaskNifSchedulerGuardTest do
  use ExUnit.Case, async: true

  @moduletag :guard

  @source Path.expand("../../native/ferricstore_bitcask/src/lib.rs", __DIR__)

  test "blocking Bitcask write/fsync NIFs run on dirty IO schedulers" do
    source = File.read!(@source)

    # These functions do synchronous file writes, fdatasync, hint commits, or
    # compaction copies. They are not the async/Tokio hot path, so keep them off
    # normal schedulers to avoid request CPU stalls under slow disk/backpressure.
    for function <- [
          "v2_append_record",
          "v2_append_tombstone",
          "v2_append_batch",
          "v2_append_batch_nosync",
          "v2_append_ops_batch_nosync",
          "v2_fsync",
          "v2_fsync_dir",
          "v2_available_disk_space",
          "v2_write_hint_file",
          "v2_copy_records",
          "v2_copy_records_preserve_tombstones"
        ] do
      assert_nif_schedule(source, function, "DirtyIo")
    end
  end

  test "blocking Bitcask cold-read and scan NIFs run on dirty IO schedulers" do
    source = File.read!(@source)

    # These functions open files and perform pread/scan/read_all work. Cold
    # reads are user-facing, and recovery/compaction scans can be large; keep
    # them off normal schedulers so slow disk cannot stall BEAM request CPU.
    for function <- [
          "v2_pread_at",
          "v2_scan_file",
          "v2_scan_file_from_offset",
          "v2_scan_tombstones",
          "v2_pread_batch",
          "v2_read_hint_file"
        ] do
      assert_nif_schedule(source, function, "DirtyIo")
    end
  end

  test "async batch append does record encoding on Tokio blocking workers" do
    source = File.read!(@source)
    body = function_body(source, "v2_append_batch_async")

    [normal_scheduler_prefix, _blocking_worker_suffix] =
      String.split(body, "spawn_blocking", parts: 2)

    refute normal_scheduler_prefix =~ "log::encode_record",
           "v2_append_batch_async must not CRC/encode records on a Normal BEAM scheduler"
  end

  test "async batch append copies BEAM binaries off normal schedulers" do
    source = File.read!(@source)

    # `v2_append_batch_async` must copy BEAM-owned binaries before handing the
    # batch to Tokio because NIF env references cannot outlive the call. That
    # copy can be proportional to full batch payload size, so keep it off
    # Normal schedulers even though the disk write itself is async.
    assert_nif_schedule(source, "v2_append_batch_async", "DirtyCpu")
  end

  defp assert_nif_schedule(source, function, schedule) do
    pattern =
      ~r/#\[rustler::nif\(schedule = "#{schedule}"\)\]\s*(?:#\[allow\([^\]]+\)\]\s*)?fn #{function}\b/

    assert source =~ pattern,
           "expected #{function}/N to use #[rustler::nif(schedule = \"#{schedule}\")]"
  end

  defp function_body(source, function) do
    [_before, rest] = String.split(source, "fn #{function}", parts: 2)
    [body, _after] = String.split(rest, "\n}\n\n", parts: 2)
    body
  end
end
