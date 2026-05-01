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
          "v2_fsync",
          "v2_fsync_dir",
          "v2_write_hint_file",
          "v2_copy_records",
          "v2_copy_records_preserve_tombstones"
        ] do
      assert_nif_schedule(source, function, "DirtyIo")
    end
  end

  defp assert_nif_schedule(source, function, schedule) do
    pattern =
      ~r/#\[rustler::nif\(schedule = "#{schedule}"\)\]\s*(?:#\[allow\([^\]]+\)\]\s*)?fn #{function}\b/

    assert source =~ pattern,
           "expected #{function}/N to use #[rustler::nif(schedule = \"#{schedule}\")]"
  end
end
