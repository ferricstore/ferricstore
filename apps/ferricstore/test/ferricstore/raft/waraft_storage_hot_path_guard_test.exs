defmodule Ferricstore.Raft.WARaftStorageHotPathGuardTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../../lib/ferricstore/raft/waraft_storage.ex", __DIR__)
  @segment_log_source_path Path.expand("../../../src/ferricstore_waraft_spike_segment_log.erl", __DIR__)

  test "segment projection registration caches expanded root path on the storage handle" do
    source = File.read!(@source_path)

    [function_source] =
      Regex.run(
        ~r/defp register_segment_projection_context\(%\{root_dir: root_dir\} = handle\).*?^  end/ms,
        source
      )

    assert function_source =~ "segment_projection_registry_key(handle, root_dir)"
    assert source =~ ":segment_projection_registry_key"

    refute function_source =~ "Path.expand",
           "storage position persistence runs on the WARaft apply hot path; root_dir expansion must be cached on the handle"
  end

  test "segment writer hot-path registry keys avoid repeated absolute path expansion" do
    source = File.read!(@segment_log_source_path)

    for function <- ["writer_key", "writer_dir_from_dir", "offset_dir_key"] do
      [function_source] =
        Regex.run(
          ~r/#{function}\([^)]*\) ->.*?\.\n/ms,
          source
        )

      assert function_source =~ "cache_dir_key",
             "#{function}/1 runs on the append hot path and should reuse absolute paths"

      refute function_source =~ "filename:absname",
             "#{function}/1 runs on the append hot path; do not normalize already-absolute paths per record"
    end
  end

  test "segment append validates file path only when opening a writer" do
    source = File.read!(@segment_log_source_path)

    [nosync_append_source] =
      Regex.run(
        ~r/write_record_group_once_nosync\(Dir, Segment, RecordsRev\) ->.*?\.\n/ms,
        source
      )

    [sync_append_source] =
      Regex.run(
        ~r/write_record_group_once\(Dir, Segment, RecordsRev\) ->.*?\.\n/ms,
        source
      )

    [open_source] =
      Regex.run(
        ~r/open_record_group_file_fd_after_inactive_close\(Registry, Key, WriterDir, Path\) ->.*?^    end\.\n/ms,
        source
      )

    refute nosync_append_source =~ "validate_segment_file_for_append",
           "nosync append may hit an already-open segment; validation belongs on writer open"

    refute sync_append_source =~ "validate_segment_file_for_append",
           "sync append may hit an already-open segment; validation belongs on writer open"

    assert open_source =~ "validate_segment_file_for_append(Path)",
           "new writer opens must still reject unsafe segment paths"
  end
end
