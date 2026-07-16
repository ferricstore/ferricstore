defmodule Ferricstore.Raft.WARaftStorageHotPathGuardTest do
  use ExUnit.Case, async: true
  @moduletag :raft

  test "LMDB release-cursor poke is explicitly opt-in" do
    source = File.read!(Path.expand("../../../lib/ferricstore/flow/lmdb_writer.ex", __DIR__))

    assert source =~ ":flow_lmdb_release_cursor_poke_enabled"

    assert source =~
             "Application.get_env(:ferricstore, :flow_lmdb_release_cursor_poke_enabled, false)"
  end

  test "segment projection registration caches expanded root path on the storage handle" do
    source = Ferricstore.Test.SourceFiles.waraft_storage_source()

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
    source = Ferricstore.Test.SourceFiles.waraft_segment_log_source()

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
    source = Ferricstore.Test.SourceFiles.waraft_segment_log_source()

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

  test "segment projection checkpoints do not copy the whole keydir with tab2list" do
    # Segment projection checkpoint/snapshot paths can run with millions of
    # keydir rows. :ets.tab2list/1 makes one large BEAM list of every row before
    # the code can skip expired/non-projectable entries, creating avoidable
    # memory spikes and scheduler latency. Use ETS folding/streaming instead.
    assert tab2list_calls(Ferricstore.Test.SourceFiles.waraft_storage_source()) == []
  end

  test "segment metadata reads are bounded and nofollow at open time" do
    source = Ferricstore.Test.SourceFiles.waraft_segment_log_source()

    for function <- ["read_append_failure_marker", "read_segment_metadata_file"] do
      [function_source] =
        Regex.run(
          ~r/#{function}\([^)]*\) ->.*?\.\n/ms,
          source
        )

      assert function_source =~ "read_segment_file_nofollow",
             "#{function}/N must not reopen a validated path with symlink following"

      refute function_source =~ "file:read_file"
    end

    assert source =~ "'Elixir.Ferricstore.Bitcask.NIF':fs_read_nofollow"
  end

  test "new segment writers verify the opened descriptor identity" do
    source = Ferricstore.Test.SourceFiles.waraft_segment_log_source()

    [open_source] =
      Regex.run(
        ~r/open_record_group_file_fd_after_inactive_close\([^)]*\) ->.*?^    end\.\n/ms,
        source
      )

    assert open_source =~ "validate_open_segment_file(Path, Fd)"

    assert String.split(open_source, "validate_open_segment_file(Path, Fd)", parts: 2)
           |> hd() =~ "file:open(Path"

    assert source =~ "file:read_file_info(Fd)"
    assert source =~ "file:read_link_info(Path)"
  end

  test "segment recovery and truncation verify descriptor identity after open" do
    source = Ferricstore.Test.SourceFiles.waraft_segment_log_source()

    refute source =~ "case file:open(Path, [read",
           "an lstat followed by an unchecked read/read-write open permits a final symlink swap"

    assert source =~ "open_verified_segment_file(Path, [read, raw, binary])"
    assert source =~ "open_verified_segment_file(Path, [read, write, raw, binary])"
  end

  test "segment metadata temporary writes use exclusive nofollow atomic replacement" do
    source = Ferricstore.Test.SourceFiles.waraft_segment_log_source()

    [write_source] =
      Regex.run(
        ~r/write_file_sync\(Path, Binary\) ->.*?^    end\./ms,
        source
      )

    assert write_source =~ "fs_atomic_replace_nofollow"
    refute write_source =~ "file:open"
  end

  test "WARaft storage metadata journal is bounded and nofollow" do
    source = Ferricstore.Test.SourceFiles.waraft_storage_source()

    [append_source] =
      Regex.run(
        ~r/defp append_metadata_journal_payload\(path, payload\).*?^      end/ms,
        source
      )

    assert source =~ "@max_metadata_journal_bytes"
    assert append_source =~ "Ferricstore.FS.append_sync_nofollow_bounded"
    refute append_source =~ "File.write"

    [size_source] =
      Regex.run(
        ~r/defp metadata_journal_size\(journal_path\).*?^      end/ms,
        source
      )

    assert size_source =~ "@max_metadata_journal_bytes"

    for function <- ["rollback_metadata_journal_append", "read_latest_storage_metadata_journal"] do
      assert source =~ "open_verified_metadata_journal"
      assert source =~ "defp #{function}"
    end
  end

  test "WARaft storage metadata temp writes use nofollow atomic replacement" do
    source = Ferricstore.Test.SourceFiles.waraft_storage_source()

    [write_source] =
      Regex.run(
        ~r/defp atomic_write_binary\(path, payload\).*?^      end/ms,
        source
      )

    assert write_source =~ "Ferricstore.FS.atomic_replace_nofollow"
    refute write_source =~ "File.write"
  end

  test "snapshot payload files are copied through a nofollow streaming primitive" do
    source = Ferricstore.Test.SourceFiles.waraft_storage_source()

    [copy_source] =
      Regex.run(
        ~r/defp copy_snapshot_payload_entry\(source, dest\).*?^      end/ms,
        source
      )

    assert copy_source =~ "Ferricstore.FS.copy_sync_nofollow"
    refute copy_source =~ "File.cp"
  end

  defp tab2list_calls(source) do
    {:ok, ast} =
      Code.string_to_quoted(source, columns: true)

    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., meta, [:ets, :tab2list]}, _call_meta, _args} = node, acc ->
          {node, [{meta[:line], meta[:column]} | acc]}

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(calls)
  end
end
