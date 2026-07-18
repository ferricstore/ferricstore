defmodule Ferricstore.BitcaskNifSchedulerGuardTest do
  use ExUnit.Case, async: true

  @moduletag :guard

  @native_src Path.expand("../../native/ferricstore_bitcask/src", __DIR__)
  @dirty_io_maintenance_nifs ~w(
    v2_build_hint_file_from_log
    v2_copy_records
    v2_copy_records_preserve_tombstones
    v2_read_hint_file_page
    v2_write_hint_file
  )
  @dirty_io_blocking_nifs ~w(
    v2_append_record
    v2_append_tombstone
    v2_append_batch
    v2_append_batch_nosync
    v2_append_ops_batch
    v2_append_ops_batch_nosync
    v2_validate_value_ref
    v2_pread_at
    v2_pread_batch
    v2_scan_file_page
    v2_scan_tombstones_page
    v2_scan_key_states
    v2_fsync
    v2_fsync_dir
    v2_available_disk_space
    io_uring_available
  )
  @dirty_io_probabilistic_nifs ~w(
    prob_file_recover
    bloom_file_create
    bloom_file_add
    bloom_file_add_at
    bloom_file_madd
    bloom_file_madd_at
    bloom_file_exists
    bloom_file_mexists
    bloom_file_card
    bloom_file_info
    cms_file_create
    cms_file_incrby
    cms_file_incrby_at
    cms_file_query
    cms_file_info
    cms_file_merge
    cms_file_merge_at
    cuckoo_file_create
    cuckoo_file_add
    cuckoo_file_add_at
    cuckoo_file_addnx
    cuckoo_file_addnx_at
    cuckoo_file_del
    cuckoo_file_del_at
    cuckoo_file_exists
    cuckoo_file_mexists
    cuckoo_file_count
    cuckoo_file_info
    topk_file_create_v2
    topk_file_add_v2
    topk_file_add_v2_at
    topk_file_incrby_v2
    topk_file_incrby_v2_at
    topk_file_query_v2
    topk_file_list_v2
    topk_file_list_with_count
    topk_file_count_v2
    topk_file_info_v2
  )
  @dirty_io_metadata_nifs ~w(
    fs_touch
    fs_mkdir_p
    fs_rename
    fs_rm
    fs_exists
    fs_is_dir
    fs_ls
    fs_read_nofollow
    fs_copy_sync_nofollow
    fs_copy_replace_sync_nofollow
    fs_hard_link_replace_sync_nofollow
    fs_append_sync_nofollow
    fs_append_sync_nofollow_bounded
    fs_atomic_replace_nofollow
  )
  @dirty_cpu_async_copy_nifs ~w(
    v2_append_batch_async
    v2_pread_batch_path_async
    v2_pread_batch_path_key_async
    v2_pread_batch_async
    v2_pread_batch_grouped_async
    v2_pread_batch_grouped_key_async
    bloom_file_exists_async
    bloom_file_mexists_async
    cms_file_query_async
    cuckoo_file_exists_async
    cuckoo_file_mexists_async
    cuckoo_file_count_async
    topk_file_query_v2_async
    topk_file_count_v2_async
  )
  @dirty_io_nifs @dirty_io_maintenance_nifs ++
                   @dirty_io_blocking_nifs ++
                   @dirty_io_probabilistic_nifs ++
                   @dirty_io_metadata_nifs

  test "only explicitly classified blocking NIFs use dirty schedulers" do
    offenders =
      @native_src
      |> Path.join("**/*.rs")
      |> Path.wildcard()
      |> Enum.flat_map(fn path ->
        path
        |> File.read!()
        |> String.split("\n")
        |> Enum.with_index(1)
        |> Enum.filter(fn {line, line_no} ->
          line =~ ~r/^\s*#\[rustler::nif\(schedule = "Dirty(?:Io|Cpu)"\)\]/ and
            not allowed_dirty_nif?(path, line_no)
        end)
        |> Enum.map(fn {line, line_no} -> "#{Path.relative_to_cwd(path)}:#{line_no}:#{line}" end)
      end)

    assert offenders == [], "unexpected dirty-scheduler NIFs:\n" <> Enum.join(offenders, "\n")
  end

  test "every synchronous filesystem NIF is classified as dirty I/O" do
    source = native_source()

    for function <-
          @dirty_io_blocking_nifs ++ @dirty_io_probabilistic_nifs ++ @dirty_io_metadata_nifs do
      assert_nif_schedule(source, function, "DirtyIo")
    end
  end

  test "blocking Bitcask write/fsync NIFs use dirty I/O schedulers" do
    source = Ferricstore.Test.SourceFiles.bitcask_native_source()

    for function <- [
          "v2_append_record",
          "v2_append_tombstone",
          "v2_append_batch",
          "v2_append_batch_nosync",
          "v2_append_ops_batch",
          "v2_append_ops_batch_nosync",
          "v2_fsync",
          "v2_fsync_dir",
          "v2_available_disk_space",
          "io_uring_available"
        ] do
      assert_nif_schedule(source, function, "DirtyIo")
    end
  end

  test "blocking Bitcask cold-read and bounded scan NIFs use dirty I/O schedulers" do
    source = Ferricstore.Test.SourceFiles.bitcask_native_source()

    for function <- [
          "v2_pread_at",
          "v2_scan_file_page",
          "v2_scan_tombstones_page",
          "v2_read_hint_file_page",
          "v2_pread_batch"
        ] do
      assert_nif_schedule(source, function, "DirtyIo")
    end
  end

  test "long compaction and hint I/O uses dirty I/O schedulers" do
    source = Ferricstore.Test.SourceFiles.bitcask_native_source()

    for function <- @dirty_io_maintenance_nifs do
      assert_nif_schedule(source, function, "DirtyIo")
    end
  end

  test "async batch append does record encoding on Tokio blocking workers" do
    source = Ferricstore.Test.SourceFiles.bitcask_native_source()
    body = function_body(source, "v2_append_batch_async")

    [normal_scheduler_prefix, _blocking_worker_suffix] =
      String.split(body, "spawn_blocking", parts: 2)

    refute normal_scheduler_prefix =~ "log::encode_record",
           "v2_append_batch_async must not CRC/encode records on a Normal BEAM scheduler"
  end

  test "bounded nofollow reads use dirty I/O and avoid an intermediate Vec" do
    source = File.read!(Path.join(@native_src, "fs_nif.rs"))
    body = function_body(source, "fs_read_nofollow")

    assert_nif_schedule(source, "fs_read_nofollow", "DirtyIo")
    assert body =~ "OwnedBinary::new"
    refute body =~ "read_to_end"
  end

  test "unbounded async batch copies use dirty CPU schedulers" do
    source = native_source()

    for function <- @dirty_cpu_async_copy_nifs do
      assert_nif_schedule(source, function, "DirtyCpu")
    end
  end

  test "hot nosync batch append NIFs use small short-lived writer buffers" do
    source = Ferricstore.Test.SourceFiles.bitcask_native_source()

    for function <- [
          "v2_append_batch_nosync",
          "v2_append_ops_batch_nosync"
        ] do
      body = function_body(source, function)

      assert body =~ "log::LogWriter::open_small",
             "#{function}/N must use open_small to avoid 256KB buffer churn per hot append"
    end
  end

  defp assert_nif_schedule(source, function, schedule) do
    pattern =
      ~r/#\[rustler::nif\(schedule = "#{schedule}"\)\]\s*(?:#\[allow\([^\]]+\)\]\s*)?(?:pub\s+)?fn #{function}\b/

    assert source =~ pattern,
           "expected #{function}/N to use #[rustler::nif(schedule = \"#{schedule}\")]"
  end

  defp function_body(source, function) do
    [_before, rest] = String.split(source, "fn #{function}", parts: 2)
    [body, _after] = String.split(rest, "\n}\n\n", parts: 2)
    body
  end

  defp lmdb_nif?(path, schedule_line_no) do
    path
    |> File.read!()
    |> String.split("\n")
    |> Enum.slice(schedule_line_no, 4)
    |> Enum.any?(&String.match?(&1, ~r/^\s*fn lmdb_/))
  end

  defp allowed_dirty_nif?(path, schedule_line_no) do
    String.ends_with?(path, "flow_index.rs") or
      lmdb_nif?(path, schedule_line_no) or
      Enum.any?(@dirty_cpu_async_copy_nifs, &nif_near_line?(path, schedule_line_no, &1)) or
      Enum.any?(@dirty_io_nifs, &nif_near_line?(path, schedule_line_no, &1))
  end

  defp nif_near_line?(path, schedule_line_no, function) do
    path
    |> File.read!()
    |> String.split("\n")
    |> Enum.slice(schedule_line_no, 12)
    |> Enum.any?(&String.match?(&1, ~r/^\s*(?:pub\s+)?fn #{function}\b/))
  end

  defp native_source do
    @native_src
    |> Path.join("**/*.rs")
    |> Path.wildcard()
    |> Enum.map_join("\n", &File.read!/1)
  end
end
