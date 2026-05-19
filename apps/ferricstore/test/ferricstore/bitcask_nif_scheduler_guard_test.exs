defmodule Ferricstore.BitcaskNifSchedulerGuardTest do
  use ExUnit.Case, async: true

  @moduletag :guard

  @native_src Path.expand("../../native/ferricstore_bitcask/src", __DIR__)
  @source Path.join(@native_src, "lib.rs")
  @bloom_source Path.join(@native_src, "bloom.rs")

  test "Bitcask Rust NIFs use dirty schedulers only for approved blocking IO paths" do
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
            not approved_dirty_nif?(path, line_no)
        end)
        |> Enum.map(fn {line, line_no} -> "#{Path.relative_to_cwd(path)}:#{line_no}:#{line}" end)
      end)

    assert offenders == [],
           "Unexpected dirty scheduler NIFs; only approved blocking IO paths may use them:\n" <>
             Enum.join(offenders, "\n")
  end

  test "blocking Bloom write/fsync NIFs use dirty IO schedulers" do
    source = File.read!(@bloom_source)

    for function <- [
          "bloom_file_create",
          "bloom_file_add",
          "bloom_file_madd"
        ] do
      assert_nif_schedule(source, function, "DirtyIo")
    end
  end

  test "blocking Bitcask write/fsync NIFs stay on normal schedulers" do
    source = File.read!(@source)

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
      assert_nif_schedule(source, function, "Normal")
    end
  end

  test "blocking Bitcask cold-read and scan NIFs stay on normal schedulers" do
    source = File.read!(@source)

    for function <- [
          "v2_pread_at",
          "v2_scan_file",
          "v2_scan_file_from_offset",
          "v2_scan_tombstones",
          "v2_pread_batch",
          "v2_read_hint_file"
        ] do
      assert_nif_schedule(source, function, "Normal")
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

  test "async batch append submit stays on normal schedulers" do
    source = File.read!(@source)

    assert_nif_schedule(source, "v2_append_batch_async", "Normal")
  end

  test "async batch cold-read submit stays on normal schedulers" do
    source = File.read!(@source)

    for function <- [
          "v2_pread_batch_path_async",
          "v2_pread_batch_async",
          "v2_pread_batch_grouped_async"
        ] do
      assert_nif_schedule(source, function, "Normal")
    end
  end

  test "hot nosync batch append NIFs use small short-lived writer buffers" do
    source = File.read!(@source)

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

  defp approved_dirty_nif?(path, schedule_line_no) do
    path
    |> File.read!()
    |> String.split("\n")
    |> Enum.slice(schedule_line_no, 4)
    |> Enum.any?(
      &(String.match?(&1, ~r/^\s*fn lmdb_/) or
          String.match?(&1, ~r/^\s*pub fn bloom_file_(create|add|madd)\b/) or
          String.match?(&1, ~r/^\s*pub fn flow_record_plan_claims\b/) or
          String.match?(&1, ~r/^\s*pub fn flow_records_terminal_after_noop\b/))
    )
  end
end
