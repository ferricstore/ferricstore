defmodule Ferricstore.Store.BlobStoreLockGuardTest do
  use ExUnit.Case, async: true

  @sources [
             "../../../lib/ferricstore/store/blob_store.ex",
             "../../../lib/ferricstore/store/blob_store/write.ex",
             "../../../lib/ferricstore/store/blob_store/protection.ex",
             "../../../lib/ferricstore/store/blob_store/read.ex",
             "../../../lib/ferricstore/store/blob_store/gc.ex",
             "../../../lib/ferricstore/store/blob_store/io.ex"
           ]
           |> Enum.map(&Path.expand(&1, __DIR__))

  test "blob store uses a local shard latch instead of global locking" do
    # Blob files are shard-local and the lock key is scoped to this BEAM node.
    # Keep this path off :global.trans/3; it adds distributed-lock machinery
    # without improving correctness for blob append segments.
    ast = raw_source() |> Code.string_to_quoted!()

    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., dot_meta, [:global, :trans]} = node, call_meta, args}, acc ->
          line = dot_meta[:line] || call_meta[:line]
          {node, [{line, length(args)} | acc]}

        node, acc ->
          {node, acc}
      end)

    assert Enum.reverse(calls) == [],
           "BlobStore should not call :global.trans/3 on the large-value hot path; " <>
             "found calls at #{inspect(Enum.reverse(calls))}"
  end

  test "active segment cache avoids per-append file stat" do
    source = source()

    cached_active_segment =
      source
      |> String.split("  defp cached_active_segment", parts: 2)
      |> List.last()
      |> String.split("  defp scan_writable_segment", parts: 2)
      |> List.first()

    refute cached_active_segment =~ "File.stat",
           "cached active blob segment should use the cached size; " <>
             "per-append File.stat adds avoidable large-value write latency"
  end

  test "blob append does not mkdir the segment directory on every write" do
    source = source()

    do_put_many =
      source
      |> source_section!(
        "  defp do_put_many(data_dir, shard_index, batch) do",
        "  defp build_segment_records"
      )

    refute do_put_many =~ "mkdir_p",
           "BlobStore should cache the segment directory after the first durable mkdir; " <>
             "repeating mkdir_p on every blob append adds avoidable NIF/filesystem work"
  end

  test "put_many does not walk payloads only to validate binaries" do
    source = source()

    put_many =
      source
      |> String.split("  def put_many(data_dir, shard_index, payloads)", parts: 2)
      |> List.last()
      |> String.split("  @doc \"Reads and validates a blob by ref.\"", parts: 2)
      |> List.first()

    refute put_many =~ "Enum.all?",
           "BlobStore.put_many/3 should validate payloads while computing batch bytes; " <>
             "a separate Enum.all?/2 pass adds avoidable CPU work for large blob batches"
  end

  test "single put avoids the batch dedupe preparation path" do
    source = source()

    put =
      source
      |> String.split("  def put(data_dir, shard_index, payload)", parts: 2)
      |> List.last()
      |> String.split("  @doc \"\"\"\n  Stores payloads", parts: 2)
      |> List.first()

    refute put =~ "put_many(data_dir, shard_index, [payload])",
           "BlobStore.put/3 is the large single-SET hot path; it should not wrap the value " <>
             "in a list and run the full batch dedupe map/list machinery"
  end

  test "single-entry put_many uses the single append preparation path" do
    source = source()

    assert source =~ "def put_many(data_dir, shard_index, [payload])",
           "BlobStore.put_many/3 should special-case one payload; Raft batch prep often has " <>
             "one externalized large value and should not pay the full batch dedupe path"
  end

  test "segment_path uses the segment filename directly" do
    source = source()

    segment_path =
      source
      |> String.split("  defp segment_path(data_dir, shard_index, segment_id) do", parts: 2)
      |> List.last()
      |> String.split("  defp stat_regular_size", parts: 2)
      |> List.first()

    refute segment_path =~ "%BlobRef{",
           "BlobStore.segment_path/3 is on the append path; it should not allocate a fake BlobRef " <>
             "or checksum just to build the append-segment filename"
  end

  test "batched segment reads group by segment id before building paths" do
    source = source()

    for {name, start_marker, end_marker} <- [
          {"verify_many segment refs", "  defp verify_segment_refs(data_dir, shard_index, refs)",
           "  defp verify_segment_refs_at_path"},
          {"get_many segment refs",
           "  defp put_segment_ref_results(results, data_dir, shard_index, refs)",
           "  defp get_segment_refs_at_path"},
          {"file_refs_many segment refs",
           "  defp put_segment_file_ref_results(results, data_dir, shard_index, refs)",
           "  defp get_segment_file_refs_at_path"}
        ] do
      section = source_section!(source, start_marker, end_marker)

      refute section =~ "BlobRef.path(data_dir, shard_index, &1)",
             "#{name} should group by segment id, then build one path per segment; " <>
               "building a full path per ref adds avoidable allocation on large cold-read batches"
    end
  end

  test "file_refs_many validates segment headers with one batched pread" do
    source = source()

    section =
      source
      |> source_section!(
        "  defp get_segment_file_refs_at_path(path, shard_index, refs) do",
        "  @doc \"\"\"\n  Returns a file ref"
      )

    assert section =~ ":file.pread(io, header_reads)",
           "file_refs_many/3 is the sendfile/MGET hot path; after grouping by segment " <>
             "it should validate all 48-byte record headers with one batched pread"

    refute section =~ "validate_open_segment_record(io",
           "file_refs_many/3 should not issue one header pread per blob ref after " <>
             "opening the segment"
  end

  test "verify_many validates segment headers with one batched pread while hashing payloads" do
    source = source()

    section =
      source
      |> source_section!(
        "  defp verify_open_segment_refs(io, path, shard_index, refs) do",
        "  defp prepare_get_many_refs"
      )

    assert section =~ "read_segment_headers(io, refs)",
           "verify_many/3 should batch segment header validation before per-ref payload hashing"

    assert section =~ "open_file_range_matches_ref?",
           "verify_many/3 must still hash payload ranges in chunks for correctness"

    refute section =~ "validate_open_segment_record(io",
           "verify_many/3 should not issue one header pread per blob ref after opening the segment"
  end

  test "get_many validates and reads segment refs with batched preads" do
    source = source()

    section =
      source
      |> source_section!(
        "  defp get_segment_refs_at_path(path, shard_index, [_first_ref | _] = refs) do",
        "  defp load_blob_file_refs"
      )

    assert section =~ "read_segment_headers(io, refs)",
           "get_many/3 should batch segment header validation before materializing payloads"

    assert section =~ ":file.pread(io, payload_reads)",
           "get_many/3 should batch same-segment payload preads instead of one pread per ref"

    refute section =~ "get_open_segment_ref(",
           "get_many/3 should not fall back to one full header+payload read per blob ref"
  end

  defp raw_source do
    Enum.map_join(@sources, "\n", &File.read!/1)
  end

  defp source do
    @sources
    |> Enum.map_join("\n", &File.read!/1)
    |> String.replace("\n    ", "\n  ")
    |> String.replace("\n    Stores", "\n  Stores")
    |> String.replace("\n    Returns", "\n  Returns")
  end

  defp source_section!(source, start_marker, end_marker) do
    [_, rest] = String.split(source, start_marker, parts: 2)
    [section | _] = String.split(rest, end_marker, parts: 2)
    section
  end
end
