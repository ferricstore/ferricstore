defmodule Ferricstore.Store.BlobStoreLockGuardTest do
  use ExUnit.Case, async: true

  @source Path.expand("../../../lib/ferricstore/store/blob_store.ex", __DIR__)

  test "blob store uses a local shard latch instead of global locking" do
    # Blob files are shard-local and the lock key is scoped to this BEAM node.
    # Keep this path off :global.trans/3; it adds distributed-lock machinery
    # without improving correctness for blob append segments.
    ast = @source |> File.read!() |> Code.string_to_quoted!()

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
    source = File.read!(@source)

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
    source = File.read!(@source)

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
    source = File.read!(@source)

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
    source = File.read!(@source)

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

  test "segment_path uses the segment filename directly" do
    source = File.read!(@source)

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
    source = File.read!(@source)

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

  defp source_section!(source, start_marker, end_marker) do
    [_, rest] = String.split(source, start_marker, parts: 2)
    [section | _] = String.split(rest, end_marker, parts: 2)
    section
  end
end
