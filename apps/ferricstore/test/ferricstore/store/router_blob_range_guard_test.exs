defmodule Ferricstore.Store.RouterBlobRangeGuardTest do
  use ExUnit.Case, async: true

  test "blob-backed GETRANGE does not materialize the full blob" do
    source = Ferricstore.Test.SourceFiles.router_source()

    cold_blob_range =
      source
      |> String.split("  defp cold_blob_range_from_location(", parts: 2)
      |> List.last()
      |> String.split("  defp cold_blob_file_ref_from_location", parts: 2)
      |> List.first()

    refute cold_blob_range =~ "BlobStore.get(",
           "blob-backed GETRANGE should validate the blob ref and pread only the requested range; " <>
             "BlobStore.get/3 materializes and hashes the whole large value"
  end
end
