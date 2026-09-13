defmodule Ferricstore.Store.BlobStoreBoundaryMatrixTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.{BlobRef, BlobStore}

  @header_bytes 48
  @window_bytes 1_048_576
  @read_bytes @window_bytes - (@header_bytes - 1)

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "blob-boundary-matrix-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    Ferricstore.DataDir.ensure_layout!(root, 2)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  test "every header alignment preserves a later record across the probe boundary", %{root: root} do
    assert {:ok, prefix_ref} = BlobStore.put(root, 0, "prefix")
    path = BlobRef.path(root, 0, prefix_ref)
    prefix_bytes = File.read!(path)

    assert {:ok, partial_ref} = BlobStore.put(root, 1, :binary.copy("p", @window_bytes * 2))
    partial_header = record_bytes(root, 1, partial_ref) |> binary_part(0, @header_bytes)

    for payload <- ["", "boundary-visible"] do
      assert {:ok, donor_ref} = BlobStore.put(root, 1, payload)
      later_record = record_bytes(root, 1, donor_ref)

      for bytes_before_boundary <- 0..@header_bytes do
        header_offset = @read_bytes - bytes_before_boundary
        filler = :binary.copy("z", header_offset - @header_bytes)
        bytes = IO.iodata_to_binary([prefix_bytes, partial_header, filler, later_record])
        File.write!(path, bytes)

        ref = %{
          donor_ref
          | segment_id: prefix_ref.segment_id,
            offset: byte_size(prefix_bytes) + header_offset + @header_bytes
        }

        assert {:ok, ^payload} = BlobStore.get(root, 0, ref)

        assert {:error, {:corrupt_blob_segment, ^path, _, :complete_record_found}} =
                 BlobStore.recover_shard(root, 0)

        assert File.read!(path) == bytes,
               "repair discarded a record with #{bytes_before_boundary} header bytes before the boundary"

        assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
        assert {:ok, "prefix"} = BlobStore.get(root, 0, prefix_ref)
      end
    end
  end

  defp record_bytes(root, shard, ref) do
    root
    |> BlobRef.path(shard, ref)
    |> File.read!()
    |> binary_part(ref.offset - @header_bytes, @header_bytes + ref.size)
  end
end
