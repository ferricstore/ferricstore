defmodule Ferricstore.Store.BlobRefTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.BlobRef

  test "rejects the obsolete content-addressed encoding" do
    checksum = :crypto.hash(:sha256, "payload")
    encoded = <<0, "FSBLOB", 1, 7::unsigned-big-64, checksum::binary>>

    refute BlobRef.encoded_size?(byte_size(encoded))
    refute BlobRef.ref?(encoded)
    assert :error == BlobRef.decode(encoded)
  end

  test "encodes a fixed-size append segment ref" do
    payload = :binary.copy("payload", 100)

    ref = BlobRef.from_segment(payload, 12, 4096)
    encoded = BlobRef.encode!(ref)

    assert byte_size(encoded) == BlobRef.encoded_size()
    assert <<0, "FSBLOB", 1, _rest::binary>> = encoded
    refute Map.has_key?(ref, :version)
    assert BlobRef.ref?(encoded)
    assert {:ok, ^ref} = BlobRef.decode(encoded)
    assert BlobRef.verify_payload?(ref, payload)
    assert BlobRef.relative_path(ref) == Path.join(["segments", "00000000000000000012.bloblog"])
  end

  test "maps refs to canonical shard-local blob paths" do
    payload = "shared-once"
    ref = BlobRef.from_segment(payload, 12, 4096)

    path = BlobRef.path("/tmp/ferricstore", 7, ref)
    relative = BlobRef.relative_path(ref)

    assert path == Path.join(["/tmp/ferricstore", "blob", "shard_7", relative])
    assert relative == Path.join(["segments", "00000000000000000012.bloblog"])
    refute path =~ ".."
  end

  test "rejects malformed or forged refs" do
    ref = BlobRef.from_segment("abc", 12, 4096)
    encoded = BlobRef.encode!(ref)

    assert :error == BlobRef.decode("abc")
    refute BlobRef.ref?(<<0, "FSBLOB", 2, "too-short">>)
    assert :error == BlobRef.decode(binary_part(encoded, 0, byte_size(encoded) - 1))
    assert :error == BlobRef.decode(encoded <> <<0>>)
    assert :error == BlobRef.decode(<<0, "FSBLOB", 2, 3::64, ref.checksum::binary>>)
  end

  test "rejects invalid ref fields before writing them to Bitcask" do
    good = BlobRef.from_segment("abc", 12, 4096)

    assert_raise ArgumentError, fn ->
      BlobRef.encode!(%{good | size: -1})
    end

    assert_raise ArgumentError, fn ->
      BlobRef.encode!(%{good | checksum: "not-32-bytes"})
    end
  end
end
