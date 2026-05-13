defmodule Ferricstore.Store.BlobRefTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.BlobRef

  test "encodes a deterministic fixed-size content-addressed ref" do
    payload = :binary.copy("payload", 100)

    ref = BlobRef.from_payload(payload)
    encoded = BlobRef.encode!(ref)

    assert byte_size(encoded) == BlobRef.encoded_size()
    assert BlobRef.ref?(encoded)
    assert {:ok, ^ref} = BlobRef.decode(encoded)
    assert BlobRef.verify_payload?(ref, payload)
    refute BlobRef.verify_payload?(ref, payload <> "!")

    assert ref == BlobRef.from_payload(payload)
    refute ref == BlobRef.from_payload(payload <> "!")
  end

  test "maps refs to canonical shard-local blob paths" do
    payload = "shared-once"
    ref = BlobRef.from_payload(payload)

    path = BlobRef.path("/tmp/ferricstore", 7, ref)
    relative = BlobRef.relative_path(ref)
    checksum = Base.encode16(ref.checksum, case: :lower)

    assert path == Path.join(["/tmp/ferricstore", "blob", "shard_7", relative])
    assert relative == Path.join([binary_part(checksum, 0, 2), checksum <> ".blob"])
    refute path =~ ".."
  end

  test "rejects malformed or forged refs" do
    ref = BlobRef.from_payload("abc")
    encoded = BlobRef.encode!(ref)

    assert :error == BlobRef.decode("abc")
    assert :error == BlobRef.decode(binary_part(encoded, 0, byte_size(encoded) - 1))
    assert :error == BlobRef.decode(encoded <> <<0>>)
    assert :error == BlobRef.decode(<<0, "FSBLOB", 2, 3::64, ref.checksum::binary>>)
  end

  test "rejects invalid ref fields before writing them to Bitcask" do
    good = BlobRef.from_payload("abc")

    assert_raise ArgumentError, fn ->
      BlobRef.encode!(%{good | size: -1})
    end

    assert_raise ArgumentError, fn ->
      BlobRef.encode!(%{good | checksum: "not-32-bytes"})
    end
  end
end
