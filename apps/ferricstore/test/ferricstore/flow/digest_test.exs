defmodule Ferricstore.Flow.DigestTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.Digest

  setup do
    previous = Digest.format()
    on_exit(fn -> Digest.put_format(previous) end)
    :ok
  end

  test "format 2 digests are the first 128 bits of SHA-256, 22 characters" do
    :ok = Digest.put_format(2)
    digest = Digest.encode("tenant-a")
    assert byte_size(digest) == 22
    assert {:ok, bytes} = Base.url_decode64(digest, padding: false)
    assert bytes == binary_part(:crypto.hash(:sha256, "tenant-a"), 0, 16)
    assert Digest.length() == 22
  end

  test "format 1 digests are the full SHA-256, 43 characters" do
    :ok = Digest.put_format(1)
    digest = Digest.encode("tenant-a")
    assert digest == Base.url_encode64(:crypto.hash(:sha256, "tenant-a"), padding: false)
    assert Digest.length() == 43
  end

  test "a format 1 digest converts to the format 2 digest of the same value" do
    :ok = Digest.put_format(1)
    legacy = Digest.encode("partition-key-42")
    :ok = Digest.put_format(2)
    assert {:ok, Digest.encode("partition-key-42")} == Digest.convert_legacy(legacy)
    assert :error == Digest.convert_legacy("not-a-digest")
  end

  test "canonical? accepts canonical digests of either length in either format" do
    for format <- [1, 2] do
      :ok = Digest.put_format(format)
      short = Base.url_encode64(binary_part(:crypto.hash(:sha256, "x"), 0, 16), padding: false)
      long = Base.url_encode64(:crypto.hash(:sha256, "x"), padding: false)
      assert Digest.canonical?(short)
      assert Digest.canonical?(long)
    end

    # A 16-byte digest's last character carries 2 data bits: only A, Q, g, w end one.
    body = String.duplicate("A", 21)
    for c <- ~c"AQgw", do: assert(Digest.canonical?(body <> <<c>>))
    for c <- ~c"BEIMRZhx-_9", do: refute(Digest.canonical?(body <> <<c>>))
    refute Digest.canonical?(String.duplicate("A", 21) <> "+")
    refute Digest.canonical?(String.duplicate("A", 23))
    refute Digest.canonical?(nil)
  end

  test "put_format rejects unknown formats" do
    assert_raise FunctionClauseError, fn -> Digest.put_format(3) end
  end
end
