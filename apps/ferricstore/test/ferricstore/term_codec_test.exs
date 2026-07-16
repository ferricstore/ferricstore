defmodule Ferricstore.TermCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.TermCodec

  test "round trips the current uncompressed deterministic format" do
    encoded = TermCodec.encode({:current, %{value: "payload"}})

    assert <<131, tag, _::binary>> = encoded
    refute tag == 80
    assert {:ok, {:current, %{value: "payload"}}} = TermCodec.decode(encoded)
  end

  test "rejects compressed and trailing external-term forms" do
    compressed =
      :erlang.term_to_binary(
        {:current, String.duplicate("compressible", 1_024)},
        compressed: 9
      )

    assert <<131, 80, _::binary>> = compressed
    assert {:error, :invalid_external_term} = TermCodec.decode(compressed)

    current = TermCodec.encode({:current, :value})
    assert {:error, :invalid_external_term} = TermCodec.decode(current <> <<0>>)
  end
end
