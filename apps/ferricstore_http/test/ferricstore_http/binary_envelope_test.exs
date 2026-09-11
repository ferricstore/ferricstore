defmodule FerricstoreHttp.BinaryEnvelopeTest do
  use ExUnit.Case, async: true

  alias FerricstoreHttp.BinaryEnvelope

  test "round trips arbitrary bytes, nested lists, and non-string map keys" do
    value = %{<<0, 255>> => ["text", <<255, 0>>], 7 => %{true => nil}}

    assert {:ok, ^value} = value |> BinaryEnvelope.encode() |> BinaryEnvelope.decode()
  end

  test "rejects malformed markers" do
    assert {:error, :malformed_binary_envelope} =
             BinaryEnvelope.decode(%{"$ferricstore_bytes" => "not base64"})

    assert {:error, :malformed_binary_envelope} =
             BinaryEnvelope.decode(%{"$ferricstore_map" => [["missing-value"]]})
  end
end
