defmodule Ferricstore.Commands.TDigestPersistedCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.TDigest
  alias Ferricstore.Test.MockStore

  @valid_metadata %{
    compression: 100,
    count: 0,
    min: nil,
    max: nil,
    buffer: [],
    buffer_size: 0,
    total_compressions: 0
  }

  test "rejects compressed and trailing persisted digests" do
    term =
      {:tdigest, [], Map.put(@valid_metadata, :padding, :binary.copy("digest-metadata", 1_000))}

    for raw <- [
          :erlang.term_to_binary(term, compressed: 9),
          :erlang.term_to_binary(term) <> <<0>>
        ] do
      store = MockStore.make(%{"digest" => {raw, 0}})
      assert {:error, message} = TDigest.handle("TDIGEST.INFO", ["digest"], store)
      assert message =~ "WRONGTYPE"
    end
  end

  test "rejects persisted digests with invalid structural fields" do
    invalid_terms = [
      {:tdigest, [], %{@valid_metadata | compression: "100"}},
      {:tdigest, [], %{@valid_metadata | compression: 1_001}},
      {:tdigest, [], %{@valid_metadata | buffer: ["not-a-number"], buffer_size: 1}},
      {:tdigest, [{"not-a-number", 1.0}], @valid_metadata},
      {:tdigest, [], %{@valid_metadata | min: 2.0, max: 1.0, count: 1}},
      {:tdigest, [],
       %{@valid_metadata | min: 1.0, max: 1.0, count: 2, buffer: [1.0], buffer_size: 1}},
      {:tdigest, [{1.0, 1.0}], @valid_metadata},
      {:tdigest, [{2.0, 1.0}], %{@valid_metadata | min: 3.0, max: 4.0, count: 1}},
      {:tdigest, [], %{@valid_metadata | count: Integer.pow(10, 1_000), min: 1.0, max: 1.0}}
    ]

    for term <- invalid_terms do
      store = MockStore.make(%{"digest" => {Ferricstore.TermCodec.encode(term), 0}})
      assert {:error, message} = TDigest.handle("TDIGEST.INFO", ["digest"], store)
      assert message =~ "WRONGTYPE"
    end
  end
end
