defmodule Ferricstore.Flow.Query.MemoryBudgetTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.Query.MemoryBudget

  test "conservatively accounts for decoded Flow record heap" do
    records = [decoded_record(%{}), decoded_record(maximum_metadata())]
    word_size = :erlang.system_info(:wordsize)

    for record <- records do
      flat_heap_bytes = :erts_debug.flat_size(record) * word_size

      assert MemoryBudget.term_bytes(record) >= flat_heap_bytes
    end
  end

  test "reserves storage and worst-case decode expansion before hydration" do
    available_bytes = 16 * 1_024 * 1_024
    maximum_input = MemoryBudget.encoded_record_input_bytes(available_bytes)

    assert maximum_input > 0
    assert maximum_input < div(available_bytes, 2)

    for metadata <- [%{}, maximum_metadata()] do
      encoded = metadata |> record() |> Codec.encode_record()
      decoded = Codec.decode_record(encoded)
      actual_heap = :erts_debug.flat_size(decoded) * :erlang.system_info(:wordsize)

      assert actual_heap + byte_size(encoded) <=
               MemoryBudget.decoded_record_reservation(byte_size(encoded))
    end
  end

  defp decoded_record(metadata),
    do: metadata |> record() |> Codec.encode_record() |> Codec.decode_record()

  defp record(metadata) do
    Map.merge(
      %{
        id: "run-1",
        type: "invoice",
        state: "failed",
        version: 1,
        created_at_ms: 1,
        updated_at_ms: 1,
        partition_key: "tenant"
      },
      metadata
    )
  end

  defp maximum_metadata do
    state_meta =
      for state <- 1..64, into: %{} do
        entries = for entry <- 1..16, into: %{}, do: {"k#{entry}", entry}
        {"s#{state}", entries}
      end

    attributes = for entry <- 1..16, into: %{}, do: {"a#{entry}", entry}
    %{attributes: attributes, state_meta: state_meta}
  end
end
