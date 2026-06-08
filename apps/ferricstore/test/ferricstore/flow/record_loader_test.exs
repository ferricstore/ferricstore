defmodule Ferricstore.Flow.RecordLoaderTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.RecordLoader

  test "decode_values preserves order and skips nil and undecodable values" do
    values = ["a", nil, "bad", "b"]

    decode = fn
      "bad" -> {:ok, nil}
      value -> {:ok, %{id: value}}
    end

    assert {:ok, records} = RecordLoader.decode_values(values, decode)
    assert Enum.map(records, & &1.id) == ["a", "b"]
  end

  test "decode_values returns router errors unchanged" do
    assert RecordLoader.decode_values(["a", {:error, "ERR shard not available"}], fn value ->
             {:ok, %{id: value}}
           end) == {:error, "ERR shard not available"}
  end
end
