defmodule Ferricstore.Flow.RecordLoaderTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.RecordLoader

  test "decode_values preserves order and skips missing values" do
    values = ["a", nil, "b"]

    decode = fn value -> {:ok, %{id: value}} end

    assert {:ok, records} = RecordLoader.decode_values(values, decode)
    assert Enum.map(records, & &1.id) == ["a", "b"]
  end

  test "decode_values returns router errors unchanged" do
    assert RecordLoader.decode_values(["a", {:error, "ERR shard not available"}], fn value ->
             {:ok, %{id: value}}
           end) == {:error, "ERR shard not available"}
  end

  test "decode_values fails closed on corrupt records and malformed backend replies" do
    assert {:error, "ERR invalid flow record"} =
             RecordLoader.decode_values(["corrupt"], fn _value ->
               {:error, "ERR invalid flow record"}
             end)

    assert {:error, "ERR storage read failed"} =
             RecordLoader.decode_values([:invalid], fn _value -> {:ok, %{}} end)

    assert {:error, "ERR storage read failed"} =
             RecordLoader.decode_values(:invalid, fn _value -> {:ok, %{}} end)
  end
end
