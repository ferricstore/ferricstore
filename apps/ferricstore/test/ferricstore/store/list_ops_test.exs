defmodule Ferricstore.Store.ListOpsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.ListOps

  test "RPUSH returns write error when element append fails" do
    store = failing_put_store()

    assert {:error, "disk full"} = ListOps.execute("list", store, {:rpush, ["a"]})
  end

  test "LPOP returns write error when element tombstone fails" do
    store = failing_delete_store("list", ["a"])

    assert {:error, "disk full"} = ListOps.execute("list", store, {:lpop, 1})
  end

  defp failing_put_store do
    %{
      compound_get: fn _redis_key, _compound_key -> nil end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms ->
        {:error, "disk full"}
      end,
      compound_delete: fn _redis_key, _compound_key -> :ok end,
      compound_scan: fn _redis_key, _prefix -> [] end
    }
  end

  defp failing_delete_store(key, values) do
    meta = {length(values), -1_000_000_000, length(values) * 1_000_000_000}

    elements =
      values
      |> Enum.with_index()
      |> Enum.map(fn {value, idx} ->
        {CompoundKey.encode_position(idx * 1_000_000_000), value}
      end)

    %{
      compound_get: fn ^key, compound_key ->
        if compound_key == CompoundKey.list_meta_key(key), do: :erlang.term_to_binary(meta)
      end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms -> :ok end,
      compound_delete: fn _redis_key, _compound_key -> {:error, "disk full"} end,
      compound_scan: fn ^key, _prefix -> elements end
    }
  end
end
