Code.require_file("hash_test/sections/hset.exs", __DIR__)
Code.require_file("hash_test/sections/hexpire.exs", __DIR__)
Code.require_file("hash_test/sections/wrongtype_enforcement_edge_cases.exs", __DIR__)

defmodule Ferricstore.Commands.HashTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Generic
  alias Ferricstore.Commands.Hash
  alias Ferricstore.Commands.List
  alias Ferricstore.Commands.Set
  alias Ferricstore.Commands.SortedSet
  alias Ferricstore.Commands.Strings
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # HSET
  # ---------------------------------------------------------------------------

  use Ferricstore.Commands.HashTest.Sections.Hset
  use Ferricstore.Commands.HashTest.Sections.Hexpire
  use Ferricstore.Commands.HashTest.Sections.WrongtypeEnforcementEdgeCases

  defp collect_hscan_fields(store, key, cursor, count) do
    collect_hscan_fields(store, key, cursor, count, [])
  end

  defp collect_hscan_fields(store, key, cursor, count, acc) do
    [next_cursor, elements] =
      Hash.handle("HSCAN", [key, cursor, "COUNT", Integer.to_string(count)], store)

    pairs = elements |> Enum.chunk_every(2) |> Enum.map(fn [k, v] -> {k, v} end)
    new_acc = acc ++ pairs

    if next_cursor == "0" do
      new_acc
    else
      collect_hscan_fields(store, key, next_cursor, count, new_acc)
    end
  end

  defp hash_cleanup_failure_store do
    type_key = CompoundKey.type_key("hash")
    field_key = CompoundKey.hash_field("hash", "f1")

    %{
      compound_get: fn "hash", ^type_key -> "hash" end,
      compound_batch_get: fn "hash", [^field_key] -> ["v1"] end,
      compound_batch_get_meta: fn "hash", [^field_key] -> [{"v1", 0}] end,
      compound_batch_delete: fn "hash", [^field_key] -> :ok end,
      compound_batch_put: fn "hash", [{^field_key, "v1", 0}] -> :ok end,
      compound_count: fn "hash", _prefix -> 0 end,
      compound_delete: fn "hash", ^type_key -> {:error, :disk_full} end
    }
  end
end
