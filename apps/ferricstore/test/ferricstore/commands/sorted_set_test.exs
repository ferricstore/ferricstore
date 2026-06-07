Code.require_file("sorted_set_test/sections/part_01.exs", __DIR__)
Code.require_file("sorted_set_test/sections/part_02.exs", __DIR__)

defmodule Ferricstore.Commands.SortedSetTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Hash, List, Set, SortedSet}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # ZADD
  # ---------------------------------------------------------------------------

  use Ferricstore.Commands.SortedSetTest.Sections.Part01

  use Ferricstore.Commands.SortedSetTest.Sections.Part02

  defp collect_zscan_members(store, key, cursor, count) do
    collect_zscan_members(store, key, cursor, count, [])
  end

  defp collect_zscan_members(store, key, cursor, count, acc) do
    [next_cursor, elements] =
      SortedSet.handle("ZSCAN", [key, cursor, "COUNT", Integer.to_string(count)], store)

    pairs = elements |> Enum.chunk_every(2) |> Enum.map(fn [m, s] -> {m, s} end)
    new_acc = acc ++ pairs

    if next_cursor == "0" do
      new_acc
    else
      collect_zscan_members(store, key, next_cursor, count, new_acc)
    end
  end

  defp zset_cleanup_failure_store do
    type_key = CompoundKey.type_key("zs")
    member_key = CompoundKey.zset_member("zs", "only")

    %{
      compound_get: fn "zs", ^type_key -> "zset" end,
      compound_batch_get: fn "zs", [^member_key] -> ["1.0"] end,
      compound_batch_delete: fn "zs", [^member_key] -> :ok end,
      compound_batch_put: fn "zs", [{^member_key, "1.0", 0}] -> :ok end,
      compound_count: fn "zs", _prefix -> 0 end,
      compound_delete: fn "zs", ^type_key -> {:error, :disk_full} end,
      compound_scan: fn "zs", _prefix -> [{"only", "1.0"}] end
    }
  end
end
