Code.require_file("set_test/sections/sadd.exs", __DIR__)
Code.require_file("set_test/sections/srandmember.exs", __DIR__)

defmodule Ferricstore.Commands.SetTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Hash, List, Set, SortedSet}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # SADD
  # ---------------------------------------------------------------------------

  use Ferricstore.Commands.SetTest.Sections.Sadd

  use Ferricstore.Commands.SetTest.Sections.Srandmember

  defp collect_sscan_members(store, key, cursor, count) do
    collect_sscan_members(store, key, cursor, count, [])
  end

  defp collect_sscan_members(store, key, cursor, count, acc) do
    [next_cursor, elements] =
      Set.handle("SSCAN", [key, cursor, "COUNT", Integer.to_string(count)], store)

    new_acc = acc ++ elements

    if next_cursor == "0" do
      new_acc
    else
      collect_sscan_members(store, key, next_cursor, count, new_acc)
    end
  end

  defp set_cleanup_failure_store do
    type_key = CompoundKey.type_key("myset")
    member_key = CompoundKey.set_member("myset", "only")

    %{
      compound_get: fn "myset", ^type_key -> "set" end,
      compound_batch_get: fn "myset", [^member_key] -> ["1"] end,
      compound_batch_delete: fn "myset", [^member_key] -> :ok end,
      compound_batch_put: fn "myset", [{^member_key, "1", 0}] -> :ok end,
      compound_count: fn "myset", _prefix -> 0 end,
      compound_delete: fn "myset", ^type_key -> {:error, :disk_full} end,
      compound_scan: fn "myset", _prefix -> [{"only", "1"}] end
    }
  end
end
