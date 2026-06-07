Code.require_file("list_test/sections/part_01.exs", __DIR__)
Code.require_file("list_test/sections/part_02.exs", __DIR__)
Code.require_file("list_test/sections/part_03.exs", __DIR__)

defmodule Ferricstore.Commands.ListTest do
  @moduledoc """
  Comprehensive tests for the List command handler.

  Tests cover all 15 list commands (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN,
  LINDEX, LSET, LREM, LTRIM, LPOS, LINSERT, LMOVE, LPUSHX, RPUSHX),
  including happy paths, error cases, edge cases, and WRONGTYPE checking.

  All tests use the MockStore which delegates to the same `List.execute/4`
  logic used by the Shard GenServer, ensuring behavioral parity.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Dispatcher, Hash, List, Strings}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  use Ferricstore.Commands.ListTest.Sections.Part01

  defp app_path(path), do: Path.expand("../../../#{path}", __DIR__)

  defp list_cleanup_failure_store do
    type_key = CompoundKey.type_key("mylist")
    meta_key = CompoundKey.list_meta_key("mylist")
    element_key = CompoundKey.list_element("mylist", 0)
    {:ok, meta_deleted} = Agent.start_link(fn -> false end)

    %{
      compound_get: fn
        "mylist", ^type_key ->
          "list"

        "mylist", ^meta_key ->
          unless Agent.get(meta_deleted, & &1) do
            :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
          end

        "mylist", ^element_key ->
          "a"
      end,
      compound_batch_delete: fn "mylist", [^element_key] -> :ok end,
      compound_delete: fn
        "mylist", ^meta_key ->
          Agent.update(meta_deleted, fn _ -> true end)
          :ok

        "mylist", ^type_key ->
          {:error, :disk_full}
      end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms -> :ok end,
      compound_scan: fn "mylist", _prefix -> [{CompoundKey.encode_position(0), "a"}] end
    }
  end

  # ===========================================================================
  # LPUSH
  # ===========================================================================

  use Ferricstore.Commands.ListTest.Sections.Part02

  use Ferricstore.Commands.ListTest.Sections.Part03
end
