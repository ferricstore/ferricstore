defmodule Ferricstore.Commands.StreamFlushLocalStateTest do
  @moduledoc false
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.{Server, Stream}
  alias Ferricstore.Test.{MockStore, ShardHelpers}

  setup_all do
    assert :ok = ShardHelpers.flush_all_keys()
    :ok
  end

  setup do
    clear_stream_tables()
    on_exit(&clear_stream_tables/0)
    :ok
  end

  test "FLUSHDB clears stream local metadata, index, and waiters" do
    store = MockStore.make()
    key = "stream_flush_#{System.unique_integer([:positive])}"

    assert "1-0" == Stream.handle("XADD", [key, "1-0", "f", "old"], store)
    assert [["1-0", "f", "old"]] == Stream.handle("XRANGE", [key, "-", "+"], store)
    Stream.register_stream_waiter(key, self(), "1-0")

    assert stream_table_size(Ferricstore.Stream.Meta) > 0
    assert stream_table_size(Ferricstore.Stream.Index) > 0
    assert stream_table_size(:ferricstore_stream_waiters) > 0

    assert :ok = Server.handle("FLUSHDB", [], store)

    assert stream_table_size(Ferricstore.Stream.Meta) == 0
    assert stream_table_size(Ferricstore.Stream.Groups) == 0
    assert stream_table_size(Ferricstore.Stream.Index) == 0
    assert stream_table_size(:ferricstore_stream_waiters) == 0
    assert_receive {:stream_waiter_notify, ^key}
  end

  test "embedded flushdb clears scoped stream state and notifies waiters" do
    ctx = FerricStore.Instance.get(:default)
    key = "stream_embedded_flush_#{System.unique_integer([:positive])}"

    assert {:ok, _id} = FerricStore.xadd(key, ["field", "value"])
    assert :ok = Stream.register_stream_waiter(key, self(), "0-0", ctx)
    assert Stream.stream_waiter_count(key, ctx) == 1

    assert :ok = FerricStore.flushdb()

    assert Stream.stream_waiter_count(key, ctx) == 0
    assert_receive {:stream_waiter_notify, ^key}
    assert :ets.lookup(Ferricstore.Stream.Meta, {ctx.name, key}) == []
  end

  defp stream_table_size(table) do
    case :ets.whereis(table) do
      :undefined -> 0
      _ref -> :ets.info(table, :size)
    end
  end

  defp clear_stream_tables do
    for table <- [
          Ferricstore.Stream.Meta,
          Ferricstore.Stream.Groups,
          Ferricstore.Stream.Index,
          :ferricstore_stream_waiters
        ] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end
  end
end
