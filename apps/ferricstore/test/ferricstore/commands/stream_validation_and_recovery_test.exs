defmodule Ferricstore.Commands.StreamValidationAndRecoveryTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Stream
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Test.MockStore

  @integer_error {:error, "ERR value is not an integer or out of range"}
  @invalid_id {:error, "ERR Invalid stream ID specified as stream command argument"}

  setup do
    Stream.clear_local_state()
    on_exit(&Stream.clear_local_state/0)
    :ok
  end

  test "XTRIM rebuilds durable metadata after local stream state is cleared" do
    store = MockStore.make()
    key = unique_key("trim-rebuild")

    assert "1-0" == Stream.handle("XADD", [key, "1-0", "field", "one"], store)
    assert "2-0" == Stream.handle("XADD", [key, "2-0", "field", "two"], store)
    Stream.clear_local_state()

    assert 1 == Stream.handle("XTRIM", [key, "MAXLEN", "1"], store)
    assert [["2-0", "field", "two"]] == Stream.handle("XRANGE", [key, "-", "+"], store)
  end

  test "XREADGROUP propagates entry read failures instead of raising" do
    base = MockStore.make()
    key = unique_key("group-read-failure")

    assert "1-0" == Stream.handle("XADD", [key, "1-0", "field", "value"], base)
    assert :ok == Stream.handle("XGROUP", ["CREATE", key, "workers", "0"], base)

    store =
      Map.put(base, :compound_batch_get, fn ^key, _compound_keys ->
        [ReadResult.failure(:disk_error)]
      end)

    assert {:error, "ERR storage read failed"} ==
             Stream.handle(
               "XREADGROUP",
               ["GROUP", "workers", "consumer", "STREAMS", key, ">"],
               store
             )
  end

  test "XGROUP subcommand and MKSTREAM are case-insensitive but trailing options are strict" do
    store = MockStore.make()
    lower_key = unique_key("group-lower")
    invalid_key = unique_key("group-option")

    assert :ok ==
             Stream.handle("XGROUP", ["create", lower_key, "workers", "0", "mkstream"], store)

    assert {:error, "ERR syntax error"} ==
             Stream.handle(
               "XGROUP",
               ["CREATE", invalid_key, "workers", "0", "MKSTREAM", "BOGUS"],
               store
             )

    assert {:error, _message} = Stream.handle("XINFO", ["STREAM", invalid_key], store)
  end

  test "XGROUP rejects malformed start IDs before persisting a group" do
    store = MockStore.make()
    key = unique_key("group-id")

    assert "1-0" == Stream.handle("XADD", [key, "1-0", "field", "value"], store)
    assert @invalid_id == Stream.handle("XGROUP", ["CREATE", key, "workers", "not-an-id"], store)

    assert {:error, message} =
             Stream.handle(
               "XREADGROUP",
               ["GROUP", "workers", "consumer", "STREAMS", key, ">"],
               store
             )

    assert message =~ "NOGROUP"
  end

  test "XINFO subcommand is case-insensitive and rejects unsupported trailing arguments" do
    store = MockStore.make()
    key = unique_key("info-options")

    assert "1-0" == Stream.handle("XADD", [key, "1-0", "field", "value"], store)
    assert %{"length" => 1} = Stream.handle("XINFO", ["stream", key], store)
    assert {:error, "ERR syntax error"} == Stream.handle("XINFO", ["STREAM", key, "BOGUS"], store)
  end

  test "prepared XADD and XTRIM validate shapes before touching storage" do
    store = MockStore.make()
    key = unique_key("prepared-validation")

    assert {:error, _message} =
             Stream.handle_ast({:xadd, key, {:auto, ["field"], nil, false}}, store)

    assert @invalid_id ==
             Stream.handle_ast({:xadd, key, {:invalid, ["field", "value"], nil, false}}, store)

    assert @integer_error ==
             Stream.handle_ast(
               {:xadd, key, {:auto, ["field", "value"], {:maxlen, false, -1}, false}},
               store
             )

    assert 0 == Stream.handle("XLEN", [key], store)

    assert @integer_error ==
             Stream.handle_ast({:xtrim, key, {:maxlen, false, -1}}, store)
  end

  test "XREADGROUP rejects malformed pending start IDs without raising" do
    store = MockStore.make()
    key = unique_key("group-pending-id")

    assert :ok == Stream.handle("XGROUP", ["CREATE", key, "workers", "0", "MKSTREAM"], store)

    assert @invalid_id ==
             Stream.handle(
               "XREADGROUP",
               ["GROUP", "workers", "consumer", "STREAMS", key, "not-an-id"],
               store
             )
  end

  test "stream reads fail closed when a batch callback returns the wrong cardinality" do
    base = MockStore.make()
    key = unique_key("batch-cardinality")

    assert "1-0" == Stream.handle("XADD", [key, "1-0", "field", "one"], base)
    assert "2-0" == Stream.handle("XADD", [key, "2-0", "field", "two"], base)

    short_store = Map.put(base, :compound_batch_get, fn ^key, _compound_keys -> [nil] end)
    invalid_store = Map.put(base, :compound_batch_get, fn ^key, _compound_keys -> :invalid end)

    assert {:error, "ERR storage read failed"} ==
             Stream.handle("XRANGE", [key, "-", "+"], short_store)

    assert {:error, "ERR storage read failed"} ==
             Stream.handle("XRANGE", [key, "-", "+"], invalid_store)
  end

  defp unique_key(prefix), do: "#{prefix}:#{System.unique_integer([:positive, :monotonic])}"
end
