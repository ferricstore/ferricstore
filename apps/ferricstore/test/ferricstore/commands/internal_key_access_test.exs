defmodule Ferricstore.Commands.InternalKeyAccessTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.MockStore

  @error "ERR access to internal Flow keys is not allowed"
  @state_key "f:{f}:s:flow-1"
  @history_entry "X:f:{f}:h:flow-1\0" <> "123-4"

  test "raw generic reads cannot inspect canonical Flow keys" do
    store = MockStore.make(%{@state_key => {"secret", 0}, @history_entry => {"event", 0}})

    assert {:error, @error} = Dispatcher.dispatch("GET", [@state_key], store)
    assert {:error, @error} = Dispatcher.dispatch("MGET", ["ordinary", @state_key], store)
    assert {:error, @error} = Dispatcher.dispatch("GET", [@history_entry], store)
  end

  test "raw generic writes cannot alter or delete canonical Flow keys" do
    store = MockStore.make(%{@state_key => {"original", 0}})

    assert {:error, @error} = Dispatcher.dispatch("SET", [@state_key, "forged"], store)
    assert {:error, @error} = Dispatcher.dispatch("DEL", [@state_key], store)
    assert "original" == store.get.(@state_key)
  end

  test "multi-key writes reject the whole request before ordinary keys change" do
    store = MockStore.make()

    assert {:error, @error} =
             Dispatcher.dispatch("MSET", ["ordinary", "value", @state_key, "forged"], store)

    assert nil == store.get.("ordinary")
    assert nil == store.get.(@state_key)
  end

  test "rename protects both the source and destination" do
    store = MockStore.make(%{"ordinary" => {"value", 0}, @state_key => {"secret", 0}})

    assert {:error, @error} = Dispatcher.dispatch("RENAME", [@state_key, "other"], store)
    assert {:error, @error} = Dispatcher.dispatch("RENAME", ["ordinary", @state_key], store)
    assert "value" == store.get.("ordinary")
    assert "secret" == store.get.(@state_key)
  end

  test "data-structure commands cannot synthesize compound entries under Flow keys" do
    store = MockStore.make()

    assert {:error, @error} = Dispatcher.dispatch("HSET", [@state_key, "field", "forged"], store)
    assert nil == store.compound_get.(@state_key, "T:" <> @state_key)
  end

  test "direct access to physical compound keys is rejected" do
    store = MockStore.make()
    physical_key = "H:ordinary\0field"

    assert {:error, @error} = Dispatcher.dispatch("SET", [physical_key, "forged"], store)
    assert {:error, @error} = Dispatcher.dispatch("GET", [physical_key], store)
    assert nil == store.get.(physical_key)
  end

  test "generic metadata commands cannot probe Flow keys" do
    store = MockStore.make(%{@state_key => {"secret", 0}})

    assert {:error, @error} = Dispatcher.dispatch("EXISTS", [@state_key], store)
    assert {:error, @error} = Dispatcher.dispatch("TYPE", [@state_key], store)
    assert {:error, @error} = Dispatcher.dispatch("TTL", [@state_key], store)
  end

  test "generic key enumeration hides Flow state and history keys" do
    store =
      MockStore.make(%{
        "ordinary" => {"value", 0},
        @state_key => {"secret", 0},
        @history_entry => {"event", 0}
      })

    assert ["ordinary"] == Dispatcher.dispatch("KEYS", ["*"], store)
    assert 1 == Dispatcher.dispatch("DBSIZE", [], store)
    assert "ordinary" == Dispatcher.dispatch("RANDOMKEY", [], store)
    assert ["0", ["ordinary"]] == Dispatcher.dispatch("SCAN", ["0", "COUNT", "100"], store)
  end

  test "ordinary generic access remains available" do
    store = MockStore.make()

    assert :ok = Dispatcher.dispatch("SET", ["ordinary", "value"], store)
    assert "value" == Dispatcher.dispatch("GET", ["ordinary"], store)
  end
end
