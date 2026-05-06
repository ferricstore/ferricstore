defmodule Ferricstore.CrossShardOpDirectStoreTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Generic
  alias Ferricstore.Test.MockStore

  @default_instance_key {FerricStore.Instance, :default}

  test "direct store callers do not need a default instance" do
    previous = :persistent_term.get(@default_instance_key, :missing)
    :persistent_term.erase(@default_instance_key)

    on_exit(fn ->
      case previous do
        :missing -> :persistent_term.erase(@default_instance_key)
        ctx -> :persistent_term.put(@default_instance_key, ctx)
      end
    end)

    store = MockStore.make(%{"src" => {"v", 0}})

    assert 1 == Generic.handle("COPY", ["src", "dst"], store)
    assert "v" == store.get.("dst")
  end
end
