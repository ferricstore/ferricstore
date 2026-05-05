defmodule Ferricstore.BatchWorkerNoDefaultInstanceTest do
  @moduledoc false
  use ExUnit.Case, async: false

  @default_key {FerricStore.Instance, :default}

  setup do
    original = :persistent_term.get(@default_key, :missing)
    :persistent_term.erase(@default_key)

    on_exit(fn ->
      case original do
        :missing -> :persistent_term.erase(@default_key)
        ctx -> :persistent_term.put(@default_key, ctx)
      end
    end)

    :ok
  end

  test "start returns a bounded error before default instance init" do
    assert {:error, :instance_not_initialized} = FerricStore.BatchWorker.start()
  end
end
