defmodule Ferricstore.HealthNoDefaultInstanceTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Health

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

  test "check/0 reports starting before a default instance is registered" do
    result = Health.check()

    assert result.status == :starting
    assert result.shard_count == Application.get_env(:ferricstore, :shard_count, 4)
    assert Enum.all?(result.shards, &(&1.status == "down"))
  end
end
