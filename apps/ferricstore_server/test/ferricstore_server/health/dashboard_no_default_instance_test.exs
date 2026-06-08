defmodule FerricstoreServer.Health.DashboardNoDefaultInstanceTest do
  @moduledoc false
  use ExUnit.Case, async: false
  @moduletag :dashboard

  alias FerricstoreServer.Health.Dashboard

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

  test "collect/0 returns a starting dashboard before a default instance exists" do
    data = Dashboard.collect()

    assert data.overview.status == :starting
    assert data.overview.total_commands == 0
    assert Enum.all?(data.shards, &(&1.status == "down"))
  end
end
