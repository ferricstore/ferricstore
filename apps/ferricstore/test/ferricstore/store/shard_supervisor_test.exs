defmodule Ferricstore.Store.ShardSupervisorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Store.ShardSupervisor

  @moduledoc """
  Tests the ShardSupervisor by inspecting the supervisor that the application
  starts automatically. Since the application tree boots the ShardSupervisor
  with `name: Ferricstore.Store.ShardSupervisor`, we query it directly rather
  than starting a second instance (which would conflict on registered names).
  """

  setup do
    Ferricstore.Test.ShardHelpers.wait_shards_alive()
    Ferricstore.Test.ShardHelpers.wait_default_pipeline_ready()
    :ok
  end

  test "starts 4 shards by default" do
    sup = Process.whereis(ShardSupervisor)
    assert is_pid(sup)

    shards =
      sup
      |> Supervisor.which_children()
      |> Enum.filter(fn {id, _, _, _} ->
        is_atom(id) and String.starts_with?(Atom.to_string(id), "shard_")
      end)

    assert length(shards) == 4
  end

  test "each shard process is alive" do
    sup = Process.whereis(ShardSupervisor)
    children = Supervisor.which_children(sup)

    for {_, pid, _, _} <- children do
      assert Process.alive?(pid)
    end
  end

  test "shards have unique registered names" do
    names = Enum.map(0..3, fn i -> Router.shard_name(FerricStore.Instance.get(:default), i) end)
    assert length(Enum.uniq(names)) == 4

    for name <- names do
      assert is_pid(Process.whereis(name))
    end
  end

  test "each shard responds to put and get" do
    for i <- 0..3 do
      name = Router.shard_name(FerricStore.Instance.get(:default), i)
      key = "sup_test_key_#{i}_#{:rand.uniform(999_999)}"
      :ok = GenServer.call(name, {:put, key, "val_#{i}", 0})
      assert "val_#{i}" == GenServer.call(name, {:get, key})
      # Clean up
      GenServer.call(name, {:delete, key})
    end
  end
end
