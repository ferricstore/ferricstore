defmodule Ferricstore.Store.RouterBypassForwardingTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router

  defmodule FakeBatcher do
    use GenServer

    def start_link(name, parent) do
      GenServer.start_link(__MODULE__, parent, name: name)
    end

    @impl true
    def init(parent), do: {:ok, parent}

    @impl true
    def handle_cast({:write_quorum, command, from}, parent) do
      send(parent, {:write_quorum, command, from})

      case from do
        {:remote_origin, _origin_node, {pid, ref}} ->
          send(pid, {ref, {:remote_applied_at, 123, :leader_result}})

        {pid, ref} ->
          send(pid, {ref, :leader_result})
      end

      {:noreply, parent}
    end
  end

  test "forwarded bypass commands preserve remote origin for local apply barrier" do
    shard_index = 20_000 + System.unique_integer([:positive])
    batcher_name = Batcher.batcher_name(shard_index)

    {:ok, pid} = FakeBatcher.start_link(batcher_name, self())

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    ctx = %{slot_map: List.duplicate(shard_index, 1024) |> List.to_tuple()}
    command = {:json_op, "JSON.SET", ["doc", "$", "1"]}
    origin_node = :origin@nohost

    assert {:remote_applied_at, 123, :leader_result} =
             apply(Router, :run_bypass_locally, [ctx, command, origin_node])

    assert_receive {:write_quorum, ^command, {:remote_origin, ^origin_node, {_pid, _ref}}}
  end
end
