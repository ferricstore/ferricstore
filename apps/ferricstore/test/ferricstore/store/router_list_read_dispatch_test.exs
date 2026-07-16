defmodule Ferricstore.Store.RouterListReadDispatchTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.{Router, SlotMap}

  defmodule Probe do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call(request, _from, owner) do
      send(owner, {:shard_request, request})
      {:reply, {:probe_reply, request}, owner}
    end
  end

  setup do
    probe = start_supervised!({Probe, self()})

    ctx = %FerricStore.Instance{
      name: :router_list_read_probe,
      shard_count: 1,
      shard_names: {probe},
      slot_map: List.duplicate(0, SlotMap.num_slots()) |> List.to_tuple()
    }

    {:ok, ctx: ctx}
  end

  test "list reads use the shard read path", %{ctx: ctx} do
    key = "list"

    for operation <- [
          :llen,
          {:lrange, 0, -1},
          {:lindex, 0},
          {:lpos, "value", 1, nil, 0}
        ] do
      assert {:probe_reply, {:list_read, ^key, ^operation}} = Router.list_op(ctx, key, operation)
      assert_receive {:shard_request, {:list_read, ^key, ^operation}}
    end
  end

  test "list mutations remain on the shard write path", %{ctx: ctx} do
    operation = {:lset, 0, "updated"}

    assert {:probe_reply, {:list_op, "list", ^operation}} =
             Router.list_op(ctx, "list", operation)

    assert_receive {:shard_request, {:list_op, "list", ^operation}}
  end
end
