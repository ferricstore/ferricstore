defmodule Ferricstore.Raft.WARaftPeerContextOwnerTest do
  use ExUnit.Case, async: false
  alias Ferricstore.Test.WARaftPeerContextOwner

  test "a transient requester does not own the peer's publication latches" do
    name = :"peer_context_owner_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "peer-context-owner-#{System.pid()}-#{name}")
    on_exit(fn -> File.rm_rf!(root) end)
    supervisor = start_supervised!({DynamicSupervisor, strategy: :one_for_one})
    parent = self()

    {requester, monitor} =
      spawn_monitor(fn ->
        {:ok, owner} =
          DynamicSupervisor.start_child(
            supervisor,
            {WARaftPeerContextOwner,
             instance_name: name, instance_opts: [data_dir: root, shard_count: 1]}
          )

        ctx = GenServer.call(owner, :context)
        send(parent, {:peer_context, owner, ctx})
      end)

    assert_receive {:peer_context, owner, ctx}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^requester, :normal}, 5_000
    assert :ets.info(elem(ctx.latch_refs, 0), :owner) == owner

    assert Ferricstore.Store.PromotedPublication.with_lifecycle(
             %{instance_ctx: ctx, index: 0},
             fn -> %{recovered: true} end
           ) == %{recovered: true}

    assert Ferricstore.Store.PromotedPublication.read(ctx, 0, fn -> :ready end) == :ready
    assert :ok = DynamicSupervisor.terminate_child(supervisor, owner)
    assert :ets.info(elem(ctx.latch_refs, 0)) == :undefined
  end
end
