defmodule Ferricstore.Flow.PolicyMirrorWriterRestartTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.{Keys, LMDB, LMDBWriter, PolicyAttributeCatalog}
  alias Ferricstore.Store.Router

  @moduletag :global_state
  @moduletag :shard_kill
  @moduletag timeout: 180_000

  test "restarted LMDB writer removes a policy member deleted while the mirror was unavailable" do
    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    type = unique_flow_id("policy-delete-recovery")
    name = "owner"
    ctx = FerricStore.Instance.get(:default)
    member_key = Keys.policy_indexed_attribute_member_key(name, type)
    member_prefix = Keys.policy_indexed_attribute_member_prefix(name)
    shard_index = 0

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, indexed_attributes: [name])
    assert :ok = LMDBWriter.flush(ctx.name, shard_index)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> LMDB.path()

    assert {:ok, [_member]} = LMDB.prefix_entries(lmdb_path, member_prefix, 1)

    writer_name = LMDBWriter.name(ctx.name, shard_index)
    writer = Process.whereis(writer_name)
    assert is_pid(writer)
    writer_ref = Process.monitor(writer)
    assert :ok = LMDBWriter.suspend(ctx.name, shard_index)

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, indexed_attributes: [])
    assert Router.get(ctx, member_key) == nil
    assert {:ok, [_member]} = LMDB.prefix_entries(lmdb_path, member_prefix, 1)

    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^writer_ref, :process, ^writer, :killed}, 5_000

    ShardHelpers.eventually(
      fn ->
        case Process.whereis(writer_name) do
          pid when is_pid(pid) -> pid != writer
          _missing -> false
        end
      end,
      "LMDB writer should restart",
      100,
      100
    )

    assert :ok = LMDBWriter.resume_all(ctx.name, ctx.shard_count)
    assert :ok = LMDBWriter.flush(ctx.name, shard_index, 60_000)

    assert {:ok, []} = LMDB.prefix_entries(lmdb_path, member_prefix, 1)
    refute PolicyAttributeCatalog.indexed_member_exists?(ctx, name)
  end
end
