defmodule Ferricstore.Flow.Query.IndexProviderTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.Query.RegistrySnapshot

  alias Ferricstore.Flow.Query.{
    IndexProvider,
    IndexSupervisor,
    CursorKeyStore,
    StatisticsStore,
    StatisticsWorker
  }

  test "serves validated ETS snapshots through the core provider contract" do
    suffix = System.unique_integer([:positive, :monotonic])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_query_provider_#{suffix}")

    {:ok, metadata_snapshot} =
      MetadataExtension.configure(FerricStore.Flow.MetadataExtension.Disabled, [])

    ctx = %{
      name: :"query_provider_instance_#{suffix}",
      data_dir: data_dir,
      shard_count: 1,
      slot_map: List.duplicate(0, 1_024) |> List.to_tuple(),
      query_index_provider: IndexProvider,
      flow_metadata_snapshot: metadata_snapshot
    }

    on_exit(fn -> File.rm_rf!(data_dir) end)

    {:ok, pid} = IndexSupervisor.start_link(instance_ctx: ctx)
    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: Supervisor.stop(pid) end)

    assert {:ok, %RegistrySnapshot{indexes: [_ | _]}} =
             FerricStore.Flow.QueryIndexProvider.snapshot(ctx, 0)

    assert {:ok, definitions} =
             FerricStore.Flow.QueryIndexProvider.projection_definitions(ctx, 0)

    assert definitions != []
    assert {:ok, []} = FerricStore.Flow.QueryIndexProvider.active_indexes(ctx, 0)

    assert is_pid(Process.whereis(StatisticsStore.server_name(ctx)))
    assert is_pid(Process.whereis(StatisticsWorker.server_name(ctx)))
    assert is_pid(Process.whereis(CursorKeyStore.server_name(ctx)))
    assert {:ok, key} = CursorKeyStore.key(ctx)
    assert byte_size(key) == 32
    assert StatisticsStore.size(ctx) == 0

    assert [child_spec] = IndexProvider.child_specs(ctx)
    assert child_spec.id == IndexSupervisor.child_id(ctx)
  end
end
