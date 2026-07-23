defmodule Ferricstore.Bench.FQLPlannerContext do
  @moduledoc false

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.Query.AdmissionController
  alias Ferricstore.Store.SlotMap

  @instance_name :fql_parser_benchmark
  @admission_name __MODULE__.Admission

  def start! do
    {:ok, metadata_snapshot} =
      MetadataExtension.configure(MetadataExtension.Disabled, [])

    {:ok, admission} =
      AdmissionController.start_link(
        name: @admission_name,
        max_scope: 1,
        max_node: 1,
        index_active_fun: fn _instance, _identity -> {:ok, true} end
      )

    ctx = %{
      name: @instance_name,
      shard_count: 1,
      slot_map: SlotMap.build_uniform(1),
      flow_metadata_snapshot: metadata_snapshot,
      query_admission_controller: admission,
      query_index_provider: FerricStore.Flow.QueryIndexProvider.Disabled
    }

    {ctx, admission}
  end
end
