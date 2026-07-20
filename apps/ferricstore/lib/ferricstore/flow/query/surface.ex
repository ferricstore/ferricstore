defmodule Ferricstore.Flow.Query.Surface do
  @moduledoc false

  @language_versions ["FQL1"]
  @default_shapes [
    "runs_by_run_id_record",
    "runs_by_partition_and_run_id_record",
    "events_by_run_id_ordered_records"
  ]
  @parser_shapes [
    "runs_by_run_id_record",
    "runs_by_partition_and_run_id_record",
    "runs_by_partition_predicates_ordered_records",
    "runs_by_partition_parent_ordered_records",
    "runs_by_partition_root_ordered_records",
    "runs_by_partition_correlation_ordered_records",
    "runs_by_partition_predicates_count",
    "events_by_run_id_ordered_records"
  ]

  @spec language_versions() :: [binary()]
  def language_versions, do: @language_versions

  @spec shapes() :: [binary()]
  def shapes, do: @parser_shapes

  @spec supported_version?(term()) :: boolean()
  def supported_version?(version), do: version in @language_versions

  @spec supported_language_versions?(term()) :: boolean()
  def supported_language_versions?(versions) when is_list(versions),
    do: Enum.all?(versions, &(&1 in @language_versions))

  def supported_language_versions?(_versions), do: false

  @spec supported_shapes?(term()) :: boolean()
  def supported_shapes?(shapes) when is_list(shapes),
    do: Enum.all?(shapes, &(&1 in @parser_shapes))

  def supported_shapes?(_shapes), do: false

  @spec default_capability_manifest() :: FerricStore.Flow.QueryEngine.capability_manifest()
  def default_capability_manifest do
    %{
      query_contract: "ferric.flow.query/v1",
      explain_contract: "ferric.flow.explain/v1",
      capabilities: ["flow_query_point_v1", "flow_query_history_v1"],
      language_versions: @language_versions,
      shapes: @default_shapes
    }
  end
end
