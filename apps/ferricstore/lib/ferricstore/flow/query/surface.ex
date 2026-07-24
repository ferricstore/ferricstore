defmodule Ferricstore.Flow.Query.Surface do
  @moduledoc false

  alias Ferricstore.Flow.Query.Shape

  @language_versions ["FQL1"]
  @request_contract "ferric.flow.query.request/v1"
  @default_result_contract "ferric.flow.query.result/v1"
  @default_explain_contract "ferric.flow.explain/v1"
  @index_status_contract "ferric.flow.query.indexes/v1"
  @spec language_versions() :: [binary()]
  def language_versions, do: @language_versions

  @spec shapes() :: [binary()]
  defdelegate shapes, to: Shape, as: :known_names

  @spec supported_version?(term()) :: boolean()
  def supported_version?(version), do: version in @language_versions

  @spec default_explain_contract() :: binary()
  def default_explain_contract, do: @default_explain_contract

  @spec request_contract() :: binary()
  def request_contract, do: @request_contract

  @spec default_result_contract() :: binary()
  def default_result_contract, do: @default_result_contract

  @spec index_status_contract() :: binary()
  def index_status_contract, do: @index_status_contract

  @spec supported_language_versions?(term()) :: boolean()
  def supported_language_versions?(versions) when is_list(versions),
    do: Enum.all?(versions, &(&1 in @language_versions))

  def supported_language_versions?(_versions), do: false

  @spec supported_shapes?(term()) :: boolean()
  defdelegate supported_shapes?(shapes), to: Shape, as: :known_names?

  @spec default_capability_manifest() :: FerricStore.Flow.QueryEngine.capability_manifest()
  def default_capability_manifest do
    %{
      request_contract: @request_contract,
      result_contract: @default_result_contract,
      explain_contract: @default_explain_contract,
      index_status_contract: @index_status_contract,
      capabilities: [
        "flow_query_v1",
        "flow_query_result_projection_v1",
        "flow_explain_v1",
        "flow_explain_analyze_v1",
        "flow_composite_index_v1",
        "flow_query_index_status_v1"
      ],
      language_versions: @language_versions,
      shapes: Shape.execution_names()
    }
  end
end
