defmodule Ferricstore.Flow.Query.Plan do
  @moduledoc false

  alias Ferricstore.Flow.Query.{CompositeRange, IndexDefinition, MandatoryScope}
  alias Ferricstore.Flow.Query.Budget

  @derive {Inspect,
           only: [
             :version,
             :path,
             :index_id,
             :index_version,
             :deduplicate,
             :order,
             :estimate,
             :stats,
             :fallback_reason,
             :query_fingerprint
           ]}
  @enforce_keys [
    :path,
    :ranges,
    :order,
    :residual_predicates,
    :recheck_predicates,
    :estimate,
    :stats,
    :budget,
    :fallback_reason,
    :query_fingerprint,
    :mandatory_scope
  ]
  defstruct version: 1,
            path: nil,
            index_id: nil,
            index_version: nil,
            index_build_id: nil,
            definition: nil,
            ranges: [],
            deduplicate: true,
            order: :none,
            residual_predicates: [],
            recheck_predicates: [],
            constraint_shapes: [],
            estimate: %{},
            stats: %{},
            budget: nil,
            fallback_reason: :none,
            query_fingerprint: nil,
            mandatory_scope: nil,
            alternatives: [],
            statistics_probes: []

  @type path ::
          :primary_key
          | :history
          | :lineage
          | :counter_lookup
          | :count_scan
          | :empty
          | :fixed_index
          | :ordered_range
          | :ordered_range_union
          | :ordered_filter
          | :reject
  @type t :: %__MODULE__{
          version: 1,
          path: path(),
          index_id: binary() | nil,
          index_version: pos_integer() | nil,
          index_build_id: binary() | nil,
          definition: IndexDefinition.t() | nil,
          ranges: [CompositeRange.t()],
          deduplicate: boolean(),
          order: :none | :native | :bounded_top_k,
          residual_predicates: [term()],
          recheck_predicates: [term()],
          constraint_shapes: [map()],
          estimate: map(),
          stats: map(),
          budget: Budget.t(),
          fallback_reason: atom(),
          query_fingerprint: binary(),
          mandatory_scope: MandatoryScope.t(),
          alternatives: [map()],
          statistics_probes: [
            %{
              definition: IndexDefinition.t(),
              equality_values: [term()],
              range: CompositeRange.t(),
              scope_prefix: [term()],
              physical_partition_key: binary(),
              statistics_key: binary(),
              scope_digest: <<_::256>>,
              prefix_digest: <<_::256>>
            }
          ]
        }
end
