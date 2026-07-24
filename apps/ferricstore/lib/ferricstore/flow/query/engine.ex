defmodule Ferricstore.Flow.Query.Engine do
  @moduledoc """
  Executes already parsed and bound canonical Flow queries.

  Only proven physical paths are accepted. The initial FQL1 slice delegates a
  partition-key/run-id equality query directly to the existing point-record
  lookup and never introduces a scan or result materialization step.
  """

  alias Ferricstore.Flow.Query.{
    ExecutionContext,
    FixedIndexExecutor,
    Limits,
    MandatoryScope,
    Planner,
    RecordProjection,
    Request,
    Shape,
    Surface
  }

  @spec execute(FerricStore.Instance.t() | map(), Request.t()) ::
          {:ok, term()} | {:error, term()}
  def execute(_ctx, %Request{source: :events, mode: :explain} = request) do
    with {:ok, _partition_key, _id, direction, _limit} <- history_values(request) do
      {:ok, history_explain(request, direction)}
    end
  end

  def execute(ctx, %Request{source: :events, mode: :execute} = request) do
    with {:ok, partition_key, id, direction, limit} <- history_values(request) do
      ctx
      |> ExecutionContext.instance_ctx()
      |> Ferricstore.Flow.HistoryAPI.history(id, history_opts(partition_key, direction, limit))
      |> normalize_history_result()
      |> project_history_result(request.projection)
    end
  end

  def execute(_ctx, %Request{source: :runs, mode: :explain} = request) do
    case point_values(request) do
      {:ok, _partition_key, _id, kind} -> {:ok, explain(request, kind)}
      {:error, :unsupported_query_shape} -> FixedIndexExecutor.execute(%{}, request)
    end
  end

  def execute(ctx, %Request{source: :runs, mode: :execute} = request) do
    case point_values(request) do
      {:ok, partition_key, id, _kind} ->
        ctx
        |> ExecutionContext.instance_ctx()
        |> Ferricstore.Flow.get(id, partition_key: partition_key)
        |> normalize_point_result()
        |> verify_point_result(partition_key, id)
        |> then(&RecordProjection.project_result(&1, :runs, request.projection))

      {:error, :unsupported_query_shape} ->
        ctx
        |> ExecutionContext.instance_ctx()
        |> FixedIndexExecutor.execute(request)
    end
  end

  def execute(_ctx, %Request{}), do: {:error, :unsupported_query_shape}

  @doc false
  @spec execute_resolved(map(), Request.t(), MandatoryScope.t()) ::
          {:ok, term()} | {:error, term()}
  def execute_resolved(
        _ctx,
        %Request{source: :events, mode: :explain} = request,
        %MandatoryScope{} = scope
      ) do
    with :ok <- MandatoryScope.validate(scope),
         {:ok, _partition_key, _id, direction, _limit} <- history_values(request) do
      {:ok, history_explain(request, direction)}
    end
  end

  def execute_resolved(
        ctx,
        %Request{source: :events, mode: :execute} = request,
        %MandatoryScope{} = scope
      )
      when is_map(ctx) do
    with :ok <- MandatoryScope.validate(scope),
         {:ok, partition_key, id, direction, limit} <- history_values(request),
         {:ok, metadata} <- MandatoryScope.single_metadata(scope) do
      ctx
      |> Ferricstore.Flow.HistoryAPI.history_resolved(
        id,
        history_opts(partition_key, direction, limit),
        metadata
      )
      |> normalize_history_result()
      |> project_history_result(request.projection)
    end
  end

  def execute_resolved(
        _ctx,
        %Request{source: :runs, mode: :explain} = request,
        %MandatoryScope{} = scope
      ) do
    with :ok <- MandatoryScope.validate(scope),
         {:ok, _partition_key, _id, kind} <- point_values(request) do
      {:ok, explain(request, kind)}
    end
  end

  def execute_resolved(
        ctx,
        %Request{source: :runs, mode: :execute} = request,
        %MandatoryScope{} = scope
      )
      when is_map(ctx) do
    with :ok <- MandatoryScope.validate(scope),
         {:ok, partition_key, id, _kind} <- point_values(request),
         {:ok, metadata} <- MandatoryScope.single_metadata(scope) do
      ctx
      |> Ferricstore.Flow.get_resolved(id, [partition_key: partition_key], metadata)
      |> normalize_point_result()
      |> verify_point_result(partition_key, id)
      |> then(&RecordProjection.project_result(&1, :runs, request.projection))
    end
  end

  def execute_resolved(_ctx, %Request{}, %MandatoryScope{}),
    do: {:error, :unsupported_query_shape}

  def execute_resolved(_ctx, _request, _scope), do: {:error, :unsupported_query_shape}

  @doc false
  @spec execute_history_page_resolved(
          map(),
          Request.t(),
          MandatoryScope.t(),
          binary() | nil
        ) :: {:ok, map()} | {:error, atom()}
  def execute_history_page_resolved(
        ctx,
        %Request{source: :events} = request,
        %MandatoryScope{} = scope,
        before_event
      )
      when is_map(ctx) and (is_nil(before_event) or is_binary(before_event)) do
    with :ok <- Request.validate_bound(request),
         :ok <- MandatoryScope.validate(scope),
         {:ok, partition_key, id, direction, limit} <- history_page_values(request),
         {:ok, metadata} <- MandatoryScope.single_metadata(scope),
         {:ok, page} <-
           Ferricstore.Flow.HistoryAPI.history_page_resolved(
             ctx,
             id,
             partition_key,
             metadata,
             limit,
             before_event,
             direction
           ),
         {:ok, records} <- project_history_events(page.events) do
      memory_high_water_bytes =
        max(
          page.memory_high_water_bytes,
          Ferricstore.TermMemory.bytes({page.events, records})
        )

      {:ok,
       %{
         records: records,
         has_more: page.has_more,
         continuation: page.continuation,
         scanned_entries: page.scanned_entries,
         hydrated_records: page.hydrated_records,
         duplicate_entries: page.duplicate_entries,
         memory_high_water_bytes: memory_high_water_bytes
       }}
    else
      {:error, reason} when is_binary(reason) -> normalize_history_result({:error, reason})
      {:error, _reason} = error -> error
    end
  end

  def execute_history_page_resolved(_ctx, _request, _scope, _before_event),
    do: {:error, :unsupported_query_shape}

  @doc false
  @spec execute_lineage_page_resolved(
          map(),
          Request.t(),
          MandatoryScope.t(),
          {non_neg_integer(), binary()} | nil
        ) :: {:ok, map()} | {:error, atom()}
  def execute_lineage_page_resolved(
        ctx,
        %Request{source: :runs} = request,
        %MandatoryScope{} = scope,
        boundary
      )
      when is_map(ctx) do
    Ferricstore.Flow.Query.LineageRead.read_page(ctx, request, scope, boundary)
  end

  def execute_lineage_page_resolved(_ctx, _request, _scope, _boundary),
    do: {:error, :unsupported_query_shape}

  defp point_values(%Request{} = request) do
    with {:ok, descriptor} <- Shape.point_descriptor(request),
         :ok <- validate_point_value_size(descriptor.partition_key, descriptor.run_id) do
      kind = if descriptor.partitioning == :auto, do: :auto_partition, else: :explicit_partition
      {:ok, descriptor.partition_key, descriptor.run_id, kind}
    end
  end

  defp history_values(%Request{cursor: nil} = request) do
    with {:ok, descriptor} <- Shape.history_descriptor(request) do
      {:ok, descriptor.partition_key, descriptor.run_id, descriptor.direction, descriptor.limit}
    end
  end

  defp history_values(%Request{}), do: {:error, :unsupported_query_shape}

  defp history_page_values(%Request{} = request) do
    with {:ok, descriptor} <- Shape.history_descriptor(request) do
      {:ok, descriptor.partition_key, descriptor.run_id, descriptor.direction, descriptor.limit}
    end
  end

  defp history_opts(partition_key, direction, limit) do
    [
      partition_key: partition_key,
      count: limit,
      rev: direction == :desc,
      include_cold: true,
      consistent_projection: true
    ]
  end

  defp normalize_history_result({:error, "ERR storage read failed"}),
    do: {:error, :query_storage_unavailable}

  defp normalize_history_result({:error, "ERR flow history projection unavailable"}),
    do: {:error, :query_storage_unavailable}

  defp normalize_history_result({:error, _reason}), do: {:error, :query_storage_inconsistent}
  defp normalize_history_result(result), do: result

  defp project_history_result({:ok, events}, :all) when is_list(events),
    do: project_history_events(events)

  defp project_history_result({:ok, events}, projection) when is_list(events) do
    with {:ok, records} <- project_history_events(events) do
      RecordProjection.project_records(records, :events, projection)
    end
  end

  defp project_history_result({:error, _reason} = error, _projection), do: error
  defp project_history_result(_invalid, _projection), do: {:error, :query_storage_inconsistent}

  defp project_history_events(events) when is_list(events) do
    events
    |> Enum.reduce_while({:ok, []}, fn
      {event_id, fields}, {:ok, acc} when is_binary(event_id) and is_map(fields) ->
        {:cont, {:ok, [%{event_id: event_id, fields: fields} | acc]}}

      _invalid, _acc ->
        {:halt, {:error, :query_storage_inconsistent}}
    end)
    |> case do
      {:ok, projected} -> {:ok, Enum.reverse(projected)}
      {:error, _reason} = error -> error
    end
  end

  defp validate_point_value_size(partition_key, id) do
    if Limits.valid_partition_key?(partition_key) and Limits.valid_run_id?(id) do
      :ok
    else
      {:error, :query_value_too_large}
    end
  end

  defp normalize_point_result({:error, "ERR storage read failed"}),
    do: {:error, :query_storage_unavailable}

  defp normalize_point_result({:error, "ERR invalid flow record"}),
    do: {:error, :query_storage_inconsistent}

  defp normalize_point_result(result), do: result

  defp verify_point_result(
         {:ok, %{partition_key: partition_key, id: id}} = result,
         partition_key,
         id
       ),
       do: result

  defp verify_point_result({:ok, nil} = result, _partition_key, _id), do: result

  defp verify_point_result({:ok, _record}, _partition_key, _id),
    do: {:error, :query_storage_inconsistent}

  defp verify_point_result(result, _partition_key, _id), do: result

  defp explain(%Request{} = request, kind) do
    ranges =
      case kind do
        :auto_partition ->
          [
            %{
              field: "run_id",
              operator: "eq",
              value: %{type: "keyword", redacted: true}
            }
          ]

        :explicit_partition ->
          [
            %{
              field: "partition_key",
              operator: "eq",
              value: %{type: "keyword", redacted: true}
            },
            %{
              field: "run_id",
              operator: "eq",
              value: %{type: "keyword", redacted: true}
            }
          ]
      end

    %{
      version: Surface.default_explain_contract(),
      query_fingerprint: Planner.query_fingerprint(request),
      status: "planned",
      capabilities: %{
        requested: [],
        available: ["flow_query_point_v1"],
        missing: []
      },
      plan: %{
        path: "primary_key",
        index: "flow_runs_primary_v1",
        fallback_reason: "none",
        projection: projection_descriptor(request),
        ranges: ranges
      },
      estimate: %{
        scan_records: 1,
        result_records: 1
      },
      bounds: %{scan_records: 1, result_records: 1, groups: 0}
    }
  end

  defp history_explain(%Request{} = request, direction) do
    %{
      version: Surface.default_explain_contract(),
      query_fingerprint: Planner.query_fingerprint(request),
      status: "planned",
      capabilities: %{
        requested: [],
        available: ["flow_query_history_v1"],
        missing: []
      },
      plan: %{
        path: "history",
        index: "flow_events_history_v1",
        fallback_reason: "none",
        projection: projection_descriptor(request),
        ranges: [
          %{
            field: "run_id",
            operator: "eq",
            value: %{type: "keyword", redacted: true}
          }
        ],
        order: %{field: "event_id", direction: Atom.to_string(direction)}
      },
      estimate: %{scan_records: "bounded_by_limit", result_records: "bounded_by_limit"},
      bounds: %{
        scan_records: Limits.max_results(),
        result_records: Limits.max_results(),
        groups: 0
      }
    }
  end

  defp projection_descriptor(%Request{projection: projection}) do
    fields =
      case RecordProjection.external_names(projection) do
        :all -> "all_allowlisted_fields"
        names -> names
      end

    %{
      fields: fields,
      application: "after_authoritative_recheck",
      index_only: false
    }
  end
end
