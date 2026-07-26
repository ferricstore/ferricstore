defmodule Ferricstore.Flow.Query.Telemetry do
  @moduledoc false

  alias Ferricstore.Flow.Query.{CoveringProjection, Plan, Request, Usage}

  @event [:ferricstore, :flow, :query, :stop]

  @spec observe(Request.t(), Plan.t() | nil, integer(), term(), keyword()) :: term()
  def observe(request, plan, started_us, result, opts \\ [])

  def observe(%Request{} = request, plan, started_us, result, opts)
      when (is_struct(plan, Plan) or is_nil(plan)) and is_integer(started_us) and is_list(opts) do
    now_us = Keyword.get_lazy(opts, :now_us, &monotonic_us/0)
    usage = result_usage(result)

    measurements =
      Usage.fields()
      |> Map.new(fn field -> {field, measurement(usage, field)} end)
      |> Map.put(:duration_us, duration_us(started_us, now_us))

    metadata =
      Map.merge(
        %{
          status: result_status(result),
          reason: result_reason(result),
          mode: bounded_enum(request.mode, [:execute, :explain, :analyze]),
          source: bounded_enum(request.source, [:runs, :events]),
          return: bounded_enum(request.return, [:record, :count])
        },
        plan_metadata(request, plan, usage)
      )

    :telemetry.execute(@event, measurements, metadata)
    result
  rescue
    _error -> result
  catch
    _kind, _reason -> result
  end

  def observe(_request, _plan, _started_us, result, _opts), do: result

  defp result_usage({:ok, %{usage: usage}}) when is_map(usage), do: usage
  defp result_usage(_result), do: nil

  defp result_status({:ok, _result}), do: :ok
  defp result_status(_result), do: :error

  defp result_reason({:error, reason}) when is_atom(reason), do: reason

  defp result_reason({:error, %{__struct__: Ferricstore.Flow.Query.Error, reason: reason}})
       when is_atom(reason),
       do: reason

  defp result_reason({:ok, _result}), do: nil
  defp result_reason(_result), do: :query_engine_failure

  defp measurement(nil, _field), do: 0

  defp measurement(usage, field) do
    case Map.get(usage, field, 0) do
      value when is_integer(value) and value >= 0 -> value
      _invalid -> 0
    end
  end

  defp duration_us(started_us, now_us) when is_integer(now_us), do: max(now_us - started_us, 0)
  defp duration_us(_started_us, _now_us), do: 0

  defp plan_metadata(request, %Plan{} = plan, usage) do
    %{
      path: plan.path,
      index_id: plan.index_id,
      index_version: plan.index_version,
      covering: CoveringProjection.classify(request, plan, usage)
    }
  end

  defp plan_metadata(_request, nil, _usage) do
    %{path: nil, index_id: nil, index_version: nil, covering: :ineligible}
  end

  defp bounded_enum(value, allowed), do: if(value in allowed, do: value, else: :invalid)

  defp monotonic_us, do: System.monotonic_time(:microsecond)
end
