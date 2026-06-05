defmodule Ferricstore.Flow.Telemetry do
  @moduledoc false

  @doc false
  def start_time, do: System.monotonic_time()

  @doc false
  def observe(command, started, result, fallback_metadata) do
    {attempt_count, fallback_metadata} = Map.pop(fallback_metadata, :_count)
    measurements = flow_measurements(started, command, result, attempt_count)
    metadata = flow_metadata(result, fallback_metadata)

    observe_flow_create_attempt(command, measurements, metadata, attempt_count)
    observe_flow_create_success(command, measurements, metadata)
    :telemetry.execute([:ferricstore, :flow, command, :stop], measurements, metadata)

    result
  end

  @doc false
  def observe_batch(command, started, results) do
    {success_count, first_record} = flow_batch_success_count_and_first_record(results)
    attempt_count = length(results)

    measurements =
      flow_measurements(started, command, {:ok, first_record}, success_count)

    metadata = flow_metadata({:ok, first_record}, %{flow_id: nil})

    observe_flow_create_attempt(command, measurements, metadata, attempt_count)
    observe_flow_create_success(command, measurements, metadata)
    :telemetry.execute([:ferricstore, :flow, command, :stop], measurements, metadata)
    :ok
  end

  @doc false
  def observe_pipeline_read_batch(started, ops) do
    :telemetry.execute(
      [:ferricstore, :flow, :pipeline_read_batch],
      %{
        count: length(ops),
        gets: Enum.count(ops, &pipeline_read_get?/1),
        histories: Enum.count(ops, &pipeline_read_history?/1),
        duration: System.monotonic_time() - started
      },
      %{source: :pipeline}
    )
  end

  @doc false
  def elapsed_us(started) do
    System.convert_time_unit(System.monotonic_time() - started, :native, :microsecond)
  end

  defp observe_flow_create_attempt(:create, measurements, metadata, count) do
    :telemetry.execute(
      [:ferricstore, :flow, :create, :attempt],
      %{measurements | count: positive_count(count)},
      metadata
    )
  end

  defp observe_flow_create_attempt(_command, _measurements, _metadata, _count), do: :ok

  defp observe_flow_create_success(:create, %{count: count} = measurements, metadata)
       when is_integer(count) and count > 0 do
    :telemetry.execute([:ferricstore, :flow, :create, :success], measurements, metadata)
  end

  defp observe_flow_create_success(_command, _measurements, _metadata), do: :ok

  defp positive_count(count) when is_integer(count) and count > 0, do: count
  defp positive_count(_count), do: 0

  defp flow_batch_success_count_and_first_record(results) do
    Enum.reduce(results, {0, nil}, fn
      :ok, {count, first_record} ->
        {count + 1, first_record}

      {:ok, record}, {count, nil} when is_map(record) ->
        {count + 1, record}

      {:ok, _record}, {count, first_record} ->
        {count + 1, first_record}

      _other, acc ->
        acc
    end)
  end

  defp pipeline_read_get?({:get, _id, _opts}), do: true
  defp pipeline_read_get?({:flow_get, _id, _opts}), do: true
  defp pipeline_read_get?(_op), do: false

  defp pipeline_read_history?({:history, _id, _opts}), do: true
  defp pipeline_read_history?({:flow_history, _id, _opts}), do: true
  defp pipeline_read_history?(_op), do: false

  defp flow_measurements(started, command, result, success_count) do
    count = result_count(result, success_count)

    %{
      duration_ms:
        System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond),
      count: count,
      claimed: if(command == :claim_due, do: count, else: 0)
    }
  end

  defp result_count(:ok, count) when is_integer(count) and count >= 0, do: count
  defp result_count({:ok, {:error, _reason}}, _count), do: 0
  defp result_count({:ok, _value}, count) when is_integer(count) and count >= 0, do: count
  defp result_count(result, _count), do: result_count(result)

  defp result_count({:ok, records}) when is_list(records), do: length(records)
  defp result_count({:ok, nil}), do: 0
  defp result_count({:ok, _record}), do: 1
  defp result_count(_result), do: 0

  defp flow_metadata({:ok, records}, fallback) when is_list(records) do
    records
    |> List.first(%{})
    |> flow_record_metadata()
    |> Map.merge(fallback, fn _key, record_value, fallback_value ->
      record_value || fallback_value
    end)
    |> Map.merge(%{result: :ok, reason: nil})
  end

  defp flow_metadata({:ok, record}, fallback) when is_map(record) do
    record
    |> flow_record_metadata()
    |> Map.merge(fallback, fn _key, record_value, fallback_value ->
      record_value || fallback_value
    end)
    |> Map.merge(%{result: :ok, reason: nil})
  end

  defp flow_metadata({:ok, {:error, reason}}, fallback) when is_binary(reason) do
    Map.merge(fallback, %{result: :error, reason: flow_error_reason(reason)})
  end

  defp flow_metadata({:ok, _value}, fallback),
    do: Map.merge(fallback, %{result: :ok, reason: nil})

  defp flow_metadata(:ok, fallback),
    do: Map.merge(fallback, %{result: :ok, reason: nil})

  defp flow_metadata({:error, reason}, fallback) when is_binary(reason) do
    Map.merge(fallback, %{result: :error, reason: flow_error_reason(reason)})
  end

  defp flow_metadata(_result, fallback),
    do: Map.merge(fallback, %{result: :error, reason: :error})

  defp flow_record_metadata(record) when is_map(record) do
    %{
      flow_id: Map.get(record, :id),
      flow_type: Map.get(record, :type),
      to_state: Map.get(record, :state),
      worker_id: Map.get(record, :lease_owner),
      fencing_token: Map.get(record, :fencing_token)
    }
  end

  defp flow_record_metadata(_record), do: %{}

  defp flow_error_reason(reason) do
    cond do
      String.contains?(reason, "wrong state") -> :wrong_state
      String.contains?(reason, "stale flow lease") -> :stale_token
      String.contains?(reason, "not found") -> :missing
      String.contains?(reason, "already exists") -> :exists
      true -> :error
    end
  end
end
