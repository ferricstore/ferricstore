defmodule Ferricstore.Flow.Governance.Telemetry do
  @moduledoc false

  @event_prefix [:ferricstore, :flow, :governance]

  def emit(action, result, metadata \\ %{}) when is_atom(action) and is_map(metadata) do
    {status, code} = result_status(result)

    :telemetry.execute(
      @event_prefix ++ [action],
      %{count: 1},
      metadata
      |> Map.put(:action, action)
      |> Map.put(:status, status)
      |> maybe_put_code(code)
    )

    result
  end

  defp result_status({:ok, _value}), do: {:ok, nil}
  defp result_status(:ok), do: {:ok, nil}
  defp result_status({:error, %{code: code}}), do: {:error, code}
  defp result_status({:error, reason}) when is_binary(reason), do: {:error, reason}
  defp result_status({:error, reason}), do: {:error, inspect(reason)}
  defp result_status(_other), do: {:unknown, nil}

  defp maybe_put_code(metadata, nil), do: metadata
  defp maybe_put_code(metadata, code), do: Map.put(metadata, :code, code)
end
