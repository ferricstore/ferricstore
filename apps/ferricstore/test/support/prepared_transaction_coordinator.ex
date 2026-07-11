defmodule Ferricstore.Test.PreparedTransactionCoordinator do
  @moduledoc false

  alias Ferricstore.Commands.PreparedCommand
  alias Ferricstore.Transaction.Coordinator

  def execute(entries, watched_keys, sandbox_namespace) when is_list(entries) do
    with {:ok, prepared_entries} <- prepare_entries(entries) do
      Coordinator.execute(prepared_entries, watched_keys, sandbox_namespace)
    end
  end

  defp prepare_entries(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, prepared} ->
      case prepare_entry(entry) do
        {:ok, current} -> {:cont, {:ok, [current | prepared]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, prepared} -> {:ok, Enum.reverse(prepared)}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_entry(%PreparedCommand{} = prepared), do: {:ok, prepared}

  defp prepare_entry({command, args, ast}) when is_binary(command) and is_list(args),
    do: {:ok, {command, args, ast}}

  defp prepare_entry({command, args}) when is_binary(command) and is_list(args),
    do: PreparedCommand.prepare(command, args)

  defp prepare_entry(_invalid), do: {:error, "ERR invalid transaction command fixture"}
end
