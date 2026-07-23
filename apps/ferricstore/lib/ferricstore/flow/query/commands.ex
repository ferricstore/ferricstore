defmodule Ferricstore.Flow.Query.Commands do
  @moduledoc false

  @behaviour Ferricstore.Commands.Extension

  alias Ferricstore.Flow.Query.IndexStatus

  @command "FLOW.QUERY.INDEXES"
  @usage "FLOW.QUERY.INDEXES [index-id]"
  @command_entries [
    %{
      name: @command,
      arity: -1,
      flags: ["readonly"],
      first_key: 0,
      last_key: 0,
      step: 0,
      access: :read,
      acl_categories: [:admin],
      summary: "Inspect Flow query index lifecycle, validation, retirement, and statistics."
    }
  ]

  @impl true
  def commands, do: @command_entries

  @impl true
  def handle(@command, args, store) do
    with {:ok, index_id} <- parse(args),
         {:ok, status} <- IndexStatus.fetch(store, index_id) do
      status
    else
      {:error, reason} -> {:error, error_message(reason)}
    end
  end

  def handle(_command, _args, _store), do: :not_found

  @impl true
  def keys(@command, _args), do: {:ok, []}
  def keys(_command, _args), do: :error

  defp parse([]), do: {:ok, nil}
  defp parse([index_id]) when is_binary(index_id), do: {:ok, index_id}
  defp parse(_args), do: {:error, :invalid_query_index_command}

  defp error_message(:invalid_query_index_command),
    do: "ERR syntax error; use #{@usage}"

  defp error_message(:invalid_query_index_filter),
    do: "ERR invalid query index id; use #{@usage}"

  defp error_message(:query_index_not_found),
    do: "ERR query index not found; use FLOW.QUERY.INDEXES to list valid indexes"

  defp error_message(:query_index_registry_unavailable),
    do: "ERR query index registry unavailable; verify the instance query services are running"

  defp error_message(_reason),
    do: "ERR query index status unavailable; verify the instance query services are running"
end
