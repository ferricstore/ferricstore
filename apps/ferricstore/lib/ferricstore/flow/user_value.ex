defmodule Ferricstore.Flow.UserValue do
  @moduledoc false

  # Flow user values (payload, result, error) are replicated inside Raft
  # commands, and replicas decode the log with safe binary_to_term. An atom that
  # only the writer's VM had created would make the log unrecoverable after a
  # restart, so user values may contain no atoms besides true, false and nil.

  @value_keys [:payload, :result, :error]

  @spec validate_opts!(term()) :: :ok
  def validate_opts!(opts) when is_list(opts) do
    Enum.each(@value_keys, fn key ->
      case Keyword.fetch(opts, key) do
        {:ok, value} -> validate!(value, key)
        :error -> :ok
      end
    end)
  end

  def validate_opts!(_opts), do: :ok

  @spec validate_items!(term()) :: :ok
  def validate_items!(items) when is_list(items), do: Enum.each(items, &validate_item!/1)
  def validate_items!(_items), do: :ok

  defp validate_item!(item) when is_tuple(item),
    do: item |> Tuple.to_list() |> Enum.each(&validate_item!/1)

  defp validate_item!(item) when is_map(item) do
    Enum.each(@value_keys, fn key ->
      case Map.fetch(item, key) do
        {:ok, value} -> validate!(value, key)
        :error -> :ok
      end
    end)
  end

  defp validate_item!(item) when is_list(item) do
    if Keyword.keyword?(item), do: validate_opts!(item), else: :ok
  end

  defp validate_item!(_item), do: :ok

  defp validate!(value, field) do
    if contains_atom?(value) do
      raise ArgumentError,
            "Flow #{field} must not contain atoms other than true, false and nil; " <>
              "use string keys and string values (atoms are not durable across restarts)"
    end

    :ok
  end

  defp contains_atom?(value) when value in [true, false, nil], do: false
  defp contains_atom?(value) when is_atom(value), do: true
  defp contains_atom?(value) when is_binary(value) or is_number(value), do: false

  defp contains_atom?(value) when is_map(value),
    do: Enum.any?(value, fn {key, item} -> contains_atom?(key) or contains_atom?(item) end)

  defp contains_atom?(value) when is_list(value), do: list_contains_atom?(value)

  defp contains_atom?(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> list_contains_atom?()

  defp contains_atom?(_other), do: false

  # Handles improper lists.
  defp list_contains_atom?([]), do: false
  defp list_contains_atom?([head | tail]), do: contains_atom?(head) or list_contains_atom?(tail)
  defp list_contains_atom?(tail), do: contains_atom?(tail)
end
