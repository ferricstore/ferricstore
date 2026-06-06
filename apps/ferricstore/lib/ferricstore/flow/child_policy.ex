defmodule Ferricstore.Flow.ChildPolicy do
  @moduledoc false

  def validate_children([_ | _]), do: :ok
  def validate_children(_children), do: {:error, "ERR flow children must be a non-empty list"}

  def validate_no_parent_child_id(parent_id, child_attrs) do
    if Enum.any?(child_attrs, &(Map.get(&1, :id) == parent_id)) do
      {:error, "ERR flow child id must differ from parent id"}
    else
      :ok
    end
  end

  def optional_policy(opts, key, default, allowed) do
    value =
      opts
      |> Keyword.get(key, default)
      |> normalize_policy_value()

    if value in allowed do
      {:ok, value}
    else
      {:error, "ERR flow #{key} has unsupported value"}
    end
  end

  def exhaust_to_opts(opts) do
    case Keyword.get(opts, :exhaust_to) do
      nil ->
        exhaust_to_states(Keyword.get(opts, :success), Keyword.get(opts, :failure))

      %{success: success, failure: failure} ->
        exhaust_to_states(success, failure)

      %{"success" => success, "failure" => failure} ->
        exhaust_to_states(success, failure)

      _ ->
        {:error, "ERR flow exhaust_to must include success and failure states"}
    end
  end

  defp normalize_policy_value(value) when is_atom(value), do: value

  defp normalize_policy_value(value) when is_binary(value) do
    value
    |> String.downcase()
    |> String.replace("-", "_")
    |> String.to_existing_atom()
  rescue
    ArgumentError -> value
  end

  defp normalize_policy_value(value), do: value

  defp exhaust_to_states(success, failure)
       when is_binary(success) and success != "" and is_binary(failure) and failure != "" do
    {:ok, %{"success" => success, "failure" => failure}}
  end

  defp exhaust_to_states(_success, _failure) do
    {:error, "ERR flow exhaust_to must include success and failure states"}
  end
end
