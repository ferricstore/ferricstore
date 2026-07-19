defmodule Ferricstore.Flow.PolicyPatch do
  @moduledoc false

  @reserved_options [:expected_generation, :replace, :state]

  @spec from_opts(keyword()) :: map()
  def from_opts(opts) when is_list(opts) do
    opts
    |> Keyword.drop(@reserved_options)
    |> Map.new()
    |> normalize_policy_patch()
  end

  @spec policy_attrs(map() | nil, map(), boolean()) :: map()
  def policy_attrs(_stored_policy, patch, true) when is_map(patch), do: patch

  def policy_attrs(stored_policy, patch, false) when is_map(patch) do
    deep_merge_policy(stored_policy || %{}, patch)
  end

  defp normalize_policy_patch(patch) do
    Map.new(patch, fn
      {key, states} when key in [:states, "states"] ->
        {key, normalize_state_patch(states)}

      {key, value} ->
        {key, normalize_keyword_maps(value)}
    end)
  end

  defp normalize_state_patch(states) when is_map(states) do
    Map.new(states, fn {state, config} -> {state, normalize_keyword_maps(config)} end)
  end

  defp normalize_state_patch(states) when is_list(states) do
    if Enum.all?(states, fn
         {state, config} when is_binary(state) and (is_map(config) or is_list(config)) -> true
         _invalid -> false
       end) do
      Map.new(states, fn {state, config} -> {state, normalize_keyword_maps(config)} end)
    else
      normalize_keyword_maps(states)
    end
  end

  defp normalize_state_patch(states), do: normalize_keyword_maps(states)

  defp normalize_keyword_maps([]), do: []

  defp normalize_keyword_maps(value) when is_list(value) do
    if Keyword.keyword?(value) do
      value
      |> Map.new()
      |> normalize_keyword_maps()
    else
      Enum.map(value, &normalize_keyword_maps/1)
    end
  end

  defp normalize_keyword_maps(value) when is_map(value) do
    Map.new(value, fn {key, nested} -> {key, normalize_keyword_maps(nested)} end)
  end

  defp normalize_keyword_maps(value), do: value

  defp deep_merge_policy(left, right) when is_map(left) and is_map(right) do
    Enum.reduce(right, left, fn {incoming_key, new_value}, merged ->
      key = equivalent_policy_key(merged, incoming_key)
      old_value = Map.get(merged, key)

      value =
        cond do
          key == :states and is_map(old_value) and is_map(new_value) ->
            merge_states(old_value, new_value)

          is_map(old_value) and is_map(new_value) ->
            deep_merge_policy(old_value, new_value)

          true ->
            new_value
        end

      Map.put(merged, key, value)
    end)
  end

  defp equivalent_policy_key(map, key) when is_binary(key) do
    if Map.has_key?(map, key) do
      key
    else
      Enum.reduce_while(map, key, fn
        {existing, _value}, _acc when is_atom(existing) ->
          if Atom.to_string(existing) == key,
            do: {:halt, existing},
            else: {:cont, key}

        _entry, _acc ->
          {:cont, key}
      end)
    end
  end

  defp equivalent_policy_key(map, key) when is_atom(key) do
    cond do
      Map.has_key?(map, key) -> key
      Map.has_key?(map, Atom.to_string(key)) -> Atom.to_string(key)
      true -> key
    end
  end

  defp equivalent_policy_key(_map, key), do: key

  defp merge_states(old_states, new_states) do
    Enum.reduce(new_states, old_states, fn
      {state, nil}, states ->
        Map.delete(states, state)

      {state, state_patch}, states ->
        Map.update(states, state, state_patch, fn old_state ->
          if is_map(old_state) and is_map(state_patch) do
            deep_merge_policy(old_state, state_patch)
          else
            state_patch
          end
        end)
    end)
  end
end
