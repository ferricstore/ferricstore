defmodule Ferricstore.Commands.Stream.AppendBatch do
  @moduledoc false

  import Bitwise, only: [band: 2, bor: 2, bsl: 2]

  @position_bitset_limit 4096

  @type group :: {binary(), [[binary()]], [non_neg_integer()]}
  @type work :: %{
          command_items: non_neg_integer(),
          compound_members: non_neg_integer(),
          replies: pos_integer()
        }

  @spec group_commands([term()]) :: {:ok, [group()], pos_integer()} | :generic
  def group_commands(commands) when is_list(commands),
    do: group_commands(commands, %{}, [], 0)

  def group_commands(_commands), do: :generic

  @spec validate_groups(term(), term()) :: :ok | {:error, atom()}
  def validate_groups(groups, count) do
    case validate_groups_with_work(groups, count) do
      {:ok, _work} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec validate_groups_with_work(term(), term()) :: {:ok, work()} | {:error, atom()}
  def validate_groups_with_work(groups, count)
      when is_list(groups) and groups != [] and is_integer(count) and count > 0 do
    position_tracker = new_position_tracker(count)

    with {:ok, ^count, position_tracker, command_items, compound_members} <-
           validate_groups(groups, 0, position_tracker, %{}, 0, 0, count),
         true <- complete_position_tracker?(position_tracker, count) do
      {:ok,
       %{
         command_items: command_items,
         compound_members: compound_members,
         replies: count
       }}
    else
      _invalid -> {:error, :invalid_stream_append_grouped_auto}
    end
  end

  def validate_groups_with_work(_groups, _count),
    do: {:error, :invalid_stream_append_grouped_auto}

  @spec expand_groups(term(), term()) :: {:ok, [term()]} | {:error, atom()}
  def expand_groups(groups, count) do
    with :ok <- validate_groups(groups, count) do
      commands =
        groups
        |> Enum.flat_map(fn {key, fields_lists, positions} ->
          Enum.zip(positions, fields_lists)
          |> Enum.map(fn {position, fields} ->
            {position, {:stream_append, key, :auto, fields, nil, false}}
          end)
        end)
        |> then(&:lists.keysort(1, &1))
        |> Enum.map(&elem(&1, 1))

      {:ok, commands}
    end
  end

  defp group_commands([], groups, key_order, count) when count > 0 do
    ordered_groups =
      key_order
      |> Enum.reverse()
      |> Enum.map(fn key ->
        {fields_lists, positions} = Map.fetch!(groups, key)
        {key, Enum.reverse(fields_lists), Enum.reverse(positions)}
      end)

    {:ok, ordered_groups, count}
  end

  defp group_commands([], _groups, _key_order, 0), do: :generic

  defp group_commands(
         [{:stream_append, key, :auto, fields, nil, false} | rest],
         groups,
         key_order,
         position
       )
       when is_binary(key) and is_list(fields) do
    {groups, key_order} =
      case Map.fetch(groups, key) do
        {:ok, {fields_lists, positions}} ->
          {Map.put(groups, key, {[fields | fields_lists], [position | positions]}), key_order}

        :error ->
          {Map.put(groups, key, {[fields], [position]}), [key | key_order]}
      end

    group_commands(rest, groups, key_order, position + 1)
  end

  defp group_commands(_commands, _groups, _key_order, _position), do: :generic

  defp validate_groups(
         [],
         total,
         position_tracker,
         _keys,
         command_items,
         compound_members,
         _position_limit
       ),
       do: {:ok, total, position_tracker, command_items, compound_members}

  defp validate_groups(
         [
           {key, [_field | _fields] = fields_lists, [_position | _positions] = group_positions}
           | groups
         ],
         total,
         position_tracker,
         keys,
         command_items,
         compound_members,
         position_limit
       )
       when is_binary(key) do
    next_keys = Map.put_new(keys, key, true)

    if map_size(next_keys) == map_size(keys) do
      :error
    else
      case validate_group_items(
             fields_lists,
             group_positions,
             0,
             position_tracker,
             command_items,
             compound_members,
             position_limit
           ) do
        {:ok, group_count, position_tracker, command_items, compound_members} ->
          validate_groups(
            groups,
            total + group_count,
            position_tracker,
            next_keys,
            command_items,
            compound_members,
            position_limit
          )

        :error ->
          :error
      end
    end
  end

  defp validate_groups(
         _groups,
         _total,
         _position_tracker,
         _keys,
         _command_items,
         _compound_members,
         _position_limit
       ),
       do: :error

  defp validate_group_items(
         [],
         [],
         count,
         position_tracker,
         command_items,
         compound_members,
         _position_limit
       ),
       do: {:ok, count, position_tracker, command_items, compound_members}

  defp validate_group_items(
         [fields | fields_lists],
         [position | remaining_positions],
         count,
         position_tracker,
         command_items,
         compound_members,
         position_limit
       )
       when is_integer(position) and position >= 0 do
    with {:ok, field_items} <- proper_list_length(fields, 0),
         {:ok, position_tracker} <-
           put_position(position_tracker, position, position_limit) do
      validate_group_items(
        fields_lists,
        remaining_positions,
        count + 1,
        position_tracker,
        command_items + max(field_items, 1) + 1,
        compound_members + 1,
        position_limit
      )
    else
      :error -> :error
    end
  end

  defp validate_group_items(
         _fields_lists,
         _positions,
         _count,
         _position_tracker,
         _command_items,
         _compound_members,
         _position_limit
       ),
       do: :error

  defp new_position_tracker(count) when count <= @position_bitset_limit, do: {:ordered, 0}
  defp new_position_tracker(_count), do: {:map, %{}}

  defp put_position(_tracker, position, limit) when position >= limit, do: :error

  defp put_position({:ordered, position}, position, _limit),
    do: {:ok, {:ordered, position + 1}}

  defp put_position({:ordered, next}, position, limit) do
    put_position({:bits, bsl(1, next) - 1}, position, limit)
  end

  defp put_position({:bits, bits}, position, _limit) do
    bit = bsl(1, position)

    if band(bits, bit) == 0 do
      {:ok, {:bits, bor(bits, bit)}}
    else
      :error
    end
  end

  defp put_position({:map, positions}, position, _limit) do
    if is_map_key(positions, position) do
      :error
    else
      {:ok, {:map, Map.put(positions, position, true)}}
    end
  end

  defp complete_position_tracker?({:bits, bits}, count),
    do: bits == bsl(1, count) - 1

  defp complete_position_tracker?({:ordered, count}, count), do: true
  defp complete_position_tracker?({:ordered, _seen}, _count), do: false

  defp complete_position_tracker?({:map, positions}, count),
    do: map_size(positions) == count

  defp proper_list_length([], count), do: {:ok, count}
  defp proper_list_length([_field, _value], 0), do: {:ok, 2}

  defp proper_list_length([_field1, _value1, _field2, _value2], 0),
    do: {:ok, 4}

  defp proper_list_length([_item | rest], count), do: proper_list_length(rest, count + 1)
  defp proper_list_length(_improper, _count), do: :error
end
