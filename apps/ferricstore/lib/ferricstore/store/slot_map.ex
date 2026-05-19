defmodule Ferricstore.Store.SlotMap do
  @moduledoc """
  Manages the 1,024-slot to shard-index mapping.

  Provides the indirection layer between stable hash output and shard assignment:

      key -> crc32(key) & 0x3FF -> slot -> slot_map[slot] -> shard_index

  The slot map is a 1,024-element tuple stored in `:persistent_term` for
  ~10ns read access. Updates via `put/1` are atomic -- all processes see
  the new map on their next read.
  """

  import Bitwise

  @num_slots 1024
  @slot_mask @num_slots - 1

  @spec num_slots() :: pos_integer()
  def num_slots, do: @num_slots

  @spec init(pos_integer()) :: :ok
  def init(shard_count) do
    map = build_uniform(shard_count)
    :persistent_term.put(:ferricstore_slot_map, map)
    :persistent_term.put(:ferricstore_shard_count, shard_count)
    :ok
  end

  @spec get() :: tuple()
  def get do
    :persistent_term.get(:ferricstore_slot_map)
  end

  @spec put(tuple()) :: :ok
  def put(new_map) when tuple_size(new_map) == @num_slots do
    :persistent_term.put(:ferricstore_slot_map, new_map)
    :ok
  end

  @spec slot_for_key(binary()) :: non_neg_integer()
  def slot_for_key("f:{" <> rest = key) do
    slot_for_flow_tag(rest, key)
  end

  def slot_for_key("X:f:{" <> rest = key) do
    slot_for_flow_tag(rest, key)
  end

  def slot_for_key(key) do
    hash_input = Ferricstore.Store.Router.extract_hash_tag(key) || key
    slot_for_hash_input(hash_input)
  end

  defp slot_for_flow_tag(rest, fallback_key) do
    case :binary.match(rest, "}") do
      {end_pos, 1} when end_pos > 0 ->
        rest
        |> binary_part(0, end_pos)
        |> slot_for_hash_input()

      _ ->
        hash_input = Ferricstore.Store.Router.extract_hash_tag(fallback_key) || fallback_key
        slot_for_hash_input(hash_input)
    end
  end

  defp slot_for_hash_input(hash_input) do
    :erlang.crc32(hash_input) |> band(@slot_mask)
  end

  @spec shard_for_slot(tuple(), non_neg_integer()) :: non_neg_integer()
  def shard_for_slot(map, slot), do: elem(map, slot)

  @spec reassign_slot(tuple(), non_neg_integer(), non_neg_integer()) :: tuple()
  def reassign_slot(map, slot, new_shard) do
    put_elem(map, slot, new_shard)
  end

  @spec slot_count_for_shard(tuple(), non_neg_integer()) :: non_neg_integer()
  def slot_count_for_shard(map, shard_index) do
    Enum.count(0..(@num_slots - 1), fn slot ->
      elem(map, slot) == shard_index
    end)
  end

  @spec slot_ranges(tuple()) :: [{non_neg_integer(), non_neg_integer(), non_neg_integer()}]
  def slot_ranges(map) do
    # Walk slots 0..1023, group contiguous runs with the same shard.
    {ranges, start, prev_shard} =
      Enum.reduce(1..(@num_slots - 1), {[], 0, elem(map, 0)}, fn slot,
                                                                 {acc, run_start, run_shard} ->
        shard = elem(map, slot)

        if shard == run_shard do
          {acc, run_start, run_shard}
        else
          {[{run_start, slot - 1, run_shard} | acc], slot, shard}
        end
      end)

    Enum.reverse([{start, @num_slots - 1, prev_shard} | ranges])
  end

  @spec build_uniform(pos_integer()) :: tuple()
  def build_uniform(shard_count) when shard_count >= 1 do
    slots_per_shard = div(@num_slots, shard_count)
    remainder = rem(@num_slots, shard_count)

    {map_list, _} =
      Enum.reduce(0..(shard_count - 1), {[], 0}, fn shard, {acc, offset} ->
        count = slots_per_shard + if(shard < remainder, do: 1, else: 0)
        entries = List.duplicate(shard, count)
        {acc ++ entries, offset + count}
      end)

    List.to_tuple(map_list)
  end
end
