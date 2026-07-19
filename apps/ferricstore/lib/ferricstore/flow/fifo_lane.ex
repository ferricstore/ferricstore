defmodule Ferricstore.Flow.FifoLane do
  @moduledoc false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  @max_u128 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF

  @type sequence :: non_neg_integer()

  @spec lane_key(binary(), binary(), binary()) :: binary()
  def lane_key(type, state, partition_key)
      when is_binary(type) and type != "" and is_binary(state) and state != "" and
             is_binary(partition_key) and partition_key != "" do
    "f:" <>
      Keys.tag(partition_key) <>
      ":fl:1:" <> Keys.index_component(type) <> ":" <> Keys.index_component(state)
  end

  @spec lane_key_from_due_key(binary(), binary(), binary()) :: binary()
  def lane_key_from_due_key(due_key, type, state)
      when is_binary(due_key) and is_binary(type) and type != "" and is_binary(state) and
             state != "" do
    case Router.extract_hash_tag(due_key) do
      tag when is_binary(tag) ->
        "f:{" <>
          tag <> "}:fl:1:" <> Keys.index_component(type) <> ":" <> Keys.index_component(state)

      nil ->
        raise ArgumentError, "due key does not contain a hash tag"
    end
  end

  @spec member(sequence(), binary()) :: binary()
  def member(sequence, id)
      when is_integer(sequence) and sequence >= 0 and sequence <= @max_u128 and
             is_binary(id) and id != "" do
    <<sequence::unsigned-big-128, id::binary>>
  end

  @spec decode_member(term()) :: {:ok, {sequence(), binary()}} | :error
  def decode_member(<<sequence::unsigned-big-128, id::binary>>) when id != "",
    do: {:ok, {sequence, id}}

  def decode_member(_invalid), do: :error

  @spec identity(map()) :: {:ok, %{lane_key: binary(), member: binary()}} | nil | :error
  def identity(record) when is_map(record) do
    logical_state = logical_state(record)

    if Ferricstore.Flow.LMDB.terminal_state?(logical_state) do
      nil
    else
      with id when is_binary(id) and id != "" <- Map.get(record, :id),
           type when is_binary(type) and type != "" <- Map.get(record, :type),
           state when is_binary(state) and state != "" <- logical_state,
           partition_key when is_binary(partition_key) and partition_key != "" <-
             Map.get(record, :partition_key),
           sequence when is_integer(sequence) and sequence >= 0 and sequence <= @max_u128 <-
             Map.get(record, :state_enter_seq) do
        {:ok,
         %{
           lane_key: lane_key(type, state, partition_key),
           member: member(sequence, id),
           score: lane_score(record)
         }}
      else
        _invalid -> :error
      end
    end
  end

  def identity(_record), do: :error

  @spec index_entry(map()) :: {binary(), binary(), -1 | 0} | nil | :error
  def index_entry(record) do
    case identity(record) do
      {:ok, %{lane_key: lane_key, member: member, score: score}} ->
        {lane_key, member, score}

      other ->
        other
    end
  end

  defp lane_score(%{state: "running"}), do: -1
  defp lane_score(_record), do: 0

  defp logical_state(%{state: "running"} = record), do: Map.get(record, :run_state) || "queued"
  defp logical_state(%{state: state}), do: state
  defp logical_state(_record), do: nil
end
