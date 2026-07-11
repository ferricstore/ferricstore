defmodule Ferricstore.Flow.Governance.LimitCatalogOutbox do
  @moduledoc false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  @meta_tag :flow_governance_limit_catalog_outbox_meta_v1
  @intent_tag :flow_governance_limit_catalog_outbox_intent_v1
  @max_page_size 256
  @max_pending 65_536
  @max_exact_version 9_007_199_254_740_991

  def empty_meta, do: %{head: 1, tail: 0}

  def append(%{head: head, tail: tail}, owner_key)
      when is_integer(head) and head > 0 and is_integer(tail) and tail >= 0 and
             is_binary(owner_key) and owner_key != "" do
    pending = max(tail - head + 1, 0)

    cond do
      head > tail + 1 ->
        corrupt_error()

      pending >= @max_pending ->
        {:error, "ERR flow limit catalog publication backlog is full"}

      tail >= @max_exact_version ->
        {:error, "ERR flow limit catalog publication sequence is exhausted"}

      true ->
        sequence = tail + 1
        {:ok, %{head: head, tail: sequence}, sequence}
    end
  end

  def append(_meta, _owner_key), do: corrupt_error()

  def acknowledge(%{head: head, tail: tail} = meta, expected_head, up_to)
      when is_integer(head) and head > 0 and is_integer(tail) and tail >= 0 and
             is_integer(expected_head) and expected_head > 0 and is_integer(up_to) and up_to > 0 do
    cond do
      up_to < head ->
        {:ok, meta, []}

      expected_head != head ->
        {:error, "ERR stale flow limit catalog publication cursor"}

      up_to > tail ->
        {:error, "ERR invalid flow limit catalog publication acknowledgement"}

      up_to - head + 1 > @max_page_size ->
        {:error, "ERR invalid flow limit catalog publication acknowledgement"}

      true ->
        {:ok, %{meta | head: up_to + 1}, Enum.to_list(head..up_to)}
    end
  end

  def acknowledge(_meta, _expected_head, _up_to), do: corrupt_error()

  def encode_meta(%{head: head, tail: tail})
      when is_integer(head) and head > 0 and is_integer(tail) and tail >= 0 and
             head <= tail + 1 do
    :erlang.term_to_binary({@meta_tag, head, tail})
  end

  def decode_meta(nil), do: {:ok, empty_meta()}

  def decode_meta(value) when is_binary(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {@meta_tag, head, tail}
      when is_integer(head) and head > 0 and is_integer(tail) and tail >= 0 and
             head <= tail + 1 and tail <= @max_exact_version ->
        {:ok, %{head: head, tail: tail}}

      _invalid ->
        corrupt_error()
    end
  rescue
    _decode_error -> corrupt_error()
  end

  def decode_meta(_value), do: corrupt_error()

  def encode_intent(owner_key) when is_binary(owner_key) and owner_key != "" do
    :erlang.term_to_binary({@intent_tag, owner_key})
  end

  def decode_intent(value) when is_binary(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {@intent_tag, owner_key} when is_binary(owner_key) and owner_key != "" ->
        {:ok, owner_key}

      _invalid ->
        entry_error()
    end
  rescue
    _decode_error -> entry_error()
  end

  def decode_intent(_value), do: entry_error()

  def read_page(ctx, shard_index, limit)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(limit) and limit > 0 and
             limit <= @max_page_size do
    meta_key = Keys.governance_limit_catalog_outbox_meta_key(shard_index)

    with {:ok, meta_value} <- read_shard_value(ctx, shard_index, meta_key),
         {:ok, meta} <- decode_meta(meta_value) do
      read_meta_page(ctx, shard_index, meta, limit)
    end
  end

  def read_page(_ctx, _shard_index, _limit),
    do: {:error, "ERR invalid flow limit catalog publication page"}

  defp read_meta_page(_ctx, shard_index, %{head: head, tail: tail}, _limit) when head > tail do
    {:ok, %{entries: [], head: head, tail: tail, shard_index: shard_index, more?: false}}
  end

  defp read_meta_page(ctx, shard_index, %{head: head, tail: tail}, limit) do
    last = min(tail, head + limit - 1)
    sequences = Enum.to_list(head..last)
    keys = Enum.map(sequences, &Keys.governance_limit_catalog_outbox_intent_key(shard_index, &1))

    with {:ok, values} <- read_shard_values(ctx, shard_index, keys),
         {:ok, entries} <- decode_entries(sequences, values) do
      {:ok,
       %{
         entries: entries,
         head: head,
         tail: tail,
         shard_index: shard_index,
         more?: last < tail
       }}
    end
  end

  defp decode_entries(sequences, values) when length(sequences) == length(values) do
    sequences
    |> Enum.zip(values)
    |> Enum.reduce_while({:ok, []}, fn {sequence, value}, {:ok, entries} ->
      case decode_intent(value) do
        {:ok, owner_key} -> {:cont, {:ok, [{sequence, owner_key} | entries]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_entries(_sequences, _values), do: entry_error()

  defp read_shard_value(ctx, shard_index, key) do
    case Router.read_shard_value(ctx, shard_index, key) do
      {:ok, _value} = ok -> ok
      :unavailable -> {:error, "ERR flow limit catalog publication shard unavailable"}
      _invalid -> {:error, "ERR flow limit catalog publication read failed"}
    end
  end

  defp read_shard_values(ctx, shard_index, keys) do
    case Router.read_shard_values(ctx, shard_index, keys) do
      {:ok, values} when is_list(values) and length(values) == length(keys) -> {:ok, values}
      :unavailable -> {:error, "ERR flow limit catalog publication shard unavailable"}
      {:error, _reason} = error -> error
      _invalid -> {:error, "ERR flow limit catalog publication read failed"}
    end
  end

  defp corrupt_error, do: {:error, "ERR flow limit catalog publication outbox is corrupt"}

  defp entry_error,
    do: {:error, "ERR flow limit catalog publication entry is missing or corrupt"}
end
