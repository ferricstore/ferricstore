defmodule Ferricstore.Flow.Governance.ReleaseOutbox do
  @moduledoc false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  @meta_tag :flow_governance_release_outbox_meta_v1
  @intent_tag :flow_governance_release_outbox_intent_v1
  @cursor_prefix "v1:"
  @completed_marker "flow-governance-release-completed-v1"
  @max_page_size 256

  @type meta :: %{head: pos_integer(), tail: non_neg_integer()}

  def empty_meta, do: %{head: 1, tail: 0}

  def append(%{head: head, tail: tail}, count)
      when is_integer(head) and head > 0 and is_integer(tail) and tail >= 0 and
             is_integer(count) and count > 0 do
    first = tail + 1
    last = tail + count
    {:ok, %{head: head, tail: last}, first..last}
  end

  def append(_meta, _count), do: {:error, "ERR flow governance release outbox is corrupt"}

  def acknowledge(%{head: head, tail: tail} = meta, expected_head, up_to)
      when is_integer(head) and head > 0 and is_integer(tail) and tail >= 0 and
             is_integer(expected_head) and expected_head > 0 and is_integer(up_to) and up_to > 0 do
    cond do
      up_to < head ->
        {:ok, meta, []}

      expected_head != head ->
        {:error, "ERR stale flow governance release outbox cursor"}

      up_to > tail ->
        {:error, "ERR invalid flow governance release outbox acknowledgement"}

      up_to - head + 1 > @max_page_size ->
        {:error, "ERR invalid flow governance release outbox acknowledgement"}

      true ->
        acknowledged = Enum.to_list(head..up_to)
        {:ok, %{meta | head: up_to + 1}, acknowledged}
    end
  end

  def acknowledge(_meta, _expected_head, _up_to),
    do: {:error, "ERR flow governance release outbox is corrupt"}

  def encode_meta(%{head: head, tail: tail})
      when is_integer(head) and head > 0 and is_integer(tail) and tail >= 0 do
    :erlang.term_to_binary({@meta_tag, head, tail})
  end

  def decode_meta(nil), do: {:ok, empty_meta()}

  def decode_meta(value) when is_binary(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {@meta_tag, head, tail}
      when is_integer(head) and head > 0 and is_integer(tail) and tail >= 0 and
             head <= tail + 1 ->
        {:ok, %{head: head, tail: tail}}

      _other ->
        {:error, "ERR flow governance release outbox is corrupt"}
    end
  rescue
    _decode_error -> {:error, "ERR flow governance release outbox is corrupt"}
  end

  def decode_meta(_value), do: {:error, "ERR flow governance release outbox is corrupt"}

  def encode_intent(%{} = intent), do: :erlang.term_to_binary({@intent_tag, intent})

  def decode_intent(value) when is_binary(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {@intent_tag, intent} when is_map(intent) -> validate_intent(intent)
      _other -> {:error, "ERR flow governance release intent is corrupt"}
    end
  rescue
    _decode_error -> {:error, "ERR flow governance release intent is corrupt"}
  end

  def decode_intent(_value), do: {:error, "ERR flow governance release intent is corrupt"}

  def completed_marker, do: @completed_marker

  def read_page(ctx, shard_index, limit), do: read_page(ctx, shard_index, limit, nil)

  def read_page(ctx, shard_index, limit, start_sequence)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(limit) and limit > 0 and
             limit <= @max_page_size and
             (is_nil(start_sequence) or (is_integer(start_sequence) and start_sequence > 0)) do
    with {:ok, meta_value} <-
           read_shard_value(
             ctx,
             shard_index,
             Keys.governance_release_outbox_meta_key(shard_index)
           ),
         {:ok, meta} <- decode_meta(meta_value) do
      read_meta_page(ctx, shard_index, meta, limit, start_sequence)
    end
  end

  def read_page(_ctx, _shard_index, _limit, _start_sequence),
    do: {:error, "ERR invalid flow governance release outbox page"}

  def encode_reconcile_cursor(next_shard, positions)
      when is_integer(next_shard) and next_shard >= 0 and is_map(positions) do
    encoded_positions =
      positions
      |> Enum.sort()
      |> Enum.map_join(",", fn {shard_index, sequence} ->
        Integer.to_string(shard_index) <> "=" <> Integer.to_string(sequence)
      end)

    @cursor_prefix <> Integer.to_string(next_shard) <> "|" <> encoded_positions
  end

  def decode_reconcile_cursor(cursor, shard_count)
      when is_integer(shard_count) and shard_count > 0 do
    max_cursor_bytes = 128 + shard_count * 96

    if is_binary(cursor) and byte_size(cursor) > max_cursor_bytes do
      {:error, "ERR invalid flow governance release outbox cursor"}
    else
      do_decode_reconcile_cursor(cursor, shard_count)
    end
  end

  def decode_reconcile_cursor(_cursor, _shard_count),
    do: {:error, "ERR invalid flow governance release outbox cursor"}

  defp read_meta_page(_ctx, shard_index, %{head: head, tail: tail}, _limit, _start_sequence)
       when head > tail do
    {:ok,
     %{
       entries: [],
       head: head,
       tail: tail,
       shard_index: shard_index,
       scan_start: nil,
       more?: false
     }}
  end

  defp read_meta_page(ctx, shard_index, %{head: head, tail: tail}, limit, start_sequence) do
    first = normalize_page_start(start_sequence, head, tail)
    last = min(tail, first + limit - 1)
    sequences = Enum.to_list(first..last)

    keys =
      Enum.flat_map(sequences, fn sequence ->
        [
          Keys.governance_release_outbox_intent_key(shard_index, sequence),
          Keys.governance_release_outbox_completed_key(shard_index, sequence)
        ]
      end)

    with {:ok, values} <- read_shard_values(ctx, shard_index, keys),
         {:ok, entries} <- decode_page_entries(sequences, values) do
      {:ok,
       %{
         entries: entries,
         head: head,
         tail: tail,
         shard_index: shard_index,
         scan_start: first,
         more?: last < tail
       }}
    end
  end

  defp do_decode_reconcile_cursor(nil, _shard_count),
    do: {:ok, %{next_shard: 0, positions: %{}}}

  defp do_decode_reconcile_cursor(@cursor_prefix <> encoded, shard_count) do
    with [encoded_shard, encoded_positions] <- String.split(encoded, "|", parts: 2),
         {:ok, next_shard} <- parse_non_negative_integer(encoded_shard),
         true <- next_shard < shard_count,
         {:ok, positions} <- decode_cursor_positions(encoded_positions, shard_count) do
      {:ok, %{next_shard: next_shard, positions: positions}}
    else
      _invalid -> {:error, "ERR invalid flow governance release outbox cursor"}
    end
  end

  defp do_decode_reconcile_cursor(_cursor, _shard_count),
    do: {:error, "ERR invalid flow governance release outbox cursor"}

  defp decode_cursor_positions("", _shard_count), do: {:ok, %{}}

  defp decode_cursor_positions(encoded_positions, shard_count) do
    entries = String.split(encoded_positions, ",")

    if length(entries) <= shard_count do
      Enum.reduce_while(entries, {:ok, %{}}, fn entry, {:ok, positions} ->
        with [encoded_shard, encoded_sequence] <- String.split(entry, "=", parts: 2),
             {:ok, shard_index} <- parse_non_negative_integer(encoded_shard),
             {:ok, sequence} <- parse_positive_integer(encoded_sequence),
             true <- shard_index < shard_count,
             false <- Map.has_key?(positions, shard_index) do
          {:cont, {:ok, Map.put(positions, shard_index, sequence)}}
        else
          _invalid -> {:halt, {:error, "ERR invalid flow governance release outbox cursor"}}
        end
      end)
    else
      {:error, "ERR invalid flow governance release outbox cursor"}
    end
  end

  defp parse_non_negative_integer(encoded) do
    case Integer.parse(encoded) do
      {shard_index, ""} when shard_index >= 0 -> {:ok, shard_index}
      _invalid -> :error
    end
  end

  defp parse_positive_integer(encoded) do
    case Integer.parse(encoded) do
      {value, ""} when value > 0 -> {:ok, value}
      _invalid -> :error
    end
  end

  defp normalize_page_start(nil, head, _tail), do: head
  defp normalize_page_start(start_sequence, head, _tail) when start_sequence < head, do: head

  defp normalize_page_start(start_sequence, _head, tail) when start_sequence <= tail,
    do: start_sequence

  defp normalize_page_start(_start_sequence, head, _tail), do: head

  defp decode_page_entries(sequences, values) when length(values) == length(sequences) * 2 do
    sequences
    |> Enum.zip(Enum.chunk_every(values, 2))
    |> Enum.reduce_while({:ok, []}, fn
      {sequence, [_intent_value, @completed_marker]}, {:ok, entries} ->
        {:cont, {:ok, [{sequence, :completed} | entries]}}

      {sequence, [intent_value, nil]}, {:ok, entries} ->
        case decode_intent(intent_value) do
          {:ok, intent} -> {:cont, {:ok, [{sequence, intent} | entries]}}
          {:error, _reason} -> {:halt, corrupt_entry_error()}
        end

      {_sequence, [_intent_value, _invalid_marker]}, {:ok, _entries} ->
        {:halt, corrupt_entry_error()}
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_page_entries(_sequences, _values), do: corrupt_entry_error()

  defp corrupt_entry_error,
    do: {:error, "ERR flow governance release outbox entry is missing or corrupt"}

  defp read_shard_value(ctx, shard_index, key) do
    case Router.read_shard_value(ctx, shard_index, key) do
      {:ok, _value} = ok -> ok
      :unavailable -> {:error, "ERR flow governance release outbox shard unavailable"}
      _invalid -> {:error, "ERR flow governance release outbox read failed"}
    end
  end

  defp read_shard_values(ctx, shard_index, keys) do
    case Router.read_shard_values(ctx, shard_index, keys) do
      {:ok, values} when is_list(values) -> {:ok, values}
      :unavailable -> {:error, "ERR flow governance release outbox shard unavailable"}
      {:error, _reason} = error -> error
      _invalid -> {:error, "ERR flow governance release outbox read failed"}
    end
  end

  defp validate_intent(
         %{
           flow_id: flow_id,
           scope: scope,
           shard_id: shard_id,
           reservation_id: reservation_id
         } = intent
       )
       when is_binary(flow_id) and flow_id != "" and is_binary(scope) and scope != "" and
              is_integer(shard_id) and shard_id >= 0 and is_binary(reservation_id) and
              reservation_id != "" do
    partition_key = Map.get(intent, :partition_key)
    enforcement = Map.get(intent, :enforcement, :approximate_global)

    if (is_nil(partition_key) or is_binary(partition_key)) and
         enforcement in [:strict_global, :approximate_global] do
      {:ok, Map.put(intent, :enforcement, enforcement)}
    else
      {:error, "ERR flow governance release intent is corrupt"}
    end
  end

  defp validate_intent(_intent), do: {:error, "ERR flow governance release intent is corrupt"}
end
