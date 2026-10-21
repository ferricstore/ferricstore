defmodule Ferricstore.Commands.Stream.ID do
  @moduledoc false

  alias Ferricstore.CommandTime

  @max_u64 18_446_744_073_709_551_615
  @invalid_id "ERR Invalid stream ID specified as stream command argument"

  @type stream_id :: {integer(), integer()}

  @spec resolve(
          term(),
          integer(),
          integer()
        ) ::
          {:ok, stream_id()} | {:error, binary()}
  def resolve(:auto, last_ms, last_seq) do
    # CommandTime uses HLC outside Raft and stamped log-entry time inside Raft,
    # keeping stream ID generation deterministic during state-machine replay.
    resolve_auto_at(CommandTime.now_ms(), last_ms, last_seq)
  end

  def resolve({:explicit, ms, seq}, last_ms, last_seq) do
    if valid_component?(ms) and valid_component?(seq) and valid_component?(last_ms) and
         valid_component?(last_seq) do
      case compare({ms, seq}, {last_ms, last_seq}) do
        :gt ->
          {:ok, {ms, seq}}

        _ ->
          smaller_id_error()
      end
    else
      {:error, @invalid_id}
    end
  end

  def resolve({:partial, ms}, last_ms, last_seq) do
    if valid_component?(ms) and valid_component?(last_ms) and valid_component?(last_seq) do
      # Partial ID: only ms given, seq auto-assigned.
      cond do
        ms > last_ms -> {:ok, {ms, 0}}
        ms == last_ms -> increment_sequence(ms, last_seq)
        true -> smaller_id_error()
      end
    else
      {:error, @invalid_id}
    end
  end

  def resolve(_invalid, _last_ms, _last_seq), do: {:error, @invalid_id}

  @doc false
  @spec resolve_auto_at(integer(), integer(), integer()) ::
          {:ok, stream_id()} | {:error, binary()}
  def resolve_auto_at(now, last_ms, last_seq) do
    cond do
      not valid_component?(last_ms) or not valid_component?(last_seq) -> {:error, @invalid_id}
      not valid_component?(now) -> {:error, @invalid_id}
      now > last_ms -> {:ok, {now, 0}}
      now == last_ms -> increment_sequence(now, last_seq)
      # HLC physical behind last_ms: keep last_ms with incremented seq.
      true -> increment_sequence(last_ms, last_seq)
    end
  end

  @spec parse_id!(binary()) :: stream_id()
  def parse_id!(id_str) do
    case parse_full_id(id_str) do
      {:ok, id} -> id
      {:error, message} -> raise ArgumentError, message
    end
  end

  @spec parse_full_id(binary()) :: {:ok, stream_id()} | {:error, binary()}
  def parse_full_id(id_str) do
    case :binary.split(id_str, "-") do
      [ms_str, seq_str] ->
        case {parse_component(ms_str), parse_component(seq_str)} do
          {{:ok, ms}, {:ok, seq}} ->
            {:ok, {ms, seq}}

          _ ->
            {:error, @invalid_id}
        end

      [ms_str] ->
        case parse_component(ms_str) do
          {:ok, ms} -> {:ok, {ms, 0}}
          _ -> {:error, @invalid_id}
        end
    end
  end

  defp parse_component(<<"+", digits::binary>>), do: parse_component_digits(digits, 0)
  defp parse_component(digits), do: parse_component_digits(digits, 0)

  defp parse_component_digits(<<>>, _value), do: :error

  defp parse_component_digits(<<digit, rest::binary>>, value) when digit in ?0..?9 do
    next = value * 10 + digit - ?0

    if next <= @max_u64 do
      if rest == <<>>, do: {:ok, next}, else: parse_component_digits(rest, next)
    else
      :error
    end
  end

  defp parse_component_digits(_invalid, _value), do: :error

  @doc """
  Parses an XRANGE/XREVRANGE bound as Redis does. `side` is `:min` for the
  start of the interval and `:max` for its end. "-" and "+" are the smallest
  and largest IDs wherever they appear; an ID without a sequence spans its
  whole millisecond (sequence 0 as a start, the largest as an end); and a
  leading "(" excludes the ID itself.
  """
  @spec parse_range_id(binary(), :min | :max) ::
          {:ok, :min | :max | stream_id()} | {:error, binary()}
  def parse_range_id(<<"(", id_str::binary>>, side) when id_str != "" do
    with {:ok, id} <- plain_range_id(id_str, side), do: exclude_range_id(id, side)
  end

  def parse_range_id(id_str, side), do: plain_range_id(id_str, side)

  defp plain_range_id("-", :min), do: {:ok, :min}
  defp plain_range_id("+", :max), do: {:ok, :max}
  defp plain_range_id("-", :max), do: {:ok, {0, 0}}
  defp plain_range_id("+", :min), do: {:ok, {@max_u64, @max_u64}}

  defp plain_range_id(id_str, side) do
    case :binary.split(id_str, "-") do
      [ms_str] ->
        case parse_component(ms_str) do
          {:ok, ms} -> {:ok, {ms, if(side == :max, do: @max_u64, else: 0)}}
          _ -> {:error, @invalid_id}
        end

      _ ->
        parse_full_id(id_str)
    end
  end

  defp exclude_range_id(:min, :min), do: exclude_range_id({0, 0}, :min)
  defp exclude_range_id(:max, :max), do: exclude_range_id({@max_u64, @max_u64}, :max)

  defp exclude_range_id({@max_u64, @max_u64}, :min),
    do: {:error, "ERR invalid start ID for the interval"}

  defp exclude_range_id({ms, @max_u64}, :min), do: {:ok, {ms + 1, 0}}
  defp exclude_range_id({ms, seq}, :min), do: {:ok, {ms, seq + 1}}
  defp exclude_range_id({0, 0}, :max), do: {:error, "ERR invalid end ID for the interval"}
  defp exclude_range_id({ms, 0}, :max), do: {:ok, {ms - 1, @max_u64}}
  defp exclude_range_id({ms, seq}, :max), do: {:ok, {ms, seq - 1}}
  defp exclude_range_id(:max, :min), do: exclude_range_id({@max_u64, @max_u64}, :min)
  defp exclude_range_id(:min, :max), do: exclude_range_id({0, 0}, :max)

  @spec normalize_ast_range_id(:min | :max | stream_id() | binary(), :min | :max) ::
          {:ok, :min | :max | stream_id()} | {:error, binary()}
  def normalize_ast_range_id(:min, _default), do: {:ok, :min}
  def normalize_ast_range_id(:max, _default), do: {:ok, :max}

  def normalize_ast_range_id({ms, seq} = id, _default)
      when ms in 0..@max_u64 and seq in 0..@max_u64,
      do: {:ok, id}

  def normalize_ast_range_id({_ms, _seq}, _default), do: {:error, @invalid_id}

  def normalize_ast_range_id(id_str, default) when is_binary(id_str),
    do: parse_range_id(id_str, default)

  @spec parse_exclusive_start(binary()) :: {:ok, :min | stream_id()} | {:error, binary()}
  def parse_exclusive_start("0"), do: {:ok, :min}
  def parse_exclusive_start("0-0"), do: {:ok, :min}

  def parse_exclusive_start(id_str) do
    case parse_full_id(id_str) do
      {:ok, {ms, seq}} -> {:ok, {ms, seq + 1}}
      err -> err
    end
  end

  @spec in_range?(stream_id(), :min | stream_id(), :max | stream_id()) :: boolean()
  def in_range?(_id, :min, :max), do: true
  def in_range?(id, :min, max), do: compare(id, max) != :gt
  def in_range?(id, min, :max), do: compare(id, min) != :lt
  def in_range?(id, min, max), do: compare(id, min) != :lt and compare(id, max) != :gt

  @spec compare(stream_id(), stream_id()) :: :lt | :gt | :eq
  def compare({ms1, seq1}, {ms2, seq2}) do
    cond do
      ms1 < ms2 -> :lt
      ms1 > ms2 -> :gt
      seq1 < seq2 -> :lt
      seq1 > seq2 -> :gt
      true -> :eq
    end
  end

  defp increment_sequence(ms, seq) when seq < @max_u64, do: {:ok, {ms, seq + 1}}
  defp increment_sequence(_ms, _seq), do: smaller_id_error()

  defp smaller_id_error do
    {:error,
     "ERR The ID specified in XADD is equal or smaller than the " <>
       "target stream top item"}
  end

  defp valid_component?(value), do: is_integer(value) and value >= 0 and value <= @max_u64
end
