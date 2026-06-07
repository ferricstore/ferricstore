defmodule Ferricstore.Commands.Hash.Helpers do
  @moduledoc false

  alias Ferricstore.CommandTime

  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808

  @spec even_length?(list()) :: boolean()
  def even_length?([]), do: true
  def even_length?([_, _ | rest]), do: even_length?(rest)
  def even_length?(_), do: false

  @spec parse_integer_value(binary() | nil) :: {:ok, integer()} | :error
  def parse_integer_value(nil), do: {:ok, 0}

  def parse_integer_value(str) when is_binary(str) do
    case Integer.parse(str) do
      {int, ""} -> {:ok, int}
      _ -> :error
    end
  end

  @spec checked_integer_add(integer(), integer()) :: {:ok, integer()} | :overflow
  def checked_integer_add(value, increment) do
    result = value + increment

    if result > @max_int64 or result < @min_int64 do
      :overflow
    else
      {:ok, result}
    end
  end

  @spec parse_float_value(binary() | nil) :: {:ok, float()} | :error
  def parse_float_value(nil), do: {:ok, 0.0}

  def parse_float_value(str) when is_binary(str) do
    case Float.parse(str) do
      {float, ""} ->
        {:ok, float}

      _ ->
        case Integer.parse(str) do
          {int, ""} -> {:ok, int * 1.0}
          _ -> :error
        end
    end
  end

  @spec format_float(float()) :: binary()
  def format_float(val) when is_float(val) do
    :erlang.float_to_binary(val, [:compact, decimals: 17])
  end

  @spec parse_positive_integer(binary(), binary()) :: {:ok, pos_integer()} | {:error, binary()}
  def parse_positive_integer(str, label) do
    case Integer.parse(str) do
      {int, ""} when int > 0 -> {:ok, int}
      _ -> {:error, "ERR #{label} is not a positive integer"}
    end
  end

  @spec parse_expiry_mode(binary(), binary()) :: {:ok, integer()} | {:error, binary()}
  def parse_expiry_mode("EX", value_str) do
    case Integer.parse(value_str) do
      {seconds, ""} when seconds > 0 ->
        {:ok, CommandTime.now_ms() + seconds * 1000}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def parse_expiry_mode("PX", value_str) do
    case Integer.parse(value_str) do
      {ms, ""} when ms > 0 ->
        {:ok, CommandTime.now_ms() + ms}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def parse_expiry_mode("EXAT", value_str) do
    case Integer.parse(value_str) do
      {ts, ""} when ts > 0 ->
        {:ok, ts * 1000}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def parse_expiry_mode("PXAT", value_str) do
    case Integer.parse(value_str) do
      {ts_ms, ""} when ts_ms > 0 ->
        {:ok, ts_ms}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  @spec validate_field_count(non_neg_integer(), [term()]) :: :ok | {:error, binary()}
  def validate_field_count(0, []), do: :ok
  def validate_field_count(n, [_ | rest]) when n > 0, do: validate_field_count(n - 1, rest)

  def validate_field_count(_, _),
    do: {:error, "ERR number of fields does not match the count argument"}

  @spec parse_cursor(binary()) :: {:ok, non_neg_integer()} | {:error, binary()}
  def parse_cursor(cursor_str) do
    case Integer.parse(cursor_str) do
      {cursor, ""} when cursor >= 0 -> {:ok, cursor}
      _ -> {:error, "ERR invalid cursor"}
    end
  end

  @spec parse_hscan_opts([binary()]) :: {:ok, binary() | nil, pos_integer()} | {:error, binary()}
  def parse_hscan_opts(opts), do: do_parse_hscan_opts(opts, nil, 10)

  @spec paginate([term()], non_neg_integer(), pos_integer()) :: {binary(), [term()]}
  def paginate(items, cursor, count) do
    rest = Enum.drop(items, cursor)

    case rest do
      [] ->
        {"0", []}

      _ ->
        {batch, remainder} = Enum.split(rest, count)

        case remainder do
          [] -> {"0", batch}
          _ -> {Integer.to_string(cursor + length(batch)), batch}
        end
    end
  end

  @spec hash_pairs_to_flat_list([{binary(), binary()}]) :: [binary()]
  def hash_pairs_to_flat_list(pairs), do: hash_pairs_to_flat_list(pairs, [])

  @spec select_random_fields([{binary(), binary()}], integer(), boolean()) :: [binary()]
  def select_random_fields(pairs, count, with_values) do
    cond do
      count == 0 ->
        []

      count > 0 ->
        selected = Enum.take_random(pairs, count)

        if with_values do
          hash_pairs_to_flat_list(selected)
        else
          Enum.map(selected, fn {field, _value} -> field end)
        end

      count < 0 ->
        abs_count = abs(count)

        if pairs == [] do
          []
        else
          tuple = List.to_tuple(pairs)
          size = tuple_size(tuple)
          selected = for _ <- 1..abs_count, do: elem(tuple, :rand.uniform(size) - 1)

          if with_values do
            hash_pairs_to_flat_list(selected)
          else
            Enum.map(selected, fn {field, _value} -> field end)
          end
        end
    end
  end

  defp do_parse_hscan_opts([], match, count), do: {:ok, match, count}

  defp do_parse_hscan_opts([opt, value | rest], match, count) do
    case String.upcase(opt) do
      "MATCH" ->
        do_parse_hscan_opts(rest, value, count)

      "COUNT" ->
        case Integer.parse(value) do
          {n, ""} when n > 0 -> do_parse_hscan_opts(rest, match, n)
          _ -> {:error, "ERR value is not an integer or out of range"}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp do_parse_hscan_opts([_ | _], _match, _count) do
    {:error, "ERR syntax error"}
  end

  defp hash_pairs_to_flat_list([{field, value} | pairs], acc) do
    hash_pairs_to_flat_list(pairs, [value, field | acc])
  end

  defp hash_pairs_to_flat_list([], acc), do: Enum.reverse(acc)
end
