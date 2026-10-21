defmodule Ferricstore.Commands.Hash.Helpers do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.CollectionScan
  alias Ferricstore.Commands.ExpiryTime

  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808
  # Redis's latest field expiry, in Unix milliseconds (2^46 - 1, in 4199).
  @max_field_expire_ms 70_368_744_177_663
  @field_expire_conditions %{"NX" => :nx, "XX" => :xx, "GT" => :gt, "LT" => :lt}

  @spec even_length?(list()) :: boolean()
  def even_length?([]), do: true
  def even_length?([_, _ | rest]), do: even_length?(rest)
  def even_length?(_), do: false

  @spec parse_integer_value(binary() | nil) :: {:ok, integer()} | :error
  def parse_integer_value(nil), do: {:ok, 0}

  def parse_integer_value(str) when is_binary(str) do
    case Integer.parse(str) do
      {int, ""} when int >= @min_int64 and int <= @max_int64 -> {:ok, int}
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
      {float, ""} when is_float(float) -> {:ok, float}
      _ -> :error
    end
  rescue
    ArgumentError -> :error
  end

  @spec checked_float_add(float(), float()) :: {:ok, float()} | :overflow
  def checked_float_add(left, right) when is_float(left) and is_float(right) do
    {:ok, left + right}
  rescue
    ArithmeticError -> :overflow
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
  def parse_expiry_mode("EX", value_str), do: parse_expiry(value_str, 1_000, :relative)
  def parse_expiry_mode("PX", value_str), do: parse_expiry(value_str, 1, :relative)
  def parse_expiry_mode("EXAT", value_str), do: parse_expiry(value_str, 1_000, :absolute)
  def parse_expiry_mode("PXAT", value_str), do: parse_expiry(value_str, 1, :absolute)

  @spec relative_expire_at(integer(), pos_integer()) ::
          {:ok, integer()} | {:error, binary()}
  def relative_expire_at(value, multiplier) when is_integer(value) and value > 0 do
    normalize_expiry_result(ExpiryTime.relative(value, multiplier))
  end

  def relative_expire_at(_value, _multiplier), do: integer_range_error()

  @spec absolute_expire_at(integer(), pos_integer()) ::
          {:ok, integer()} | {:error, binary()}
  def absolute_expire_at(value, multiplier) when is_integer(value) and value > 0 do
    normalize_expiry_result(ExpiryTime.absolute(value, multiplier))
  end

  def absolute_expire_at(_value, _multiplier), do: integer_range_error()

  @type field_expire_condition :: nil | :nx | :xx | :gt | :lt
  @type field_time_unit :: :ex | :px | :exat | :pxat
  @type getex_mode :: :none | :persist | {field_time_unit(), non_neg_integer()}
  @type setex_condition :: nil | :fnx | :fxx
  @type setex_expiry :: nil | :keepttl | {field_time_unit(), non_neg_integer()}

  @field_expire_units %{
    "hexpire" => :ex,
    "hpexpire" => :px
  }
  @time_options %{"EX" => :ex, "PX" => :px, "EXAT" => :exat, "PXAT" => :pxat}

  @doc """
  Parses the arguments after the key of HEXPIRE and HPEXPIRE (`command`,
  lowercase), as Redis 8 does: the TTL, then NX, XX, GT or LT and the FIELDS
  block in any order.
  """
  @spec parse_field_expire(binary(), binary(), [binary()]) ::
          {:ok, non_neg_integer(), field_expire_condition(), [binary()]} | {:error, binary()}
  def parse_field_expire(command, ttl, args) do
    with {:ok, ttl} <- parse_field_time(command, field_expire_unit(command), ttl),
         {:ok, condition, fields} <- parse_field_expire_args(args, nil, nil) do
      {:ok, ttl, condition, fields}
    end
  end

  @doc "The time unit of HEXPIRE or HPEXPIRE (lowercase)."
  @spec field_expire_unit(binary()) :: field_time_unit()
  def field_expire_unit(command), do: Map.fetch!(@field_expire_units, command)

  @doc """
  Where a parsed field expiry `time` in `unit` expires, in Unix
  milliseconds; a time at or before now deletes the field.
  """
  @spec field_expire_at(field_time_unit(), non_neg_integer()) ::
          {:ok, integer()} | {:error, binary()}
  def field_expire_at(unit, time) when unit in [:exat, :pxat], do: {:ok, time * unit_ms(unit)}
  def field_expire_at(_unit, 0), do: {:ok, CommandTime.now_ms()}
  def field_expire_at(unit, ttl), do: relative_expire_at(ttl, unit_ms(unit))

  @doc """
  Parses HGETEX's arguments after the key, as Redis 8 does: EX, PX, EXAT,
  PXAT or PERSIST and the FIELDS block in any order.
  """
  @spec parse_getex([binary()]) :: {:ok, getex_mode(), [binary()]} | {:error, binary()}
  def parse_getex(args), do: parse_getex_args(args, nil, nil)

  @doc """
  Parses HSETEX's Redis 8 arguments after the key: FNX or FXX, EX, PX,
  EXAT, PXAT or KEEPTTL, and the FIELDS block of field-value pairs, in any
  order.
  """
  @spec parse_setex([binary()]) ::
          {:ok, setex_condition(), setex_expiry(), [binary()]} | {:error, binary()}
  def parse_setex(args), do: parse_setex_args(args, nil, nil, nil)

  @doc "Whether HSETEX's argument after the key starts Redis 8's form."
  @spec setex_option?(binary()) :: boolean()
  def setex_option?(arg), do: String.upcase(arg) in ~w(FNX FXX EX PX EXAT PXAT KEEPTTL FIELDS)

  @doc """
  Parses `FIELDS count field [field ...]` for HTTL, HPTTL, HPERSIST,
  HEXPIRETIME and HGETDEL, with Redis's errors.
  """
  @spec parse_field_list([binary()]) :: {:ok, [binary()]} | {:error, binary()}
  def parse_field_list([keyword, count | fields]) do
    with :ok <- fields_keyword(keyword),
         {:ok, count} <- field_list_count(count) do
      if length(fields) == count,
        do: {:ok, fields},
        else: {:error, "ERR The `numfields` parameter must match the number of arguments"}
    end
  end

  def parse_field_list(_args), do: fields_keyword(nil)

  defp fields_keyword(keyword) when is_binary(keyword) do
    if String.upcase(keyword) == "FIELDS", do: :ok, else: fields_keyword(nil)
  end

  defp fields_keyword(_keyword),
    do: {:error, "ERR Mandatory argument FIELDS is missing or not at the right position"}

  defp field_list_count(count) do
    with :error <- strict_positive_integer(count),
         do: {:error, "ERR Number of fields must be a positive integer"}
  end

  defp unit_ms(unit) when unit in [:ex, :exat], do: 1_000
  defp unit_ms(unit) when unit in [:px, :pxat], do: 1

  defp parse_field_time(command, unit, raw) do
    case ExpiryTime.parse_integer(raw) do
      {:ok, time} when time < 0 ->
        {:error, "ERR invalid expire time, must be >= 0"}

      {:ok, time} ->
        if time * unit_ms(unit) > latest_field_time_ms(unit),
          do: {:error, "ERR invalid expire time in '#{command}' command"},
          else: {:ok, time}

      :error ->
        integer_range_error()
    end
  end

  defp latest_field_time_ms(unit) when unit in [:exat, :pxat], do: @max_field_expire_ms
  defp latest_field_time_ms(_unit), do: @max_field_expire_ms - CommandTime.now_ms()

  defp parse_getex_args([], _mode, nil), do: {:error, "ERR wrong number of arguments"}
  defp parse_getex_args([], mode, fields), do: {:ok, mode || :none, fields}

  defp parse_getex_args([arg | rest], mode, fields) do
    case {String.upcase(arg), mode} do
      {"FIELDS", _mode} when fields != nil ->
        {:error, "ERR FIELDS keyword specified multiple times"}

      {"FIELDS", _mode} ->
        with {:ok, fields, rest} <- take_counted(rest, 1),
             do: parse_getex_args(rest, mode, fields)

      {"PERSIST", nil} ->
        parse_getex_args(rest, :persist, fields)

      {option, nil} when is_map_key(@time_options, option) ->
        with {:ok, expiry, rest} <- take_time("hgetex", @time_options[option], rest),
             do: parse_getex_args(rest, expiry, fields)

      {option, _mode} when option == "PERSIST" or is_map_key(@time_options, option) ->
        {:error, "ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified"}

      _unknown ->
        {:error, "ERR unknown argument: #{arg}"}
    end
  end

  defp parse_setex_args([], _condition, _expiry, nil),
    do: {:error, "ERR wrong number of arguments"}

  defp parse_setex_args([], condition, expiry, pairs), do: {:ok, condition, expiry, pairs}

  defp parse_setex_args([arg | rest], condition, expiry, pairs) do
    case String.upcase(arg) do
      "FIELDS" when pairs != nil ->
        {:error, "ERR FIELDS keyword specified multiple times"}

      "FIELDS" ->
        with {:ok, pairs, rest} <- take_counted(rest, 2),
             do: parse_setex_args(rest, condition, expiry, pairs)

      flag when flag in ["FNX", "FXX"] and condition != nil ->
        {:error, "ERR Only one of FXX or FNX arguments can be specified"}

      flag when flag in ["FNX", "FXX"] ->
        parse_setex_args(rest, if(flag == "FNX", do: :fnx, else: :fxx), expiry, pairs)

      option when (option == "KEEPTTL" or is_map_key(@time_options, option)) and expiry != nil ->
        {:error, "ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified"}

      "KEEPTTL" ->
        parse_setex_args(rest, condition, :keepttl, pairs)

      option when is_map_key(@time_options, option) ->
        with {:ok, expiry, rest} <- take_time("hsetex", @time_options[option], rest),
             do: parse_setex_args(rest, condition, expiry, pairs)

      _unknown ->
        {:error, "ERR unknown argument: #{arg}"}
    end
  end

  defp take_time(command, unit, [raw | rest]) do
    with {:ok, time} <- parse_field_time(command, unit, raw), do: {:ok, {unit, time}, rest}
  end

  defp take_time(_command, _unit, []), do: integer_range_error()

  # HGETEX's and HSETEX's FIELDS block: a count, then that many fields (or
  # field-value pairs, `width` 2).
  defp take_counted([count | args], width) do
    case strict_positive_integer(count) do
      {:ok, count} ->
        {items, rest} = Enum.split(args, count * width)

        if length(items) == count * width,
          do: {:ok, items, rest},
          else: {:error, "ERR wrong number of arguments"}

      :error ->
        {:error, "ERR invalid number of fields"}
    end
  end

  defp take_counted([], _width), do: {:error, "ERR wrong number of arguments"}

  defp parse_field_expire_args([], _condition, nil), do: {:error, "ERR missing FIELDS argument"}
  defp parse_field_expire_args([], condition, fields), do: {:ok, condition, fields}

  defp parse_field_expire_args([arg | rest], condition, fields) do
    case String.upcase(arg) do
      "FIELDS" when fields != nil ->
        {:error, "ERR FIELDS keyword specified multiple times"}

      "FIELDS" ->
        with {:ok, fields, rest} <- take_counted_fields(rest),
             do: parse_field_expire_args(rest, condition, fields)

      option ->
        case Map.fetch(@field_expire_conditions, option) do
          {:ok, flag} when condition in [nil, flag] ->
            parse_field_expire_args(rest, flag, fields)

          {:ok, _other_flag} ->
            {:error, "ERR Multiple condition flags specified"}

          :error ->
            {:error, "ERR unknown argument: #{arg}"}
        end
    end
  end

  defp take_counted_fields([count, _field | _rest] = args) do
    case strict_positive_integer(count) do
      {:ok, count} ->
        {fields, rest} = Enum.split(tl(args), count)

        if length(fields) == count,
          do: {:ok, fields, rest},
          else: {:error, "ERR wrong number of arguments"}

      :error ->
        {:error, "ERR Parameter `numFields` should be greater than 0"}
    end
  end

  defp take_counted_fields(_args),
    do: {:error, "ERR FIELDS requires at least numfields and one field argument"}

  # Redis's integer syntax: no sign, no leading zero.
  defp strict_positive_integer(<<digit, _rest::binary>> = value) when digit in ?1..?9 do
    case Integer.parse(value) do
      {int, ""} when int <= @max_int64 -> {:ok, int}
      _invalid -> :error
    end
  end

  defp strict_positive_integer(_value), do: :error

  @spec parse_cursor(binary()) ::
          {:ok, CollectionScan.cursor()} | {:error, binary()}
  def parse_cursor(cursor_str), do: CollectionScan.parse_cursor(cursor_str)

  @spec parse_hscan_opts([binary()]) :: {:ok, binary() | nil, pos_integer()} | {:error, binary()}
  def parse_hscan_opts(opts), do: do_parse_hscan_opts(opts, nil, 10)

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

  defp parse_expiry(raw, multiplier, mode) do
    with {:ok, value} when value > 0 <- ExpiryTime.parse_integer(raw) do
      case mode do
        :relative -> relative_expire_at(value, multiplier)
        :absolute -> absolute_expire_at(value, multiplier)
      end
    else
      _invalid -> integer_range_error()
    end
  end

  defp normalize_expiry_result({:ok, expire_at_ms}), do: {:ok, expire_at_ms}
  defp normalize_expiry_result(:error), do: integer_range_error()

  defp integer_range_error, do: {:error, "ERR value is not an integer or out of range"}

  defp hash_pairs_to_flat_list([{field, value} | pairs], acc) do
    hash_pairs_to_flat_list(pairs, [value, field | acc])
  end

  defp hash_pairs_to_flat_list([], acc), do: Enum.reverse(acc)
end
