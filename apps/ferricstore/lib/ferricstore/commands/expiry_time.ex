defmodule Ferricstore.Commands.ExpiryTime do
  @moduledoc false

  alias Ferricstore.CommandTime

  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808
  @max_hlc_physical_ms 281_474_976_710_655
  @max_relative_ms @max_int64 - @max_hlc_physical_ms

  @spec parse_integer(binary()) :: {:ok, integer()} | :error
  def parse_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= @min_int64 and parsed <= @max_int64 -> {:ok, parsed}
      _invalid -> :error
    end
  end

  @spec relative(integer(), pos_integer()) :: {:ok, integer()} | :error
  def relative(value, multiplier)
      when is_integer(value) and is_integer(multiplier) and multiplier > 0 do
    delta = value * multiplier

    if value >= @min_int64 and value <= @max_int64 and delta >= @min_int64 and
         delta <= @max_relative_ms do
      expire_at_ms = CommandTime.now_ms() + delta

      if expire_at_ms >= @min_int64 and expire_at_ms <= @max_int64,
        do: {:ok, expire_at_ms},
        else: :error
    else
      :error
    end
  end

  def relative(_value, _multiplier), do: :error

  @spec absolute(integer(), pos_integer()) :: {:ok, integer()} | :error
  def absolute(value, multiplier)
      when is_integer(value) and is_integer(multiplier) and multiplier > 0 do
    expire_at_ms = value * multiplier

    if value >= @min_int64 and value <= @max_int64 and expire_at_ms >= @min_int64 and
         expire_at_ms <= @max_int64,
       do: {:ok, expire_at_ms},
       else: :error
  end

  def absolute(_value, _multiplier), do: :error

  @spec persisted?(term()) :: boolean()
  def persisted?(expire_at_ms),
    do: is_integer(expire_at_ms) and expire_at_ms >= 0 and expire_at_ms <= @max_int64
end
