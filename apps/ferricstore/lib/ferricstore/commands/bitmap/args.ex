defmodule Ferricstore.Commands.Bitmap.Args do
  @moduledoc false

  @max_bit_offset 4_294_967_295

  @spec check_bit_offset(integer()) :: :ok | {:error, binary()}
  def check_bit_offset(offset) when offset > @max_bit_offset do
    {:error, "ERR bit offset is not an integer or out of range"}
  end

  def check_bit_offset(_offset), do: :ok

  @spec parse_non_negative_integer(binary(), binary()) ::
          {:ok, non_neg_integer()} | {:error, binary()}
  def parse_non_negative_integer(str, label) do
    case Integer.parse(str) do
      {n, ""} when n >= 0 -> {:ok, n}
      {_n, ""} -> {:error, "ERR #{label} is not an integer or out of range"}
      _ -> {:error, "ERR #{label} is not an integer or out of range"}
    end
  end

  @spec parse_integer(binary()) :: {:ok, integer()} | {:error, binary()}
  def parse_integer(str) do
    case Integer.parse(str) do
      {n, ""} -> {:ok, n}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  @spec parse_bit_value(binary()) :: {:ok, 0 | 1} | {:error, binary()}
  def parse_bit_value("0"), do: {:ok, 0}
  def parse_bit_value("1"), do: {:ok, 1}

  def parse_bit_value(_) do
    {:error, "ERR bit is not an integer or out of range"}
  end

  @spec parse_bitcount_mode([binary()]) :: {:ok, :byte | :bit} | {:error, binary()}
  def parse_bitcount_mode([]), do: {:ok, :byte}

  def parse_bitcount_mode([mode_str]) do
    case String.upcase(mode_str) do
      "BYTE" -> {:ok, :byte}
      "BIT" -> {:ok, :bit}
      _ -> {:error, "ERR syntax error"}
    end
  end

  def parse_bitcount_mode(_), do: {:error, "ERR syntax error"}
end
