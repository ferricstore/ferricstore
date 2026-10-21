defmodule Ferricstore.Commands.Strings.GetEx do
  @moduledoc false

  alias Ferricstore.Commands.ExpiryTime
  alias Ferricstore.Commands.Strings.Compound
  alias Ferricstore.Store.Ops

  @syntax_error {:error, "ERR syntax error"}
  @invalid_expire_time {:error, "ERR invalid expire time in 'getex' command"}

  def do_getex(key, opts, store) do
    case parse_getex_opts(opts) do
      {:ok, expire_at_ms} -> getex_parsed(key, expire_at_ms, store)
      @syntax_error -> @syntax_error
      {:error, _reason} = value_error -> after_lookup(key, value_error, store)
    end
  end

  def getex_parsed(key, expire_at_ms, store) do
    if ExpiryTime.persisted?(expire_at_ms) do
      case Compound.ensure_string_key(key, store) do
        :ok -> Ops.getex(store, key, expire_at_ms)
        {:error, _} = error -> error
      end
    else
      after_lookup(key, @invalid_expire_time, store)
    end
  end

  @doc """
  Redis checks GETEX's option syntax, then looks the key up, then checks
  the expiry value: a missing key answers nil and another type WRONGTYPE
  before a bad value's error.
  """
  def after_lookup(key, {:error, _reason} = value_error, store) do
    with :ok <- Compound.ensure_string_key(key, store) do
      if Ops.exists?(store, key) == true, do: value_error, else: nil
    end
  end

  defp parse_getex_opts(["PERSIST"]), do: {:ok, 0}

  defp parse_getex_opts(["EX", secs_str]) do
    parse_expiry(secs_str, 1_000, :relative)
  end

  defp parse_getex_opts(["PX", ms_str]) do
    parse_expiry(ms_str, 1, :relative)
  end

  defp parse_getex_opts(["EXAT", ts_str]) do
    parse_expiry(ts_str, 1_000, :absolute)
  end

  defp parse_getex_opts(["PXAT", ts_str]) do
    parse_expiry(ts_str, 1, :absolute)
  end

  defp parse_getex_opts([opt | rest]) when is_binary(opt) do
    case String.upcase(opt) do
      ^opt -> {:error, "ERR syntax error"}
      normalized -> parse_getex_opts([normalized | rest])
    end
  end

  defp parse_getex_opts(_), do: {:error, "ERR syntax error"}

  defp parse_expiry(raw, multiplier, mode) do
    case ExpiryTime.parse_integer(raw) do
      {:ok, value} when value <= 0 ->
        @invalid_expire_time

      {:ok, value} ->
        case expiry_time(value, multiplier, mode) do
          {:ok, expire_at_ms} -> {:ok, expire_at_ms}
          :error -> @invalid_expire_time
        end

      :error ->
        integer_range_error()
    end
  end

  defp expiry_time(value, multiplier, :relative), do: ExpiryTime.relative(value, multiplier)
  defp expiry_time(value, multiplier, :absolute), do: ExpiryTime.absolute(value, multiplier)

  defp integer_range_error, do: {:error, "ERR value is not an integer or out of range"}
end
