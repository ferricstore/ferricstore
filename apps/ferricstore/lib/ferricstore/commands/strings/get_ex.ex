defmodule Ferricstore.Commands.Strings.GetEx do
  @moduledoc false

  alias Ferricstore.Commands.ExpiryTime
  alias Ferricstore.Commands.Strings.Compound
  alias Ferricstore.Store.Ops

  def do_getex(key, opts, store) do
    case parse_getex_opts(opts) do
      {:ok, expire_at_ms} -> getex_parsed(key, expire_at_ms, store)
      {:error, _} = err -> err
    end
  end

  def getex_parsed(key, expire_at_ms, store) do
    if ExpiryTime.persisted?(expire_at_ms) do
      case Compound.ensure_string_key(key, store) do
        :ok -> Ops.getex(store, key, expire_at_ms)
        {:error, _} = error -> error
      end
    else
      integer_range_error()
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
        {:error, "ERR invalid expire time in 'getex' command"}

      {:ok, value} ->
        case expiry_time(value, multiplier, mode) do
          {:ok, expire_at_ms} -> {:ok, expire_at_ms}
          :error -> integer_range_error()
        end

      :error ->
        integer_range_error()
    end
  end

  defp expiry_time(value, multiplier, :relative), do: ExpiryTime.relative(value, multiplier)
  defp expiry_time(value, multiplier, :absolute), do: ExpiryTime.absolute(value, multiplier)

  defp integer_range_error, do: {:error, "ERR value is not an integer or out of range"}
end
