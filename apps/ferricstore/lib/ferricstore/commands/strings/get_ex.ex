defmodule Ferricstore.Commands.Strings.GetEx do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.Strings.Compound
  alias Ferricstore.Store.Ops

  def do_getex(key, opts, store) do
    case parse_getex_opts(opts) do
      {:ok, expire_at_ms} -> getex_parsed(key, expire_at_ms, store)
      {:error, _} = err -> err
    end
  end

  def getex_parsed(key, expire_at_ms, store) do
    case Compound.ensure_string_key(key, store) do
      :ok -> Ops.getex(store, key, expire_at_ms)
      {:error, _} = error -> error
    end
  end

  defp parse_getex_opts(["PERSIST"]), do: {:ok, 0}

  defp parse_getex_opts(["EX", secs_str]) do
    case Integer.parse(secs_str) do
      {secs, ""} when secs > 0 -> {:ok, CommandTime.now_ms() + secs * 1_000}
      {_secs, ""} -> {:error, "ERR invalid expire time in 'getex' command"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(["PX", ms_str]) do
    case Integer.parse(ms_str) do
      {ms, ""} when ms > 0 -> {:ok, CommandTime.now_ms() + ms}
      {_ms, ""} -> {:error, "ERR invalid expire time in 'getex' command"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(["EXAT", ts_str]) do
    case Integer.parse(ts_str) do
      {ts, ""} when ts > 0 -> {:ok, ts * 1_000}
      {_ts, ""} -> {:error, "ERR invalid expire time in 'getex' command"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(["PXAT", ts_str]) do
    case Integer.parse(ts_str) do
      {ts, ""} when ts > 0 -> {:ok, ts}
      {_ts, ""} -> {:error, "ERR invalid expire time in 'getex' command"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts([opt | rest]) when is_binary(opt) do
    case String.upcase(opt) do
      ^opt -> {:error, "ERR syntax error"}
      normalized -> parse_getex_opts([normalized | rest])
    end
  end

  defp parse_getex_opts(_), do: {:error, "ERR syntax error"}
end
