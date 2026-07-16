defmodule Ferricstore.Commands.Strings.SetOptions do
  @moduledoc false

  alias Ferricstore.Commands.ExpiryTime

  def parse(opts, default), do: parse(opts, default, default)

  defp parse([], acc, _default) do
    if acc.nx and acc.xx do
      {:error, "ERR XX and NX options at the same time are not compatible"}
    else
      {:ok, acc}
    end
  end

  defp parse(["NX" | rest], acc, default), do: parse(rest, %{acc | nx: true}, default)
  defp parse(["XX" | rest], acc, default), do: parse(rest, %{acc | xx: true}, default)
  defp parse(["GET" | rest], acc, default), do: parse(rest, %{acc | get: true}, default)

  defp parse(["KEEPTTL" | rest], acc, default) do
    if acc.has_expiry do
      {:error, "ERR syntax error"}
    else
      parse(rest, %{acc | keepttl: true, has_expiry: true}, default)
    end
  end

  defp parse(["EX", secs_str | rest], acc, default) do
    parse_expiry(rest, acc, default, secs_str, 1000, "set")
  end

  defp parse(["PX", ms_str | rest], acc, default) do
    parse_expiry(rest, acc, default, ms_str, 1, "set")
  end

  defp parse(["EXAT", ts_str | rest], acc, default) do
    parse_expiry(rest, acc, default, ts_str, 1000, "set", :absolute)
  end

  defp parse(["PXAT", ts_str | rest], acc, default) do
    parse_expiry(rest, acc, default, ts_str, 1, "set", :absolute)
  end

  defp parse([unknown | rest], acc, default) when is_binary(unknown) do
    case String.upcase(unknown) do
      ^unknown -> {:error, "ERR syntax error, option '#{unknown}' not recognized"}
      normalized -> parse([normalized | rest], acc, default)
    end
  end

  defp parse_expiry(rest, acc, default, raw, multiplier, command, mode \\ :relative) do
    if acc.has_expiry do
      {:error, "ERR syntax error"}
    else
      case ExpiryTime.parse_integer(raw) do
        {:ok, value} when value <= 0 ->
          {:error, "ERR invalid expire time in '#{command}' command"}

        {:ok, value} ->
          case expiry_time(value, multiplier, mode) do
            {:ok, expire_at_ms} ->
              parse(rest, %{acc | expire_at_ms: expire_at_ms, has_expiry: true}, default)

            :error ->
              integer_range_error()
          end

        :error ->
          integer_range_error()
      end
    end
  end

  def from_ast(opts, default), do: from_ast(opts, default, default)

  defp from_ast([], acc, _default) do
    if acc.nx and acc.xx do
      {:error, "ERR XX and NX options at the same time are not compatible"}
    else
      {:ok, acc}
    end
  end

  defp from_ast([:nx | rest], acc, default), do: from_ast(rest, %{acc | nx: true}, default)
  defp from_ast([:xx | rest], acc, default), do: from_ast(rest, %{acc | xx: true}, default)
  defp from_ast([:get | rest], acc, default), do: from_ast(rest, %{acc | get: true}, default)

  defp from_ast([:keepttl | rest], acc, default) do
    if acc.has_expiry do
      {:error, "ERR syntax error"}
    else
      from_ast(rest, %{acc | keepttl: true, has_expiry: true}, default)
    end
  end

  defp from_ast([{:ex, seconds} | rest], acc, default) when is_integer(seconds) do
    parse_ast_expiry(rest, acc, default, seconds, 1000, :relative)
  end

  defp from_ast([{:px, ms} | rest], acc, default) when is_integer(ms) do
    parse_ast_expiry(rest, acc, default, ms, 1, :relative)
  end

  defp from_ast([{:exat, seconds} | rest], acc, default) when is_integer(seconds) do
    parse_ast_expiry(rest, acc, default, seconds, 1000, :absolute)
  end

  defp from_ast([{:pxat, ms} | rest], acc, default) when is_integer(ms) do
    parse_ast_expiry(rest, acc, default, ms, 1, :absolute)
  end

  defp from_ast(_opts, _acc, _default), do: {:error, "ERR syntax error"}

  defp parse_ast_expiry(_rest, _acc, _default, value, _multiplier, _mode)
       when value <= 0,
       do: {:error, "ERR invalid expire time in 'set' command"}

  defp parse_ast_expiry(_rest, %{has_expiry: true}, _default, _value, _multiplier, _mode),
    do: {:error, "ERR syntax error"}

  defp parse_ast_expiry(rest, acc, default, value, multiplier, mode) do
    case expiry_time(value, multiplier, mode) do
      {:ok, expire_at_ms} ->
        from_ast(rest, %{acc | expire_at_ms: expire_at_ms, has_expiry: true}, default)

      :error ->
        integer_range_error()
    end
  end

  defp expiry_time(value, multiplier, :relative), do: ExpiryTime.relative(value, multiplier)
  defp expiry_time(value, multiplier, :absolute), do: ExpiryTime.absolute(value, multiplier)

  defp integer_range_error, do: {:error, "ERR value is not an integer or out of range"}
end
