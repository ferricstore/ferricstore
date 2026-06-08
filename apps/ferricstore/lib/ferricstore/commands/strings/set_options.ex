defmodule Ferricstore.Commands.Strings.SetOptions do
  @moduledoc false

  alias Ferricstore.CommandTime

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
      with {value, ""} <- Integer.parse(raw),
           true <- value > 0 do
        expire_at_ms =
          case mode do
            :absolute -> value * multiplier
            :relative -> CommandTime.now_ms() + value * multiplier
          end

        parse(rest, %{acc | expire_at_ms: expire_at_ms, has_expiry: true}, default)
      else
        false -> {:error, "ERR invalid expire time in '#{command}' command"}
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def from_ast(opts, default), do: from_ast(opts, default, default)

  defp from_ast([], acc, _default), do: {:ok, acc}
  defp from_ast([:nx | rest], acc, default), do: from_ast(rest, %{acc | nx: true}, default)
  defp from_ast([:xx | rest], acc, default), do: from_ast(rest, %{acc | xx: true}, default)
  defp from_ast([:get | rest], acc, default), do: from_ast(rest, %{acc | get: true}, default)

  defp from_ast([:keepttl | rest], acc, default) do
    from_ast(rest, %{acc | keepttl: true, has_expiry: true}, default)
  end

  defp from_ast([{:ex, seconds} | rest], acc, default) when is_integer(seconds) do
    from_ast(
      rest,
      %{acc | expire_at_ms: CommandTime.now_ms() + seconds * 1000, has_expiry: true},
      default
    )
  end

  defp from_ast([{:px, ms} | rest], acc, default) when is_integer(ms) do
    from_ast(rest, %{acc | expire_at_ms: CommandTime.now_ms() + ms, has_expiry: true}, default)
  end

  defp from_ast([{:exat, seconds} | rest], acc, default) when is_integer(seconds) do
    from_ast(rest, %{acc | expire_at_ms: seconds * 1000, has_expiry: true}, default)
  end

  defp from_ast([{:pxat, ms} | rest], acc, default) when is_integer(ms) do
    from_ast(rest, %{acc | expire_at_ms: ms, has_expiry: true}, default)
  end

  defp from_ast(_opts, _acc, _default), do: {:error, "ERR syntax error"}
end
