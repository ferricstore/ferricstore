defmodule Ferricstore.Commands.Set.Scan do
  @moduledoc false

  def typed_scan_opts(opts), do: do_typed_scan_opts(opts, nil, 10)

  def parse_cursor(cursor_str) do
    case Integer.parse(cursor_str) do
      {cursor, ""} when cursor >= 0 -> {:ok, cursor}
      _ -> {:error, "ERR invalid cursor"}
    end
  end

  def parse_sscan_opts(opts), do: do_parse_sscan_opts(opts, nil, 10)

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

  defp do_typed_scan_opts([], match, count), do: {:ok, match, count}

  defp do_typed_scan_opts([{:match, pattern} | rest], _match, count) when is_binary(pattern) do
    do_typed_scan_opts(rest, pattern, count)
  end

  defp do_typed_scan_opts([{:count, count} | rest], match, _count)
       when is_integer(count) and count > 0 do
    do_typed_scan_opts(rest, match, count)
  end

  defp do_typed_scan_opts(_opts, _match, _count), do: {:error, "ERR syntax error"}

  defp do_parse_sscan_opts([], match, count), do: {:ok, match, count}

  defp do_parse_sscan_opts([opt, value | rest], match, count) do
    case String.upcase(opt) do
      "MATCH" ->
        do_parse_sscan_opts(rest, value, count)

      "COUNT" ->
        case Integer.parse(value) do
          {n, ""} when n > 0 -> do_parse_sscan_opts(rest, match, n)
          _ -> {:error, "ERR value is not an integer or out of range"}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp do_parse_sscan_opts([_ | _], _match, _count) do
    {:error, "ERR syntax error"}
  end
end
