defmodule Ferricstore.Commands.Set.Scan do
  @moduledoc false

  alias Ferricstore.Commands.CollectionScan

  def typed_scan_opts(opts), do: do_typed_scan_opts(opts, nil, 10)

  def parse_cursor(cursor_str), do: CollectionScan.parse_cursor(cursor_str)

  def parse_sscan_opts(opts), do: do_parse_sscan_opts(opts, nil, 10)

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
