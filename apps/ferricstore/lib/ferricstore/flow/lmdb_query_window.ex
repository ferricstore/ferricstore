defmodule Ferricstore.Flow.LMDBQueryWindow do
  @moduledoc false

  @u64_decimal_zero_pad "00000000000000000000"

  def time_seek_key(prefix, ms) when is_binary(prefix) and is_integer(ms),
    do: prefix <> pad_u64(ms)

  def time_upper_seek_key(prefix, ms) when is_binary(prefix) and is_integer(ms),
    do: prefix <> pad_u64(ms) <> <<255>>

  def pad_u64(value) when is_integer(value) and value >= 0 do
    encoded = Integer.to_string(value)

    case byte_size(encoded) do
      size when size < 20 -> binary_part(@u64_decimal_zero_pad, 0, 20 - size) <> encoded
      _size -> encoded
    end
  end

  def query_scan_count(count, default_scan_limit) when is_integer(count) and count > 0 do
    max_scan =
      Application.get_env(
        :ferricstore,
        :flow_lmdb_query_scan_limit,
        default_scan_limit
      )

    max_scan =
      case max_scan do
        value when is_integer(value) and value > 0 -> value
        _ -> default_scan_limit
      end

    scan_count(count, max_scan)
  end

  def history_query_scan_count(count, true, _max_history_events)
      when is_integer(count) and count > 0,
      do: count

  def history_query_scan_count(count, false, max_history_events)
      when is_integer(count) and count > 0 do
    max_scan =
      Application.get_env(
        :ferricstore,
        :flow_lmdb_history_query_scan_limit,
        max_history_events
      )

    max_scan =
      case max_scan do
        value when is_integer(value) and value > 0 -> min(value, max_history_events)
        _ -> max_history_events
      end

    scan_count(count, max_scan)
  end

  defp scan_count(count, max_scan) do
    count
    |> Kernel.+(64)
    |> max(count * 4)
    |> min(max_scan)
    |> max(count)
  end
end
