defmodule FerricstoreServer.Health.Dashboard.Format do
  @moduledoc false

  require Logger

  def sampled_tag(1), do: ""

  def sampled_tag(rate) do
    ~s(<span class="sampled-tag" title="Estimated from 1:#{rate} sampling">~1:#{rate}</span>)
  end

  def hotcold_has_samples?(hotcold) do
    case Map.get(hotcold, :total_lookups, 0) do
      value when is_number(value) -> value > 0
      _ -> false
    end
  end

  def bounded_sample_label(filtered, sampled, scan_cap)
      when is_integer(filtered) and is_integer(sampled) and is_integer(scan_cap) do
    if filtered == sampled do
      "#{format_number(filtered)} matching records · scan cap #{format_number(scan_cap)}"
    else
      "#{format_number(filtered)} matching of #{format_number(sampled)} sampled · scan cap #{format_number(scan_cap)}"
    end
  end

  def sampled_scan_label(sampled, scan_cap)
      when is_integer(sampled) and is_integer(scan_cap) do
    "#{format_number(sampled)} sampled records · scan cap #{format_number(scan_cap)}"
  end

  def accessible_table(label, table_html) when is_binary(label) and is_binary(table_html) do
    ~s(<div class="table-scroll" role="region" aria-label="#{escape_attr(label)}" tabindex="0">#{table_html}</div>)
  end

  def hit_rate_color(ratio) do
    cond do
      ratio >= 90.0 -> "#3fb950"
      ratio >= 70.0 -> "#d29922"
      true -> "#f85149"
    end
  end

  def mem_bar_color(pct) do
    cond do
      pct >= 95.0 -> "#f85149"
      pct >= 85.0 -> "#da3633"
      pct >= 70.0 -> "#d29922"
      true -> "#3fb950"
    end
  end

  def format_rate(rate) when rate >= 1_000_000.0 do
    "#{Float.round(rate / 1_000_000, 1)}M"
  end

  def format_rate(rate) when rate >= 1_000.0 do
    "#{Float.round(rate / 1_000, 1)}K"
  end

  def format_rate(rate) do
    "#{Float.round(rate, 1)}"
  end

  def format_uptime(seconds) do
    days = div(seconds, 86_400)
    hours = div(rem(seconds, 86_400), 3_600)
    mins = div(rem(seconds, 3_600), 60)
    secs = rem(seconds, 60)

    cond do
      days > 0 -> "#{days}d #{hours}h #{mins}m"
      hours > 0 -> "#{hours}h #{mins}m #{secs}s"
      mins > 0 -> "#{mins}m #{secs}s"
      true -> "#{secs}s"
    end
  end

  def format_bytes(bytes) when bytes >= 1_073_741_824 do
    "#{Float.round(bytes / 1_073_741_824, 2)} GB"
  end

  def format_bytes(bytes) when bytes >= 1_048_576 do
    "#{Float.round(bytes / 1_048_576, 2)} MB"
  end

  def format_bytes(bytes) when bytes >= 1_024 do
    "#{Float.round(bytes / 1_024, 2)} KB"
  end

  def format_bytes(bytes), do: "#{bytes} B"

  def format_duration_us(duration_us) do
    "#{Float.round(duration_us / 1000.0, 2)} ms"
  end

  def format_number(n) when n >= 1_000_000 do
    "#{Float.round(n / 1_000_000, 2)}M"
  end

  def format_number(n) when n >= 1_000 do
    "#{Float.round(n / 1_000, 1)}K"
  end

  def format_number(n), do: Integer.to_string(n)

  def format_timestamp_us(timestamp_us) do
    timestamp_us
    |> DateTime.from_unix!(:microsecond)
    |> readable_utc_timestamp()
  end

  def format_timestamp_ms(timestamp_ms) do
    timestamp_ms
    |> DateTime.from_unix!(:millisecond)
    |> readable_utc_timestamp()
  end

  defp readable_utc_timestamp(datetime) do
    datetime
    |> DateTime.to_iso8601()
    |> String.replace("T", " ")
    |> String.replace_suffix("Z", " UTC")
  end

  def format_timestamp_ms_or_dash(timestamp_ms)
      when is_integer(timestamp_ms) and timestamp_ms > 0 do
    format_timestamp_ms(timestamp_ms)
  rescue
    _ -> "-"
  end

  def format_timestamp_ms_or_dash(_timestamp_ms), do: "-"

  def format_timeline_timestamp_ms(timestamp_ms)
      when is_integer(timestamp_ms) and timestamp_ms > 0 do
    timestamp_ms
    |> DateTime.from_unix!(:millisecond)
    |> readable_utc_timestamp()
    |> String.split(" ", parts: 2)
    |> List.last()
  rescue
    _ -> "-"
  end

  def format_timeline_timestamp_ms(_timestamp_ms), do: "-"

  def format_duration_ms(ms) when ms >= 86_400_000 do
    "#{Float.round(ms / 86_400_000, 1)}d"
  end

  def format_duration_ms(ms) when ms >= 3_600_000 do
    "#{Float.round(ms / 3_600_000, 1)}h"
  end

  def format_duration_ms(ms) when ms >= 60_000 do
    "#{Float.round(ms / 60_000, 1)}m"
  end

  def format_duration_ms(ms) when ms >= 1_000 do
    "#{Float.round(ms / 1_000, 1)}s"
  end

  def format_duration_ms(ms), do: "#{ms}ms"

  def dashboard_internal_error(message, reason),
    do: dashboard_internal_error(message, :error, reason)

  def dashboard_internal_error(message, kind, reason) do
    Logger.error(fn ->
      "FerricStore dashboard internal error: #{message}: #{inspect({kind, reason}, limit: 20)}"
    end)

    message
  end

  def info_icon(text, label \\ "Metric help") do
    attr = escape_attr(text)

    ~s(<button type="button" class="info-icon" aria-label="#{escape_attr(label)}" data-tooltip="#{attr}" title="#{attr}">i</button>)
  end

  def escape(str) when is_binary(str) do
    str
    |> valid_html_text()
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
  end

  def escape_attr(str), do: escape(str)

  defp valid_html_text(str) do
    if String.valid?(str) do
      str
    else
      inspect(str, binaries: :as_binaries, limit: 256, printable_limit: 256)
    end
  end

  def safe_ets_size(table) do
    try do
      case :ets.info(table, :size) do
        :undefined -> 0
        n when is_integer(n) -> n
        _ -> 0
      end
    rescue
      ArgumentError -> 0
    end
  end
end
