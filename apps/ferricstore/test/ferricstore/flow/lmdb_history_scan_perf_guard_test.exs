defmodule Ferricstore.Flow.LMDBHistoryScanPerfGuardTest do
  @moduledoc """
  A history scan reads past its page only to finish the page's last
  millisecond. Where every event has its own millisecond that costs one empty
  bounded read, not another page of entries.
  """
  use ExUnit.Case, async: false
  @moduletag :perf_guard

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDB.HistoryScan

  @history_key "f:{f}:h:scan-cost"
  @reads 2_000
  @max_ratio 6

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_history_scan_cost_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    events =
      for ms <- 1..2_000 do
        event_id = "#{ms}-#{ms}"
        {:put, LMDB.history_index_key(@history_key, event_id, ms), String.duplicate("v", 64)}
      end

    assert :ok = LMDB.write_batch(path, events)
    %{path: path, prefix: LMDB.history_index_prefix(@history_key)}
  end

  test "a page costs about a raw read of the same entries", %{path: path, prefix: prefix} do
    raw_us = best_us(fn -> LMDB.prefix_entries(path, prefix, 10, true) end)

    for {name, scan} <- [
          last: fn -> HistoryScan.last(path, @history_key, 10) end,
          first: fn -> HistoryScan.first(path, @history_key, 10) end,
          after_event: fn -> HistoryScan.after_event(path, @history_key, "500-500", 10) end
        ] do
      scan_us = best_us(scan)

      assert scan_us < raw_us * @max_ratio,
             "#{name} took #{scan_us} us for #{@reads} reads, raw #{raw_us} us"
    end
  end

  defp best_us(fun) do
    for _ <- 1..200, do: fun.()

    1..3
    |> Enum.map(fn _round ->
      {us, _result} = :timer.tc(fn -> for _ <- 1..@reads, do: fun.() end)
      us
    end)
    |> Enum.min()
  end
end
