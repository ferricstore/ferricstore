defmodule Ferricstore.Bench.QuickWriteBenchTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  test "write throughput - 50 writers, 10 seconds" do
    prefix = "qb_#{System.unique_integer([:positive])}"
    stop = :atomics.new(1, [])

    counter = :counters.new(1, [:atomics])

    writers =
      for i <- 1..50 do
        Task.async(fn ->
          l = fn l, n ->
            if :atomics.get(stop, 1) == 1, do: throw(:stop)
            FerricStore.set("#{prefix}:#{i}:#{n}", "value")
            :counters.add(counter, 1, 1)
            l.(l, n + 1)
          end

          try do
            l.(l, 0)
          catch
            :throw, :stop -> :ok
          end
        end)
      end

    Process.sleep(10_000)
    :atomics.put(stop, 1, 1)
    Task.await_many(writers, 30_000)
    total = :counters.get(counter, 1)

    IO.puts("\n=== WRITE THROUGHPUT: #{div(total, 10)} writes/sec (#{total} total) ===\n")
    assert total > 0
  end
end
