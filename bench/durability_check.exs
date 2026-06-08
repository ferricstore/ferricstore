# FerricStore Durability Verification
#
# Writes at 100 req/s for 2 minutes (12,000 total), then verifies EVERY
# write exists in both ETS (keydir) and on disk (Bitcask).
#
# Proves that quorum writes are truly durable — not just in memory.
#
# Usage:
#   mix run bench/durability_check.exs
#
# Expected: 12,000 writes, 0 missing from ETS, 0 missing from disk.

alias Ferricstore.Store.Router
alias Ferricstore.Bitcask.NIF

rate = String.to_integer(System.get_env("RATE", "100"))
duration_sec = String.to_integer(System.get_env("DURATION", "120"))
total = rate * duration_sec
payload = String.duplicate("x", 256)

ctx = FerricStore.Instance.get(:default)

IO.puts("=== FerricStore Durability Check ===")
IO.puts("Rate: #{rate} writes/sec")
IO.puts("Duration: #{duration_sec}s")
IO.puts("Total writes: #{total}")
IO.puts("Payload: #{byte_size(payload)} bytes")
IO.puts("Mode: quorum (Raft consensus + fdatasync)")
IO.puts("")

# ---------------------------------------------------------------------------
# Phase 1: Write at controlled rate
# ---------------------------------------------------------------------------

IO.puts("Phase 1: Writing #{total} keys at #{rate}/sec...")

start = System.monotonic_time(:millisecond)
errors = :counters.new(1, [:atomics])

for i <- 1..total do
  key = "durable:#{String.pad_leading(Integer.to_string(i), 6, "0")}"

  case Router.put(ctx, key, payload, 0) do
    :ok ->
      :ok

    {:error, reason} ->
      :counters.add(errors, 1, 1)

      if :counters.get(errors, 1) <= 5 do
        IO.puts("  ERROR on write #{i}: #{inspect(reason)}")
      end
  end

  # Rate limiting: sleep to maintain target rate
  elapsed = System.monotonic_time(:millisecond) - start
  expected_elapsed = div(i * 1000, rate)
  sleep_ms = expected_elapsed - elapsed

  if sleep_ms > 0, do: Process.sleep(sleep_ms)

  # Progress every 10 seconds
  if rem(i, rate * 10) == 0 do
    actual_elapsed = div(System.monotonic_time(:millisecond) - start, 1000)
    actual_rate = div(i * 1000, max(1, System.monotonic_time(:millisecond) - start))
    IO.puts("  #{i}/#{total} written (#{actual_elapsed}s elapsed, #{actual_rate} ops/sec actual)")
  end
end

write_elapsed = System.monotonic_time(:millisecond) - start
write_errors = :counters.get(errors, 1)

IO.puts("")
IO.puts("Write phase complete:")
IO.puts("  Duration: #{div(write_elapsed, 1000)}s")
IO.puts("  Errors: #{write_errors}/#{total}")
IO.puts("  Actual rate: #{div(total * 1000, max(1, write_elapsed))} ops/sec")

# Flush everything to disk
IO.puts("")
IO.puts("Flushing to disk...")
shard_count = ctx.shard_count

for i <- 0..(shard_count - 1) do
  shard_name = Router.shard_name(ctx, i)

  try do
    GenServer.call(shard_name, :flush, 30_000)
  catch
    _, _ -> :ok
  end
end

Ferricstore.Store.BitcaskWriter.flush_all(shard_count)
Process.sleep(1000)

# ---------------------------------------------------------------------------
# Phase 2: Verify every key in ETS (keydir)
# ---------------------------------------------------------------------------

IO.puts("")
IO.puts("Phase 2: Verifying #{total} keys in ETS...")

ets_missing = :counters.new(1, [:atomics])
ets_wrong_value = :counters.new(1, [:atomics])

for i <- 1..total do
  key = "durable:#{String.pad_leading(Integer.to_string(i), 6, "0")}"
  value = Router.get(ctx, key)

  cond do
    value == nil ->
      :counters.add(ets_missing, 1, 1)

      if :counters.get(ets_missing, 1) <= 5 do
        IO.puts("  MISSING in ETS: #{key}")
      end

    value != payload ->
      :counters.add(ets_wrong_value, 1, 1)

      if :counters.get(ets_wrong_value, 1) <= 5 do
        IO.puts("  WRONG VALUE in ETS: #{key} (got #{byte_size(value)} bytes)")
      end

    true ->
      :ok
  end
end

ets_miss = :counters.get(ets_missing, 1)
ets_wrong = :counters.get(ets_wrong_value, 1)

IO.puts("ETS verification:")
IO.puts("  Missing: #{ets_miss}/#{total}")
IO.puts("  Wrong value: #{ets_wrong}/#{total}")
IO.puts("  OK: #{total - ets_miss - ets_wrong}/#{total}")

# ---------------------------------------------------------------------------
# Phase 3: Verify every key on disk (Bitcask files)
# ---------------------------------------------------------------------------

IO.puts("")
IO.puts("Phase 3: Verifying #{total} keys on disk (Bitcask)...")

disk_missing = :counters.new(1, [:atomics])
disk_wrong_value = :counters.new(1, [:atomics])

for i <- 1..total do
  key = "durable:#{String.pad_leading(Integer.to_string(i), 6, "0")}"
  idx = Router.shard_for(ctx, key)
  keydir = Router.resolve_keydir(ctx, idx)

  case :ets.lookup(keydir, key) do
    [{^key, _val, _exp, _lfu, fid, offset, _vsize}] when is_integer(fid) and fid >= 0 ->
      shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)

      file_path =
        Path.join(shard_path, "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log")

      case NIF.v2_pread_at(file_path, offset) do
        {:ok, disk_value} ->
          if disk_value != payload do
            :counters.add(disk_wrong_value, 1, 1)

            if :counters.get(disk_wrong_value, 1) <= 5 do
              IO.puts("  WRONG on disk: #{key}")
            end
          end

        {:error, reason} ->
          :counters.add(disk_missing, 1, 1)

          if :counters.get(disk_missing, 1) <= 5 do
            IO.puts("  DISK READ ERROR: #{key} — #{inspect(reason)}")
          end
      end

    [{^key, _val, _exp, _lfu, fid, _off, _vsize}] ->
      :counters.add(disk_missing, 1, 1)

      if :counters.get(disk_missing, 1) <= 5 do
        IO.puts("  PENDING file_id: #{key} (fid=#{inspect(fid)})")
      end

    [] ->
      :counters.add(disk_missing, 1, 1)

      if :counters.get(disk_missing, 1) <= 5 do
        IO.puts("  NOT IN KEYDIR: #{key}")
      end
  end
end

disk_miss = :counters.get(disk_missing, 1)
disk_wrong = :counters.get(disk_wrong_value, 1)

IO.puts("Disk verification:")
IO.puts("  Missing: #{disk_miss}/#{total}")
IO.puts("  Wrong value: #{disk_wrong}/#{total}")
IO.puts("  OK: #{total - disk_miss - disk_wrong}/#{total}")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

IO.puts("")
IO.puts("=== SUMMARY ===")
IO.puts("Writes:     #{total - write_errors}/#{total} succeeded")
IO.puts("ETS:        #{total - ets_miss - ets_wrong}/#{total} correct")
IO.puts("Disk:       #{total - disk_miss - disk_wrong}/#{total} correct")

if write_errors == 0 and ets_miss == 0 and ets_wrong == 0 and disk_miss == 0 and disk_wrong == 0 do
  IO.puts("")
  IO.puts("PASS: All #{total} writes are durable (ETS + disk)")
else
  IO.puts("")
  IO.puts("FAIL: #{write_errors + ets_miss + ets_wrong + disk_miss + disk_wrong} issues found")
  System.halt(1)
end
