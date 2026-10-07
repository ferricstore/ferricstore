defmodule Ferricstore.Raft.WARaftBackend.HsetCadence do
  @moduledoc false
  import Bitwise
  @key {__MODULE__, :cadence}
  @window_ms 100
  @threshold 5
  @count_mask 255
  @bucket_shift 16
  @max_bucket 281_474_976_710_655

  def init(shard_count) do
    :persistent_term.put(@key, new(shard_count, System.monotonic_time(:millisecond)))
  end

  def clear, do: :persistent_term.erase(@key)
  def new(shard_count, origin_ms), do: {origin_ms, :atomics.new(shard_count, signed: false)}

  def busy?(shard_index) do
    case :persistent_term.get(@key, nil) do
      nil -> false
      cadence -> busy?(cadence, shard_index, System.monotonic_time(:millisecond))
    end
  end

  # Only an admission heuristic: correctness does not depend on the rate. Use
  # elapsed time, not the possibly negative VM monotonic timestamp, in the packed
  # unsigned word. A completed window avoids treating a sparse synchronized
  # burst as sustained load; the current window allows a busy stream to ramp up.
  def busy?({origin_ms, ref}, shard_index, now_ms) do
    if is_integer(shard_index) and shard_index >= 0 and shard_index < :atomics.info(ref).size do
      elapsed = max(now_ms - origin_ms, 0)
      update(ref, shard_index + 1, min(div(elapsed, @window_ms), @max_bucket))
    else
      false
    end
  end

  defp update(ref, index, bucket) do
    old = :atomics.get(ref, index)
    old_bucket = old >>> @bucket_shift
    old_current = old &&& @count_mask
    old_previous = old >>> 8 &&& @count_mask

    # A caller can be descheduled after sampling its timestamp. Never roll the
    # shared window backwards when it resumes after a newer caller. Eight-bit
    # counts suffice for the threshold and leave 48 bits for elapsed windows.
    bucket = max(bucket, old_bucket)

    {current, previous} =
      cond do
        bucket == old_bucket -> {min(old_current + 1, @count_mask), old_previous}
        bucket == old_bucket + 1 -> {1, old_current}
        true -> {1, 0}
      end

    next = bucket <<< @bucket_shift ||| previous <<< 8 ||| current

    if :atomics.compare_exchange(ref, index, old, next) == :ok do
      current >= @threshold or previous >= @threshold
    else
      update(ref, index, bucket)
    end
  end
end
