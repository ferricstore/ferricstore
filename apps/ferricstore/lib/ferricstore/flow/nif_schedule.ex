defmodule Ferricstore.Flow.NifSchedule do
  @moduledoc false

  # The Flow codec NIFs come in pairs: the plain name runs on a normal
  # scheduler and the `_dirty` twin on a dirty CPU scheduler. A dirty call
  # moves the process to another scheduler and back, which costs more than
  # encoding or decoding a typical record, so a call stays on the normal
  # scheduler while its work is small. Each bound keeps a normal call near
  # 250 µs at most, well inside the 1 ms a NIF may hold a scheduler.
  #
  # Measured on an M4 Max (2026-11-20):
  #   record encode/decode       ~1 µs typical, ~40 µs at 1 MiB
  #   batch record decode        ~0.8 µs a record
  #   claim planning             ~0.5 µs a candidate, ~1 µs with history
  #   terminal no-op check       ~0.1 µs a record
  @max_record_bytes 1_048_576
  @max_record_batch 256
  @max_claim_plan 512
  @max_claim_plan_with_history 256
  @max_terminal_check 2_048

  @spec normal_record?(non_neg_integer()) :: boolean()
  def normal_record?(bytes), do: bytes <= @max_record_bytes

  @spec normal_record_batch?(non_neg_integer()) :: boolean()
  def normal_record_batch?(count), do: count <= @max_record_batch

  @spec normal_claim_plan?(non_neg_integer(), boolean()) :: boolean()
  def normal_claim_plan?(count, false), do: count <= @max_claim_plan
  def normal_claim_plan?(count, true), do: count <= @max_claim_plan_with_history

  @spec normal_terminal_check?(non_neg_integer()) :: boolean()
  def normal_terminal_check?(count), do: count <= @max_terminal_check

  @doc "Total bytes of the binaries in `values`; other terms count as zero."
  @spec binary_bytes([term()]) :: non_neg_integer()
  def binary_bytes(values) do
    Enum.reduce(values, 0, fn
      value, acc when is_binary(value) -> acc + byte_size(value)
      _value, acc -> acc
    end)
  end
end
