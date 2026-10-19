defmodule Ferricstore.Store.RateLimit do
  @moduledoc false

  @max_int64 9_223_372_036_854_775_807
  # RATELIMIT.ADD's bound: the key expires two windows after a timestamp.
  @max_window_ms div(@max_int64 - 281_474_976_710_656 + 1, 2)

  @doc """
  Checks RATELIMIT.ADD arguments with the command's bounds.

  A malformed one raises while the write is applied, which crashes the
  shard's storage process, and again on every replay of the entry.
  """
  @spec validate_args(term(), term(), term()) :: :ok | {:error, binary()}
  def validate_args(window_ms, max, count)
      when is_integer(window_ms) and window_ms > 0 and window_ms <= @max_window_ms and
             is_integer(max) and max > 0 and max <= @max_int64 and is_integer(count) and
             count > 0 and count <= @max_int64,
      do: :ok

  def validate_args(_window_ms, _max, _count),
    do: {:error, "ERR value is not an integer or out of range"}

  @doc """
  Checks RATELIMIT.ADD arguments before the state machine applies them.

  Rejects only what raises while applying. An older release logged and
  applied wider values (any positive window, any numeric limit and count),
  so a replica replaying that entry must apply it the same way.
  """
  @spec validate_apply_args(term(), term(), term()) :: :ok | {:error, binary()}
  def validate_apply_args(window_ms, max, count)
      when is_integer(window_ms) and window_ms > 0 and is_number(max) and is_number(count),
      do: :ok

  def validate_apply_args(_window_ms, _max, _count),
    do: {:error, "ERR value is not an integer or out of range"}

  @spec effective_count(
          non_neg_integer(),
          non_neg_integer(),
          integer(),
          pos_integer()
        ) :: non_neg_integer()
  def effective_count(current_count, previous_count, elapsed_ms, window_ms)
      when is_integer(current_count) and current_count >= 0 and is_integer(previous_count) and
             previous_count >= 0 and is_integer(elapsed_ms) and is_integer(window_ms) and
             window_ms > 0 do
    remaining_ms = window_ms - min(max(elapsed_ms, 0), window_ms)

    weighted_previous =
      div(previous_count * remaining_ms + div(window_ms, 2), window_ms)

    current_count + weighted_previous
  end
end
