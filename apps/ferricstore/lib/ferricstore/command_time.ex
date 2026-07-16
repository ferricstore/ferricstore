defmodule Ferricstore.CommandTime do
  @moduledoc """
  Shared clock for command code that can run both outside and inside Raft apply.

  Outside Raft, command code uses the cluster-adjusted HLC time. Inside Raft
  apply, relative expiry and other time-derived command results must use the
  timestamp stored in the log entry so every replica computes the same state.
  """

  alias Ferricstore.HLC

  @apply_now_key :ferricstore_raft_apply_now_ms
  @apply_now_unset :__ferricstore_raft_apply_now_unset__

  @doc """
  Returns the command time in milliseconds.

  If a Raft state machine apply scope installed a stamped log-entry time, that
  value is returned. Otherwise this falls back to the local cluster-adjusted
  HLC clock.
  """
  @spec now_ms() :: non_neg_integer()
  def now_ms do
    Process.get(@apply_now_key) || HLC.now_ms()
  end

  @doc """
  Runs `fun` with a stamped Raft apply time visible to command modules.
  """
  @spec with_now_ms(non_neg_integer(), (-> result)) :: result when result: term()
  def with_now_ms(now_ms, fun)
      when is_integer(now_ms) and now_ms >= 0 and is_function(fun, 0) do
    previous = Process.get(@apply_now_key, @apply_now_unset)
    Process.put(@apply_now_key, now_ms)

    try do
      fun.()
    after
      case previous do
        @apply_now_unset -> Process.delete(@apply_now_key)
        value -> Process.put(@apply_now_key, value)
      end
    end
  end
end
