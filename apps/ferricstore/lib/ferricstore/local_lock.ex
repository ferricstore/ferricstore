defmodule Ferricstore.LocalLock do
  @moduledoc false

  # Node-local mutual exclusion on :global lock ids. :global.trans/3 retries a
  # busy lock after random sleeps that double from 0.25 s up to 8 s, so a
  # contended lock sits unused long after its holder lets go. This keeps
  # :global as the lock table (same ids, released when the holder dies, same
  # nesting behavior) and only retries sooner.
  @max_backoff_ms 8

  @spec trans(:global.id(), (-> result)) :: result when result: term()
  def trans(id, fun) when is_function(fun, 0) do
    acquire(id, 1)

    try do
      fun.()
    after
      :global.del_lock(id, [node()])
    end
  end

  # A single attempt: {:ok, result} when the lock was free, :busy otherwise.
  # For callers that must not block behind the holder.
  @spec try_trans(:global.id(), (-> result)) :: {:ok, result} | :busy when result: term()
  def try_trans(id, fun) when is_function(fun, 0) do
    if :global.set_lock(id, [node()], 0) do
      try do
        {:ok, fun.()}
      after
        :global.del_lock(id, [node()])
      end
    else
      :busy
    end
  end

  defp acquire(id, backoff_ms) do
    if :global.set_lock(id, [node()], 0) do
      :ok
    else
      # Jitter keeps waiters that started together from polling in lockstep.
      Process.sleep(:rand.uniform(backoff_ms))
      acquire(id, min(backoff_ms * 2, @max_backoff_ms))
    end
  end
end
