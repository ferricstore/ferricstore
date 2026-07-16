defmodule Ferricstore.Store.SegmentLock do
  @moduledoc false

  @spec with_lock(binary(), (-> result)) :: result | {:error, term()} when result: term()
  def with_lock(path, fun) when is_binary(path) and is_function(fun, 0) do
    resource = {__MODULE__, Path.expand(path)}

    case :global.trans({resource, self()}, fun, [node()]) do
      {:aborted, reason} -> {:error, {:segment_lock_aborted, reason}}
      result -> result
    end
  end
end
