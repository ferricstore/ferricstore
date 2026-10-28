defmodule Ferricstore.Store.ScanRead do
  @moduledoc false

  # A background scan, such as a Flow query index backfill, reads every value
  # of a shard. The cold values it reads stay on disk: caching them would undo
  # hibernation and memory pressure for the whole shard.

  @key {__MODULE__, :active}

  @doc "Runs `fun` in this process as a scan read."
  @spec run((-> result)) :: result when result: term()
  def run(fun) when is_function(fun, 0) do
    previous = Process.put(@key, true)

    try do
      fun.()
    after
      if previous == nil, do: Process.delete(@key), else: Process.put(@key, previous)
    end
  end

  @doc "Whether this process is reading for a scan, so cold reads stay uncached."
  @spec active?() :: boolean()
  def active?, do: Process.get(@key) == true
end
