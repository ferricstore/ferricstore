defmodule Ferricstore.Waiters.Cleanup do
  @moduledoc false

  @table :ferricstore_waiters

  @spec cleanup(pid()) :: :ok
  def cleanup(pid) when is_pid(pid) do
    :ets.match_delete(@table, {:_, pid, :_, :_})
    :ok
  end
end
