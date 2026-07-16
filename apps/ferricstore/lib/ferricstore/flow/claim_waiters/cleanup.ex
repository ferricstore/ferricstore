defmodule Ferricstore.Flow.ClaimWaiters.Cleanup do
  @moduledoc false

  @table :ferricstore_flow_claim_waiters

  @spec cleanup(pid()) :: :ok
  def cleanup(pid) when is_pid(pid) do
    if :ets.whereis(@table) != :undefined do
      :ets.match_delete(@table, {:_, pid, :_, :_, :_})
      :ets.match_delete(@table, {:_, pid, :_, :_})
    end

    Ferricstore.Flow.ClaimWaiters.prune_scheduled_ready_for_cleanup()

    :ok
  end
end
