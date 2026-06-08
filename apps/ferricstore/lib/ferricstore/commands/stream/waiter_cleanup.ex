defmodule Ferricstore.Commands.Stream.WaiterCleanup do
  @moduledoc false

  @stream_waiters_table :ferricstore_stream_waiters

  @spec cleanup(pid()) :: :ok
  def cleanup(pid) when is_pid(pid) do
    if :ets.whereis(@stream_waiters_table) != :undefined do
      :ets.match_delete(@stream_waiters_table, {:_, pid, :_, :_})
    end

    :ok
  end
end
