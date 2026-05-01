defmodule Ferricstore.Store.Shard.Transaction do
  @moduledoc "Executes a queued MULTI/EXEC command batch inside a shard using a local transaction store."

  alias Ferricstore.Store.LocalTxStore

  # -------------------------------------------------------------------
  # Transaction execution handler
  # -------------------------------------------------------------------

  @spec handle_tx_execute([{binary(), [term()]}], binary() | nil, map()) ::
          {:reply, [term()], map()}
  @doc false
  def handle_tx_execute(queue, sandbox_namespace, state) do
    Process.put(:tx_deleted_keys, MapSet.new())
    Process.put(:tx_pending_values, %{})
    store = LocalTxStore.new(state)

    results =
      try do
        Enum.map(queue, fn {cmd, args} ->
          namespaced_args = namespace_args(args, sandbox_namespace)

          try do
            Ferricstore.Commands.Dispatcher.dispatch(cmd, namespaced_args, store)
          catch
            :exit, {:noproc, _} ->
              {:error, "ERR server not ready, shard process unavailable"}

            :exit, {reason, _} ->
              {:error, "ERR internal error: #{inspect(reason)}"}
          end
        end)
      after
        Process.delete(:tx_deleted_keys)
        Process.delete(:tx_pending_values)
      end

    {:reply, results, state}
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  @spec namespace_args([term()], binary() | nil) :: [term()]
  @doc false
  def namespace_args(args, nil), do: args
  def namespace_args([], _ns), do: []
  def namespace_args([key | rest], ns) when is_binary(key), do: [ns <> key | rest]
  def namespace_args(args, _ns), do: args
end
