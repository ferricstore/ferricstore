defmodule Ferricstore.Store.Shard.Transaction do
  @moduledoc "Executes a queued MULTI/EXEC command batch inside a shard using a local transaction store."

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Transaction.Ast, as: TxAst
  alias Ferricstore.Transaction.ExecutionEntry

  # -------------------------------------------------------------------
  # Transaction execution handler
  # -------------------------------------------------------------------

  @spec handle_tx_execute([TxAst.queue_entry()], binary() | nil, map()) ::
          {:reply, [term()] | {:error, binary()}, map()}
  @doc false
  def handle_tx_execute(queue, sandbox_namespace, state) do
    results = execute(queue, sandbox_namespace, LocalTxStore.new(state))
    {:reply, results, state}
  end

  @spec execute([TxAst.queue_entry()], binary() | nil, map()) ::
          [term()] | {:error, binary()}
  @doc false
  def execute(queue, sandbox_namespace, store) when is_list(queue) and is_map(store) do
    with :ok <- validate_queue(queue) do
      Process.put(:tx_deleted_keys, MapSet.new())
      Process.put(:tx_pending_values, %{})

      try do
        dispatch_queue(queue, sandbox_namespace, store)
      after
        Process.delete(:tx_deleted_keys)
        Process.delete(:tx_pending_values)
      end
    end
  end

  defp validate_queue(queue) do
    if Enum.all?(queue, &ExecutionEntry.valid?/1) do
      :ok
    else
      {:error, "ERR invalid transaction command"}
    end
  end

  defp dispatch_queue(queue, sandbox_namespace, store) do
    queue
    |> Enum.reduce_while([], fn entry, results ->
      ast =
        entry
        |> TxAst.command_ast()
        |> TxAst.namespace_ast_keys(sandbox_namespace)

      case dispatch_entry(ast, store) do
        {:ok, result} -> {:cont, [result | results]}
        {:fatal, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      results -> Enum.reverse(results)
    end
  end

  defp dispatch_entry(ast, store) do
    {:ok, Dispatcher.dispatch_ast(ast, store)}
  rescue
    _error -> {:fatal, "ERR transaction command failed during replicated apply"}
  catch
    :throw, {:transaction_store_failure, reason} -> {:fatal, reason}
    _kind, _reason -> {:fatal, "ERR transaction command failed during replicated apply"}
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  @doc false
  def namespace_args(args, nil), do: args
  def namespace_args([], _ns), do: []
  def namespace_args([key | rest], ns) when is_binary(key), do: [ns <> key | rest]
  def namespace_args(args, _ns), do: args
end
