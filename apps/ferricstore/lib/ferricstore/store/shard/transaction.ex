defmodule Ferricstore.Store.Shard.Transaction do
  @moduledoc "Executes a queued MULTI/EXEC command batch inside a shard using a local transaction store."

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Transaction.Ast, as: TxAst
  alias Ferricstore.Transaction.ExecutionEntry

  # -------------------------------------------------------------------
  # Transaction execution handler
  # -------------------------------------------------------------------

  @spec execute([TxAst.queue_entry()], binary() | nil, map()) ::
          [term()] | {:error, term()}
  @doc false
  def execute(_queue, _sandbox_namespace, %LocalTxStore{}),
    do: {:error, :local_tx_store_not_supported}

  def execute(queue, sandbox_namespace, store) when is_list(queue) and is_map(store) do
    with :ok <-
           validate_queue(
             queue,
             Map.get(store, :transaction_command_budget, :unlimited),
             Map.get(store, :transaction_key_apply_budget, :unlimited)
           ) do
      with_execution_context(store, fn ->
        dispatch_queue(queue, sandbox_namespace, store)
      end)
    end
  end

  @doc false
  @spec with_execution_context(map(), (-> result)) :: result when result: term()
  def with_execution_context(store, fun) when is_map(store) and is_function(fun, 0) do
    Process.put(:tx_deleted_keys, MapSet.new())
    Process.put(:tx_pending_values, %{})
    Process.put(:tx_pending_compound_keys, %{})
    Process.put(:tx_deleted_compound_keys, %{})
    Process.put(:tx_compound_member_work_used, 0)
    Process.put(:tx_result_bytes_used, 0)

    Process.put(
      :tx_result_byte_budget,
      Map.get(store, :transaction_result_byte_budget, :unlimited)
    )

    Process.put(:tx_current_command_precharged_bytes, 0)
    Process.put(:tx_materialization_bytes_reserved, 0)
    Process.put(:tx_result_read_projection, :none)
    Process.put(:tx_compound_scan_projection, :pairs)
    Process.put(:tx_current_command_compound_reads, MapSet.new())

    try do
      fun.()
    after
      Process.delete(:tx_deleted_keys)
      Process.delete(:tx_pending_values)
      Process.delete(:tx_pending_compound_keys)
      Process.delete(:tx_deleted_compound_keys)
      Process.delete(:tx_compound_member_work_used)
      Process.delete(:tx_result_bytes_used)
      Process.delete(:tx_result_byte_budget)
      Process.delete(:tx_current_command_precharged_bytes)
      Process.delete(:tx_materialization_bytes_reserved)
      Process.delete(:tx_result_read_projection)
      Process.delete(:tx_compound_scan_projection)
      Process.delete(:tx_current_command_compound_reads)
    end
  end

  defp validate_queue(queue, command_budget, key_budget) do
    with {:ok, command_budget} <- normalize_queue_budget(command_budget, :command),
         {:ok, key_budget} <- normalize_queue_budget(key_budget, :key) do
      validate_queue_entries(queue, command_budget, key_budget, 0, 0)
    end
  end

  defp normalize_queue_budget(:unlimited, _kind), do: {:ok, :unlimited}

  defp normalize_queue_budget(budget, _kind) when is_integer(budget) and budget >= 0,
    do: {:ok, budget}

  defp normalize_queue_budget(_invalid, :command),
    do: {:error, :invalid_transaction_command_budget}

  defp normalize_queue_budget(_invalid, :key),
    do: {:error, :invalid_transaction_key_apply_budget}

  defp validate_queue_entries([], _command_budget, _key_budget, _commands, _key_work),
    do: :ok

  defp validate_queue_entries(
         [_entry | _rest],
         command_budget,
         _key_budget,
         commands,
         _key_work
       )
       when is_integer(command_budget) and commands >= command_budget,
       do: {:error, :transaction_command_budget_exceeded}

  defp validate_queue_entries(
         [%ExecutionEntry{command_keys: command_keys} = entry | rest],
         command_budget,
         key_budget,
         commands,
         key_work
       ) do
    remaining = remaining_queue_budget(key_budget, key_work)

    with {:ok, entry_key_work} <- key_work_up_to(command_keys, remaining, 0),
         true <- ExecutionEntry.valid?(entry) do
      validate_queue_entries(
        rest,
        command_budget,
        key_budget,
        commands + 1,
        key_work + entry_key_work
      )
    else
      :limit_exceeded -> {:error, :transaction_key_apply_budget_exceeded}
      false -> {:error, "ERR invalid transaction command"}
      :invalid -> {:error, "ERR invalid transaction command"}
    end
  end

  defp validate_queue_entries(
         [_invalid | _rest],
         _command_budget,
         _key_budget,
         _commands,
         _key_work
       ),
       do: {:error, "ERR invalid transaction command"}

  defp remaining_queue_budget(:unlimited, _used), do: :unlimited
  defp remaining_queue_budget(budget, used), do: max(budget - used, 0)

  defp key_work_up_to([], _remaining, count), do: {:ok, count}
  defp key_work_up_to([_key | _rest], 0, _count), do: :limit_exceeded

  defp key_work_up_to([key | rest], remaining, count) when is_binary(key) do
    next_remaining = if remaining == :unlimited, do: :unlimited, else: remaining - 1
    key_work_up_to(rest, next_remaining, count + 1)
  end

  defp key_work_up_to(_invalid, _remaining, _count), do: :invalid

  defp dispatch_queue(queue, sandbox_namespace, store) do
    queue
    |> Enum.reduce_while([], fn entry, results ->
      Process.put(:tx_current_command_precharged_bytes, 0)
      Process.put(:tx_materialization_bytes_reserved, 0)
      Process.put(:tx_result_read_projection, result_read_projection(entry))
      Process.put(:tx_compound_scan_projection, compound_scan_projection(entry))
      Process.put(:tx_current_command_compound_reads, MapSet.new())

      ast =
        entry
        |> TxAst.command_ast()
        |> TxAst.namespace_ast_keys(sandbox_namespace)

      case dispatch_entry(ast, store) do
        {:ok, result} ->
          case charge_transaction_result(result) do
            :ok -> {:cont, [result | results]}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        {:fatal, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      results -> Enum.reverse(results)
    end
  end

  defp result_read_projection(%ExecutionEntry{command: command, ast: ast}) do
    cond do
      command in ["GET", "GETSET", "GETDEL", "GETEX", "HGET", "HGETDEL", "HGETEX"] ->
        :point

      command in ["MGET", "HMGET"] ->
        :batch

      command == "SET" and set_returns_old_value?(ast) ->
        :point

      true ->
        :none
    end
  end

  defp set_returns_old_value?({:set, _key, _value, opts}) when is_list(opts),
    do: :get in opts

  defp set_returns_old_value?(_ast), do: false

  defp compound_scan_projection(%ExecutionEntry{command: "HKEYS"}), do: :fields
  defp compound_scan_projection(%ExecutionEntry{command: "SMEMBERS"}), do: :fields
  defp compound_scan_projection(%ExecutionEntry{command: "HVALS"}), do: :values
  defp compound_scan_projection(%ExecutionEntry{}), do: :pairs

  defp dispatch_entry(ast, store) do
    {:ok, Dispatcher.dispatch_ast(ast, store)}
  rescue
    _error -> {:fatal, "ERR transaction command failed during replicated apply"}
  catch
    :throw, {:transaction_store_failure, reason} -> {:fatal, reason}
    _kind, _reason -> {:fatal, "ERR transaction command failed during replicated apply"}
  end

  defp charge_transaction_result(result) do
    case Process.get(:tx_result_byte_budget, :unlimited) do
      :unlimited ->
        :ok

      budget when is_integer(budget) and budget >= 0 ->
        used = Process.get(:tx_result_bytes_used, 0)
        precharged = Process.get(:tx_current_command_precharged_bytes, 0)

        case admit_result_bytes(result, budget, used, precharged) do
          {:ok, new_used} ->
            Process.put(:tx_result_bytes_used, new_used)
            :ok

          {:error, _reason} = error ->
            error
        end

      _invalid ->
        {:error, :invalid_transaction_result_byte_budget}
    end
  end

  @doc false
  def __admit_result_bytes_for_test__(result, budget, used, precharged),
    do: admit_result_bytes(result, budget, used, precharged)

  defp admit_result_bytes(result, budget, used, precharged)
       when is_integer(budget) and budget >= 0 and is_integer(used) and used >= 0 and
              is_integer(precharged) and precharged >= 0 do
    remaining = max(budget - used, 0)

    case transaction_result_bytes_up_to(result, precharged + remaining) do
      {:ok, total_bytes} ->
        {:ok, used + max(total_bytes - precharged, 0)}

      :limit_exceeded ->
        {:error, :transaction_result_byte_budget_exceeded}

      :invalid ->
        {:error, :invalid_transaction_result}
    end
  end

  defp transaction_result_bytes_up_to(value, limit) when is_binary(value),
    do: admit_scalar_bytes(byte_size(value), limit)

  defp transaction_result_bytes_up_to(value, limit) when is_integer(value),
    do: value |> to_string() |> byte_size() |> admit_scalar_bytes(limit)

  defp transaction_result_bytes_up_to(value, limit) when is_float(value),
    do: value |> Float.to_string() |> byte_size() |> admit_scalar_bytes(limit)

  defp transaction_result_bytes_up_to(value, limit) when is_atom(value),
    do: value |> Atom.to_string() |> byte_size() |> admit_scalar_bytes(limit)

  defp transaction_result_bytes_up_to(value, limit) when is_list(value),
    do: transaction_list_bytes_up_to(value, limit, 0)

  defp transaction_result_bytes_up_to(value, limit) when is_tuple(value),
    do: transaction_tuple_bytes_up_to(value, 0, tuple_size(value), limit, 0)

  defp transaction_result_bytes_up_to(value, limit) when is_map(value),
    do: transaction_map_bytes_up_to(:maps.iterator(value), limit, 0)

  defp transaction_result_bytes_up_to(_value, _limit), do: {:ok, 0}

  defp admit_scalar_bytes(bytes, limit) when bytes <= limit, do: {:ok, bytes}
  defp admit_scalar_bytes(_bytes, _limit), do: :limit_exceeded

  defp transaction_list_bytes_up_to([], _remaining, total), do: {:ok, total}

  defp transaction_list_bytes_up_to([item | rest], remaining, total) do
    case transaction_result_bytes_up_to(item, remaining) do
      {:ok, bytes} ->
        transaction_list_bytes_up_to(rest, remaining - bytes, total + bytes)

      error ->
        error
    end
  end

  defp transaction_list_bytes_up_to(_improper_tail, _remaining, _total), do: :invalid

  defp transaction_tuple_bytes_up_to(_tuple, size, size, _remaining, total),
    do: {:ok, total}

  defp transaction_tuple_bytes_up_to(tuple, index, size, remaining, total) do
    case transaction_result_bytes_up_to(elem(tuple, index), remaining) do
      {:ok, bytes} ->
        transaction_tuple_bytes_up_to(
          tuple,
          index + 1,
          size,
          remaining - bytes,
          total + bytes
        )

      error ->
        error
    end
  end

  defp transaction_map_bytes_up_to(iterator, remaining, total) do
    case :maps.next(iterator) do
      :none ->
        {:ok, total}

      {key, value, next_iterator} ->
        with {:ok, key_bytes} <- transaction_result_bytes_up_to(key, remaining),
             {:ok, value_bytes} <-
               transaction_result_bytes_up_to(value, remaining - key_bytes) do
          transaction_map_bytes_up_to(
            next_iterator,
            remaining - key_bytes - value_bytes,
            total + key_bytes + value_bytes
          )
        end

      _invalid ->
        :invalid
    end
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
