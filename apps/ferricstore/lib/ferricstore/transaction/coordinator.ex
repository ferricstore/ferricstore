defmodule Ferricstore.Transaction.Coordinator do
  @moduledoc """
  Transaction coordinator for MULTI/EXEC.

  Transactions are accepted only when every routed and watched key belongs to
  one shard. The complete queue and WATCH snapshot are then checked and applied
  by that shard's Raft state machine in one ordered entry. Independent Raft
  groups cannot provide atomic commit through an anchor-group write, so
  cross-shard transactions fail with `CROSSSLOT` before mutation.

  ## WATCH conflict detection

  WATCH tokens use the key's logical Raft write index when available and a
  content digest for projected Bitcask values. Capture and validation are both
  ordered through the owning shard's Raft log.
  """

  alias Ferricstore.Commands.{PreparedCommand, TransactionPolicy}
  alias Ferricstore.Raft.Backend
  alias Ferricstore.Store.{Router, WriteVersion}
  alias Ferricstore.Transaction.ExecutionEntry

  @type queue_entry :: PreparedCommand.t()

  @spec execute([queue_entry()], %{binary() => term()}, binary() | nil) ::
          [term()] | nil | {:error, binary()}
  def execute([], watched_keys, _sandbox_namespace) when map_size(watched_keys) == 0, do: []

  def execute(queue, watched_keys, sandbox_namespace) do
    maybe_run_after_watch_preflight_hook()

    case classify_shards(queue, sandbox_namespace, watched_keys) do
      {:ok, shard_groups, write_shards} when map_size(shard_groups) == 1 ->
        execute_single_shard(shard_groups, sandbox_namespace, watched_keys, write_shards)

      {:ok, _shard_groups, _write_shards} ->
        {:error, "CROSSSLOT Keys in request don't hash to the same slot"}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec execute_pipeline([queue_entry()], binary() | nil) :: [term()] | {:error, binary()}
  def execute_pipeline([], _sandbox_namespace), do: []

  def execute_pipeline(queue, sandbox_namespace) do
    ctx = FerricStore.Instance.get(:default)

    with {:ok, classified} <- classify_entries(queue, ctx, sandbox_namespace),
         {:ok, groups} <- pipeline_groups(classified) do
      execute_pipeline_groups(groups, length(queue), sandbox_namespace)
    end
  end

  # ---------------------------------------------------------------------------
  # Single-shard Raft execution
  # ---------------------------------------------------------------------------

  defp execute_single_shard(shard_groups, sandbox_namespace, watched_keys, write_shards) do
    [{shard_idx, indexed_entries}] = Map.to_list(shard_groups)
    queue = Enum.map(indexed_entries, fn {_index, entry} -> entry end)
    command = {:tx_execute, queue, sandbox_namespace, watched_keys}

    try do
      case Backend.write(shard_idx, command) do
        {:error, :noproc} ->
          raft_unavailable(:noproc)

        {:error, _reason} ->
          raft_unavailable(:pipeline_rejected)

        nil ->
          nil

        results ->
          Enum.each(write_shards, &WriteVersion.increment/1)
          results
      end
    catch
      :exit, {:noproc, _} ->
        raft_unavailable(:noproc)
    end
  end

  defp execute_pipeline_groups(groups, command_count, sandbox_namespace) do
    group_specs =
      groups
      |> Enum.sort_by(fn {shard_idx, _entries} -> shard_idx end)
      |> Enum.map(fn {shard_idx, entries} ->
        queue = Enum.map(entries, & &1.entry)
        write_shards = entries |> Enum.flat_map(& &1.write_shards) |> Enum.uniq()
        {shard_idx, entries, write_shards, {:tx_execute, queue, sandbox_namespace, %{}}}
      end)

    backend_results =
      try do
        case group_specs do
          [{shard_idx, _entries, _write_shards, command}] ->
            [Backend.write(shard_idx, command)]

          _multiple_groups ->
            group_specs
            |> Enum.map(fn {shard_idx, _entries, _write_shards, command} ->
              {shard_idx, command}
            end)
            |> Backend.write_many()
        end
      catch
        :exit, {:noproc, _} -> :noproc
        :exit, _reason -> :pipeline_rejected
      end

    with {:ok, indexed_results} <-
           index_pipeline_results(group_specs, backend_results, command_count) do
      for index <- 0..(command_count - 1), do: Map.fetch!(indexed_results, index)
    end
  end

  defp index_pipeline_results(group_specs, backend_results, command_count)
       when is_list(backend_results) and length(group_specs) == length(backend_results) do
    indexed_results =
      group_specs
      |> Enum.zip(backend_results)
      |> Enum.flat_map(fn {{_shard_idx, entries, write_shards, _command}, result} ->
        index_pipeline_group(entries, write_shards, result)
      end)
      |> Map.new()

    if map_size(indexed_results) == command_count do
      {:ok, indexed_results}
    else
      {:error, "ERR pipeline result count mismatch"}
    end
  end

  defp index_pipeline_results(group_specs, reason, _command_count) do
    error = pipeline_raft_unavailable(reason)

    {:ok,
     group_specs
     |> Enum.flat_map(fn {_shard_idx, entries, _write_shards, _command} ->
       Enum.map(entries, &{&1.index, error})
     end)
     |> Map.new()}
  end

  defp index_pipeline_group(entries, write_shards, results)
       when is_list(results) and length(entries) == length(results) do
    Enum.each(write_shards, &WriteVersion.increment/1)
    Enum.zip_with(entries, results, fn entry, result -> {entry.index, result} end)
  end

  defp index_pipeline_group(entries, _write_shards, {:error, :noproc}) do
    pipeline_group_error(entries, pipeline_raft_unavailable(:noproc))
  end

  defp index_pipeline_group(entries, _write_shards, {:error, _reason}) do
    pipeline_group_error(entries, pipeline_raft_unavailable(:pipeline_rejected))
  end

  defp index_pipeline_group(entries, _write_shards, _invalid_result) do
    pipeline_group_error(entries, {:error, "ERR pipeline result count mismatch"})
  end

  defp pipeline_group_error(entries, error), do: Enum.map(entries, &{&1.index, error})

  defp raft_unavailable(reason) do
    {:error, "ERR transaction raft unavailable: #{inspect(reason)}"}
  end

  defp pipeline_raft_unavailable(reason) do
    {:error, "ERR pipeline raft unavailable: #{inspect(reason)}"}
  end

  # ---------------------------------------------------------------------------
  # Shard classification
  # ---------------------------------------------------------------------------

  @spec classify_shards([queue_entry()], binary() | nil, %{binary() => term()}) ::
          {:ok, %{non_neg_integer() => list()}, [non_neg_integer()]}
          | {:error, binary()}
  defp classify_shards(queue, sandbox_namespace, watched_keys) do
    ctx = FerricStore.Instance.get(:default)

    with {:ok, classified} <- classify_entries(queue, ctx, sandbox_namespace) do
      watched_shards =
        watched_keys
        |> Map.keys()
        |> Enum.map(&Router.shard_for(ctx, &1))

      default_shard =
        Enum.find_value(classified, List.first(watched_shards) || 0, fn
          %{routing_shards: [first | _rest]} -> first
          _entry -> nil
        end)

      classified =
        Enum.map(classified, fn entry ->
          execution_shard = List.first(entry.routing_shards) || default_shard

          write_shards =
            case entry.write_shards do
              :execution -> [execution_shard]
              shards -> shards
            end

          entry
          |> Map.put(:execution_shard, execution_shard)
          |> Map.put(:write_shards, write_shards)
        end)

      touched_shards =
        (watched_shards ++
           Enum.flat_map(classified, fn entry ->
             [entry.execution_shard | entry.routing_shards]
           end))
        |> Enum.uniq()
        |> Enum.sort()

      shard_groups =
        touched_shards
        |> Map.new(&{&1, []})
        |> then(fn groups ->
          Enum.reduce(classified, groups, fn entry, acc ->
            Map.update!(acc, entry.execution_shard, &[{entry.index, entry.entry} | &1])
          end)
        end)
        |> Map.new(fn {shard_idx, entries} -> {shard_idx, Enum.reverse(entries)} end)

      write_shards =
        classified
        |> Enum.flat_map(& &1.write_shards)
        |> Enum.uniq()
        |> Enum.sort()

      {:ok, shard_groups, write_shards}
    end
  end

  defp classify_entries(queue, ctx, sandbox_namespace) do
    queue
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {entry, index}, {:ok, acc} ->
      case classify_entry(entry, index, ctx, sandbox_namespace) do
        {:ok, classified} -> {:cont, {:ok, [classified | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, classified} -> {:ok, Enum.reverse(classified)}
      {:error, _reason} = error -> error
    end
  end

  defp classify_entry(
         %PreparedCommand{routing_scope: :coordinated},
         _index,
         _ctx,
         _sandbox_namespace
       ) do
    {:error, "ERR coordinated commands are not supported inside transactions"}
  end

  defp classify_entry(
         %PreparedCommand{transaction_mode: :request, command: command},
         _index,
         _ctx,
         _sandbox_namespace
       ) do
    TransactionPolicy.error(command)
  end

  defp classify_entry(
         %PreparedCommand{routing_scope: :none, routing_keys: [], write_keys: []} = prepared,
         index,
         _ctx,
         _sandbox_namespace
       ) do
    with {:ok, entry} <- ExecutionEntry.from_prepared(prepared) do
      {:ok,
       %{
         index: index,
         entry: entry,
         routing_shards: [],
         write_shards: []
       }}
    end
  end

  defp classify_entry(
         %PreparedCommand{routing_scope: :keys, routing_keys: [_first | _rest] = routing_keys} =
           prepared,
         index,
         ctx,
         sandbox_namespace
       ) do
    if valid_prepared_keys?(routing_keys) and valid_prepared_keys?(prepared.write_keys) do
      routing_shards = command_shards(routing_keys, sandbox_namespace, ctx)

      write_shards =
        if prepared.write_keys == routing_keys do
          routing_shards
        else
          command_shards(prepared.write_keys, sandbox_namespace, ctx)
        end

      with {:ok, entry} <- ExecutionEntry.from_prepared(prepared) do
        {:ok,
         %{
           index: index,
           entry: entry,
           routing_shards: Enum.uniq(routing_shards ++ write_shards),
           write_shards: write_shards
         }}
      end
    else
      invalid_prepared_routing()
    end
  end

  defp classify_entry(%PreparedCommand{}, _index, _ctx, _sandbox_namespace),
    do: invalid_prepared_routing()

  defp classify_entry(_invalid, _index, _ctx, _sandbox_namespace),
    do: {:error, "ERR invalid transaction command"}

  defp valid_prepared_keys?(keys) when is_list(keys), do: Enum.all?(keys, &is_binary/1)
  defp valid_prepared_keys?(_keys), do: false

  defp invalid_prepared_routing,
    do: {:error, "ERR invalid prepared command routing metadata"}

  defp command_shards(keys, sandbox_namespace, ctx) do
    Enum.map(keys, fn key ->
      key
      |> namespace_key(sandbox_namespace)
      |> then(&Router.shard_for(ctx, &1))
    end)
  end

  defp namespace_key(key, nil), do: key
  defp namespace_key(key, ""), do: key
  defp namespace_key(key, namespace) when is_binary(namespace), do: namespace <> key

  defp pipeline_groups(classified) do
    default_shard =
      Enum.find_value(classified, 0, fn
        %{routing_shards: [first | _rest]} -> first
        _entry -> nil
      end)

    classified
    |> Enum.reduce_while({:ok, %{}}, fn entry, {:ok, groups} ->
      touched_shards = Enum.uniq(entry.routing_shards ++ entry.write_shards)

      case touched_shards do
        [] ->
          grouped = Map.update(groups, default_shard, [entry], &[entry | &1])
          {:cont, {:ok, grouped}}

        [shard_idx] ->
          grouped = Map.update(groups, shard_idx, [entry], &[entry | &1])
          {:cont, {:ok, grouped}}

        _multiple_shards ->
          {:halt, {:error, "CROSSSLOT Keys in request don't hash to the same slot"}}
      end
    end)
    |> case do
      {:ok, groups} ->
        {:ok, Map.new(groups, fn {shard_idx, entries} -> {shard_idx, Enum.reverse(entries)} end)}

      {:error, _reason} = error ->
        error
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH support
  # ---------------------------------------------------------------------------

  defp maybe_run_after_watch_preflight_hook do
    case Process.get(:ferricstore_tx_after_watch_preflight_hook) do
      hook when is_function(hook, 0) -> hook.()
      _ -> :ok
    end
  end
end
