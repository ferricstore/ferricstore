defmodule Ferricstore.Transaction.Coordinator do
  @moduledoc """
  Transaction coordinator for MULTI/EXEC.

  Raft-enabled transactions submit a single Raft log entry to an "anchor shard"
  containing commands for all involved shards. The StateMachine's `apply/3`
  writes to all shards' ETS tables and Bitcask files in one deterministic pass.
  This includes single-shard write transactions; otherwise they would bypass
  the quorum write path and only mutate the local shard process.

  ## WATCH conflict detection

  WATCH uses per-key tokens rather than per-shard write-version counters. Hot
  keys include the in-memory value hash plus the live Bitcask location; cold
  keys snapshot the live keydir location, so large values do not have to be
  materialized just to enter or check WATCH.
  """

  alias Ferricstore.Commands.PreparedCommand
  alias Ferricstore.Raft.Backend
  alias Ferricstore.Store.{Router, WriteVersion}
  alias Ferricstore.Transaction.Ast, as: TxAst

  @type queue_entry :: TxAst.queue_entry() | PreparedCommand.t()

  @spec execute([queue_entry()], %{binary() => term()}, binary() | nil) ::
          [term()] | nil | {:error, binary()}
  def execute([], _watched_keys, _sandbox_namespace), do: []

  def execute(queue, watched_keys, sandbox_namespace) do
    if watches_clean?(watched_keys) do
      maybe_run_after_watch_preflight_hook()

      case classify_shards(queue, sandbox_namespace) do
        {:ok, shard_groups, write_shards} ->
          execute_cross_shard(
            shard_groups,
            length(queue),
            sandbox_namespace,
            watched_keys,
            write_shards
          )

        {:error, _reason} = error ->
          error
      end
    else
      nil
    end
  end

  # ---------------------------------------------------------------------------
  # Cross-shard path: anchor shard Raft entry or sequential GenServer fallback
  # ---------------------------------------------------------------------------

  defp execute_cross_shard(
         shard_groups,
         total,
         sandbox_namespace,
         watched_keys,
         write_shards
       ) do
    anchor_idx = shard_groups |> Map.keys() |> Enum.min()

    shard_batches =
      Enum.map(shard_groups, fn {shard_idx, cmds_with_indices} ->
        {shard_idx, cmds_with_indices, sandbox_namespace}
      end)

    command = {:cross_shard_tx, shard_batches, watched_keys}

    try do
      case Backend.write(anchor_idx, command) do
        {:error, :noproc} ->
          maybe_execute_cross_shard_sequential(
            shard_groups,
            total,
            sandbox_namespace,
            :noproc
          )

        {:error, _reason} ->
          maybe_execute_cross_shard_sequential(
            shard_groups,
            total,
            sandbox_namespace,
            :pipeline_rejected
          )

        nil ->
          nil

        shard_results ->
          Enum.each(write_shards, &WriteVersion.increment/1)

          reassemble_results(shard_results, shard_groups, total)
      end
    catch
      :exit, {:noproc, _} ->
        maybe_execute_cross_shard_sequential(
          shard_groups,
          total,
          sandbox_namespace,
          :noproc
        )
    end
  end

  # The default application instance owns Raft, so transaction submit failures
  # must fail closed instead of acknowledging local-only writes.
  defp maybe_execute_cross_shard_sequential(
         shard_groups,
         total,
         sandbox_namespace,
         reason
       ) do
    _ = {shard_groups, total, sandbox_namespace}
    {:error, "ERR transaction raft unavailable: #{inspect(reason)}"}
  end

  # Reassembles per-shard results back into the original command order.
  defp reassemble_results(shard_results, shard_groups, total) do
    indexed_results =
      Enum.reduce(shard_groups, %{}, fn {shard_idx, cmds_with_indices}, acc ->
        shard_results
        |> Map.fetch!(shard_idx)
        |> then(fn results_for_shard ->
          cmds_with_indices
          |> Enum.map(fn {orig_idx, _entry} -> orig_idx end)
          |> Enum.zip(results_for_shard)
        end)
        |> Enum.reduce(acc, fn {orig_idx, result}, inner ->
          Map.put(inner, orig_idx, result)
        end)
      end)

    Enum.map(0..(total - 1)//1, &Map.fetch!(indexed_results, &1))
  end

  # ---------------------------------------------------------------------------
  # Shard classification
  # ---------------------------------------------------------------------------

  # Commands that don't target a specific key. These are assigned to
  # whichever shard the keyed commands target, so they never cause CROSSSLOT.
  @keyless_commands MapSet.new(~w(PING ECHO DBSIZE TIME RANDOMKEY))

  @spec classify_shards([queue_entry()], binary() | nil) ::
          {:ok, %{non_neg_integer() => list()}, [non_neg_integer()]}
          | {:error, binary()}
  defp classify_shards(queue, sandbox_namespace) do
    ctx = FerricStore.Instance.get(:default)

    with {:ok, classified} <- classify_entries(queue, ctx, sandbox_namespace) do
      default_shard =
        Enum.find_value(classified, 0, fn
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
        classified
        |> Enum.flat_map(fn entry -> [entry.execution_shard | entry.routing_shards] end)
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
         %PreparedCommand{routing_scope: :none, routing_keys: [], write_keys: []} = prepared,
         index,
         _ctx,
         _sandbox_namespace
       ) do
    {:ok,
     %{
       index: index,
       entry: prepared_entry(prepared),
       routing_shards: [],
       write_shards: []
     }}
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
      write_shards = command_shards(prepared.write_keys, sandbox_namespace, ctx)

      {:ok,
       %{
         index: index,
         entry: prepared_entry(prepared),
         routing_shards: Enum.uniq(routing_shards ++ write_shards),
         write_shards: write_shards
       }}
    else
      invalid_prepared_routing()
    end
  end

  defp classify_entry(%PreparedCommand{}, _index, _ctx, _sandbox_namespace),
    do: invalid_prepared_routing()

  defp classify_entry({cmd, args, _ast} = entry, index, ctx, sandbox_namespace)
       when is_binary(cmd) and is_list(args) do
    {cmd, args, ast} = TxAst.normalize_entry(entry)

    routing_shards =
      if MapSet.member?(@keyless_commands, cmd) do
        []
      else
        [command_shard(args, sandbox_namespace, ctx)]
      end

    {:ok,
     %{
       index: index,
       entry: {cmd, args, ast},
       routing_shards: routing_shards,
       write_shards: :execution
     }}
  end

  defp classify_entry(_invalid, _index, _ctx, _sandbox_namespace),
    do: {:error, "ERR invalid transaction command"}

  defp prepared_entry(%PreparedCommand{command: command, args: args, ast: ast}),
    do: {command, args, ast}

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

  defp command_shard(args, sandbox_namespace, ctx) do
    key = args |> extract_key() |> namespace_key(sandbox_namespace)

    Router.shard_for(ctx, key)
  end

  defp namespace_key(key, nil), do: key
  defp namespace_key(key, ""), do: key
  defp namespace_key(key, namespace) when is_binary(namespace), do: namespace <> key

  @spec extract_key([binary()]) :: binary()
  defp extract_key([key | _]) when is_binary(key), do: key
  defp extract_key(_args), do: ""

  # ---------------------------------------------------------------------------
  # WATCH support
  # ---------------------------------------------------------------------------

  defp watches_clean?(watched) when map_size(watched) == 0, do: true

  defp watches_clean?(watched) do
    ctx = FerricStore.Instance.get(:default)

    Enum.all?(watched, fn {key, saved_token} ->
      try do
        Router.watch_token(ctx, key) == saved_token
      catch
        :exit, _ -> false
      end
    end)
  end

  defp maybe_run_after_watch_preflight_hook do
    case Process.get(:ferricstore_tx_after_watch_preflight_hook) do
      hook when is_function(hook, 0) -> hook.()
      _ -> :ok
    end
  end
end
