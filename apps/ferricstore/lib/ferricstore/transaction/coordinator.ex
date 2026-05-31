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

  alias Ferricstore.Raft.Backend
  alias Ferricstore.Store.Router
  alias Ferricstore.Transaction.Ast, as: TxAst

  @spec execute([TxAst.queue_entry()], %{binary() => term()}, binary() | nil) ::
          [term()] | nil | {:error, binary()}
  def execute([], _watched_keys, _sandbox_namespace), do: []

  def execute(queue, watched_keys, sandbox_namespace) do
    if watches_clean?(watched_keys) do
      maybe_run_after_watch_preflight_hook()

      case classify_shards(queue, sandbox_namespace) do
        {:single_shard, shard_idx} ->
          execute_single_shard_raft(queue, shard_idx, sandbox_namespace, watched_keys)

        {:multi_shard, shard_groups} ->
          execute_cross_shard(
            shard_groups,
            length(queue),
            sandbox_namespace,
            watched_keys
          )
      end
    else
      nil
    end
  end

  # ---------------------------------------------------------------------------
  # Single-shard path
  # ---------------------------------------------------------------------------

  defp execute_single_shard_raft(queue, shard_idx, sandbox_namespace, watched_keys) do
    shard_groups = %{
      shard_idx =>
        queue
        |> Enum.with_index()
        |> Enum.map(fn {entry, orig_idx} -> {orig_idx, entry} end)
    }

    execute_cross_shard(
      shard_groups,
      length(queue),
      sandbox_namespace,
      watched_keys
    )
  end

  # ---------------------------------------------------------------------------
  # Cross-shard path: anchor shard Raft entry or sequential GenServer fallback
  # ---------------------------------------------------------------------------

  defp execute_cross_shard(shard_groups, total, sandbox_namespace, watched_keys) do
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
          Enum.each(Map.keys(shard_groups), fn idx ->
            Ferricstore.Store.WriteVersion.increment(idx)
          end)

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

  @spec classify_shards([TxAst.queue_entry()], binary() | nil) ::
          {:single_shard, non_neg_integer()}
          | {:multi_shard, %{non_neg_integer() => list()}}
  defp classify_shards(queue, sandbox_namespace) do
    indexed =
      queue
      |> Enum.with_index()
      |> Enum.map(fn {entry, idx} ->
        cmd = TxAst.command_name(entry)
        args = TxAst.command_args(entry)

        shard_idx =
          if MapSet.member?(@keyless_commands, cmd) do
            :keyless
          else
            command_shard(args, sandbox_namespace)
          end

        {idx, entry, shard_idx}
      end)

    # Find the first keyed shard to assign keyless commands to.
    # If all commands are keyless, they all go to shard 0.
    default_shard =
      Enum.find_value(indexed, 0, fn
        {_, _, :keyless} -> nil
        {_, _, shard} -> shard
      end)

    # Replace :keyless with the default shard
    indexed =
      Enum.map(indexed, fn
        {idx, entry, :keyless} -> {idx, entry, default_shard}
        entry -> entry
      end)

    shard_indices = indexed |> Enum.map(fn {_, _, s} -> s end) |> Enum.uniq()

    case shard_indices do
      [single] ->
        {:single_shard, single}

      _multiple ->
        shard_groups =
          Enum.group_by(
            indexed,
            fn {_idx, _entry, shard_idx} -> shard_idx end,
            fn {idx, entry, _shard_idx} -> {idx, entry} end
          )

        {:multi_shard, shard_groups}
    end
  end

  defp command_shard(args, sandbox_namespace) do
    key = extract_key(args)

    full_key =
      case sandbox_namespace do
        nil -> key
        ns -> ns <> key
      end

    ctx = FerricStore.Instance.get(:default)
    Router.shard_for(ctx, full_key)
  end

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
