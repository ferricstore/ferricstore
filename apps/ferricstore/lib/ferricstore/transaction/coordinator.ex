defmodule Ferricstore.Transaction.Coordinator do
  @moduledoc """
  Transaction coordinator for MULTI/EXEC.

  Single-shard transactions are dispatched as a batch to the target shard's
  GenServer, executing within a single `handle_call` to guarantee atomicity
  (no other client can interleave on that shard).

  Cross-shard transactions submit a single Raft log entry to an "anchor shard"
  containing commands for ALL involved shards. The StateMachine's `apply/3`
  writes to all shards' ETS tables and Bitcask files in one deterministic pass.

  ## WATCH conflict detection

  WATCH uses per-key value hashes (`:erlang.phash2/1` of the value) rather
  than per-shard write-version counters. At WATCH time the connection snapshots
  `phash2(Router.get(key))` for each watched key. At EXEC time `watches_clean?/1`
  re-reads each key and compares hashes. This eliminates false-positive aborts
  caused by unrelated writes to the same shard (the old shard-version approach).
  """

  alias Ferricstore.Raft.CommandClock
  alias Ferricstore.Store.Router

  @spec execute([{binary(), [binary()]}], %{binary() => non_neg_integer()}, binary() | nil) ::
          [term()] | nil | {:error, binary()}
  def execute([], _watched_keys, _sandbox_namespace), do: []

  def execute(queue, watched_keys, sandbox_namespace) do
    if watches_clean?(watched_keys) do
      case classify_shards(queue, sandbox_namespace) do
        {:single_shard, shard_idx} ->
          execute_single_shard(queue, shard_idx, sandbox_namespace)

        {:multi_shard, shard_groups, index_map} ->
          execute_cross_shard(shard_groups, index_map, length(queue), sandbox_namespace)
      end
    else
      nil
    end
  end

  # ---------------------------------------------------------------------------
  # Single-shard path: dispatch all commands as a batch to the shard GenServer
  # ---------------------------------------------------------------------------

  defp execute_single_shard(queue, shard_idx, sandbox_namespace) do
    ctx = FerricStore.Instance.get(:default)
    shard = Router.resolve_shard(ctx, shard_idx)

    try do
      GenServer.call(shard, {:tx_execute, queue, sandbox_namespace}, 10_000)
    catch
      :exit, {:noproc, _} -> {:error, "ERR server not ready"}
      :exit, {reason, _} -> {:error, "ERR internal error: #{inspect(reason)}"}
    end
  end

  # ---------------------------------------------------------------------------
  # Cross-shard path: anchor shard Raft entry or sequential GenServer fallback
  # ---------------------------------------------------------------------------

  defp execute_cross_shard(shard_groups, index_map, total, sandbox_namespace) do
    anchor_idx = shard_groups |> Map.keys() |> Enum.min()

    shard_batches =
      Enum.map(shard_groups, fn {shard_idx, cmds_with_indices} ->
        cmds = Enum.map(cmds_with_indices, fn {_orig_idx, cmd, args} -> {cmd, args} end)
        {shard_idx, cmds, sandbox_namespace}
      end)

    command = {:cross_shard_tx, shard_batches}
    shard_id = Ferricstore.Raft.Cluster.shard_server_id(anchor_idx)
    corr = make_ref()

    try do
      case CommandClock.pipeline_command(shard_id, command, corr, :normal) do
        :ok ->
          case wait_for_ra_result(corr, shard_id, anchor_idx, command) do
            {:ok, shard_results} ->
              Enum.each(Map.keys(shard_groups), fn idx ->
                Ferricstore.Store.WriteVersion.increment(idx)
              end)

              reassemble_results(shard_results, index_map, total)

            {:error, _} = err ->
              err
          end

        {:error, :noproc} ->
          execute_cross_shard_sequential(shard_groups, index_map, total, sandbox_namespace)

        {:error, _reason} ->
          execute_cross_shard_sequential(shard_groups, index_map, total, sandbox_namespace)
      end
    catch
      :exit, {:noproc, _} ->
        execute_cross_shard_sequential(shard_groups, index_map, total, sandbox_namespace)
    end
  end

  # Fallback: dispatch to each shard's GenServer sequentially.
  # Used when the ra server is temporarily unavailable (e.g. during restart).
  defp execute_cross_shard_sequential(shard_groups, index_map, total, sandbox_namespace) do
    shard_results =
      Enum.reduce(shard_groups, %{}, fn {shard_idx, cmds_with_indices}, acc ->
        queue = Enum.map(cmds_with_indices, fn {_orig_idx, cmd, args} -> {cmd, args} end)
        ctx = FerricStore.Instance.get(:default)
        shard = Router.resolve_shard(ctx, shard_idx)

        results =
          try do
            GenServer.call(shard, {:tx_execute, queue, sandbox_namespace}, 10_000)
          catch
            :exit, {:noproc, _} ->
              Enum.map(queue, fn _ -> {:error, "ERR server not ready"} end)

            :exit, {reason, _} ->
              Enum.map(queue, fn _ -> {:error, "ERR internal error: #{inspect(reason)}"} end)
          end

        Map.put(acc, shard_idx, results)
      end)

    reassemble_results(shard_results, index_map, total)
  end

  # Waits for ra_event with our correlation ref. Mirrors Router.wait_for_ra_applied.
  defp wait_for_ra_result(corr, shard_id, idx, command) do
    receive do
      {:ra_event, _leader, {:applied, applied_list}} ->
        case List.keyfind(applied_list, corr, 0) do
          {^corr, result} ->
            {:ok, unwrap_applied(result)}

          nil ->
            wait_for_ra_result(corr, shard_id, idx, command)
        end

      {:ra_event, _from, {:rejected, {:not_leader, maybe_leader, ^corr}}} ->
        leader =
          if maybe_leader in [nil, :undefined],
            do: shard_id,
            else: maybe_leader

        retry_corr = make_ref()

        case CommandClock.pipeline_command(leader, command, retry_corr, :normal) do
          :ok ->
            wait_for_ra_result(retry_corr, leader, idx, command)

          _err ->
            {:error, "ERR cross-shard transaction failed: leader redirect"}
        end

      {:ra_event, _from, {:rejected, {_reason, _hint, ^corr}}} ->
        {:error, "ERR cross-shard transaction rejected"}
    after
      10_000 ->
        {:error, "ERR cross-shard transaction timeout"}
    end
  end

  # State machine wraps every reply as {:applied_at, ra_index, real_result}
  # for read-your-write consistency. Unwrap before passing to reassemble_results.
  defp unwrap_applied({:applied_at, _idx, real}), do: real
  defp unwrap_applied(other), do: other

  # Reassembles per-shard results back into the original command order.
  defp reassemble_results(shard_results, index_map, total) do
    Enum.map(0..(total - 1), fn orig_idx ->
      {shard_idx, pos} = Map.fetch!(index_map, orig_idx)
      results_for_shard = Map.fetch!(shard_results, shard_idx)
      Enum.at(results_for_shard, pos)
    end)
  end

  # ---------------------------------------------------------------------------
  # Shard classification
  # ---------------------------------------------------------------------------

  # Commands that don't target a specific key. These are assigned to
  # whichever shard the keyed commands target, so they never cause CROSSSLOT.
  @keyless_commands MapSet.new(~w(PING ECHO DBSIZE TIME RANDOMKEY))

  @spec classify_shards([{binary(), [binary()]}], binary() | nil) ::
          {:single_shard, non_neg_integer()}
          | {:multi_shard, %{non_neg_integer() => list()}, %{non_neg_integer() => tuple()}}
  defp classify_shards(queue, sandbox_namespace) do
    indexed =
      queue
      |> Enum.with_index()
      |> Enum.map(fn {{cmd, args}, idx} ->
        shard_idx =
          if MapSet.member?(@keyless_commands, cmd) do
            :keyless
          else
            command_shard(args, sandbox_namespace)
          end

        {idx, cmd, args, shard_idx}
      end)

    # Find the first keyed shard to assign keyless commands to.
    # If all commands are keyless, they all go to shard 0.
    default_shard =
      Enum.find_value(indexed, 0, fn
        {_, _, _, :keyless} -> nil
        {_, _, _, shard} -> shard
      end)

    # Replace :keyless with the default shard
    indexed =
      Enum.map(indexed, fn
        {idx, cmd, args, :keyless} -> {idx, cmd, args, default_shard}
        entry -> entry
      end)

    shard_indices = indexed |> Enum.map(fn {_, _, _, s} -> s end) |> Enum.uniq()

    case shard_indices do
      [single] ->
        {:single_shard, single}

      _multiple ->
        shard_groups =
          Enum.group_by(
            indexed,
            fn {_idx, _cmd, _args, shard_idx} -> shard_idx end,
            fn {idx, cmd, args, _shard_idx} -> {idx, cmd, args} end
          )

        index_map = build_index_map(shard_groups)

        {:multi_shard, shard_groups, index_map}
    end
  end

  defp build_index_map(shard_groups) do
    Enum.reduce(shard_groups, %{}, fn {shard_idx, cmds}, acc ->
      cmds
      |> Enum.with_index()
      |> Enum.reduce(acc, fn {{orig_idx, _cmd, _args}, pos}, inner ->
        Map.put(inner, orig_idx, {shard_idx, pos})
      end)
    end)
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

    Enum.all?(watched, fn {key, saved_hash} ->
      try do
        :erlang.phash2(Router.get(ctx, key)) == saved_hash
      catch
        :exit, _ -> false
      end
    end)
  end
end
