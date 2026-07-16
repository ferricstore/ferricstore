defmodule Ferricstore.CrossShardOp.IntentResolver do
  @moduledoc """
  Resolves stale intents left by crashed cross-shard operation coordinators.

  On startup (or on-demand), scans all shards for current intent records with
  status `:executing`. For each stale intent:

    * Checks if the intent is old enough to be considered stale (>10s)
    * Uses the flat watch-token key set to release owner-matched locks
    * Deletes the cleanup record without re-executing a non-idempotent command

  Intent records are self-describing: they contain the command type, involved
  keys, and a non-empty watch-token map. Token values are retained for
  diagnostics; recovery never performs extra reads because token comparison
  cannot make arbitrary command re-execution safe.
  """

  alias Ferricstore.CrossShardOp.Intent
  alias Ferricstore.HLC
  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Raft.CommandClock
  alias Ferricstore.Store.Router

  @stale_threshold_ms 10_000

  @doc """
  Scans all shards for stale intents and cleans them up.

  This function is safe to call multiple times. It only removes intents
  whose operations have either completed or are stale.
  """
  @spec resolve_stale_intents() :: :ok
  def resolve_stale_intents do
    shard_count =
      :persistent_term.get(
        :ferricstore_shard_count,
        Application.get_env(:ferricstore, :shard_count, 4)
      )

    for shard_idx <- 0..(shard_count - 1) do
      resolve_shard_intents(shard_idx)
    end

    :ok
  end

  @doc false
  @spec resolve_shard_intents(non_neg_integer()) :: :ok
  def resolve_shard_intents(shard_idx) do
    shard_id = Cluster.shard_server_id(shard_idx)

    case CommandClock.process_command(shard_id, {:get_intents}) do
      {:ok, {:applied_at, _idx, intents}, _} when is_map(intents) and map_size(intents) > 0 ->
        Enum.each(intents, fn {owner_ref, intent} ->
          resolve_single_intent(shard_idx, owner_ref, intent)
        end)

      {:ok, intents, _} when is_map(intents) and map_size(intents) > 0 ->
        Enum.each(intents, fn {owner_ref, intent} ->
          resolve_single_intent(shard_idx, owner_ref, intent)
        end)

      _ ->
        :ok
    end
  end

  defp resolve_single_intent(shard_idx, owner_ref, intent) do
    delete_intent = fn owner_ref ->
      shard_idx
      |> Cluster.shard_server_id()
      |> CommandClock.process_command({:delete_intent, owner_ref})
      |> command_ok()
    end

    resolve_stale_intent(
      owner_ref,
      intent,
      HLC.now_ms(),
      &unlock_intent_keys/2,
      delete_intent
    )
  end

  defp resolve_stale_intent(owner_ref, intent, now_ms, unlock, delete_intent) do
    case Intent.validate(owner_ref, intent) do
      {:ok, keys} ->
        if now_ms - intent.created_at > @stale_threshold_ms do
          with :ok <- unlock.(keys, owner_ref),
               :ok <- delete_intent.(owner_ref) do
            :ok
          end
        else
          :ok
        end

      {:error, :invalid_cross_shard_intent} ->
        :ok
    end
  end

  defp unlock_intent_keys(keys, owner_ref) do
    ctx = FerricStore.Instance.get(:default)

    keys
    |> Enum.group_by(&Router.shard_for(ctx, &1))
    |> Enum.reduce_while(:ok, fn {shard_idx, shard_keys}, :ok ->
      result =
        shard_idx
        |> Cluster.shard_server_id()
        |> CommandClock.process_command({:unlock_keys, shard_keys, owner_ref})
        |> command_ok()

      case result do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp command_ok({:ok, {:applied_at, _index, :ok}, _leader}), do: :ok
  defp command_ok({:ok, :ok, _leader}), do: :ok
  defp command_ok({:ok, {:applied_at, _index, {:error, reason}}, _leader}), do: {:error, reason}
  defp command_ok({:ok, {:error, reason}, _leader}), do: {:error, reason}
  defp command_ok({:error, reason}), do: {:error, reason}
  defp command_ok(other), do: {:error, {:unexpected_command_result, other}}

  @doc false
  def __resolve_stale_intent_for_test__(owner_ref, intent, now_ms, unlock, delete_intent)
      when is_function(unlock, 2) and is_function(delete_intent, 1) do
    resolve_stale_intent(owner_ref, intent, now_ms, unlock, delete_intent)
  end
end
