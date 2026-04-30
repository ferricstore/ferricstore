defmodule Ferricstore.CrossShardOp.IntentResolver do
  @moduledoc """
  Resolves stale intents left by crashed cross-shard operation coordinators.

  On startup (or on-demand), scans all shards for intent records with
  status `:executing`. For each stale intent:

    * Checks if the intent is old enough to be considered stale (>10s)
    * Reads current state of involved keys
    * Compares value hashes with `:erlang.phash2/1`
    * If hash matches: data unchanged, safe to clean up the intent
    * If hash doesn't match: someone wrote new data, clean up intent (don't re-execute)
    * If key doesn't exist: already completed or expired, clean up intent

  Intent records are self-describing: they contain the command type, involved
  keys, and expected value hashes.
  """

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
    now = HLC.now_ms()
    created_at = Map.get(intent, :created_at, 0)

    if now - created_at > @stale_threshold_ms do
      # Intent is old enough to be considered stale. Check value hashes
      # to determine the state of the data.
      value_hashes = Map.get(intent, :value_hashes, %{})
      keys_map = Map.get(intent, :keys, %{})

      should_cleanup =
        if map_size(value_hashes) == 0 do
          # No hashes stored — legacy intent or no keys tracked.
          # Clean up based on age alone.
          true
        else
          # Check each key's current value against the stored hash.
          check_value_hashes(value_hashes, keys_map)
        end

      if should_cleanup do
        shard_id = Cluster.shard_server_id(shard_idx)
        CommandClock.process_command(shard_id, {:delete_intent, owner_ref})

        # Also unlock any keys associated with this intent on their respective shards
        unlock_intent_keys(keys_map, owner_ref)
      end
    end

    :ok
  end

  # Checks current values of involved keys against stored hashes.
  # Returns true if the intent should be cleaned up (always true — whether
  # data matches or not, the intent is stale and should be removed).
  #
  # The distinction matters for logging/observability but the action is the
  # same: stale intents are always cleaned up. Re-execution is not safe
  # because we can't guarantee idempotency of arbitrary commands.
  defp check_value_hashes(value_hashes, _keys_map) do
    Enum.all?(value_hashes, fn {key, stored_hash} ->
      current_value = read_current_value(key)
      current_hash = :erlang.phash2(current_value)

      # Whether the hash matches or not, the intent is stale.
      # Hash match means data is unchanged (operation may not have executed).
      # Hash mismatch means someone wrote new data (don't re-execute).
      # nil value means key was deleted or expired (already completed or cleaned up).
      # In all cases, cleaning up the intent is the correct action.
      _matches = current_hash == stored_hash
      true
    end)
  end

  # Unlocks keys listed in the intent's keys map on their respective shards.
  defp unlock_intent_keys(keys_map, owner_ref) do
    keys_map
    |> Map.values()
    |> Enum.uniq()
    |> Enum.each(fn key ->
      ctx = FerricStore.Instance.get(:default)
      shard_idx = Router.shard_for(ctx, key)
      shard_id = Cluster.shard_server_id(shard_idx)
      CommandClock.process_command(shard_id, {:unlock_keys, [key], owner_ref})
    end)
  end

  # Reads the current value of a key through the Router.
  # Returns nil if the key doesn't exist.
  defp read_current_value(key) do
    ctx = FerricStore.Instance.get(:default)
    Router.get(ctx, key)
  end
end
