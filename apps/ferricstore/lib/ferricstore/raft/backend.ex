defmodule Ferricstore.Raft.Backend do
  @moduledoc """
  Runtime write-backend selector.

  The default remains the current `:ra` implementation. `:waraft` is a gated
  replacement candidate used by tests and benchmarks until it satisfies the
  full migration checklist.
  """

  alias Ferricstore.Raft.WARaftBackend

  @spec selected() :: :ra | :waraft
  def selected do
    Application.get_env(:ferricstore, :raft_backend, :ra)
    |> normalize_selected()
  end

  defp normalize_selected(value) when value in [:ra, "ra"], do: :ra
  defp normalize_selected(value) when value in [:waraft, "waraft"], do: :waraft

  defp normalize_selected(value) do
    raise ArgumentError,
          "invalid :ferricstore :raft_backend #{inspect(value)}; expected :ra or :waraft"
  end

  @spec waraft?() :: boolean()
  def waraft?, do: selected() == :waraft

  @spec write(non_neg_integer(), tuple()) :: term()
  def write(shard_index, command),
    do: WARaftBackend.write(shard_index, normalize_command(command))

  @spec write_many([{non_neg_integer(), tuple()}]) :: [term()]
  def write_many(shard_commands) when is_list(shard_commands) do
    shard_commands
    |> Enum.map(fn {shard_index, command} -> {shard_index, normalize_command(command)} end)
    |> WARaftBackend.write_many()
  end

  @spec write_put_batch(non_neg_integer(), [{binary(), binary(), non_neg_integer()}]) :: term()
  def write_put_batch(shard_index, entries),
    do: WARaftBackend.write_put_batch(shard_index, entries)

  @spec write_delete_batch(non_neg_integer(), [binary()]) :: term()
  def write_delete_batch(shard_index, keys),
    do: WARaftBackend.write_delete_batch(shard_index, keys)

  @spec write_batch(non_neg_integer(), [tuple()]) :: term()
  def write_batch(shard_index, commands),
    do: WARaftBackend.write_batch(shard_index, Enum.map(commands, &normalize_command/1))

  # Router/Shard-forwarded compound commands include the parent redis_key so the
  # old Ra path can enter through the Shard GenServer. WARaft applies straight
  # through the state machine, whose compact command shape derives redis_key
  # from the internal compound key.
  defp normalize_command({:compound_put, _redis_key, compound_key, value, expire_at_ms}),
    do: {:compound_put, compound_key, value, expire_at_ms}

  defp normalize_command({:compound_delete, _redis_key, compound_key}),
    do: {:compound_delete, compound_key}

  defp normalize_command({:compound_delete_prefix, _redis_key, prefix}),
    do: {:compound_delete_prefix, prefix}

  defp normalize_command(command), do: command
end
