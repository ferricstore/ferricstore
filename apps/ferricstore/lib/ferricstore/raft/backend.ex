defmodule Ferricstore.Raft.Backend do
  @moduledoc """
  Runtime write-backend selector.

  The default remains the current `:ra` implementation. `:waraft` is a gated
  replacement candidate used by tests and benchmarks until it satisfies the
  full migration checklist.
  """

  alias Ferricstore.Raft.WARaftBackend

  @running_backend_key {__MODULE__, :running_backend}

  @spec selected() :: :ra | :waraft
  def selected do
    Application.get_env(:ferricstore, :raft_backend, :ra)
    |> normalize_selected()
  end

  @doc false
  @spec put_running!(:ra | :waraft) :: :ok
  def put_running!(backend) when backend in [:ra, :waraft] do
    :persistent_term.put(@running_backend_key, backend)
    :ok
  end

  @doc false
  @spec clear_running() :: :ok
  def clear_running do
    :persistent_term.erase(@running_backend_key)
    :ok
  end

  @doc """
  Returns the backend used by the currently running default application.

  This is intentionally separate from `selected/0`: `selected/0` reads mutable
  application env and is useful before startup, while default-instance runtime
  routing must stay pinned to the backend that actually booted.
  """
  @spec running() :: :ra | :waraft | :undefined
  def running do
    :persistent_term.get(@running_backend_key, :undefined)
  end

  @spec running_or_selected() :: :ra | :waraft
  def running_or_selected do
    case running() do
      backend when backend in [:ra, :waraft] -> backend
      :undefined -> selected()
    end
  end

  @spec running_waraft?() :: boolean()
  def running_waraft?, do: running_or_selected() == :waraft

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
