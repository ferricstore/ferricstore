defmodule Ferricstore.Raft.Backend do
  @moduledoc """
  Runtime write backend.

  WARaft is the only supported production path. Runtime selection is fixed here
  so deploys and benchmarks cannot drift through config flags.
  """

  alias Ferricstore.LatencyTrace
  alias Ferricstore.Raft.WARaftBackend

  @running_backend_key {__MODULE__, :running_backend}

  @spec selected() :: :waraft
  def selected, do: :waraft

  @doc false
  @spec put_running!(:waraft) :: :ok
  def put_running!(:waraft = backend) do
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

  This is intentionally separate from `selected/0`: default-instance runtime
  routing must stay pinned to the backend that actually booted.
  """
  @spec running() :: :waraft | :undefined
  def running do
    :persistent_term.get(@running_backend_key, :undefined)
  end

  @spec running_or_selected() :: :waraft
  def running_or_selected do
    case running() do
      :waraft -> :waraft
      :undefined -> selected()
    end
  end

  @spec running_waraft?() :: boolean()
  def running_waraft?, do: running_or_selected() == :waraft

  @spec waraft?() :: boolean()
  def waraft?, do: true

  @spec write(non_neg_integer(), tuple()) :: term()
  def write(shard_index, command) do
    trace_ra_wait(fn ->
      WARaftBackend.write(shard_index, command |> normalize_command() |> trace_command())
    end)
  end

  @spec write_many([{non_neg_integer(), tuple()}]) :: [term()]
  def write_many(shard_commands) when is_list(shard_commands) do
    trace_ra_wait(fn ->
      trace_enabled? = LatencyTrace.enabled?()

      shard_commands
      |> Enum.map(fn {shard_index, command} ->
        {shard_index, command |> normalize_command() |> trace_command(trace_enabled?)}
      end)
      |> WARaftBackend.write_many()
    end)
  end

  @spec write_put_batch(non_neg_integer(), [{binary(), binary(), non_neg_integer()}]) :: term()
  def write_put_batch(shard_index, entries) do
    trace_ra_wait(fn ->
      if LatencyTrace.enabled?() do
        WARaftBackend.write(shard_index, LatencyTrace.maybe_wrap_command({:put_batch, entries}))
      else
        WARaftBackend.write_put_batch(shard_index, entries)
      end
    end)
  end

  @spec write_delete_batch(non_neg_integer(), [binary()]) :: term()
  def write_delete_batch(shard_index, keys) do
    trace_ra_wait(fn ->
      if LatencyTrace.enabled?() do
        WARaftBackend.write(shard_index, LatencyTrace.maybe_wrap_command({:delete_batch, keys}))
      else
        WARaftBackend.write_delete_batch(shard_index, keys)
      end
    end)
  end

  @spec write_batch(non_neg_integer(), [tuple()]) :: term()
  def write_batch(shard_index, commands),
    do:
      trace_ra_wait(fn ->
        trace_enabled? = LatencyTrace.enabled?()

        WARaftBackend.write_batch(
          shard_index,
          Enum.map(commands, &(&1 |> normalize_command() |> trace_command(trace_enabled?)))
        )
      end)

  defp trace_command(command), do: trace_command(command, LatencyTrace.enabled?())
  defp trace_command(command, true), do: LatencyTrace.wrap_command(command)
  defp trace_command(command, false), do: command

  defp trace_ra_wait(fun) do
    if LatencyTrace.enabled?() do
      LatencyTrace.span("server_ra_wait_us", fn ->
        fun.() |> LatencyTrace.merge_result()
      end)
    else
      fun.()
    end
  end

  # Router/Shard-forwarded compound commands may include the parent redis_key.
  # WARaft applies straight through the state machine, whose compact command
  # shape derives redis_key from the internal compound key.
  defp normalize_command({:compound_put, _redis_key, compound_key, value, expire_at_ms}),
    do: {:compound_put, compound_key, value, expire_at_ms}

  defp normalize_command({:compound_delete, _redis_key, compound_key}),
    do: {:compound_delete, compound_key}

  defp normalize_command({:compound_delete_prefix, _redis_key, prefix}),
    do: {:compound_delete_prefix, prefix}

  defp normalize_command(command), do: command
end
