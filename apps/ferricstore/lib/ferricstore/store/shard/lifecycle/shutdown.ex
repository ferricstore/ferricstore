defmodule Ferricstore.Store.Shard.Lifecycle.Shutdown do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  require Logger

  @spec do_terminate(term(), map()) :: :ok
  @doc false
  def do_terminate(_reason, state) do
    t0 = System.monotonic_time(:microsecond)

    # Step 1: drain any in-flight async flush and flush remaining pending
    # writes synchronously to guarantee all data hits disk before exit.
    state = ShardFlush.await_in_flight(state)
    state = ShardFlush.flush_pending_sync(state)
    pending_write_count = length(state.pending)

    pending_flush_result =
      case Map.get(state, :last_flush_error) do
        nil -> :ok
        reason -> {:error, reason}
      end

    t_flush = System.monotonic_time(:microsecond)

    # Step 2: write v2 hint file for the active file so the next startup
    # can rebuild the keydir from hints instead of replaying the full log.
    hint_result =
      cond do
        pending_flush_result != :ok ->
          {:error, :unflushed_pending_writes}

        Map.get(state, :promotion_recovery_required, false) ->
          {:error, :promotion_recovery_required}

        true ->
          ShardFlush.write_hint_for_file(state, state.active_file_id)
      end

    hint_dir_fsync_result =
      if hint_result == :ok do
        NIF.v2_fsync_dir(state.shard_data_path)
      end

    fsync_result = NIF.v2_fsync(state.active_file_path)

    shutdown_errors =
      shutdown_errors(pending_flush_result, hint_result, hint_dir_fsync_result, fsync_result)

    shutdown_status = if shutdown_errors == [], do: :ok, else: :warning

    t_hint = System.monotonic_time(:microsecond)

    # Step 3: emit shutdown telemetry for operator visibility.
    :telemetry.execute(
      [:ferricstore, :shard, :shutdown],
      %{
        flush_duration_us: t_flush - t0,
        hint_duration_us: t_hint - t_flush,
        total_duration_us: t_hint - t0
      },
      %{
        shard_index: state.index,
        status: shutdown_status,
        errors: shutdown_errors,
        pending_write_count: pending_write_count
      }
    )

    log_shutdown_result(state.index, t_flush - t0, t_hint - t_flush, shutdown_errors)

    :ok
  end

  defp shutdown_errors(pending_flush_result, hint_result, hint_dir_fsync_result, fsync_result) do
    []
    |> maybe_shutdown_error(:pending_flush, pending_flush_result)
    |> maybe_shutdown_error(:hint_write, hint_result)
    |> maybe_shutdown_error(:hint_dir_fsync, hint_dir_fsync_result)
    |> maybe_shutdown_error(:active_fsync, fsync_result)
    |> Enum.reverse()
  end

  defp maybe_shutdown_error(errors, _operation, result) when result in [:ok, nil], do: errors

  defp maybe_shutdown_error(errors, operation, {:error, reason}),
    do: [{operation, reason} | errors]

  defp maybe_shutdown_error(errors, operation, other),
    do: [{operation, {:unexpected_result, other}} | errors]

  defp log_shutdown_result(index, flush_us, hint_us, []) do
    Logger.info(
      "Shard #{index}: shutdown complete " <>
        "(flush=#{flush_us}us, hint=#{hint_us}us)"
    )
  end

  defp log_shutdown_result(index, flush_us, hint_us, errors) do
    Logger.warning(
      "Shard #{index}: shutdown complete with warnings " <>
        "(flush=#{flush_us}us, hint=#{hint_us}us, errors=#{inspect(errors)})"
    )
  end
end
