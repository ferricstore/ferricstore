defmodule Ferricstore.Raft.WARaftBackend.Sections.Part05 do
  @moduledoc false

  # Extracted from WARaftBackend: emit_commit_timeout_if_needed .. profile_startup_phase
  defmacro __using__(_opts) do
    quote do
alias Ferricstore.ErrorReasons
alias Ferricstore.NamespaceConfig
alias Ferricstore.Raft.BlobCommand
alias Ferricstore.Raft.CommandStamp
alias Ferricstore.Raft.WARaftBackend.Batcher, as: NamespaceBatcher
alias Ferricstore.Raft.WARaftBackend.BatcherSupervisor, as: NamespaceBatcherSupervisor
alias Ferricstore.Raft.WARaftBackend.SyncGate
        defp emit_commit_timeout_if_needed(
               shard_index,
               command,
               {:error, :timeout},
               acquired_bytes,
               started_mono,
               path
             ) do
          emit_commit_timeout(shard_index, command_shape(command), acquired_bytes, started_mono, path)
        end
      
        defp emit_commit_timeout_if_needed(
               _shard_index,
               _command,
               _result,
               _acquired_bytes,
               _started_mono,
               _path
             ),
             do: :ok
      
        defp emit_commit_timeout(shard_index, command_shape, acquired_bytes, started_mono, path) do
          duration_us =
            System.monotonic_time()
            |> Kernel.-(started_mono)
            |> System.convert_time_unit(:native, :microsecond)
      
          :telemetry.execute(
            [:ferricstore, :waraft, :commit, :timeout],
            %{
              count: 1,
              duration_us: max(duration_us, 0),
              timeout_ms: @timeout,
              acquired_bytes: acquired_bytes,
              inflight_bytes: inflight_commit_bytes(shard_index)
            },
            %{
              shard_index: shard_index,
              command_shape: command_shape,
              path: path,
              reason: :timeout
            }
          )
        rescue
          _ -> :ok
        end
      
        defp command_shape({:put_batch, _entries}), do: :put_batch
        defp command_shape({:delete_batch, _keys}), do: :delete_batch
        defp command_shape({:batch, _commands}), do: :batch
        defp command_shape(command) when is_tuple(command), do: elem(command, 0)
        defp command_shape(_command), do: :unknown
      
        defp emit_commit_bytes_rejected(shard_index, bytes, current, max_bytes) do
          :telemetry.execute(
            [:ferricstore, :waraft, :commit_bytes, :rejected],
            %{count: 1, bytes: bytes, current_bytes: current, max_bytes: max_bytes},
            %{shard_index: shard_index}
          )
        rescue
          _ -> :ok
        end
      
        defp profile_startup_phase(phase, metadata, fun) when is_function(fun, 0) do
          {duration_us, result} = :timer.tc(fun)
      
          :telemetry.execute(
            [:ferricstore, :waraft, :backend, :startup_phase],
            %{duration_us: duration_us},
            Map.put(metadata, :phase, phase)
          )
      
          result
        end
    end
  end
end
