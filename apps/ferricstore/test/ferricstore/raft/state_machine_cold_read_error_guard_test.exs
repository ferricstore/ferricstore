defmodule Ferricstore.Raft.StateMachineColdReadErrorGuardTest do
  use ExUnit.Case, async: true
  @moduletag :raft

  test "state-machine batch cold reads report per-index NIF errors" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    for function <- [
          "cross_shard_read_cold_bitcask_values",
          "cross_shard_read_cold_meta_bitcask_batch",
          "sm_store_read_bitcask_cold_batch"
        ] do
      bodies = private_function_bodies(source, function)

      assert Enum.any?(bodies, &String.contains?(&1, "normalize_state_machine_batch_values")),
             "#{function}/... must preserve per-index batch errors instead of converting them to nil"

      assert Enum.any?(bodies, &String.contains?(&1, "emit_state_machine_batch_cold_errors")),
             "#{function}/... must emit telemetry for corrupt/missing cold records"
    end
  end

  defp private_function_bodies(source, function) do
    pattern = ~r/^\s*defp #{function}\b.*?(?=^\s*defp\s+|\z)/ms

    pattern
    |> Regex.scan(source)
    |> List.flatten()
  end
end
