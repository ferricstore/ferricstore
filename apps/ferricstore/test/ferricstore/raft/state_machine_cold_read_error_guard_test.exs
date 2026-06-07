defmodule Ferricstore.Raft.StateMachineColdReadErrorGuardTest do
  use ExUnit.Case, async: true

  test "state-machine batch cold reads report per-index NIF errors" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    for function <- [
          "cross_shard_read_cold_bitcask_values",
          "cross_shard_read_cold_meta_bitcask_batch",
          "sm_store_read_bitcask_cold_batch"
        ] do
      [_, body] = Regex.run(~r/defp #{function}\([^\n]*\) do(.*?)(?=\n  defp )/s, source)

      assert body =~ "normalize_state_machine_batch_values",
             "#{function}/... must preserve per-index batch errors instead of converting them to nil"

      assert body =~ "emit_state_machine_batch_cold_errors",
             "#{function}/... must emit telemetry for corrupt/missing cold records"
    end
  end
end
