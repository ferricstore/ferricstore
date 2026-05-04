defmodule Ferricstore.Raft.StateMachineColdReadErrorGuardTest do
  use ExUnit.Case, async: true

  @state_machine_path "lib/ferricstore/raft/state_machine.ex"

  test "state-machine batch cold reads report per-index NIF errors" do
    source = File.read!(@state_machine_path)

    for function <- [
          "cross_shard_read_cold_batch",
          "cross_shard_read_cold_meta_batch",
          "sm_store_read_cold_batch"
        ] do
      [_, body] = Regex.run(~r/defp #{function}\([^\n]*\) do(.*?)(?=\n  defp )/s, source)

      assert body =~ "normalize_state_machine_batch_values",
             "#{function}/... must preserve per-index batch errors instead of converting them to nil"

      assert body =~ "emit_state_machine_batch_cold_errors",
             "#{function}/... must emit telemetry for corrupt/missing cold records"
    end
  end
end
