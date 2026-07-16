defmodule Ferricstore.Raft.StateMachineDerivedValueLimitGuardTest do
  use ExUnit.Case, async: true

  test "SETRANGE validates its target size before constructing the output" do
    source =
      Ferricstore.Test.SourceFiles.state_machine_source()
      |> Ferricstore.Test.SourceFiles.private_function_source!("do_setrange")

    assert source =~ "ApplyLimits.setrange_size"
    assert source =~ "ApplyLimits.validate_value_size"
    assert before?(source, "ApplyLimits.validate_value_size", "sm_apply_setrange")
  end

  test "SETBIT validates its target size before zero-padding the bitmap" do
    source =
      Ferricstore.Test.SourceFiles.state_machine_source()
      |> Ferricstore.Test.SourceFiles.private_function_source!("do_setbit")

    assert source =~ "ApplyLimits.setbit_size"
    assert source =~ "ApplyLimits.validate_value_size"
    assert before?(source, "ApplyLimits.validate_value_size", ":binary.copy")
  end

  defp before?(source, first, second) do
    :binary.match(source, first) < :binary.match(source, second)
  end
end
