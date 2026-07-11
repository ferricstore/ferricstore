defmodule Ferricstore.Flow.RetentionGuardTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.RetentionGuard

  test "encodes the current state-entry identity" do
    assert {7, {:state_enter_seq, 11}} =
             %{version: 7, state_enter_seq: 11}
             |> RetentionGuard.encode()
             |> :erlang.binary_to_term([:safe])
  end

  test "rejects records without the current state-entry identity" do
    assert_raise ArgumentError, ~r/state_enter_seq/, fn ->
      RetentionGuard.encode(%{version: 7, created_at_ms: 1_000})
    end
  end
end
