defmodule Ferricstore.ErrorReasonsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.ErrorReasons

  test "write timeout is represented as unknown outcome for embedded callers" do
    assert ErrorReasons.write_timeout_unknown() == {:error, {:timeout, :unknown_outcome}}
  end

  # Native and HTTP clients got the reason's raw text, "{:timeout, :unknown_outcome}".
  test "clients read a timed-out write as one that may have applied" do
    {:error, reason} = ErrorReasons.write_timeout_unknown()

    assert ErrorReasons.client_message(reason, nil) ==
             "ERR write timed out and may or may not have applied; read before retrying"
  end
end
