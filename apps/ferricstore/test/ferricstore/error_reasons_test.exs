defmodule Ferricstore.ErrorReasonsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.ErrorReasons

  test "write timeout is represented as unknown outcome for embedded callers" do
    assert ErrorReasons.write_timeout_unknown() == {:error, {:timeout, :unknown_outcome}}
  end
end
