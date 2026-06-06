defmodule Ferricstore.Flow.HistoryProjector.LogTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.HistoryProjector.Log

  test "read_value rejects non history locators" do
    assert Log.read_value("/tmp/missing", :not_history, 0) == {:error, :not_flow_history}
  end
end
