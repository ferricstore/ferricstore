defmodule Ferricstore.FlowHistoryRetentionGuardTest do
  use ExUnit.Case, async: true

  @moduletag :guard

  test "terminal Flow apply does not synchronously scan and rewrite all history records" do
    source =
      File.read!(Path.expand("../../lib/ferricstore/raft/state_machine.ex", __DIR__))

    refute source =~ "flow_refresh_terminal_history_expirations"
    refute source =~ "flow_history_keys_for_prefix"
  end
end
