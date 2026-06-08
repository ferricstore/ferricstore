defmodule Ferricstore.FlowHistoryRetentionGuardTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  @moduletag :guard

  test "terminal Flow apply does not synchronously scan and rewrite all history records" do
    source =
      Ferricstore.Test.SourceFiles.state_machine_source()

    refute source =~ "flow_refresh_terminal_history_expirations"
    refute source =~ "flow_history_keys_for_prefix"
  end
end
