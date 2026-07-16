defmodule Ferricstore.Raft.CommandPrefixTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.CommandPrefix
  alias Ferricstore.Raft.CommandStamp

  test "accepted command envelopes preserve namespace extraction" do
    command = {:put, "tenant:key", "value", 0}

    wrapped_commands = [
      {:ferricstore_latency_trace, command},
      {:ferricstore_apply_context, <<1>>, command},
      {:flow_policy_fence, [], command},
      {:flow_shared_ref_write, 2, command},
      {:async, :"origin@node", command},
      {command, %{hlc_ts: {1, 0}}},
      CommandStamp.to_ttb(command)
    ]

    assert CommandPrefix.extract(command) == "tenant"
    assert Enum.all?(wrapped_commands, &(CommandPrefix.extract(&1) == "tenant"))
  end

  test "origin-checked commands keep their explicit guarded key" do
    inner = {:put, "other:key", "value", 0}

    command =
      {:ferricstore_latency_trace,
       {:origin_checked, "tenant:key", inner, nil, 0, "value", 0}}

    assert CommandPrefix.extract(command) == "tenant"
  end
end
