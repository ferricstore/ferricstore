defmodule FerricstoreServer.Acl.RulesTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Acl.Rules

  test "incremental SETUSER updates cannot grow retained patterns past the state limit" do
    regex = ~r/^tenant:/

    user = %{
      enabled: true,
      password: nil,
      commands: MapSet.new(),
      denied_commands: MapSet.new(),
      keys: List.duplicate({"tenant:*", :read, regex}, Rules.max_patterns()),
      channels: List.duplicate({"events:*", regex}, Rules.max_patterns())
    }

    assert {:error, key_reason} = Rules.apply_rules(user, ["~another:*"])
    assert key_reason =~ "more than 4096 key patterns"

    assert {:error, channel_reason} = Rules.apply_rules(user, ["&another:*"])
    assert channel_reason =~ "more than 4096 channel patterns"
  end

  test "missing or malformed channel state is fail closed" do
    assert Rules.user_channels(%{}) == []
    assert Rules.user_channels(%{channels: :invalid}) == []
  end

  test "rule validation rejects excessive non-pattern modifiers before repeated processing" do
    assert {:error, reason} = Rules.validate_rule_limits(List.duplicate("on", 16_385))
    assert reason =~ "more than 16384 rule tokens"
  end
end
