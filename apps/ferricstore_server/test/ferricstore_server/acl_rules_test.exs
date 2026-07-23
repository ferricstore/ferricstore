defmodule FerricstoreServer.Acl.RulesTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Acl.{CommandCategories, Rules}

  test "native client connection controls are individually grantable" do
    commands = ~w(
      ROUTE ROUTE_BATCH SHARDS BACKPRESSURE WINDOW_UPDATE
      SUBSCRIBE_EVENTS UNSUBSCRIBE_EVENTS
    )

    rules = ["-@all" | Enum.map(commands, &("+" <> &1))]

    assert {:ok, user} = Rules.apply_rules(base_user(), rules)
    assert user.commands == MapSet.new(commands)

    assert {:ok, connection_commands} = CommandCategories.category_commands("CONNECTION")
    assert MapSet.subset?(MapSet.new(commands), connection_commands)
  end

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

  test "SETUSER hashes only the final effective password" do
    test_pid = self()

    hasher = fn password ->
      send(test_pid, {:hashed_password, password})
      "hashed:" <> password
    end

    assert {:ok, %{password: "hashed:final"}} =
             Rules.apply_rules(
               base_user(),
               [">first", ">second", "nopass", ">final"],
               hasher
             )

    assert_receive {:hashed_password, "final"}
    refute_receive {:hashed_password, _password}

    assert {:ok, %{password: nil}} =
             Rules.apply_rules(base_user(), [">first", ">second", "resetpass"], hasher)

    refute_receive {:hashed_password, _password}
  end

  test "SETUSER does not hash when a later rule or final state is invalid" do
    test_pid = self()

    hasher = fn password ->
      send(test_pid, {:hashed_password, password})
      "hashed:" <> password
    end

    assert {:error, reason} =
             Rules.apply_rules(base_user(), [">secret", "+unknown-command"], hasher)

    assert reason =~ "Unknown command"
    refute_receive {:hashed_password, _password}

    user_at_pattern_limit = %{
      base_user()
      | keys:
          List.duplicate(
            {"tenant:*", :read, ~r/^tenant:/},
            Rules.max_patterns()
          )
    }

    assert {:error, retained_reason} =
             Rules.apply_rules(user_at_pattern_limit, [">secret", "~another:*"], hasher)

    assert retained_reason =~ "more than 4096 key patterns"
    refute_receive {:hashed_password, _password}
  end

  defp base_user do
    %{
      enabled: true,
      password: nil,
      commands: MapSet.new(),
      denied_commands: MapSet.new(),
      keys: [],
      channels: []
    }
  end
end
