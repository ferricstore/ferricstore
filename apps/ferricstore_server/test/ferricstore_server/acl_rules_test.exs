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

  # Redis: "reset" returns the user to its state right after creation
  # (resetpass, resetkeys, resetchannels, off, -@all); "nocommands" is -@all.
  test "reset clears every earlier permission and nocommands removes commands" do
    hasher = &("hashed:" <> &1)

    user = %{
      base_user()
      | password: "hashed:old",
        commands: :all,
        keys: :all,
        channels: :all
    }

    assert {:ok, reset} = Rules.apply_rules(user, [">pass", "~app:*", "+get", "reset"], hasher)

    assert %{
             enabled: false,
             password: nil,
             commands: commands,
             denied_commands: denied,
             keys: [],
             channels: []
           } = reset

    assert MapSet.size(commands) == 0 and MapSet.size(denied) == 0

    assert {:ok, rebuilt} =
             Rules.apply_rules(user, ["reset", "on", ">new", "~x:*", "+set"], hasher)

    assert %{enabled: true, password: "hashed:new", keys: [{"x:*", :rw, _regex}]} = rebuilt
    assert rebuilt.commands == MapSet.new(["SET"])

    assert {:ok, %{commands: none}} = Rules.apply_rules(user, ["nocommands"], hasher)
    assert none == MapSet.new()
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
