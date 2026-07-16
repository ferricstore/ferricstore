defmodule FerricstoreServer.AclFileParserTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Acl.FileParser
  alias FerricstoreServer.Acl.Formatter

  test "ACL files reject unknown explicit allow and deny command rules" do
    for rule <- ["+definitely-not-a-command", "-definitely-not-a-command"] do
      contents = "user default on nopass ~* &* +@all #{rule}\n"

      assert {:error, reason} = FileParser.parse(contents)
      assert reason =~ "Unknown command"
      assert reason =~ rule
    end
  end

  test "ACL files reject invalid UTF-8 without raising" do
    contents = <<"user invalid-", 0xFF, " on nopass ~* &* +@all\n">>

    assert {:error, reason} = FileParser.parse(contents)
    assert reason =~ "UTF-8"
  end

  test "encoded ACL usernames cannot bypass UTF-8 validation" do
    encoded = Base.url_encode64(<<0xFF>>, padding: false)
    contents = "user64 b#{encoded} on nopass ~* &* +@all\n"

    assert {:error, reason} = FileParser.parse(contents)
    assert reason =~ "UTF-8"
  end

  @tag :acl_duplicate_file_user
  test "ACL files reject duplicate user definitions instead of silently replacing rules" do
    contents =
      "user default on nopass ~allowed:* &* +get\n" <>
        "user default off nopass ~* &* +@all\n"

    assert {:error, reason} = FileParser.parse(contents)
    assert reason =~ "line 2"
    assert reason =~ "duplicate user 'default'"
  end

  test "file formatting round-trips usernames that are not plain tokens" do
    user = %{
      enabled: true,
      password: nil,
      commands: :all,
      denied_commands: MapSet.new(),
      keys: :all,
      channels: :all
    }

    for username <- ["", "user name", "line\nbreak"] do
      contents = Formatter.format_user_for_file({username, user}) <> "\n"

      assert {:ok, [{^username, decoded}]} = FileParser.parse(contents)
      assert decoded.enabled
      assert decoded.commands == :all
    end
  end

  @tag :acl_list_encoded_username
  test "ACL list rules encode usernames that would make the rule ambiguous" do
    user = %{
      enabled: true,
      commands: :all,
      denied_commands: MapSet.new(),
      keys: :all,
      channels: :all
    }

    for username <- ["", "user name", "line\nbreak"] do
      rule = Formatter.format_user_rule({username, user})

      assert String.starts_with?(rule, "user64 b")
      refute rule =~ "\n"
      refute rule =~ "user #{username} "
    end
  end

  @tag :acl_encoded_patterns
  test "file formatting round-trips key and channel patterns containing whitespace" do
    key_patterns = [
      {"tenant one:*", :rw},
      {"line\nread:*", :read},
      {"tab\twrite:*", :write}
    ]

    channel_patterns = ["news room:*", "line\nchannel:*"]

    user = %{
      enabled: true,
      password: nil,
      commands: :all,
      denied_commands: MapSet.new(),
      keys: Enum.map(key_patterns, fn {glob, mode} -> {glob, mode, nil} end),
      channels: Enum.map(channel_patterns, &{&1, nil})
    }

    contents = Formatter.format_user_for_file({"default", user}) <> "\n"

    assert length(String.split(contents, "\n", trim: true)) == 1
    assert contents =~ "key64:rw:b"
    assert contents =~ "key64:read:b"
    assert contents =~ "key64:write:b"
    assert contents =~ "channel64:b"

    assert {:ok, [{"default", decoded}]} = FileParser.parse(contents)
    assert Enum.map(decoded.keys, fn {glob, mode, _compiled} -> {glob, mode} end) == key_patterns
    assert Enum.map(decoded.channels, fn {glob, _compiled} -> glob end) == channel_patterns
  end
end
