defmodule FerricstoreServer.AclFileParserTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl.FileParser
  alias FerricstoreServer.Acl.Formatter
  alias FerricstoreServer.Acl.Persistence

  setup do
    previous = Application.fetch_env(:ferricstore, :max_acl_users)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:ferricstore, :max_acl_users, value)
        :error -> Application.delete_env(:ferricstore, :max_acl_users)
      end
    end)

    :ok
  end

  test "ACL parsing rejects the total byte budget before processing lines" do
    contents = "user default on nopass ~* &* +@all\n"

    assert {:error, reason} = FileParser.parse(contents, max_file_bytes: 8)
    assert reason =~ "ACL file too large"
    assert reason =~ "max 8"
  end

  test "ACL parsing enforces the user budget while building the result" do
    contents =
      "user first on nopass ~* &* +@all\n" <>
        "user second on nopass ~* &* +@all\n"

    assert {:error, reason} = FileParser.parse(contents, max_users: 1)
    assert reason =~ "line 2"
    assert reason =~ "max ACL users reached (1)"
  end

  test "ACL parsing rejects oversized physical lines before trimming trailing whitespace" do
    contents = "user default on nopass ~* &* +@all" <> String.duplicate(" ", 32) <> "\n"

    assert {:error, reason} = FileParser.parse(contents, max_line_bytes: 48)
    assert reason =~ "line 1"
    assert reason =~ "line exceeds maximum length"
  end

  test "ACL parsing bounds work for blank-line floods" do
    assert {:error, reason} = FileParser.parse("\n\n\n", max_lines: 2)
    assert reason =~ "line 3"
    assert reason =~ "maximum line count"
  end

  test "ACL parser defaults are immutable and do not read application state" do
    Application.put_env(:ferricstore, :max_acl_users, 0)

    assert {:ok, [{"default", _user}]} =
             FileParser.parse("user default on nopass ~* &* +@all\n")
  end

  test "ACL persistence rejects invalid max-user configuration consistently" do
    contents = "user default on nopass ~* &* +@all\n"

    for invalid <- [0, -1, 100_001, :invalid] do
      Application.put_env(:ferricstore, :max_acl_users, invalid)

      assert {:error, "ERR invalid max ACL users configuration"} =
               Persistence.parse_contents(contents)
    end
  end

  test "ACL persistence derives a line budget that can reload configured user capacity" do
    Application.put_env(:ferricstore, :max_acl_users, 50_000)

    contents =
      String.duplicate("\n", 100_001) <>
        "user default on nopass ~* &* +@all\n"

    assert {:ok, [{"default", _user}]} = Persistence.parse_contents(contents)
  end

  test "incremental scanner preserves BOM and CRLF parsing" do
    contents = <<0xEF, 0xBB, 0xBF>> <> "user default on nopass ~* &* +@all\r\n"

    assert {:ok, [{"default", user}]} = FileParser.parse(contents)
    assert user.enabled
  end

  test "ACL parser accepts exact work-budget boundaries and rejects invalid limits" do
    line = "user default on nopass ~* &* +@all"
    contents = line <> "\n"

    assert {:ok, [{"default", _user}]} =
             FileParser.parse(
               contents,
               max_file_bytes: byte_size(contents),
               max_line_bytes: byte_size(line),
               max_lines: 1,
               max_users: 1
             )

    assert {:ok, []} = FileParser.parse("", max_lines: 0, max_users: 0)

    for opts <- [
          [max_file_bytes: -1],
          [max_line_bytes: -1],
          [max_lines: -1],
          [max_users: -1],
          [max_lines: :invalid]
        ] do
      assert {:error, "ERR Invalid ACL parser limits"} = FileParser.parse(contents, opts)
    end
  end

  test "ACL file line limit accepts the largest serialization shape allowed by the catalog" do
    pattern = String.duplicate("x", 4_095) <> " "

    user = %{
      enabled: true,
      password: nil,
      commands: :all,
      denied_commands: MapSet.new(),
      keys: List.duplicate({pattern, :rw, nil}, 200),
      channels: :all
    }

    line = Formatter.format_user_for_file({"default", user})
    assert byte_size(line) > 1_048_576

    assert {:ok, [{"default", decoded}]} = FileParser.parse(line)
    assert length(decoded.keys) == 200
  end

  test "ACL persistence refuses to render a user line beyond the reload limit" do
    pattern = String.duplicate("x", 4_095) <> " "

    user = %{
      enabled: true,
      password: nil,
      commands: :all,
      denied_commands: MapSet.new(),
      keys: List.duplicate({pattern, :rw, nil}, 400),
      channels: :all
    }

    assert {:error, reason} = Persistence.validate_user_line({"default", user})
    assert reason =~ "ACL user line too large"
    assert reason =~ "max 2097152"

    assert {:error, {:line_too_large, bytes, 2_097_152}} =
             Persistence.build_contents(
               [{"default", user}],
               0,
               "2026-07-16T00:00:00Z",
               50_000_000
             )

    assert bytes > 2_097_152
  end

  test "ACL parsing bounds rule tokenization before materializing the complete line" do
    contents = "user default on off on off on nopass ~* &* +@all\n"

    assert {:error, reason} = FileParser.parse(contents, max_rule_tokens: 4)
    assert reason =~ "line 1"
    assert reason =~ "more than 4 rule tokens"
  end

  test "ACL parsing scans lines incrementally instead of splitting the whole file" do
    source =
      File.read!(Path.expand("../../lib/ferricstore_server/acl/file_parser.ex", __DIR__))

    refute source =~ "String.split(~r/\\r?\\n/)"
    assert source =~ ":binary.match(contents, \"\\n\")"
  end

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
