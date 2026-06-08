defmodule FerricstoreServer.Acl.Rules do
  @moduledoc false

  alias FerricstoreServer.Acl.CommandCategories
  alias FerricstoreServer.Acl.Password

  @type user :: map()

  @spec apply_rules(user(), [binary()]) :: {:ok, user()} | {:error, binary()}
  def apply_rules(user, rules), do: apply_rules(user, rules, [])

  defp apply_rules(user, [], pending_key_patterns),
    do: {:ok, flush_pending_key_patterns(user, pending_key_patterns)}

  defp apply_rules(user, [rule | rest], pending_key_patterns) do
    case parse_key_rule(user, rule, pending_key_patterns) do
      {:ok, updated, next_pending} ->
        apply_rules(updated, rest, next_pending)

      :not_key_rule ->
        case parse_rule(user, rule) do
          {:ok, updated} -> apply_rules(updated, rest, pending_key_patterns)
          {:error, _} = err -> err
        end
    end
  end

  @spec parse_rule(user(), binary()) :: {:ok, user()} | {:error, binary()}
  def parse_rule(user, "on"), do: {:ok, %{user | enabled: true}}
  def parse_rule(user, "off"), do: {:ok, %{user | enabled: false}}

  def parse_rule(user, ">" <> password) do
    {:ok, %{user | password: Password.hash(password)}}
  end

  def parse_rule(user, "nopass"), do: {:ok, %{user | password: nil}}
  def parse_rule(user, "resetpass"), do: {:ok, %{user | password: nil}}

  def parse_rule(user, "allcommands") do
    {:ok, %{user | commands: :all, denied_commands: MapSet.new()}}
  end

  def parse_rule(user, "allchannels"), do: {:ok, Map.put(user, :channels, :all)}
  def parse_rule(user, "resetchannels"), do: {:ok, Map.put(user, :channels, [])}

  def parse_rule(user, "&" <> pattern) do
    {:ok, add_channel_pattern(user, pattern)}
  end

  def parse_rule(user, "+@all") do
    {:ok, %{user | commands: :all, denied_commands: MapSet.new()}}
  end

  def parse_rule(user, "-@all"),
    do: {:ok, %{user | commands: MapSet.new(), denied_commands: MapSet.new()}}

  def parse_rule(user, "+@" <> category) do
    cat = String.upcase(category)

    case CommandCategories.category_commands(cat) do
      {:ok, cat_cmds} ->
        case user.commands do
          :all ->
            new_denied = MapSet.difference(user.denied_commands, cat_cmds)
            {:ok, %{user | denied_commands: new_denied}}

          cmds ->
            {:ok, %{user | commands: MapSet.union(cmds, cat_cmds)}}
        end

      :error ->
        {:error,
         "ERR Error in ACL SETUSER modifier '+@#{category}': Unknown command category '#{category}'"}
    end
  end

  def parse_rule(user, "-@" <> category) do
    cat = String.upcase(category)

    case CommandCategories.category_commands(cat) do
      {:ok, cat_cmds} ->
        case user.commands do
          :all ->
            new_denied = MapSet.union(user.denied_commands, cat_cmds)
            {:ok, %{user | denied_commands: new_denied}}

          cmds ->
            {:ok, %{user | commands: MapSet.difference(cmds, cat_cmds)}}
        end

      :error ->
        {:error,
         "ERR Error in ACL SETUSER modifier '-@#{category}': Unknown command category '#{category}'"}
    end
  end

  def parse_rule(user, "+" <> command) do
    cmd = normalize_acl_command_name(command)

    with :ok <- validate_acl_command_rule("+" <> command, command, cmd) do
      case user.commands do
        :all ->
          new_denied = MapSet.delete(user.denied_commands, cmd)
          {:ok, %{user | denied_commands: new_denied}}

        cmds ->
          {:ok, %{user | commands: MapSet.put(cmds, cmd)}}
      end
    end
  end

  def parse_rule(user, "-" <> command) do
    cmd = normalize_acl_command_name(command)

    with :ok <- validate_acl_command_rule("-" <> command, command, cmd) do
      case user.commands do
        :all ->
          new_denied = MapSet.put(user.denied_commands, cmd)
          {:ok, %{user | denied_commands: new_denied}}

        cmds ->
          {:ok, %{user | commands: MapSet.delete(cmds, cmd)}}
      end
    end
  end

  def parse_rule(_user, rule) do
    {:error, "ERR Error in ACL SETUSER modifier '#{rule}': Syntax error"}
  end

  @spec validate_acl_command_rule(binary(), binary(), binary()) :: :ok | {:error, binary()}
  def validate_acl_command_rule(rule, original_command, upper_command) do
    if MapSet.member?(CommandCategories.acl_supported_commands(), upper_command) do
      :ok
    else
      {:error,
       "ERR Error in ACL SETUSER modifier '#{rule}': Unknown command '#{original_command}'"}
    end
  end

  @spec normalize_acl_command_name(binary()) :: binary()
  def normalize_acl_command_name(command) when is_binary(command) do
    command
    |> String.upcase()
    |> String.replace("|", ".")
  end

  @spec command_denied?(MapSet.t(binary()), binary()) :: boolean()
  def command_denied?(commands, cmd) do
    MapSet.member?(commands, cmd) or
      (command_parent_supported?(cmd) and MapSet.member?(commands, command_parent(cmd)))
  end

  @spec command_allowed?(MapSet.t(binary()), binary()) :: boolean()
  def command_allowed?(commands, cmd) do
    MapSet.member?(commands, cmd) or
      (command_parent_supported?(cmd) and MapSet.member?(commands, command_parent(cmd)))
  end

  defp command_parent_supported?(cmd) do
    case command_parent(cmd) do
      nil -> false
      parent -> MapSet.member?(CommandCategories.acl_supported_commands(), parent)
    end
  end

  defp command_parent(cmd) do
    case String.split(cmd, ".", parts: 2) do
      [parent, _subcommand] -> parent
      _ -> nil
    end
  end

  defp parse_key_rule(user, "%R~" <> pattern, pending_key_patterns) do
    queue_key_pattern(
      user,
      {pattern, :read, FerricstoreServer.Acl.compile_glob(pattern)},
      pending_key_patterns
    )
  end

  defp parse_key_rule(user, "%W~" <> pattern, pending_key_patterns) do
    queue_key_pattern(
      user,
      {pattern, :write, FerricstoreServer.Acl.compile_glob(pattern)},
      pending_key_patterns
    )
  end

  defp parse_key_rule(user, "~" <> pattern, pending_key_patterns) do
    queue_key_pattern(
      user,
      {pattern, :rw, FerricstoreServer.Acl.compile_glob(pattern)},
      pending_key_patterns
    )
  end

  defp parse_key_rule(user, "resetkeys", _pending_key_patterns), do: {:ok, %{user | keys: []}, []}
  defp parse_key_rule(user, "allkeys", _pending_key_patterns), do: {:ok, %{user | keys: :all}, []}
  defp parse_key_rule(_user, _rule, _pending_key_patterns), do: :not_key_rule

  @spec add_channel_pattern(user(), binary()) :: user()
  def add_channel_pattern(user, "*"), do: Map.put(user, :channels, :all)
  def add_channel_pattern(%{channels: :all} = user, _pattern), do: user

  def add_channel_pattern(user, pattern) do
    case user_channels(user) do
      :all ->
        user

      channels ->
        Map.put(
          user,
          :channels,
          channels ++ [{pattern, FerricstoreServer.Acl.compile_glob(pattern)}]
        )
    end
  end

  defp queue_key_pattern(%{keys: :all} = user, compiled_pattern, pending_key_patterns) do
    {:ok, %{user | keys: []}, [compiled_pattern | pending_key_patterns]}
  end

  defp queue_key_pattern(user, compiled_pattern, pending_key_patterns) do
    {:ok, user, [compiled_pattern | pending_key_patterns]}
  end

  @spec flush_pending_key_patterns(user(), list()) :: user()
  def flush_pending_key_patterns(user, []), do: user

  def flush_pending_key_patterns(%{keys: []} = user, pending_key_patterns) do
    %{user | keys: Enum.reverse(pending_key_patterns)}
  end

  def flush_pending_key_patterns(user, pending_key_patterns) do
    %{user | keys: user.keys ++ Enum.reverse(pending_key_patterns)}
  end

  @spec user_channels(user()) :: :all | list()
  def user_channels(%{channels: channels}) when channels == :all or is_list(channels),
    do: channels

  def user_channels(_user), do: :all

  @spec format_acl_command_rule_name(binary()) :: binary()
  def format_acl_command_rule_name(cmd) do
    case String.split(cmd, ".", parts: 2) do
      [parent, subcommand] when parent in ["ACL", "CLIENT"] ->
        "#{String.downcase(parent)}|#{String.downcase(subcommand)}"

      _ ->
        String.downcase(cmd)
    end
  end
end
