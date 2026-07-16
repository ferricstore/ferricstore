defmodule FerricstoreServer.Acl.Rules do
  @moduledoc false

  alias FerricstoreServer.Acl.CommandCategories
  alias FerricstoreServer.Acl.Password

  @max_username_bytes 1_024
  @max_password_bytes Password.max_password_bytes()
  @max_patterns 4_096
  @max_pattern_bytes 4_096

  @type user :: map()

  @doc false
  @spec max_username_bytes() :: pos_integer()
  def max_username_bytes, do: @max_username_bytes

  @doc false
  @spec max_patterns() :: pos_integer()
  def max_patterns, do: @max_patterns

  @doc false
  @spec max_pattern_bytes() :: pos_integer()
  def max_pattern_bytes, do: @max_pattern_bytes

  @spec validate_username(term()) :: :ok | {:error, binary()}
  def validate_username(username) when is_binary(username) do
    cond do
      not String.valid?(username) ->
        {:error, "ERR ACL username must be valid UTF-8"}

      byte_size(username) > @max_username_bytes ->
        {:error, "ERR ACL username exceeds #{@max_username_bytes} bytes"}

      true ->
        :ok
    end
  end

  def validate_username(_username), do: {:error, "ERR ACL username must be a binary"}

  @doc false
  @spec validate_rule_limits([term()]) :: :ok | {:error, binary()}
  def validate_rule_limits(rules) when is_list(rules) do
    Enum.reduce_while(rules, {:ok, 0, 0}, &validate_rule_limit/2)
    |> case do
      {:ok, _key_count, _channel_count} -> :ok
      {:error, _reason} = error -> error
    end
  end

  def validate_rule_limits(_rules), do: {:error, "ERR ACL rules must be a list"}

  @spec apply_rules(user(), [binary()]) :: {:ok, user()} | {:error, binary()}
  def apply_rules(user, rules) do
    with :ok <- validate_rule_limits(rules),
         {:ok, updated} <- apply_rules(user, rules, []),
         :ok <- validate_retained_pattern_limits(updated) do
      {:ok, updated}
    end
  end

  defp validate_rule_limit(">" <> password, {:ok, key_count, channel_count}) do
    if byte_size(password) <= @max_password_bytes do
      {:cont, {:ok, key_count, channel_count}}
    else
      {:halt, {:error, "ERR ACL password exceeds #{@max_password_bytes} bytes"}}
    end
  end

  defp validate_rule_limit("%R~" <> pattern, {:ok, key_count, channel_count}),
    do: validate_pattern_limit(:key, pattern, key_count, channel_count)

  defp validate_rule_limit("%W~" <> pattern, {:ok, key_count, channel_count}),
    do: validate_pattern_limit(:key, pattern, key_count, channel_count)

  defp validate_rule_limit("~" <> pattern, {:ok, key_count, channel_count}),
    do: validate_pattern_limit(:key, pattern, key_count, channel_count)

  defp validate_rule_limit("&" <> pattern, {:ok, key_count, channel_count}),
    do: validate_pattern_limit(:channel, pattern, key_count, channel_count)

  defp validate_rule_limit(rule, {:ok, key_count, channel_count}) when is_binary(rule) do
    if String.valid?(rule) do
      {:cont, {:ok, key_count, channel_count}}
    else
      {:halt, {:error, "ERR ACL rule must be valid UTF-8"}}
    end
  end

  defp validate_rule_limit(_rule, _counts),
    do: {:halt, {:error, "ERR ACL rules must be binaries"}}

  defp validate_pattern_limit(kind, pattern, key_count, channel_count) do
    cond do
      not String.valid?(pattern) ->
        {:halt, {:error, "ERR ACL #{kind} pattern must be valid UTF-8"}}

      byte_size(pattern) > @max_pattern_bytes ->
        {:halt, {:error, "ERR ACL #{kind} pattern exceeds #{@max_pattern_bytes} bytes"}}

      kind == :key and key_count >= @max_patterns ->
        {:halt, {:error, "ERR ACL rules contain more than #{@max_patterns} key patterns"}}

      kind == :channel and channel_count >= @max_patterns ->
        {:halt, {:error, "ERR ACL rules contain more than #{@max_patterns} channel patterns"}}

      kind == :key ->
        {:cont, {:ok, key_count + 1, channel_count}}

      true ->
        {:cont, {:ok, key_count, channel_count + 1}}
    end
  end

  defp validate_retained_pattern_limits(user) do
    cond do
      not patterns_within_limit?(Map.get(user, :keys, :all), @max_patterns) ->
        {:error, "ERR ACL rules contain more than #{@max_patterns} key patterns"}

      not patterns_within_limit?(user_channels(user), @max_patterns) ->
        {:error, "ERR ACL rules contain more than #{@max_patterns} channel patterns"}

      true ->
        :ok
    end
  end

  defp patterns_within_limit?(:all, _remaining), do: true
  defp patterns_within_limit?([], _remaining), do: true
  defp patterns_within_limit?([_pattern | _rest], 0), do: false

  defp patterns_within_limit?([_pattern | rest], remaining),
    do: patterns_within_limit?(rest, remaining - 1)

  defp patterns_within_limit?(_invalid, _remaining), do: false

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

  def user_channels(_user), do: []

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
