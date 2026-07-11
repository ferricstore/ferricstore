defmodule FerricstoreServer.Acl.FileParser do
  @moduledoc false

  alias FerricstoreServer.Acl.CommandCategories
  alias FerricstoreServer.Acl.Password
  alias FerricstoreServer.Acl.Rules

  @max_line_length 1_048_576
  @type user :: map()

  @spec parse(binary()) :: {:ok, [{binary(), user()}]} | {:error, binary()}
  def parse(contents) do
    contents = strip_bom(contents)

    lines =
      contents
      |> String.split(~r/\r?\n/)
      |> Enum.with_index(1)

    result =
      Enum.reduce_while(lines, {:ok, %{}}, fn {line, line_num}, {:ok, acc} ->
        line = String.trim_trailing(line)

        cond do
          line == "" ->
            {:cont, {:ok, acc}}

          String.starts_with?(line, "#") ->
            {:cont, {:ok, acc}}

          byte_size(line) > @max_line_length ->
            {:halt, {:error, "ERR Invalid ACL line #{line_num}: line exceeds maximum length"}}

          true ->
            case parse_acl_line(line, line_num) do
              {:ok, {username, user_map}} -> {:cont, {:ok, Map.put(acc, username, user_map)}}
              {:error, _} = err -> {:halt, err}
            end
        end
      end)

    case result do
      {:ok, users_map} -> {:ok, Enum.to_list(users_map)}
      {:error, _} = err -> err
    end
  end

  defp parse_acl_line(line, line_num) do
    tokens = String.split(line, ~r/\s+/, trim: true)

    case tokens do
      ["user", username | rule_tokens] ->
        if Enum.any?(rule_tokens, &String.starts_with?(&1, ">")) do
          {:error,
           "ERR Invalid ACL line #{line_num}: plaintext passwords (>) are not allowed in ACL files, use #<hash>"}
        else
          parse_file_rules(username, rule_tokens, line_num)
        end

      _ ->
        {:error, "ERR Invalid ACL line #{line_num}: expected 'user <username> <rules...>'"}
    end
  end

  defp parse_file_rules(username, tokens, line_num) do
    base = %{
      enabled: false,
      password: nil,
      commands: MapSet.new(),
      denied_commands: MapSet.new(),
      keys: [],
      channels: []
    }

    case parse_file_rules(tokens, line_num, base, []) do
      {:ok, user_map} -> {:ok, {username, user_map}}
      {:error, _} = err -> err
    end
  end

  defp parse_file_rules([], _line_num, user, pending_key_patterns) do
    {:ok, Rules.flush_pending_key_patterns(user, pending_key_patterns)}
  end

  defp parse_file_rules([token | rest], line_num, user, pending_key_patterns) do
    case parse_file_key_token(user, token, pending_key_patterns) do
      {:ok, updated, next_pending} ->
        parse_file_rules(rest, line_num, updated, next_pending)

      :not_key_token ->
        case parse_file_token(user, token) do
          {:ok, updated} -> parse_file_rules(rest, line_num, updated, pending_key_patterns)
          {:error, reason} -> {:error, "ERR Invalid ACL line #{line_num}: #{reason}"}
        end
    end
  end

  defp parse_file_token(user, "on"), do: {:ok, %{user | enabled: true}}
  defp parse_file_token(user, "off"), do: {:ok, %{user | enabled: false}}
  defp parse_file_token(user, "nopass"), do: {:ok, %{user | password: nil}}
  defp parse_file_token(user, "resetpass"), do: {:ok, %{user | password: nil}}

  defp parse_file_token(user, "#" <> hash) do
    cond do
      Password.valid_stored_hash_format?(hash) ->
        {:ok, %{user | password: hash}}

      String.starts_with?(hash, "pbkdf2-sha256$") ->
        {:error, "invalid password hash"}

      true ->
        case Base.decode64(hash) do
          {:ok, _decoded} -> {:error, "invalid password hash length"}
          :error -> {:error, "invalid password hash encoding"}
        end
    end
  end

  defp parse_file_token(user, "allchannels"), do: {:ok, Map.put(user, :channels, :all)}
  defp parse_file_token(user, "resetchannels"), do: {:ok, Map.put(user, :channels, [])}
  defp parse_file_token(user, "&" <> pattern), do: {:ok, Rules.add_channel_pattern(user, pattern)}

  defp parse_file_token(user, "allcommands") do
    {:ok, %{user | commands: :all, denied_commands: MapSet.new()}}
  end

  defp parse_file_token(user, "nocommands") do
    {:ok, %{user | commands: MapSet.new(), denied_commands: MapSet.new()}}
  end

  defp parse_file_token(user, "+@all") do
    {:ok, %{user | commands: :all, denied_commands: MapSet.new()}}
  end

  defp parse_file_token(user, "-@all") do
    {:ok, %{user | commands: MapSet.new(), denied_commands: MapSet.new()}}
  end

  defp parse_file_token(user, "+@" <> category) do
    cat = String.upcase(category)

    case CommandCategories.category_commands(cat) do
      {:ok, cat_cmds} ->
        case user.commands do
          :all ->
            {:ok, %{user | denied_commands: MapSet.difference(user.denied_commands, cat_cmds)}}

          cmds ->
            {:ok, %{user | commands: MapSet.union(cmds, cat_cmds)}}
        end

      :error ->
        {:error, "unknown command category '@#{category}'"}
    end
  end

  defp parse_file_token(user, "-@" <> category) do
    cat = String.upcase(category)

    case CommandCategories.category_commands(cat) do
      {:ok, cat_cmds} ->
        case user.commands do
          :all -> {:ok, %{user | denied_commands: MapSet.union(user.denied_commands, cat_cmds)}}
          cmds -> {:ok, %{user | commands: MapSet.difference(cmds, cat_cmds)}}
        end

      :error ->
        {:error, "unknown command category '@#{category}'"}
    end
  end

  defp parse_file_token(user, "+" <> command) do
    cmd = Rules.normalize_acl_command_name(command)

    case user.commands do
      :all -> {:ok, %{user | denied_commands: MapSet.delete(user.denied_commands, cmd)}}
      cmds -> {:ok, %{user | commands: MapSet.put(cmds, cmd)}}
    end
  end

  defp parse_file_token(user, "-" <> command) do
    cmd = Rules.normalize_acl_command_name(command)

    case user.commands do
      :all -> {:ok, %{user | denied_commands: MapSet.put(user.denied_commands, cmd)}}
      cmds -> {:ok, %{user | commands: MapSet.delete(cmds, cmd)}}
    end
  end

  defp parse_file_token(_user, token), do: {:error, "unknown token '#{token}'"}

  defp parse_file_key_token(user, "~*", _pending_key_patterns),
    do: {:ok, %{user | keys: :all}, []}

  defp parse_file_key_token(user, "allkeys", _pending_key_patterns),
    do: {:ok, %{user | keys: :all}, []}

  defp parse_file_key_token(user, "resetkeys", _pending_key_patterns),
    do: {:ok, %{user | keys: []}, []}

  defp parse_file_key_token(user, "%R~" <> pattern, pending_key_patterns) do
    queue_file_key_pattern(
      user,
      {pattern, :read, FerricstoreServer.Acl.compile_glob(pattern)},
      pending_key_patterns
    )
  end

  defp parse_file_key_token(user, "%W~" <> pattern, pending_key_patterns) do
    queue_file_key_pattern(
      user,
      {pattern, :write, FerricstoreServer.Acl.compile_glob(pattern)},
      pending_key_patterns
    )
  end

  defp parse_file_key_token(user, "~" <> pattern, pending_key_patterns) do
    queue_file_key_pattern(
      user,
      {pattern, :rw, FerricstoreServer.Acl.compile_glob(pattern)},
      pending_key_patterns
    )
  end

  defp parse_file_key_token(_user, _token, _pending_key_patterns), do: :not_key_token

  defp queue_file_key_pattern(user, compiled_pattern, pending_key_patterns) do
    case user.keys do
      :all -> {:ok, user, pending_key_patterns}
      _patterns -> {:ok, user, [compiled_pattern | pending_key_patterns]}
    end
  end

  defp strip_bom(<<0xEF, 0xBB, 0xBF, rest::binary>>), do: rest
  defp strip_bom(contents), do: contents
end
