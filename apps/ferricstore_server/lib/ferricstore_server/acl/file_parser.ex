defmodule FerricstoreServer.Acl.FileParser do
  @moduledoc false

  alias FerricstoreServer.Acl.CommandCategories
  alias FerricstoreServer.Acl.Limits
  alias FerricstoreServer.Acl.Password
  alias FerricstoreServer.Acl.Rules

  @default_max_line_bytes Limits.max_file_line_bytes()
  @max_file_size 50_000_000
  @default_max_users Limits.default_max_users()
  @default_max_lines Limits.max_file_lines(@default_max_users)
  @default_max_rule_tokens Rules.max_rule_tokens()
  @type user :: map()

  @spec parse(binary()) :: {:ok, [{binary(), user()}]} | {:error, binary()}
  def parse(contents), do: parse(contents, [])

  @doc false
  @spec parse(binary(), keyword()) :: {:ok, [{binary(), user()}]} | {:error, binary()}
  def parse(contents, opts) when is_binary(contents) and is_list(opts) do
    with true <- Keyword.keyword?(opts),
         {:ok, max_file_bytes} <-
           parse_limit(Keyword.get(opts, :max_file_bytes, @max_file_size)),
         {:ok, max_users} <-
           parse_limit(Keyword.get(opts, :max_users, @default_max_users)),
         {:ok, max_line_bytes} <-
           parse_limit(Keyword.get(opts, :max_line_bytes, @default_max_line_bytes)),
         {:ok, max_lines} <-
           parse_limit(Keyword.get(opts, :max_lines, @default_max_lines)),
         {:ok, max_rule_tokens} <-
           parse_limit(Keyword.get(opts, :max_rule_tokens, @default_max_rule_tokens)) do
      cond do
        byte_size(contents) > max_file_bytes ->
          {:error, "ERR ACL file too large (#{byte_size(contents)} bytes, max #{max_file_bytes})"}

        String.valid?(contents) ->
          parse_utf8(contents, max_users, max_line_bytes, max_lines, max_rule_tokens)

        true ->
          {:error, "ERR Invalid ACL file: contents must be valid UTF-8"}
      end
    else
      _invalid -> {:error, "ERR Invalid ACL parser limits"}
    end
  end

  def parse(_contents, _opts), do: {:error, "ERR Invalid ACL file contents"}

  defp parse_utf8(contents, max_users, max_line_bytes, max_lines, max_rule_tokens) do
    contents = strip_bom(contents)

    parse_lines(
      contents,
      1,
      %{},
      max_users,
      max_line_bytes,
      max_lines,
      max_rule_tokens
    )
  end

  defp parse_lines(
         "",
         _line_num,
         users,
         _max_users,
         _max_line_bytes,
         _max_lines,
         _max_rule_tokens
       ),
       do: {:ok, Enum.to_list(users)}

  defp parse_lines(
         _contents,
         line_num,
         _users,
         _max_users,
         _max_line_bytes,
         max_lines,
         _max_rule_tokens
       )
       when line_num > max_lines do
    {:error, "ERR Invalid ACL line #{line_num}: maximum line count exceeded (#{max_lines})"}
  end

  defp parse_lines(
         contents,
         line_num,
         users,
         max_users,
         max_line_bytes,
         max_lines,
         max_rule_tokens
       ) do
    case :binary.match(contents, "\n") do
      {newline_at, 1} ->
        line = binary_part(contents, 0, newline_at)
        rest_at = newline_at + 1
        rest = binary_part(contents, rest_at, byte_size(contents) - rest_at)

        with {:ok, users} <-
               parse_line(
                 line,
                 line_num,
                 users,
                 max_users,
                 max_line_bytes,
                 max_rule_tokens
               ) do
          parse_lines(
            rest,
            line_num + 1,
            users,
            max_users,
            max_line_bytes,
            max_lines,
            max_rule_tokens
          )
        end

      :nomatch ->
        case parse_line(
               contents,
               line_num,
               users,
               max_users,
               max_line_bytes,
               max_rule_tokens
             ) do
          {:ok, users} -> {:ok, Enum.to_list(users)}
          {:error, _reason} = error -> error
        end
    end
  end

  defp parse_line(line, line_num, users, max_users, max_line_bytes, max_rule_tokens) do
    cond do
      byte_size(line) > max_line_bytes ->
        {:error, "ERR Invalid ACL line #{line_num}: line exceeds maximum length"}

      true ->
        parse_trimmed_line(
          String.trim_trailing(line),
          line_num,
          users,
          max_users,
          max_rule_tokens
        )
    end
  end

  defp parse_trimmed_line(line, line_num, users, max_users, max_rule_tokens) do
    cond do
      line == "" ->
        {:ok, users}

      String.starts_with?(line, "#") ->
        {:ok, users}

      true ->
        case parse_acl_line(line, line_num, max_rule_tokens) do
          {:ok, {username, user_map}} ->
            cond do
              Map.has_key?(users, username) ->
                {:error, "ERR Invalid ACL line #{line_num}: duplicate user '#{username}'"}

              map_size(users) >= max_users ->
                {:error, "ERR Invalid ACL line #{line_num}: max ACL users reached (#{max_users})"}

              true ->
                {:ok, Map.put(users, username, user_map)}
            end

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp parse_limit(limit) when is_integer(limit) and limit >= 0, do: {:ok, limit}
  defp parse_limit(_invalid), do: :error

  defp parse_acl_line(line, line_num, max_rule_tokens) do
    max_tokens = max_rule_tokens + 2
    tokens = String.split(line, ~r/\s+/, trim: true, parts: max_tokens + 1)

    if length(tokens) > max_tokens do
      {:error, "ERR Invalid ACL line #{line_num}: more than #{max_rule_tokens} rule tokens"}
    else
      case tokens do
        ["user", username | rule_tokens] ->
          parse_acl_user(username, rule_tokens, line_num)

        ["user64", "b" <> encoded_username | rule_tokens] ->
          case Base.url_decode64(encoded_username, padding: false) do
            {:ok, username} -> parse_acl_user(username, rule_tokens, line_num)
            :error -> {:error, "ERR Invalid ACL line #{line_num}: invalid encoded username"}
          end

        _ ->
          {:error, "ERR Invalid ACL line #{line_num}: expected 'user <username> <rules...>'"}
      end
    end
  end

  defp parse_acl_user(username, rule_tokens, line_num) do
    with {:ok, rule_tokens} <- decode_file_rule_tokens(rule_tokens, line_num),
         :ok <- Rules.validate_username(username),
         :ok <- Rules.validate_rule_limits(rule_tokens) do
      if Enum.any?(rule_tokens, &String.starts_with?(&1, ">")) do
        {:error,
         "ERR Invalid ACL line #{line_num}: plaintext passwords (>) are not allowed in ACL files, use #<hash>"}
      else
        parse_file_rules(username, rule_tokens, line_num)
      end
    end
  end

  defp decode_file_rule_tokens(tokens, line_num) do
    Enum.reduce_while(tokens, {:ok, []}, fn token, {:ok, acc} ->
      case decode_file_rule_token(token) do
        {:ok, decoded} ->
          {:cont, {:ok, [decoded | acc]}}

        :not_encoded ->
          {:cont, {:ok, [token | acc]}}

        :error ->
          {:halt, {:error, "ERR Invalid ACL line #{line_num}: invalid encoded pattern token"}}
      end
    end)
    |> case do
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_file_rule_token("key64:rw:b" <> encoded),
    do: decode_pattern_token(encoded, "~")

  defp decode_file_rule_token("key64:read:b" <> encoded),
    do: decode_pattern_token(encoded, "%R~")

  defp decode_file_rule_token("key64:write:b" <> encoded),
    do: decode_pattern_token(encoded, "%W~")

  defp decode_file_rule_token("channel64:b" <> encoded),
    do: decode_pattern_token(encoded, "&")

  defp decode_file_rule_token("key64:" <> _invalid), do: :error
  defp decode_file_rule_token("channel64:" <> _invalid), do: :error
  defp decode_file_rule_token(_token), do: :not_encoded

  defp decode_pattern_token(encoded, prefix) do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, pattern} -> {:ok, prefix <> pattern}
      :error -> :error
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

    with :ok <- Rules.validate_acl_command_rule("+" <> command, command, cmd) do
      case user.commands do
        :all -> {:ok, %{user | denied_commands: MapSet.delete(user.denied_commands, cmd)}}
        cmds -> {:ok, %{user | commands: MapSet.put(cmds, cmd)}}
      end
    end
  end

  defp parse_file_token(user, "-" <> command) do
    cmd = Rules.normalize_acl_command_name(command)

    with :ok <- Rules.validate_acl_command_rule("-" <> command, command, cmd) do
      case user.commands do
        :all -> {:ok, %{user | denied_commands: MapSet.put(user.denied_commands, cmd)}}
        cmds -> {:ok, %{user | commands: MapSet.delete(cmds, cmd)}}
      end
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
