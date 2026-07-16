defmodule FerricstoreServer.Acl.Formatter do
  @moduledoc false

  alias FerricstoreServer.Acl.Rules

  @type user :: map()

  @spec format_user_rule({binary(), user()}) :: binary()
  def format_user_rule({name, user}) do
    flag = if user.enabled, do: "on", else: "off"
    keys = format_keys(user.keys)
    channels = format_channels(Rules.user_channels(user))
    cmds = format_user_commands(user)
    username = name |> file_username_tokens() |> Enum.join(" ")
    "#{username} #{flag} #{keys} #{channels} #{cmds}"
  end

  @doc false
  @spec split_user_rule(binary()) :: {:ok, {binary(), binary(), binary()}} | :error
  def split_user_rule(rule) when is_binary(rule) do
    case String.split(rule, " ", parts: 4, trim: true) do
      ["user", username, state, summary] ->
        {:ok, {username, state, summary}}

      ["user64", "b" <> encoded_username, state, summary] ->
        case Base.url_decode64(encoded_username, padding: false) do
          {:ok, username} -> {:ok, {username, state, summary}}
          :error -> :error
        end

      _other ->
        :error
    end
  end

  def split_user_rule(_rule), do: :error

  @spec format_user_commands(user()) :: binary()
  def format_user_commands(%{commands: :all, denied_commands: denied}) do
    denied_parts =
      denied
      |> MapSet.to_list()
      |> Enum.sort()
      |> Enum.map(&"-#{Rules.format_acl_command_rule_name(&1)}")

    Enum.join(["+@all" | denied_parts], " ")
  end

  def format_user_commands(%{commands: cmds}), do: format_commands(cmds)

  @spec format_commands(:all | MapSet.t(binary())) :: binary()
  def format_commands(:all), do: "+@all"

  def format_commands(cmds) when is_struct(cmds, MapSet) do
    cmds
    |> MapSet.to_list()
    |> Enum.sort()
    |> Enum.map_join(" ", &"+#{Rules.format_acl_command_rule_name(&1)}")
  end

  @spec format_keys(:all | list()) :: binary()
  def format_keys(:all), do: "~*"

  def format_keys(patterns) when is_list(patterns) do
    Enum.map_join(patterns, " ", fn
      {glob, :rw, _regex} -> format_key_rule_token(glob, :rw)
      {glob, :read, _regex} -> format_key_rule_token(glob, :read)
      {glob, :write, _regex} -> format_key_rule_token(glob, :write)
    end)
  end

  @spec format_channels(:all | list()) :: binary()
  def format_channels(:all), do: "&*"
  def format_channels([]), do: "resetchannels"

  def format_channels(patterns) when is_list(patterns) do
    Enum.map_join(patterns, " ", fn {glob, _regex} -> format_channel_rule_token(glob) end)
  end

  @spec format_user_for_file({binary(), user()}) :: binary()
  def format_user_for_file({name, user}) do
    parts = file_username_tokens(name)
    parts = parts ++ [if(user.enabled, do: "on", else: "off")]

    parts =
      case user.password do
        nil -> parts ++ ["nopass"]
        hash -> parts ++ ["#" <> hash]
      end

    parts =
      case user.keys do
        :all ->
          parts ++ ["~*"]

        patterns ->
          parts ++
            Enum.map(patterns, fn
              {glob, :rw, _regex} -> format_key_rule_token(glob, :rw)
              {glob, :read, _regex} -> format_key_rule_token(glob, :read)
              {glob, :write, _regex} -> format_key_rule_token(glob, :write)
            end)
      end

    parts = parts ++ format_channel_rule_tokens(Rules.user_channels(user))

    parts =
      case user.commands do
        :all ->
          denied = user.denied_commands

          if MapSet.size(denied) == 0 do
            parts ++ ["+@all"]
          else
            parts ++
              ["+@all"] ++
              (denied
               |> MapSet.to_list()
               |> Enum.sort()
               |> Enum.map(&"-#{Rules.format_acl_command_rule_name(&1)}"))
          end

        cmds ->
          if MapSet.size(cmds) == 0 do
            parts
          else
            parts ++
              (cmds
               |> MapSet.to_list()
               |> Enum.sort()
               |> Enum.map(&"+#{Rules.format_acl_command_rule_name(&1)}"))
          end
      end

    Enum.join(parts, " ")
  end

  defp format_channel_rule_tokens(:all), do: ["&*"]
  defp format_channel_rule_tokens([]), do: ["resetchannels"]

  defp format_channel_rule_tokens(patterns) when is_list(patterns) do
    Enum.map(patterns, fn {glob, _regex} -> format_channel_rule_token(glob) end)
  end

  defp file_username_tokens(name) when is_binary(name) do
    if name != "" and plain_file_token?(name) do
      ["user", name]
    else
      ["user64", "b" <> Base.url_encode64(name, padding: false)]
    end
  end

  defp format_key_rule_token(glob, mode) do
    if plain_file_token?(glob) do
      key_rule_prefix(mode) <> glob
    else
      "key64:#{mode}:b" <> Base.url_encode64(glob, padding: false)
    end
  end

  defp format_channel_rule_token(glob) do
    if plain_file_token?(glob) do
      "&" <> glob
    else
      "channel64:b" <> Base.url_encode64(glob, padding: false)
    end
  end

  defp key_rule_prefix(:rw), do: "~"
  defp key_rule_prefix(:read), do: "%R~"
  defp key_rule_prefix(:write), do: "%W~"

  defp plain_file_token?(value) when is_binary(value) do
    String.valid?(value) and not Regex.match?(~r/[\s\p{C}]/u, value)
  end
end
