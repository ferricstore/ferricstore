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
    "user #{name} #{flag} #{keys} #{channels} #{cmds}"
  end

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
      {glob, :rw, _regex} -> "~#{glob}"
      {glob, :read, _regex} -> "%R~#{glob}"
      {glob, :write, _regex} -> "%W~#{glob}"
    end)
  end

  @spec format_channels(:all | list()) :: binary()
  def format_channels(:all), do: "&*"
  def format_channels([]), do: "resetchannels"

  def format_channels(patterns) when is_list(patterns) do
    Enum.map_join(patterns, " ", fn {glob, _regex} -> "&#{glob}" end)
  end

  @spec format_user_for_file({binary(), user()}) :: binary()
  def format_user_for_file({name, user}) do
    parts = ["user", name]
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
              {glob, :rw, _regex} -> "~#{glob}"
              {glob, :read, _regex} -> "%R~#{glob}"
              {glob, :write, _regex} -> "%W~#{glob}"
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
    Enum.map(patterns, fn {glob, _regex} -> "&#{glob}" end)
  end
end
