defmodule FerricstoreServer.Connection.Auth do
  @moduledoc """
  Shared ACL cache and authorization helpers for FerricStore protocol sessions.

  This module intentionally contains no wire encoding. The native protocol owns
  response formatting; this module only answers whether a command/key/channel is
  allowed and broadcasts ACL invalidation to live sessions.
  """

  alias Ferricstore.Commands.{KeyDiscovery, PreparedCommand}
  alias FerricstoreServer.Acl.CatalogProjector
  alias FerricstoreServer.Acl.CommandCategories

  @acl_pg_group :ferricstore_acl_connections
  @global_keyspace_enumeration_commands ~w(KEYS SCAN RANDOMKEY DBSIZE)
  @acl_subcommands ~w(CAT DELUSER GETUSER LIST LOAD LOG SAVE SETUSER WHOAMI)

  @spec acl_pg_group() :: atom()
  def acl_pg_group, do: @acl_pg_group

  @spec user_requires_auth?(binary()) :: boolean()
  def user_requires_auth?(username) do
    if CatalogProjector.ready?() do
      case FerricstoreServer.Acl.get_user(username) do
        %{password: password} when is_binary(password) -> true
        _ -> false
      end
    else
      true
    end
  end

  @spec build_acl_cache(binary()) :: map() | :full_access | :denied
  def build_acl_cache(username) do
    if CatalogProjector.ready?() do
      case FerricstoreServer.Acl.get_user(username) do
        nil ->
          :denied

        user ->
          denied = Map.get(user, :denied_commands, MapSet.new())
          channels = Map.get(user, :channels, :all)

          if user.enabled and user.commands == :all and MapSet.size(denied) == 0 and
               user.keys == :all and channels == :all do
            :full_access
          else
            %{
              commands: user.commands,
              denied_commands: denied,
              keys: user.keys,
              channels: channels,
              enabled: user.enabled
            }
          end
      end
    else
      :denied
    end
  end

  @spec ensure_acl_projection_ready() :: :ok | {:error, binary()}
  def ensure_acl_projection_ready do
    if CatalogProjector.ready?() do
      :ok
    else
      {:error, "NOPERM ACL catalog projection unavailable"}
    end
  end

  @spec check_command_cached(map() | :full_access | :denied | nil, binary()) ::
          :ok | {:error, binary()}
  def check_command_cached(cache, command) do
    with :ok <- ensure_acl_projection_ready() do
      do_check_command_cached(cache, command)
    end
  end

  defp do_check_command_cached(:full_access, "ACL.WHOAMI"), do: :ok
  defp do_check_command_cached(%{enabled: true}, "ACL.WHOAMI"), do: :ok

  defp do_check_command_cached(:denied, _cmd),
    do: {:error, "NOPERM user session expired or user was deleted"}

  defp do_check_command_cached(nil, _cmd),
    do: {:error, "NOPERM user session expired or user was deleted"}

  defp do_check_command_cached(:full_access, _cmd), do: :ok

  defp do_check_command_cached(
         %{commands: :all, denied_commands: %MapSet{map: denied_map}, enabled: true},
         _cmd
       )
       when map_size(denied_map) == 0,
       do: :ok

  defp do_check_command_cached(cache, cmd) do
    cond do
      not cache.enabled ->
        noperm_command(cmd)

      cache.commands == :all and not cached_command_denied?(cache.denied_commands, cmd) ->
        :ok

      cache.commands == :all ->
        noperm_command(cmd)

      cached_command_allowed?(cache.commands, cmd) and
          not cached_command_denied?(cache.denied_commands, cmd) ->
        :ok

      true ->
        noperm_command(cmd)
    end
  end

  @spec check_keys_cached(map() | :full_access | :denied | nil, binary(), [binary()]) ::
          :ok | {:error, binary()}
  def check_keys_cached(cache, command, keys) do
    with :ok <- ensure_acl_projection_ready() do
      do_check_keys_cached(cache, command, keys)
    end
  end

  defp do_check_keys_cached(:denied, _cmd, _keys),
    do: {:error, "NOPERM user session expired or user was deleted"}

  defp do_check_keys_cached(nil, _cmd, _keys),
    do: {:error, "NOPERM user session expired or user was deleted"}

  defp do_check_keys_cached(:full_access, _cmd, _keys), do: :ok
  defp do_check_keys_cached(%{keys: :all}, _cmd, _keys), do: :ok

  defp do_check_keys_cached(%{keys: patterns}, cmd, []) when is_list(patterns) do
    if global_keyspace_enumeration_command?(cmd) and not unrestricted_read_key_patterns?(patterns) do
      noperm_key()
    else
      :ok
    end
  end

  defp do_check_keys_cached(%{keys: patterns}, cmd, keys) when is_list(keys) do
    {read_keys, write_keys} = KeyDiscovery.access_keys(cmd, keys)

    with :ok <- check_all_keys(read_keys, :read, patterns) do
      check_all_keys(write_keys, :write, patterns)
    end
  end

  @spec check_keys_cached(map() | :full_access | :denied | nil, PreparedCommand.t()) ::
          :ok | {:error, binary()}
  def check_keys_cached(cache, %PreparedCommand{} = prepared) do
    with :ok <- ensure_acl_projection_ready() do
      do_check_keys_cached(cache, prepared)
    end
  end

  defp do_check_keys_cached(:denied, %PreparedCommand{}),
    do: {:error, "NOPERM user session expired or user was deleted"}

  defp do_check_keys_cached(nil, %PreparedCommand{}),
    do: {:error, "NOPERM user session expired or user was deleted"}

  defp do_check_keys_cached(:full_access, %PreparedCommand{}), do: :ok
  defp do_check_keys_cached(%{keys: :all}, %PreparedCommand{}), do: :ok

  defp do_check_keys_cached(%{keys: patterns} = cache, %PreparedCommand{} = prepared)
       when is_list(patterns) do
    if prepared.acl_keys == [] do
      do_check_keys_cached(cache, prepared.command, [])
    else
      with :ok <- check_all_keys(prepared.read_keys, :read, patterns) do
        check_all_keys(prepared.write_keys, :write, patterns)
      end
    end
  end

  @spec check_channels_cached(map() | :full_access | :denied | nil, [binary()]) ::
          :ok | {:error, binary()}
  def check_channels_cached(cache, channels) do
    with :ok <- ensure_acl_projection_ready() do
      do_check_channels_cached(cache, channels)
    end
  end

  defp do_check_channels_cached(:denied, _channels),
    do: {:error, "NOPERM user session expired or user was deleted"}

  defp do_check_channels_cached(nil, _channels),
    do: {:error, "NOPERM user session expired or user was deleted"}

  defp do_check_channels_cached(:full_access, _channels), do: :ok
  defp do_check_channels_cached(%{channels: :all}, _channels), do: :ok
  defp do_check_channels_cached(%{channels: patterns}, []) when is_list(patterns), do: :ok

  defp do_check_channels_cached(%{channels: patterns}, channels)
       when is_list(patterns) and is_list(channels) do
    if Enum.all?(channels, &FerricstoreServer.Acl.channel_matches_any?(&1, patterns)) do
      :ok
    else
      {:error,
       "NOPERM this user has no permissions to access one of the channels mentioned in the command"}
    end
  end

  @spec check_all_keys([binary()], :read | :write | :rw, [FerricstoreServer.Acl.key_pattern()]) ::
          :ok | {:error, binary()}
  def check_all_keys([], _access_type, _patterns), do: :ok

  def check_all_keys([key | rest], access_type, patterns) do
    types_to_check = if access_type == :rw, do: [:read, :write], else: [access_type]

    if Enum.all?(types_to_check, &FerricstoreServer.Acl.key_matches_any?(key, &1, patterns)) do
      check_all_keys(rest, access_type, patterns)
    else
      noperm_key()
    end
  end

  @spec command_access_type(binary()) :: :read | :write | :rw
  def command_access_type(cmd), do: CommandCategories.command_access_type(cmd)

  @spec acl_command_name(binary(), [binary()], term()) :: binary()
  def acl_command_name("CLIENT", ["HELLO" | _rest], _ast), do: "HELLO"

  def acl_command_name("CLIENT", [subcmd | _rest], _ast) when is_binary(subcmd),
    do: "CLIENT." <> String.upcase(subcmd)

  def acl_command_name("ACL", [subcmd | _rest], _ast) when is_binary(subcmd) do
    subcmd = String.upcase(subcmd)
    if subcmd in @acl_subcommands, do: "ACL." <> subcmd, else: "ACL"
  end

  def acl_command_name(cmd, _args, _ast), do: cmd

  @spec broadcast_acl_invalidation(binary() | :all) :: :ok
  def broadcast_acl_invalidation(username) do
    broadcast_acl_invalidation_message({:acl_invalidate, username})
  end

  @spec broadcast_acl_invalidation(binary() | :all, non_neg_integer()) :: :ok
  def broadcast_acl_invalidation(username, revision)
      when is_integer(revision) and revision >= 0 do
    broadcast_acl_invalidation_message({:acl_invalidate, username, revision})
  end

  defp broadcast_acl_invalidation_message(message) do
    members =
      try do
        :pg.get_members(@acl_pg_group, @acl_pg_group)
      catch
        :error, _ -> []
      end

    for pid <- members, pid != self(), do: send(pid, message)
    :ok
  end

  @spec maybe_refresh_acl_cache(map(), binary() | :all) :: map()
  def maybe_refresh_acl_cache(state, :all), do: refresh_acl_session(state)

  def maybe_refresh_acl_cache(state, invalidated_username) do
    if invalidated_username == state.username, do: refresh_acl_session(state), else: state
  end

  @spec refresh_acl_session(map()) :: map()
  def refresh_acl_session(state) do
    %{
      state
      | acl_cache: build_acl_cache(state.username),
        require_auth: user_requires_auth?(state.username)
    }
  end

  @doc false
  def constant_time_equal?(a, b) when is_binary(a) and is_binary(b) do
    byte_size(a) == byte_size(b) and :crypto.hash_equals(a, b)
  end

  def constant_time_equal?(_a, _b), do: false

  defp global_keyspace_enumeration_command?(cmd),
    do: String.upcase(cmd) in @global_keyspace_enumeration_commands

  defp unrestricted_read_key_patterns?(patterns) do
    Enum.any?(patterns, fn
      {"*", mode, _regex} when mode in [:rw, :read] -> true
      _other -> false
    end)
  end

  defp cached_command_denied?(commands, cmd) do
    MapSet.member?(commands, cmd) or
      (cached_parent_supported?(cmd) and MapSet.member?(commands, cached_command_parent(cmd)))
  end

  defp cached_command_allowed?(commands, cmd) do
    MapSet.member?(commands, cmd) or
      (cached_parent_supported?(cmd) and MapSet.member?(commands, cached_command_parent(cmd)))
  end

  defp cached_parent_supported?(cmd) do
    case cached_command_parent(cmd) do
      nil -> false
      parent -> MapSet.member?(CommandCategories.acl_supported_commands(), parent)
    end
  end

  defp cached_command_parent(cmd) do
    case String.split(cmd, ".", parts: 2) do
      [parent, _subcommand] -> parent
      _ -> nil
    end
  end

  defp noperm_command(cmd),
    do:
      {:error, "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

  defp noperm_key,
    do:
      {:error,
       "NOPERM this user has no permissions to access one of the keys mentioned in the command"}
end
