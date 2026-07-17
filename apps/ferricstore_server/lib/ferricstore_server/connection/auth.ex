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
  alias FerricstoreServer.Connection.Registry

  @acl_projector_scope :ferricstore_acl_projector_scope
  @acl_projector_group :ferricstore_acl_projectors
  @global_keyspace_enumeration_commands ~w(KEYS SCAN RANDOMKEY DBSIZE)
  @acl_subcommands ~w(CAT DELUSER GETUSER LIST LOAD LOG SAVE SETUSER WHOAMI)

  @spec acl_projector_scope() :: atom()
  def acl_projector_scope, do: @acl_projector_scope

  @doc false
  @spec acl_projector_group() :: atom()
  def acl_projector_group, do: @acl_projector_group

  @doc false
  @spec move_acl_invalidation_group(pos_integer(), binary(), binary()) :: :ok
  def move_acl_invalidation_group(client_id, previous_username, username)
      when is_integer(client_id) and is_binary(previous_username) and is_binary(username),
      do: Registry.replace_acl_user(client_id, self(), previous_username, username)

  @doc false
  @spec begin_acl_authentication(map(), binary()) :: :ok
  def begin_acl_authentication(%{client_id: client_id, username: username}, username)
      when is_integer(client_id) and is_binary(username),
      do: :ok

  def begin_acl_authentication(%{client_id: client_id}, username)
      when is_integer(client_id) and is_binary(username),
      do: Registry.add_acl_user(client_id, self(), username)

  @doc false
  @spec cancel_acl_authentication(map(), binary()) :: :ok
  def cancel_acl_authentication(%{client_id: client_id, username: username}, username)
      when is_integer(client_id) and is_binary(username),
      do: :ok

  def cancel_acl_authentication(%{client_id: client_id}, username)
      when is_integer(client_id) and is_binary(username),
      do: Registry.remove_acl_user(client_id, self(), username)

  @doc false
  @spec activate_authenticated_user(map(), binary(), non_neg_integer()) ::
          {:ok, map()} | {:error, :acl_changed_during_authentication}
  def activate_authenticated_user(state, username, expected_auth_epoch)
      when is_binary(username) and is_integer(expected_auth_epoch) and expected_auth_epoch >= 0 do
    previous_username = state.username
    :ok = Registry.add_acl_user(state.client_id, self(), username)

    case {CatalogProjector.ready?(), FerricstoreServer.Acl.get_user(username)} do
      {true, %{enabled: true, auth_epoch: ^expected_auth_epoch}} ->
        cache = build_acl_cache(username)

        if cache == :denied do
          :ok = cancel_acl_authentication(state, username)
          {:error, :acl_changed_during_authentication}
        else
          if previous_username != username do
            :ok = Registry.remove_acl_user(state.client_id, self(), previous_username)
          end

          {:ok,
           %{
             state
             | username: username,
               authenticated: true,
               require_auth: false,
               acl_cache: cache
           }}
        end

      _changed_or_unavailable ->
        :ok = cancel_acl_authentication(state, username)
        {:error, :acl_changed_during_authentication}
    end
  end

  @doc false
  @spec broadcast_local_acl_invalidation(binary() | :all, non_neg_integer()) :: :ok
  def broadcast_local_acl_invalidation(username, revision)
      when (is_binary(username) or username == :all) and is_integer(revision) and revision >= 0 do
    pids =
      case username do
        :all -> Registry.all_pids()
        username -> Registry.acl_user_pids(username)
      end

    Enum.each(pids, &send(&1, {:acl_invalidate, username, revision}))

    :ok
  end

  @doc false
  @spec broadcast_acl_catalog_change(
          :upsert | :delete | :all,
          binary() | :all,
          integer(),
          non_neg_integer()
        ) :: :ok
  def broadcast_acl_catalog_change(kind, username, previous_revision, revision)
      when kind in [:upsert, :delete, :all] and (is_binary(username) or username == :all) and
             is_integer(previous_revision) and previous_revision >= -1 and
             is_integer(revision) and revision >= 0 do
    message = {:acl_catalog_changed, kind, username, previous_revision, revision}

    @acl_projector_scope
    |> group_members(@acl_projector_group)
    |> Enum.each(&send(&1, message))

    :ok
  end

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

  @spec check_prepared_resources_cached(
          map() | :full_access | :denied | nil,
          PreparedCommand.t()
        ) :: :ok | {:error, binary()}
  def check_prepared_resources_cached(cache, %PreparedCommand{} = prepared) do
    with :ok <- ensure_acl_projection_ready(),
         :ok <- do_check_channels_cached(cache, prepared.channel_keys) do
      do_check_keys_cached(cache, prepared)
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

  defp group_members(scope, group) do
    :pg.get_members(scope, group)
  catch
    :error, _reason -> []
    :exit, _reason -> []
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
