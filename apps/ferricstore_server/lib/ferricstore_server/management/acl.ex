defmodule FerricstoreServer.Management.ACL do
  @moduledoc """
  Server-backed implementation of the core ACL management contract.

  The core `:ferricstore` application keeps ACL management pluggable so
  embedded deployments can opt in explicitly. The standalone server owns the
  network ACL table, so it wires this adapter during application startup.
  """

  @behaviour FerricStore.Management.ACL

  alias FerricstoreServer.Acl.{CommandCategories, Password, Persistence, Rules}
  alias Ferricstore.ServerCatalog
  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec
  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.CatalogProjector

  @catalog_value_tag :ferricstore_acl_user
  @max_catalog_value_bytes 1_048_576
  @max_catalog_patterns Rules.max_patterns()
  @max_catalog_pattern_bytes Rules.max_pattern_bytes()
  @catalog_cas_attempts 16

  @doc false
  @spec prepare_catalog_value(map() | nil, binary(), [binary()]) ::
          {:ok, binary()} | {:error, binary()}
  def prepare_catalog_value(existing, username, rules)
      when (is_map(existing) or is_nil(existing)) and is_binary(username) and is_list(rules) do
    base = existing || Acl.new_user()

    with :ok <- Rules.validate_username(username),
         {:ok, user} <- Rules.apply_rules(base, rules),
         {:ok, canonical} <- encode_catalog_user(user) do
      encoded = TermCodec.encode(canonical)

      if byte_size(encoded) <= @max_catalog_value_bytes do
        {:ok, encoded}
      else
        {:error, "ERR ACL user record is too large"}
      end
    end
  end

  def prepare_catalog_value(_existing, _username, _rules),
    do: {:error, "ERR invalid ACL catalog mutation"}

  @doc false
  @spec decode_catalog_value(binary()) :: {:ok, map()} | {:error, :invalid_acl_catalog_value}
  def decode_catalog_value(encoded)
      when is_binary(encoded) and byte_size(encoded) <= @max_catalog_value_bytes do
    case TermCodec.decode(encoded) do
      {:ok, {@catalog_value_tag, enabled, password, commands, denied_commands, keys, channels}} ->
        decode_catalog_user(enabled, password, commands, denied_commands, keys, channels)

      _invalid ->
        {:error, :invalid_acl_catalog_value}
    end
  rescue
    _error -> {:error, :invalid_acl_catalog_value}
  end

  def decode_catalog_value(_encoded), do: {:error, :invalid_acl_catalog_value}

  defp encode_catalog_user(%{
         enabled: enabled,
         password: password,
         commands: commands,
         denied_commands: denied_commands,
         keys: keys,
         channels: channels
       }) do
    with true <- is_boolean(enabled),
         :ok <- validate_catalog_password(password),
         {:ok, commands} <- encode_catalog_commands(commands, true),
         {:ok, denied_commands} <- encode_catalog_commands(denied_commands, false),
         {:ok, keys} <- encode_catalog_keys(keys),
         {:ok, channels} <- encode_catalog_channels(channels) do
      {:ok, {@catalog_value_tag, enabled, password, commands, denied_commands, keys, channels}}
    else
      _invalid -> {:error, "ERR invalid ACL user record"}
    end
  rescue
    _error -> {:error, "ERR invalid ACL user record"}
  end

  defp encode_catalog_user(_invalid), do: {:error, "ERR invalid ACL user record"}

  defp decode_catalog_user(enabled, password, commands, denied_commands, keys, channels) do
    with true <- is_boolean(enabled),
         :ok <- validate_catalog_password(password),
         {:ok, commands} <- decode_catalog_commands(commands, true),
         {:ok, denied_commands} <- decode_catalog_commands(denied_commands, false),
         {:ok, keys} <- decode_catalog_keys(keys),
         {:ok, channels} <- decode_catalog_channels(channels) do
      {:ok,
       %{
         enabled: enabled,
         password: password,
         commands: commands,
         denied_commands: denied_commands,
         keys: keys,
         channels: channels
       }}
    else
      _invalid -> {:error, :invalid_acl_catalog_value}
    end
  end

  defp validate_catalog_password(nil), do: :ok

  defp validate_catalog_password(password) when is_binary(password) do
    if Password.valid_stored_hash_format?(password),
      do: :ok,
      else: {:error, :invalid_acl_catalog_value}
  end

  defp validate_catalog_password(_invalid), do: {:error, :invalid_acl_catalog_value}

  defp encode_catalog_commands(:all, true), do: {:ok, :all}

  defp encode_catalog_commands(%MapSet{} = commands, _allow_all?) do
    commands
    |> MapSet.to_list()
    |> validate_catalog_command_list()
  end

  defp encode_catalog_commands(_invalid, _allow_all?),
    do: {:error, :invalid_acl_catalog_value}

  defp decode_catalog_commands(:all, true), do: {:ok, :all}

  defp decode_catalog_commands(commands, _allow_all?) when is_list(commands) do
    with {:ok, commands} <- validate_catalog_command_list(commands) do
      {:ok, MapSet.new(commands)}
    end
  end

  defp decode_catalog_commands(_invalid, _allow_all?),
    do: {:error, :invalid_acl_catalog_value}

  defp validate_catalog_command_list(commands) do
    supported = CommandCategories.acl_supported_commands()

    if length(commands) <= MapSet.size(supported) and
         Enum.all?(commands, &(is_binary(&1) and MapSet.member?(supported, &1))) do
      {:ok, Enum.sort(Enum.uniq(commands))}
    else
      {:error, :invalid_acl_catalog_value}
    end
  end

  defp encode_catalog_keys(:all), do: {:ok, :all}

  defp encode_catalog_keys(keys) when is_list(keys) and length(keys) <= @max_catalog_patterns do
    Enum.reduce_while(keys, {:ok, []}, fn
      {pattern, mode, %Regex{}}, {:ok, acc}
      when is_binary(pattern) and mode in [:read, :write, :rw] and
             byte_size(pattern) <= @max_catalog_pattern_bytes ->
        {:cont, {:ok, [{pattern, mode} | acc]}}

      _invalid, _acc ->
        {:halt, {:error, :invalid_acl_catalog_value}}
    end)
    |> reverse_catalog_list()
  end

  defp encode_catalog_keys(_invalid), do: {:error, :invalid_acl_catalog_value}

  defp decode_catalog_keys(:all), do: {:ok, :all}

  defp decode_catalog_keys(keys) when is_list(keys) and length(keys) <= @max_catalog_patterns do
    Enum.reduce_while(keys, {:ok, []}, fn
      {pattern, mode}, {:ok, acc}
      when is_binary(pattern) and mode in [:read, :write, :rw] and
             byte_size(pattern) <= @max_catalog_pattern_bytes ->
        {:cont, {:ok, [{pattern, mode, Acl.compile_glob(pattern)} | acc]}}

      _invalid, _acc ->
        {:halt, {:error, :invalid_acl_catalog_value}}
    end)
    |> reverse_catalog_list()
  end

  defp decode_catalog_keys(_invalid), do: {:error, :invalid_acl_catalog_value}

  defp encode_catalog_channels(:all), do: {:ok, :all}

  defp encode_catalog_channels(channels)
       when is_list(channels) and length(channels) <= @max_catalog_patterns do
    Enum.reduce_while(channels, {:ok, []}, fn
      {pattern, %Regex{}}, {:ok, acc}
      when is_binary(pattern) and byte_size(pattern) <= @max_catalog_pattern_bytes ->
        {:cont, {:ok, [pattern | acc]}}

      _invalid, _acc ->
        {:halt, {:error, :invalid_acl_catalog_value}}
    end)
    |> reverse_catalog_list()
  end

  defp encode_catalog_channels(_invalid), do: {:error, :invalid_acl_catalog_value}

  defp decode_catalog_channels(:all), do: {:ok, :all}

  defp decode_catalog_channels(channels)
       when is_list(channels) and length(channels) <= @max_catalog_patterns do
    Enum.reduce_while(channels, {:ok, []}, fn
      pattern, {:ok, acc}
      when is_binary(pattern) and byte_size(pattern) <= @max_catalog_pattern_bytes ->
        {:cont, {:ok, [{pattern, Acl.compile_glob(pattern)} | acc]}}

      _invalid, _acc ->
        {:halt, {:error, :invalid_acl_catalog_value}}
    end)
    |> reverse_catalog_list()
  end

  defp decode_catalog_channels(_invalid), do: {:error, :invalid_acl_catalog_value}

  defp reverse_catalog_list({:ok, values}), do: {:ok, Enum.reverse(values)}
  defp reverse_catalog_list({:error, _reason} = error), do: error

  @impl true
  def set_user(username, rules, opts) do
    with :ok <- Rules.validate_username(username),
         {:ok, store} <- mutation_store(opts) do
      set_user_catalog(store, username, rules, @catalog_cas_attempts)
    end
  end

  @impl true
  def del_user(username, opts) do
    with :ok <- Rules.validate_username(username),
         {:ok, store} <- mutation_store(opts) do
      del_user_catalog(store, username, @catalog_cas_attempts)
    end
  end

  @impl true
  def del_users([username], opts) when is_binary(username), do: del_user(username, opts)

  def del_users(usernames, opts) when is_list(usernames) do
    with {:ok, store} <- mutation_store(opts),
         {:ok, usernames} <- validate_deleted_usernames(usernames) do
      delete_catalog_users(store, usernames, @catalog_cas_attempts)
    end
  end

  def del_users(_usernames, _opts), do: {:error, "ERR invalid ACL user list"}

  @impl true
  def get_user(username, _opts) do
    {:ok, Acl.get_user_info(username) || []}
  end

  @impl true
  def list_users(_opts) do
    {:ok, Acl.list_users()}
  end

  @impl true
  def save(_opts) do
    Acl.save()
  end

  @impl true
  def load(opts) do
    with {:ok, store} <- mutation_store(opts),
         data_dir <-
           Keyword.get(opts, :data_dir, Application.get_env(:ferricstore, :data_dir, "data")),
         {:ok, contents} <- Persistence.read_file_contents(data_dir) do
      import_contents(contents, store: store)
    end
  end

  @doc false
  @spec import_contents(binary(), keyword()) :: :ok | {:error, term()}
  def import_contents(contents, opts) when is_binary(contents) and is_list(opts) do
    with {:ok, store} <- mutation_store(opts),
         {:ok, users} <- Persistence.parse_contents(contents) do
      replace_users(users, store: store)
    end
  end

  def import_contents(_contents, _opts), do: {:error, "ERR invalid ACL file contents"}

  @doc false
  @spec replace_users([{binary(), map()}], keyword()) :: :ok | {:error, term()}
  def replace_users(users, opts) when is_list(users) and is_list(opts) do
    with {:ok, store} <- mutation_store(opts),
         {:ok, desired} <- encode_catalog_snapshot(users),
         {:ok, max_users} <- configured_max_users(),
         :ok <- validate_desired_user_count(desired, max_users) do
      replace_catalog_users(store, desired, max_users, @catalog_cas_attempts)
    end
  end

  def replace_users(_users, _opts), do: {:error, "ERR invalid ACL catalog snapshot"}

  @doc false
  def catalog_entries(store) do
    with {:ok, entries} <- Router.server_catalog_entries(store, "acl") do
      Enum.reduce_while(entries, {:ok, []}, fn {username, encoded}, {:ok, acc} ->
        case ServerCatalog.decode_entry(encoded) do
          {:ok, entry} ->
            {:cont, {:ok, [{username, entry, encoded} | acc]}}

          {:error, :invalid_server_catalog_entry} ->
            {:halt, {:error, :invalid_acl_catalog_entry}}
        end
      end)
      |> case do
        {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
        {:error, _reason} = error -> error
      end
    end
  end

  @doc false
  @spec reconcile_catalog(FerricStore.Instance.t()) :: :ok | {:error, term()}
  def reconcile_catalog(store), do: reconcile_catalog(store, [])

  @doc false
  @spec reconcile_catalog(FerricStore.Instance.t(), keyword()) :: :ok | {:error, term()}
  def reconcile_catalog(store, opts) when is_list(opts) do
    await_projector? = Keyword.get(opts, :await_projector, true)

    with :ok <- ensure_default_catalog(store) do
      do_reconcile_catalog(store, @catalog_cas_attempts, await_projector?)
    end
  end

  @doc false
  @spec ensure_default_catalog(FerricStore.Instance.t()) :: :ok | {:error, term()}
  def ensure_default_catalog(store) do
    ensure_default_catalog(store, @catalog_cas_attempts)
  end

  defp do_reconcile_catalog(store, attempts, await_projector?) do
    with {:ok, before_revision, before_version} <- catalog_revision(store),
         true <- before_version >= 0,
         {:ok, entries} <- catalog_entries(store),
         {:ok, after_revision, _after_version} <- catalog_revision(store) do
      if before_revision == after_revision do
        with {:ok, users} <- decode_catalog_snapshot(entries) do
          case Acl.replace_catalog_snapshot(users, before_version) do
            :ok ->
              if await_projector?, do: await_catalog_projection(before_version), else: :ok

            {:error, :stale_acl_catalog_snapshot} when attempts > 1 ->
              do_reconcile_catalog(store, attempts - 1, await_projector?)

            result ->
              result
          end
        end
      else
        if attempts > 1 do
          do_reconcile_catalog(store, attempts - 1, await_projector?)
        else
          {:error, :acl_catalog_changed_concurrently}
        end
      end
    else
      false -> {:error, :missing_acl_catalog_revision}
      {:error, _reason} = error -> error
    end
  end

  defp decode_catalog_snapshot(entries) do
    entries
    |> Enum.reduce_while({:ok, []}, fn
      {username, %{version: version, value: :deleted}, _encoded}, {:ok, users}
      when is_binary(username) and is_integer(version) and version >= 0 ->
        case Rules.validate_username(username) do
          :ok -> {:cont, {:ok, users}}
          {:error, _reason} -> {:halt, {:error, :invalid_acl_catalog_entry}}
        end

      {username, %{version: version, value: value}, _encoded}, {:ok, users}
      when is_binary(username) and is_integer(version) and version >= 0 and is_binary(value) ->
        with :ok <- Rules.validate_username(username),
             {:ok, user} <- decode_catalog_value(value) do
          {:cont, {:ok, [{username, user, version} | users]}}
        else
          {:error, _invalid} -> {:halt, {:error, :invalid_acl_catalog_value}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_acl_catalog_entry}}
    end)
    |> case do
      {:ok, users} ->
        users = Enum.reverse(users)

        if Enum.any?(users, fn {username, _user, _version} -> username == "default" end) do
          {:ok, users}
        else
          {:error, :missing_default_acl_user}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp encode_catalog_snapshot(users) do
    Enum.reduce_while(users, {:ok, %{}}, fn
      {username, user}, {:ok, desired} when is_binary(username) and is_map(user) ->
        with :ok <- Rules.validate_username(username),
             false <- Map.has_key?(desired, username),
             {:ok, value} <- encode_catalog_snapshot_user(user) do
          {:cont, {:ok, Map.put(desired, username, value)}}
        else
          true -> {:halt, {:error, "ERR duplicate ACL user '#{username}'"}}
          {:error, _reason} = error -> {:halt, error}
        end

      _invalid, _acc ->
        {:halt, {:error, "ERR invalid ACL catalog snapshot"}}
    end)
    |> case do
      {:ok, desired} ->
        if Map.has_key?(desired, "default") do
          {:ok, desired}
        else
          {:error, "ERR ACL file must contain a 'default' user definition"}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp encode_catalog_snapshot_user(user) do
    with {:ok, canonical} <- encode_catalog_user(user) do
      value = TermCodec.encode(canonical)

      if byte_size(value) <= @max_catalog_value_bytes do
        {:ok, value}
      else
        {:error, "ERR ACL user record is too large"}
      end
    end
  end

  defp validate_desired_user_count(desired, max_users) do
    if map_size(desired) <= max_users,
      do: :ok,
      else: {:error, "ERR max ACL users reached (#{max_users})"}
  end

  defp replace_catalog_users(store, desired, max_users, attempts) do
    with {:ok, before_revision, _before_version} <- catalog_revision(store),
         {:ok, entries} <- catalog_entries(store),
         {:ok, after_revision, _after_version} <- catalog_revision(store) do
      if before_revision == after_revision do
        with {:ok, current} <- catalog_values(entries) do
          mutations = catalog_replacement_mutations(current, desired)

          case mutations do
            [] ->
              reconcile_catalog(store)

            _changes ->
              case Router.server_catalog_replace(
                     store,
                     "acl",
                     before_revision,
                     mutations,
                     map_size(desired),
                     max_users
                   ) do
                {:ok, _revision} ->
                  reconcile_catalog(store)

                {:error, :stale_server_catalog_revision} when attempts > 1 ->
                  replace_catalog_users(store, desired, max_users, attempts - 1)

                {:error, :stale_server_catalog_revision} ->
                  {:error, "ERR ACL catalog changed concurrently"}

                {:error, {:server_catalog_limit_reached, max}} ->
                  {:error, "ERR max ACL users reached (#{max})"}

                {:error, reason} ->
                  {:error, reason}

                :unavailable ->
                  {:error, "ERR ACL catalog unavailable"}

                other ->
                  {:error, other}
              end
          end
        end
      else
        if attempts > 1 do
          replace_catalog_users(store, desired, max_users, attempts - 1)
        else
          {:error, "ERR ACL catalog changed concurrently"}
        end
      end
    end
  end

  defp catalog_values(entries) do
    Enum.reduce_while(entries, {:ok, %{}}, fn
      {username, %{value: value}, _encoded}, {:ok, values}
      when is_binary(username) and is_binary(value) ->
        {:cont, {:ok, Map.put(values, username, value)}}

      _invalid, _acc ->
        {:halt, {:error, "ERR invalid durable ACL catalog entry"}}
    end)
  end

  defp catalog_replacement_mutations(current, desired) do
    updates =
      Enum.reduce(desired, [], fn {username, value}, acc ->
        if Map.get(current, username) == value, do: acc, else: [{username, value} | acc]
      end)

    deletes =
      Enum.reduce(current, [], fn {username, _value}, acc ->
        if Map.has_key?(desired, username), do: acc, else: [{username, :deleted} | acc]
      end)

    Enum.sort_by(updates ++ deletes, &elem(&1, 0))
  end

  defp set_user_catalog(store, username, rules, attempts) do
    with {:ok, expected, expected_revision, existing} <-
           current_catalog_user(store, username),
         {:ok, max_users} <- configured_max_users(),
         {:ok, value} <- prepare_catalog_value(existing, username, rules) do
      case Router.server_catalog_mutate(
             store,
             "acl",
             username,
             expected,
             expected_revision,
             value,
             max_users
           ) do
        {:ok, encoded} ->
          project_set_user(store, username, encoded, expected_revision)

        {:error, stale}
        when stale in [:stale_server_catalog_entry, :stale_server_catalog_revision] and
               attempts > 1 ->
          set_user_catalog(store, username, rules, attempts - 1)

        {:error, stale}
        when stale in [:stale_server_catalog_entry, :stale_server_catalog_revision] ->
          {:error, "ERR ACL user changed concurrently"}

        {:error, {:server_catalog_limit_reached, max}} ->
          {:error, "ERR max ACL users reached (#{max})"}

        {:error, reason} ->
          {:error, reason}

        :unavailable ->
          {:error, "ERR ACL catalog unavailable"}

        other ->
          {:error, other}
      end
    end
  end

  defp del_user_catalog(_store, "default", _attempts),
    do: {:error, "ERR The 'default' user cannot be removed"}

  defp del_user_catalog(store, username, attempts) do
    with {:ok, expected, expected_revision, existing} <-
           current_catalog_user(store, username) do
      if existing == nil do
        {:error, "ERR User '#{username}' does not exist"}
      else
        case Router.server_catalog_mutate(
               store,
               "acl",
               username,
               expected,
               expected_revision,
               :deleted,
               0
             ) do
          {:ok, encoded} ->
            project_deleted_user(store, username, encoded, expected_revision)

          {:error, stale}
          when stale in [:stale_server_catalog_entry, :stale_server_catalog_revision] and
                 attempts > 1 ->
            del_user_catalog(store, username, attempts - 1)

          {:error, stale}
          when stale in [:stale_server_catalog_entry, :stale_server_catalog_revision] ->
            {:error, "ERR ACL user changed concurrently"}

          {:error, reason} ->
            {:error, reason}

          :unavailable ->
            {:error, "ERR ACL catalog unavailable"}

          other ->
            {:error, other}
        end
      end
    end
  end

  defp validate_deleted_usernames([]),
    do: {:error, "ERR wrong number of arguments for 'acl deluser' command"}

  defp validate_deleted_usernames(usernames) do
    cond do
      not Enum.all?(usernames, &is_binary/1) ->
        {:error, "ERR invalid ACL user list"}

      "default" in usernames ->
        {:error, "ERR The 'default' user cannot be removed"}

      true ->
        Enum.reduce_while(usernames, {:ok, []}, fn username, {:ok, valid} ->
          case Rules.validate_username(username) do
            :ok -> {:cont, {:ok, [username | valid]}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
        |> case do
          {:ok, valid} -> {:ok, valid |> Enum.reverse() |> Enum.uniq()}
          {:error, _reason} = error -> error
        end
    end
  end

  defp delete_catalog_users(store, usernames, attempts) do
    with {:ok, before_revision, _before_version} <- catalog_revision(store),
         {:ok, entries} <- catalog_entries(store),
         {:ok, after_revision, _after_version} <- catalog_revision(store) do
      if before_revision == after_revision do
        with {:ok, current} <- catalog_values(entries),
             :ok <- validate_deleted_users_exist(current, usernames) do
          next_count = map_size(current) - length(usernames)
          mutations = Enum.map(usernames, &{&1, :deleted})

          case Router.server_catalog_replace(
                 store,
                 "acl",
                 before_revision,
                 mutations,
                 next_count,
                 next_count
               ) do
            {:ok, _revision} ->
              with :ok <- reconcile_catalog(store), do: {:ok, length(usernames)}

            {:error, :stale_server_catalog_revision} when attempts > 1 ->
              delete_catalog_users(store, usernames, attempts - 1)

            {:error, :stale_server_catalog_revision} ->
              {:error, "ERR ACL catalog changed concurrently"}

            {:error, reason} ->
              {:error, reason}

            :unavailable ->
              {:error, "ERR ACL catalog unavailable"}

            other ->
              {:error, other}
          end
        end
      else
        if attempts > 1 do
          delete_catalog_users(store, usernames, attempts - 1)
        else
          {:error, "ERR ACL catalog changed concurrently"}
        end
      end
    end
  end

  defp validate_deleted_users_exist(current, usernames) do
    case Enum.find(usernames, &(not Map.has_key?(current, &1))) do
      nil -> :ok
      missing -> {:error, "ERR User '#{missing}' does not exist"}
    end
  end

  defp current_catalog_user(store, username) do
    with {:ok, expected_revision} <- current_catalog_revision(store) do
      case Router.server_catalog_entry(store, "acl", username) do
        {:ok, nil} ->
          {:ok, nil, expected_revision, nil}

        {:ok, encoded} when is_binary(encoded) ->
          with {:ok, %{value: value}} <- ServerCatalog.decode_entry(encoded) do
            case value do
              :deleted ->
                {:ok, encoded, expected_revision, nil}

              value when is_binary(value) ->
                case decode_catalog_value(value) do
                  {:ok, user} ->
                    {:ok, encoded, expected_revision, user}

                  {:error, :invalid_acl_catalog_value} ->
                    {:error, "ERR invalid durable ACL user"}
                end

              _invalid ->
                {:error, "ERR invalid durable ACL user"}
            end
          else
            {:error, :invalid_server_catalog_entry} ->
              {:error, "ERR invalid durable ACL catalog entry"}
          end

        :unavailable ->
          {:error, "ERR ACL catalog unavailable"}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp current_catalog_revision(store) do
    with {:ok, encoded, _version} <- catalog_revision(store) do
      {:ok, encoded}
    end
  end

  defp catalog_revision(store) do
    case Router.server_catalog_revision(store, "acl") do
      {:ok, nil} ->
        {:ok, nil, -1}

      {:ok, encoded} when is_binary(encoded) ->
        case ServerCatalog.decode_revision(encoded) do
          {:ok, version} -> {:ok, encoded, version}
          {:error, _invalid} -> {:error, "ERR invalid durable ACL catalog revision"}
        end

      :unavailable ->
        {:error, "ERR ACL catalog unavailable"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_default_catalog(store, attempts) do
    with {:ok, expected, expected_revision, existing} <-
           current_catalog_user(store, "default") do
      if existing do
        :ok
      else
        with {:ok, max_users} <- configured_max_users(),
             {:ok, value} <- prepare_catalog_value(Acl.default_user(), "default", []) do
          case Router.server_catalog_mutate(
                 store,
                 "acl",
                 "default",
                 expected,
                 expected_revision,
                 value,
                 max_users
               ) do
            {:ok, _encoded} ->
              :ok

            {:error, stale}
            when stale in [:stale_server_catalog_entry, :stale_server_catalog_revision] and
                   attempts > 1 ->
              ensure_default_catalog(store, attempts - 1)

            {:error, {:server_catalog_limit_reached, max}} ->
              {:error, "ERR max ACL users reached (#{max})"}

            {:error, reason} ->
              {:error, reason}

            :unavailable ->
              {:error, "ERR ACL catalog unavailable"}

            other ->
              {:error, other}
          end
        end
      end
    end
  end

  defp project_set_user(store, username, encoded, expected_revision) do
    case Acl.project_catalog_entry(username, encoded, expected_revision) do
      :ok -> await_catalog_entry_projection(encoded)
      {:error, :acl_catalog_projection_gap} -> reconcile_catalog(store)
      {:error, reason} -> {:error, reason}
    end
  end

  defp project_deleted_user(store, username, encoded, expected_revision) do
    case Acl.project_catalog_entry(username, encoded, expected_revision) do
      :ok ->
        with :ok <- await_catalog_entry_projection(encoded), do: {:ok, 1}

      {:error, :acl_catalog_projection_gap} ->
        with :ok <- reconcile_catalog(store), do: {:ok, 1}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp await_catalog_entry_projection(encoded) when is_binary(encoded) do
    case ServerCatalog.decode_entry(encoded) do
      {:ok, %{version: revision}} when is_integer(revision) and revision >= 0 ->
        await_catalog_projection(revision)

      _invalid ->
        {:error, :invalid_acl_catalog_entry}
    end
  end

  defp await_catalog_projection(revision) when is_integer(revision) and revision >= 0 do
    case Process.whereis(CatalogProjector) do
      projector when projector == self() ->
        :ok

      projector when is_pid(projector) ->
        case CatalogProjector.require_revision(revision) do
          %{ready: true, revision: current} when is_integer(current) and current >= revision ->
            :ok

          _not_ready ->
            {:error, :acl_catalog_projection_unavailable}
        end

      nil ->
        {:error, :acl_catalog_projection_unavailable}
    end
  rescue
    _error -> {:error, :acl_catalog_projection_unavailable}
  catch
    :exit, _reason -> {:error, :acl_catalog_projection_unavailable}
  end

  defp configured_max_users do
    case Application.get_env(:ferricstore, :max_acl_users, 10_000) do
      max when is_integer(max) and max >= 1 -> {:ok, max}
      _invalid -> {:error, "ERR invalid max ACL users configuration"}
    end
  end

  defp mutation_store(opts) when is_list(opts) do
    case Keyword.fetch(opts, :store) do
      {:ok, %{shard_count: shard_count} = store}
      when is_integer(shard_count) and shard_count > 0 ->
        {:ok, store}

      _other ->
        {:error, "ERR ACL mutation requires a FerricStore instance"}
    end
  end

  defp mutation_store(_opts), do: {:error, "ERR ACL mutation requires a FerricStore instance"}
end
