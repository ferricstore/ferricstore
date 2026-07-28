defmodule FerricstoreServer.Health.Dashboard.Accounts do
  @moduledoc false

  alias FerricstoreServer.Acl.{CatalogProjector, Password, Tables}
  alias FerricstoreServer.Management.ACL, as: ManagementACL

  @minimum_password_characters 12
  @observer_commands ~w(
    INFO SLOWLOG CLUSTER.STATUS CLIENT.LIST FERRICSTORE.CAPABILITIES ACL.LIST PUBSUB
  )
  @separate_control_modifiers MapSet.new(~w(on off nopass resetpass))

  @spec minimum_password_characters() :: pos_integer()
  def minimum_password_characters, do: @minimum_password_characters

  @spec bootstrap_available?() :: boolean()
  def bootstrap_available? do
    CatalogProjector.ready?() and
      Tables.read(fn table ->
        :ets.info(table, :size) == 1 and initial_default_user?(:ets.lookup(table, "default"))
      end)
  rescue
    ArgumentError -> false
  catch
    :exit, _reason -> false
  end

  @spec bootstrap(map()) :: {:ok, binary()} | {:error, binary()}
  def bootstrap(params) when is_map(params) do
    with {:ok, password} <- confirmed_password(params),
         :ok <- map_management_error(ManagementACL.bootstrap_default(password, store_opts())) do
      {:ok, "default"}
    end
  end

  def bootstrap(_params), do: {:error, "Setup form is invalid."}

  @spec create_user(binary(), map()) :: {:ok, binary()} | {:error, binary()}
  def create_user(actor, params) when is_binary(actor) and is_map(params) do
    with {:ok, username} <- username_param(params),
         {:ok, password} <- confirmed_password(params),
         {:ok, rules} <- create_rules(params, password),
         :ok <-
           map_management_error(ManagementACL.create_user(username, rules, store_opts())) do
      {:ok, username}
    end
  end

  def create_user(_actor, _params), do: {:error, "Account form is invalid."}

  @spec set_enabled(binary(), binary(), boolean()) :: {:ok, binary()} | {:error, binary()}
  def set_enabled(actor, username, enabled)
      when is_binary(actor) and is_binary(username) and is_boolean(enabled) do
    with :ok <- protect_state_change(actor, username, enabled),
         :ok <-
           map_management_error(
             ManagementACL.update_user(
               username,
               [if(enabled, do: "on", else: "off")],
               store_opts()
             )
           ) do
      {:ok, username}
    end
  end

  def set_enabled(_actor, _username, _enabled), do: {:error, "Account state is invalid."}

  @spec reset_password(binary(), map()) :: {:ok, binary()} | {:error, binary()}
  def reset_password(_actor, params) when is_map(params) do
    with {:ok, username} <- username_param(params),
         {:ok, password} <- confirmed_password(params),
         :ok <-
           map_management_error(
             ManagementACL.update_user(username, [">" <> password], store_opts())
           ) do
      {:ok, username}
    end
  end

  def reset_password(_actor, _params), do: {:error, "Password form is invalid."}

  @spec apply_modifiers(binary(), map()) :: {:ok, binary()} | {:error, binary()}
  def apply_modifiers(_actor, params) when is_map(params) do
    with {:ok, username} <- username_param(params),
         {:ok, modifiers} <- custom_modifiers(Map.get(params, "modifiers", "")),
         :ok <-
           map_management_error(ManagementACL.update_user(username, modifiers, store_opts())) do
      {:ok, username}
    end
  end

  def apply_modifiers(_actor, _params), do: {:error, "ACL modifier form is invalid."}

  @spec delete_user(binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def delete_user(actor, username) when is_binary(actor) and is_binary(username) do
    cond do
      username == "default" ->
        {:error, "The default recovery administrator cannot be deleted."}

      username == actor ->
        {:error, "You cannot delete your own account from the dashboard."}

      true ->
        with {:ok, 1} <- ManagementACL.delete_user(username, store_opts()) do
          {:ok, username}
        else
          {:error, reason} -> {:error, human_error(reason)}
          _other -> {:error, "Account deletion did not complete."}
        end
    end
  end

  def delete_user(_actor, _username), do: {:error, "Account deletion is invalid."}

  defp create_rules(%{"role" => "admin"}, password) do
    {:ok, ["on", ">" <> password, "~*", "&*", "+@all"]}
  end

  defp create_rules(%{"role" => "observer"} = params, password) do
    key_pattern = normalized_pattern(Map.get(params, "key_pattern", "*"), "key")
    channel_pattern = normalized_pattern(Map.get(params, "channel_pattern", "*"), "channel")

    with {:ok, key_pattern} <- key_pattern,
         {:ok, channel_pattern} <- channel_pattern do
      command_rules = Enum.map(@observer_commands, &("+" <> &1))

      {:ok,
       [
         "on",
         ">" <> password,
         "resetkeys",
         "%R~" <> key_pattern,
         "resetchannels",
         "&" <> channel_pattern,
         "-@all",
         "+@read"
         | command_rules
       ]}
    end
  end

  defp create_rules(%{"role" => "custom"} = params, password) do
    with {:ok, modifiers} <- custom_modifiers(Map.get(params, "modifiers", "")) do
      {:ok, ["on", ">" <> password, "resetkeys", "resetchannels", "-@all" | modifiers]}
    end
  end

  defp create_rules(_params, _password),
    do: {:error, "Choose an administrator, observer, or custom access profile."}

  defp custom_modifiers(value) when is_binary(value) do
    modifiers =
      value
      |> String.split(["\r\n", "\n", "\r"], trim: false)
      |> Enum.reject(&(&1 == ""))

    cond do
      modifiers == [] ->
        {:error, "Enter at least one ACL modifier for custom access."}

      Enum.any?(modifiers, &credential_or_state_modifier?/1) ->
        {:error, "Password and account state modifiers use separate controls."}

      true ->
        {:ok, modifiers}
    end
  end

  defp custom_modifiers(_value), do: {:error, "ACL modifiers must be text."}

  defp credential_or_state_modifier?(modifier) do
    String.starts_with?(modifier, ">") or MapSet.member?(@separate_control_modifiers, modifier)
  end

  defp confirmed_password(params) do
    password = Map.get(params, "password")
    confirmation = Map.get(params, "password_confirmation")

    cond do
      not is_binary(password) or not is_binary(confirmation) ->
        {:error, "Enter and confirm a password."}

      password != confirmation ->
        {:error, "Password and confirmation must match."}

      byte_size(password) > Password.max_password_bytes() ->
        {:error, "Password exceeds #{Password.max_password_bytes()} bytes."}

      not String.valid?(password) ->
        {:error, "Password must be valid UTF-8."}

      String.length(password) < @minimum_password_characters ->
        {:error, "Password must be at least #{@minimum_password_characters} characters."}

      true ->
        {:ok, password}
    end
  end

  defp username_param(params) do
    case Map.get(params, "username") do
      username when is_binary(username) ->
        if username == "",
          do: {:error, "Username is required."},
          else: {:ok, username}

      _other ->
        {:error, "Username is required."}
    end
  end

  defp normalized_pattern(value, label) when is_binary(value) do
    case value do
      "" -> {:error, "#{String.capitalize(label)} pattern is required."}
      pattern -> {:ok, pattern}
    end
  end

  defp normalized_pattern(_value, label),
    do: {:error, "#{String.capitalize(label)} pattern is required."}

  defp protect_state_change(_actor, "default", false),
    do: {:error, "The default recovery administrator cannot be disabled."}

  defp protect_state_change(actor, actor, false),
    do: {:error, "You cannot disable your own account from the dashboard."}

  defp protect_state_change(_actor, _username, _enabled), do: :ok

  defp map_management_error(:ok), do: :ok
  defp map_management_error({:error, reason}), do: {:error, human_error(reason)}
  defp map_management_error(other), do: {:error, human_error(other)}

  defp human_error(:dashboard_already_configured),
    do: "Dashboard setup has already been completed."

  defp human_error(:acl_user_already_exists), do: "An account with that username already exists."
  defp human_error(:acl_user_not_found), do: "That account no longer exists."
  defp human_error(reason) when is_binary(reason), do: String.trim_leading(reason, "ERR ")
  defp human_error(_reason), do: "The ACL catalog could not complete this operation."

  defp store_opts, do: [store: FerricStore.Instance.get(:default)]

  defp initial_default_user?([
         {"default",
          %{
            enabled: true,
            password: nil,
            commands: :all,
            denied_commands: denied_commands,
            keys: :all,
            channels: :all
          }}
       ]) do
    is_struct(denied_commands, MapSet) and MapSet.size(denied_commands) == 0
  end

  defp initial_default_user?(_entries), do: false
end
