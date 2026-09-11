defmodule FerricstoreServer.Health.Dashboard.Data.Security do
  @moduledoc false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.{CommandCategories, Formatter, Tables}
  alias FerricstoreServer.Health.Dashboard.Access
  alias FerricstoreServer.Health.Endpoint.RouteRequirements

  @dashboard_routes [
    {"Overview", "GET", "/dashboard"},
    {"Security", "GET", "/dashboard/security"},
    {"Capabilities", "GET", "/dashboard/capabilities"},
    {"Config", "GET", "/dashboard/config"},
    {"Keyspace", "GET", "/dashboard/keyspace"},
    {"Prefixes", "GET", "/dashboard/prefixes"},
    {"Read Path", "GET", "/dashboard/reads"},
    {"Commands", "GET", "/dashboard/commands"},
    {"Streams", "GET", "/dashboard/streams"},
    {"Pub/Sub", "GET", "/dashboard/pubsub"},
    {"Flow Overview", "GET", "/dashboard/flow"},
    {"Flow States", "GET", "/dashboard/flow/states"},
    {"Flow Workers", "GET", "/dashboard/flow/workers"},
    {"Flow Due", "GET", "/dashboard/flow/due"},
    {"Flow Schedules", "GET", "/dashboard/flow/schedules"},
    {"Flow Failures", "GET", "/dashboard/flow/failures"},
    {"Flow Lineage", "GET", "/dashboard/flow/lineage"},
    {"Flow Query", "GET", "/dashboard/flow/query"},
    {"Flow Signals", "GET", "/dashboard/flow/signals"},
    {"Flow Policies", "GET", "/dashboard/flow/policies"},
    {"Flow Governance", "GET", "/dashboard/flow/governance"},
    {"Flow Retention", "GET", "/dashboard/flow/retention"},
    {"Slow Log", "GET", "/dashboard/slowlog"},
    {"Merge", "GET", "/dashboard/merge"},
    {"Clients", "GET", "/dashboard/clients"},
    {"Consensus", "GET", "/dashboard/raft"},
    {"Storage", "GET", "/dashboard/storage"},
    {"Doctor", "GET", "/dashboard/doctor"}
  ]

  @spec collect_page(keyword() | map()) :: map()
  def collect_page(opts \\ []) do
    current_user = Access.keyspace_acl_username(opts)
    params = normalize_params(opts)
    users = acl_user_summaries()

    %{
      protected_mode: safe_boolean(&Acl.protected_mode?/0),
      configured_users: safe_boolean(&Acl.has_configured_users?/0),
      current_user: current_user,
      acl_user_count: length(users),
      acl_users: users,
      can_manage_users: command_allowed?(current_user, "ACL.SETUSER"),
      can_delete_users: command_allowed?(current_user, "ACL.DELUSER"),
      flash: collect_flash(params),
      tester: collect_tester(params, current_user),
      route_requirements: route_requirements(),
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  def account_error_page(actor, params, message) do
    draft =
      params
      |> Map.take(~w(username role key_pattern channel_pattern modifiers))
      |> Map.update("modifiers", "", fn value ->
        value
        |> String.split(["\r\n", "\n", "\r"])
        |> Enum.reject(&(String.trim_leading(&1) |> String.starts_with?([">", "<", "#", "!"])))
        |> Enum.join("\n")
      end)

    %{
      current_user: actor,
      account_form_only?: true,
      account_draft: draft,
      can_manage_users: true,
      flash: %{status: "error", message: message}
    }
  end

  defp normalize_params(params) when is_map(params) do
    Map.new(params, fn {key, value} -> {to_string(key), normalize_value(value)} end)
  end

  defp normalize_params(params) when is_list(params) do
    params
    |> Enum.map(fn {key, value} -> {to_string(key), normalize_value(value)} end)
    |> Map.new()
  end

  defp normalize_params(_params), do: %{}

  defp normalize_value(value) when is_binary(value), do: value
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: to_string(value || "")

  defp safe_boolean(fun) do
    fun.()
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  defp acl_user_summaries do
    Tables.read(&:ets.tab2list/1)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&acl_user_summary/1)
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp acl_user_summary({username, user}) when is_binary(username) and is_map(user) do
    %{
      username: username,
      state: if(Map.get(user, :enabled) == true, do: "on", else: "off"),
      rule: Formatter.format_user_rule({username, user}),
      password_configured: is_binary(Map.get(user, :password)),
      access: access_label(user)
    }
  rescue
    _error -> invalid_acl_user_summary(username)
  end

  defp acl_user_summary(_invalid), do: invalid_acl_user_summary("unknown")

  defp invalid_acl_user_summary(username) do
    %{
      username: username,
      state: "unknown",
      rule: "Invalid ACL record",
      password_configured: false,
      access: "Invalid record"
    }
  end

  defp access_label(%{commands: :all} = user) do
    no_denials? = MapSet.size(Map.get(user, :denied_commands, MapSet.new())) == 0

    if no_denials? and unrestricted_keys?(Map.get(user, :keys)) and
         unrestricted_channels?(Map.get(user, :channels)) do
      "Full administrator"
    else
      if no_denials?, do: "All commands; scoped access", else: "All commands with exceptions"
    end
  end

  defp access_label(%{commands: commands}) when is_struct(commands, MapSet) do
    "#{MapSet.size(commands)} explicit commands"
  end

  defp access_label(_user), do: "Restricted"

  defp unrestricted_keys?(:all), do: true

  defp unrestricted_keys?(patterns) when is_list(patterns) do
    Enum.all?([:read, :write], fn access ->
      Enum.any?(patterns, fn {glob, mode, _regex} ->
        wildcard?(glob) and mode in [:rw, access]
      end)
    end)
  end

  defp unrestricted_keys?(_patterns), do: false
  defp unrestricted_channels?(:all), do: true

  defp unrestricted_channels?(patterns) when is_list(patterns),
    do: Enum.any?(patterns, fn {glob, _regex} -> wildcard?(glob) end)

  defp unrestricted_channels?(_patterns), do: false
  defp wildcard?(glob), do: glob != "" and String.trim(glob, "*") == ""

  defp command_allowed?(username, command) when is_binary(username) do
    Acl.check_command(username, command) == :ok
  rescue
    _error -> false
  catch
    :exit, _reason -> false
  end

  defp command_allowed?(_username, _command), do: false

  defp collect_flash(params) do
    status = Map.get(params, "status")
    message = Map.get(params, "message")

    if status in ["ok", "error"] and is_binary(message) and message != "" do
      %{status: status, message: String.slice(message, 0, 512)}
    end
  end

  defp collect_tester(params, current_user) do
    user = Map.get_lazy(params, "user", fn -> current_user || "default" end)
    command = params |> Map.get("command", "") |> String.trim() |> String.upcase()
    key = Map.get(params, "key", "")
    key_access = normalize_key_access(Map.get(params, "key_access", "read"))
    channel = Map.get(params, "channel", "")
    route_path = params |> Map.get("route_path", "") |> String.trim()
    route_method = params |> Map.get("route_method", "GET") |> String.trim() |> String.upcase()
    submitted? = Enum.any?(~w(user command key channel route_path), &Map.has_key?(params, &1))

    errors =
      if submitted? do
        %{}
        |> tester_user_error(user)
        |> tester_command_error(command)
        |> tester_target_error([command, key, channel, route_path])
      else
        %{}
      end

    valid_user? = not Map.has_key?(errors, :user)

    %{
      errors: errors,
      input: %{
        user: user,
        command: command,
        key: key,
        key_access: key_access,
        channel: channel,
        route_method: route_method,
        route_path: route_path
      },
      command:
        if(valid_user?,
          do: check_command(user, command),
          else: idle_result("Command not checked")
        ),
      key:
        if(valid_user?,
          do: check_key(user, key, key_access),
          else: idle_result("Key not checked")
        ),
      channel:
        if(valid_user?,
          do: check_channel(user, channel),
          else: idle_result("Channel not checked")
        ),
      route:
        if(valid_user?,
          do: check_route(user, route_method, route_path),
          else: idle_result("Route not checked")
        )
    }
  end

  defp tester_user_error(errors, user) do
    case Acl.get_user(user) do
      nil ->
        Map.put(errors, :user, "User does not exist. Choose an account from the list above.")

      %{enabled: false} ->
        Map.put(errors, :user, "User is disabled. Choose an enabled account to test access.")

      %{enabled: true} ->
        errors

      _ ->
        Map.put(
          errors,
          :user,
          "User has an invalid ACL record. Review the account configuration."
        )
    end
  rescue
    _ ->
      Map.put(errors, :user, "Account lookup unavailable. Try again after ACL service recovers.")
  catch
    :exit, _ ->
      Map.put(errors, :user, "Account lookup unavailable. Try again after ACL service recovers.")
  end

  defp tester_command_error(errors, ""), do: errors

  defp tester_command_error(errors, command) do
    if supported_command?(command),
      do: errors,
      else:
        Map.put(
          errors,
          :command,
          "#{command} is not a supported command. Use a command name such as GET or ACL.LIST."
        )
  end

  defp tester_target_error(errors, targets) do
    if Enum.all?(targets, &(&1 == "")),
      do: Map.put(errors, :targets, "Enter a command, key, channel, or route to check."),
      else: errors
  end

  defp supported_command?(command),
    do: MapSet.member?(CommandCategories.acl_supported_commands(), command)

  defp normalize_key_access(:write), do: :write

  defp normalize_key_access(value) when is_binary(value) do
    if String.downcase(String.trim(value)) == "write", do: :write, else: :read
  end

  defp normalize_key_access(_value), do: :read

  defp check_command(_user, ""), do: idle_result("Command not checked")

  defp check_command(user, command) do
    if supported_command?(command) do
      case Acl.check_command(user, command) do
        :ok -> allowed_result("Command allowed", "+#{command}")
        {:error, reason} -> denied_result("Command denied", reason)
      end
    else
      %{status: :unsupported, label: "Unsupported command", detail: command}
    end
  rescue
    _ -> denied_result("Command denied", "ACL lookup failed")
  catch
    :exit, _ -> denied_result("Command denied", "ACL lookup failed")
  end

  defp check_key(_user, "", _access), do: idle_result("Key not checked")

  defp check_key(user, key, access) do
    case Acl.check_key_access(user, key, access) do
      :ok -> allowed_result("Key allowed", "%#{access_tag(access)}~#{key}")
      {:error, reason} -> denied_result("Key denied", reason)
    end
  rescue
    _ -> denied_result("Key denied", "ACL lookup failed")
  catch
    :exit, _ -> denied_result("Key denied", "ACL lookup failed")
  end

  defp access_tag(:write), do: "W"
  defp access_tag(:read), do: "R"

  defp check_channel(_user, ""), do: idle_result("Channel not checked")

  defp check_channel(user, channel) do
    case Acl.get_user(user) do
      nil ->
        denied_result("Channel denied", "user does not exist")

      %{enabled: false} ->
        denied_result("Channel denied", "user is disabled")

      %{channels: :all} ->
        allowed_result("Channel allowed", "&*")

      %{channels: patterns} when is_list(patterns) ->
        if Acl.channel_matches_any?(channel, patterns) do
          allowed_result("Channel allowed", "channel matches ACL pattern")
        else
          denied_result("Channel denied", "channel does not match any ACL channel pattern")
        end

      _invalid_user ->
        denied_result("Channel denied", "invalid ACL user state")
    end
  rescue
    _ -> denied_result("Channel denied", "ACL lookup failed")
  catch
    :exit, _ -> denied_result("Channel denied", "ACL lookup failed")
  end

  defp check_route(_user, _method, ""), do: idle_result("Route not checked")

  defp check_route(user, method, path) do
    requirement = RouteRequirements.known_dashboard_route_requirement(method, path)
    evaluated = "#{method} #{path}"

    if requirement == :unsupported do
      %{status: :unsupported, label: "Unsupported route", detail: evaluated}
    else
      case requirement_allowed?(user, requirement) do
        :ok ->
          allowed_result("Route allowed", "#{evaluated}: #{format_requirement(requirement)}")

        {:error, reason} ->
          denied_result(
            "Route denied",
            "#{evaluated}: #{format_requirement(requirement)}: #{reason}"
          )
      end
    end
  rescue
    _ -> denied_result("Route denied", "route lookup failed")
  catch
    :exit, _ -> denied_result("Route denied", "route lookup failed")
  end

  defp requirement_allowed?(user, {"*", _opts}), do: Acl.check_permission(user, "*")

  defp requirement_allowed?(user, requirements) when is_list(requirements) do
    Enum.reduce_while(requirements, :ok, fn requirement, :ok ->
      case requirement_allowed?(user, requirement) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp requirement_allowed?(user, {command, opts}) do
    with :ok <- Acl.check_command(user, command) do
      check_requirement_key(user, opts)
    end
  end

  defp check_requirement_key(user, opts) do
    case Keyword.get(opts, :key) do
      nil -> :ok
      {key, access} -> Acl.check_key_access(user, key, access)
    end
  end

  defp allowed_result(label, detail), do: %{status: :allowed, label: label, detail: detail}
  defp denied_result(label, detail), do: %{status: :denied, label: label, detail: detail}
  defp idle_result(label), do: %{status: :idle, label: label, detail: ""}

  defp route_requirements do
    Enum.map(@dashboard_routes, fn {section, method, path} ->
      requirement = RouteRequirements.dashboard_route_requirement(method, path)

      %{
        section: section,
        method: method,
        path: path,
        command: requirement_command(requirement),
        key: requirement_key(requirement),
        requirement: format_requirement(requirement)
      }
    end)
  end

  defp requirement_command({command, _opts}), do: command

  defp requirement_command(requirements) when is_list(requirements),
    do: requirements |> Enum.map(&requirement_command/1) |> Enum.join(", ")

  defp requirement_key({_command, opts}) do
    case Keyword.get(opts, :key) do
      {key, access} -> "#{access}:#{key}"
      nil -> ""
    end
  end

  defp requirement_key(requirements) when is_list(requirements) do
    requirements
    |> Enum.map(&requirement_key/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.join(", ")
  end

  defp format_requirement({command, opts}) do
    case requirement_key({command, opts}) do
      "" -> command
      key -> "#{command} #{key}"
    end
  end

  defp format_requirement(requirements) when is_list(requirements),
    do: requirements |> Enum.map(&format_requirement/1) |> Enum.join(" AND ")
end
