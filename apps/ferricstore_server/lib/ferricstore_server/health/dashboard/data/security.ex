defmodule FerricstoreServer.Health.Dashboard.Data.Security do
  @moduledoc false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.Formatter
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
      tester: collect_tester(params, current_user),
      route_requirements: route_requirements(),
      generated_at_ms: System.system_time(:millisecond)
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
    Acl.list_users()
    |> Enum.map(&parse_acl_rule/1)
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp parse_acl_rule(rule) when is_binary(rule) do
    case Formatter.split_user_rule(rule) do
      {:ok, {username, state, summary}} ->
        %{username: username, state: state, rule: rule, summary: summary}

      :error ->
        %{username: "unknown", state: "unknown", rule: rule, summary: rule}
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

    %{
      input: %{
        user: user,
        command: command,
        key: key,
        key_access: key_access,
        channel: channel,
        route_method: route_method,
        route_path: route_path
      },
      command: check_command(user, command),
      key: check_key(user, key, key_access),
      channel: check_channel(user, channel),
      route: check_route(user, route_method, route_path)
    }
  end

  defp normalize_key_access(:write), do: :write

  defp normalize_key_access(value) when is_binary(value) do
    if String.downcase(String.trim(value)) == "write", do: :write, else: :read
  end

  defp normalize_key_access(_value), do: :read

  defp check_command(_user, ""), do: idle_result("Command not checked")

  defp check_command(user, command) do
    case Acl.check_command(user, command) do
      :ok -> allowed_result("Command allowed", "+#{command}")
      {:error, reason} -> denied_result("Command denied", reason)
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
    requirement = RouteRequirements.dashboard_route_requirement(method, path)

    case requirement_allowed?(user, requirement) do
      :ok ->
        allowed_result("Route allowed", format_requirement(requirement))

      {:error, reason} ->
        denied_result("Route denied", "#{format_requirement(requirement)}: #{reason}")
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
