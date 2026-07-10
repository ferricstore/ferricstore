defmodule FerricstoreServer.Health.Endpoint.Auth do
  @moduledoc false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Health.Endpoint.Login
  alias FerricstoreServer.Health.Endpoint.RouteRequirements
  alias FerricstoreServer.Health.Endpoint.Session

  @spec observability_authorized?(term(), map()) :: boolean()
  def observability_authorized?(peer, headers) do
    not Acl.protected_mode?() or Session.session_user(headers) != nil or
      loopback_peer_allowed_for_observability?(peer)
  end

  @spec authorize_request(binary(), binary(), term(), map()) ::
          :ok
          | {:unauthorized, binary()}
          | {:redirect_login, binary()}
          | {:forbidden, RouteRequirements.requirement(), binary()}
  def authorize_request(method, path, peer, headers) do
    if RouteRequirements.dashboard_path?(path) and not dashboard_transport_allowed?(peer, headers) do
      {:forbidden, {"DASHBOARD", []}, dashboard_transport_error(peer, headers)}
    else
      do_authorize_request(method, path, peer, headers)
    end
  end

  defp do_authorize_request(_method, "/dashboard/login", _peer, _headers), do: :ok

  defp do_authorize_request(_method, "/dashboard/login?" <> _query, _peer, _headers),
    do: :ok

  defp do_authorize_request("POST", "/dashboard/logout", _peer, _headers), do: :ok

  defp do_authorize_request(method, path, peer, headers) do
    cond do
      RouteRequirements.dashboard_path?(path) ->
        authorize_dashboard_request(method, path, peer, headers)

      path == "/metrics" ->
        authorize_command_request(peer, headers, {"FERRICSTORE.METRICS", []}, :json)

      true ->
        :ok
    end
  end

  @doc false
  @spec dashboard_transport_allowed?(term(), map()) :: boolean()
  def dashboard_transport_allowed?(peer, headers) do
    (loopback_peer?(peer) and not Session.trusted_proxy_peer?(peer) and
       not Map.has_key?(headers, "x-forwarded-proto")) or
      (Application.get_env(:ferricstore, :dashboard_remote_access, false) == true and
         (Session.secure_request?(peer, headers) or
            Application.get_env(:ferricstore, :dashboard_allow_insecure_http, false) == true))
  end

  @spec authorize_command_request(term(), map(), RouteRequirements.requirement(), :html | :json) ::
          :ok
          | {:unauthorized, binary()}
          | {:redirect_login, binary()}
          | {:forbidden, term(), binary()}
  def authorize_command_request(peer, headers, requirement, response_kind) do
    case dashboard_identity(peer, headers) do
      {:ok, :open} -> :ok
      {:ok, {:acl, username}} -> authorize_acl_requirement(username, requirement)
      :error when response_kind == :json -> {:unauthorized, "login required"}
      :error -> {:redirect_login, Login.location("/dashboard")}
    end
  end

  @spec dashboard_collect_opts(term(), map()) :: map()
  def dashboard_collect_opts(peer, headers) do
    case dashboard_identity(peer, headers) do
      {:ok, {:acl, username}} -> %{"acl_username" => username}
      _other -> %{}
    end
  end

  @spec dashboard_flow_collect_opts(term(), map()) :: keyword()
  def dashboard_flow_collect_opts(peer, headers),
    do: dashboard_flow_collect_opts([], peer, headers)

  @spec dashboard_flow_collect_opts(keyword(), term(), map()) :: keyword()
  def dashboard_flow_collect_opts(opts, peer, headers) when is_list(opts) do
    case dashboard_collect_opts(peer, headers) do
      %{"acl_username" => username} when is_binary(username) ->
        Keyword.put(opts, :acl_username, username)

      _other ->
        opts
    end
  end

  defp authorize_dashboard_request(method, path, peer, headers) do
    requirement = RouteRequirements.dashboard_route_requirement(method, path)

    case dashboard_identity(peer, headers) do
      {:ok, :open} ->
        :ok

      {:ok, {:acl, username}} ->
        authorize_acl_requirement(username, requirement)

      :error ->
        if RouteRequirements.dashboard_api_path?(path) do
          {:unauthorized, "login required"}
        else
          {:redirect_login, Login.location(path)}
        end
    end
  end

  defp authorize_acl_requirement(username, {"*", opts}),
    do: require_enabled_acl_user({"*", opts}, username)

  defp authorize_acl_requirement(username, {command, opts}) do
    with :ok <- Acl.check_command(username, command),
         :ok <- authorize_acl_key(username, command, opts) do
      :ok
    else
      {:error, reason} -> {:forbidden, {command, opts}, reason}
    end
  end

  defp require_enabled_acl_user(requirement, username) do
    case Acl.check_permission(username, "*") do
      :ok -> :ok
      {:error, reason} -> {:forbidden, requirement, reason}
    end
  end

  defp authorize_acl_key(username, command, opts) do
    case Keyword.get(opts, :key) do
      nil ->
        :ok

      {key, access} ->
        case Acl.check_key_access(username, key, access) do
          :ok -> :ok
          {:error, reason} -> {:error, "#{reason} (#{command} key)"}
        end
    end
  end

  defp dashboard_identity(_peer, headers) do
    cond do
      not Acl.protected_mode?() ->
        {:ok, :open}

      true ->
        case Session.session_user(headers) do
          username when is_binary(username) -> {:ok, {:acl, username}}
          _ -> :error
        end
    end
  end

  defp loopback_peer_allowed_for_observability?(peer) do
    not Acl.protected_mode?() and loopback_peer?(peer)
  end

  defp loopback_peer?({127, _, _, _}), do: true
  defp loopback_peer?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp loopback_peer?({0, 0, 0, 0, 0, 65_535, 32_512, _}), do: true
  defp loopback_peer?(_peer), do: false

  defp dashboard_transport_error(peer, headers) do
    if not loopback_peer?(peer) and
         Application.get_env(:ferricstore, :dashboard_remote_access, false) != true do
      "remote dashboard access is disabled"
    else
      if Session.secure_request?(peer, headers) or
           Application.get_env(:ferricstore, :dashboard_allow_insecure_http, false) == true do
        "remote dashboard access is disabled"
      else
        "secure dashboard transport is required"
      end
    end
  end
end
