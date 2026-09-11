defmodule FerricstoreHttp.Config do
  @moduledoc """
  Validated runtime configuration for one FerricStore HTTP listener.
  """

  @default_backend FerricstoreHttp.Backends.Ferricstore
  @default_auth_provider FerricstoreHttp.Auth.Providers.Basic
  @positive_integer_options [
    :acceptors,
    :request_timeout_ms,
    :max_body_bytes,
    :max_batch_commands,
    :command_batch_max_commands,
    :command_batch_shards,
    :max_connections,
    :max_in_flight_requests,
    :idle_timeout_ms,
    :max_keepalive_requests,
    :request_header_timeout_ms,
    :max_headers,
    :max_header_name_bytes,
    :max_header_value_bytes,
    :max_request_line_bytes,
    :auth_cache_ttl_ms,
    :auth_cache_max_entries,
    :runner_poll_interval_ms,
    :runner_default_lease_ms,
    :runner_default_claim_limit,
    :runner_default_retry_delay_ms,
    :runner_target_timeout_ms,
    :target_max_response_bytes
  ]

  @enforce_keys [
    :enabled,
    :ip,
    :port,
    :acceptors,
    :http2_enabled,
    :metrics_enabled,
    :command_batching_enabled,
    :command_batch_window_ms,
    :command_batch_max_commands,
    :command_batch_shards,
    :request_timeout_ms,
    :max_body_bytes,
    :max_batch_commands,
    :max_connections,
    :max_in_flight_requests,
    :idle_timeout_ms,
    :max_keepalive_requests,
    :request_header_timeout_ms,
    :max_headers,
    :max_header_name_bytes,
    :max_header_value_bytes,
    :max_request_line_bytes,
    :backend,
    :auth_provider,
    :auth_options,
    :trust_context_headers,
    :auth_cache_enabled,
    :auth_cache_ttl_ms,
    :auth_cache_max_entries,
    :invocations_enabled,
    :invocation_definitions_file,
    :runner_enabled,
    :runner_id,
    :runner_poll_interval_ms,
    :runner_default_lease_ms,
    :runner_default_claim_limit,
    :runner_default_retry_delay_ms,
    :runner_target_timeout_ms,
    :runner_username_env,
    :runner_password_env,
    :target_allowed_hosts,
    :target_require_https,
    :target_deny_private_networks,
    :target_private_network_allowlist,
    :target_max_response_bytes,
    :tls
  ]
  defstruct @enforce_keys

  @type tls :: %{
          required(:enabled) => boolean(),
          optional(:certfile) => binary(),
          optional(:keyfile) => binary()
        }

  @type t :: %__MODULE__{
          enabled: boolean(),
          ip: :inet.ip_address(),
          port: non_neg_integer(),
          acceptors: pos_integer(),
          http2_enabled: boolean(),
          metrics_enabled: boolean(),
          command_batching_enabled: boolean(),
          command_batch_window_ms: non_neg_integer(),
          command_batch_max_commands: pos_integer(),
          command_batch_shards: pos_integer(),
          request_timeout_ms: pos_integer(),
          max_body_bytes: pos_integer(),
          max_batch_commands: pos_integer(),
          max_connections: pos_integer(),
          max_in_flight_requests: pos_integer(),
          idle_timeout_ms: pos_integer(),
          max_keepalive_requests: pos_integer(),
          request_header_timeout_ms: pos_integer(),
          max_headers: pos_integer(),
          max_header_name_bytes: pos_integer(),
          max_header_value_bytes: pos_integer(),
          max_request_line_bytes: pos_integer(),
          backend: module(),
          auth_provider: module(),
          auth_options: keyword(),
          trust_context_headers: boolean(),
          auth_cache_enabled: boolean(),
          auth_cache_ttl_ms: pos_integer(),
          auth_cache_max_entries: pos_integer(),
          invocations_enabled: boolean(),
          invocation_definitions_file: binary() | nil,
          runner_enabled: boolean(),
          runner_id: binary(),
          runner_poll_interval_ms: pos_integer(),
          runner_default_lease_ms: pos_integer(),
          runner_default_claim_limit: pos_integer(),
          runner_default_retry_delay_ms: pos_integer(),
          runner_target_timeout_ms: pos_integer(),
          runner_username_env: binary(),
          runner_password_env: binary(),
          target_allowed_hosts: [binary()],
          target_require_https: boolean(),
          target_deny_private_networks: boolean(),
          target_private_network_allowlist: [binary()],
          target_max_response_bytes: pos_integer(),
          tls: tls()
        }

  @spec load() :: {:ok, t()} | {:error, term()}
  def load do
    :ferricstore_http
    |> Application.get_all_env()
    |> apply_environment()
    |> new()
  end

  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts) when is_list(opts) do
    defaults = defaults()
    opts = Keyword.merge(defaults, opts)

    with :ok <- boolean_option(opts, :enabled),
         :ok <- boolean_option(opts, :http2_enabled),
         :ok <- boolean_option(opts, :metrics_enabled),
         :ok <- boolean_option(opts, :command_batching_enabled),
         :ok <- boolean_option(opts, :trust_context_headers),
         :ok <- boolean_option(opts, :auth_cache_enabled),
         :ok <- boolean_option(opts, :invocations_enabled),
         :ok <- boolean_option(opts, :runner_enabled),
         :ok <- boolean_option(opts, :target_require_https),
         :ok <- boolean_option(opts, :target_deny_private_networks),
         :ok <- non_negative_integer_option(opts, :command_batch_window_ms),
         :ok <- port_option(opts),
         :ok <- ip_option(opts),
         :ok <- positive_integer_options(opts),
         :ok <- keyword_option(opts, :auth_options),
         :ok <- optional_binary_option(opts, :invocation_definitions_file),
         :ok <- nonempty_binary_option(opts, :runner_id),
         :ok <- nonempty_binary_option(opts, :runner_username_env),
         :ok <- nonempty_binary_option(opts, :runner_password_env),
         :ok <- binary_list_option(opts, :target_allowed_hosts),
         :ok <- binary_list_option(opts, :target_private_network_allowlist),
         :ok <- runner_dependency(opts),
         :ok <- callback_module(opts, :backend, authenticate: 3, execute_batch: 3, ready?: 0),
         :ok <- callback_module(opts, :auth_provider, authenticate: 2),
         {:ok, tls} <- tls_options(Keyword.get(opts, :tls, [])) do
      {:ok,
       struct!(__MODULE__,
         enabled: Keyword.fetch!(opts, :enabled),
         ip: Keyword.fetch!(opts, :ip),
         port: Keyword.fetch!(opts, :port),
         acceptors: Keyword.fetch!(opts, :acceptors),
         http2_enabled: Keyword.fetch!(opts, :http2_enabled),
         metrics_enabled: Keyword.fetch!(opts, :metrics_enabled),
         command_batching_enabled: Keyword.fetch!(opts, :command_batching_enabled),
         command_batch_window_ms: Keyword.fetch!(opts, :command_batch_window_ms),
         command_batch_max_commands: Keyword.fetch!(opts, :command_batch_max_commands),
         command_batch_shards: Keyword.fetch!(opts, :command_batch_shards),
         request_timeout_ms: Keyword.fetch!(opts, :request_timeout_ms),
         max_body_bytes: Keyword.fetch!(opts, :max_body_bytes),
         max_batch_commands: Keyword.fetch!(opts, :max_batch_commands),
         max_connections: Keyword.fetch!(opts, :max_connections),
         max_in_flight_requests: Keyword.fetch!(opts, :max_in_flight_requests),
         idle_timeout_ms: Keyword.fetch!(opts, :idle_timeout_ms),
         max_keepalive_requests: Keyword.fetch!(opts, :max_keepalive_requests),
         request_header_timeout_ms: Keyword.fetch!(opts, :request_header_timeout_ms),
         max_headers: Keyword.fetch!(opts, :max_headers),
         max_header_name_bytes: Keyword.fetch!(opts, :max_header_name_bytes),
         max_header_value_bytes: Keyword.fetch!(opts, :max_header_value_bytes),
         max_request_line_bytes: Keyword.fetch!(opts, :max_request_line_bytes),
         backend: Keyword.fetch!(opts, :backend),
         auth_provider: Keyword.fetch!(opts, :auth_provider),
         auth_options: Keyword.fetch!(opts, :auth_options),
         trust_context_headers: Keyword.fetch!(opts, :trust_context_headers),
         auth_cache_enabled: Keyword.fetch!(opts, :auth_cache_enabled),
         auth_cache_ttl_ms: Keyword.fetch!(opts, :auth_cache_ttl_ms),
         auth_cache_max_entries: Keyword.fetch!(opts, :auth_cache_max_entries),
         invocations_enabled: Keyword.fetch!(opts, :invocations_enabled),
         invocation_definitions_file: Keyword.fetch!(opts, :invocation_definitions_file),
         runner_enabled: Keyword.fetch!(opts, :runner_enabled),
         runner_id: Keyword.fetch!(opts, :runner_id),
         runner_poll_interval_ms: Keyword.fetch!(opts, :runner_poll_interval_ms),
         runner_default_lease_ms: Keyword.fetch!(opts, :runner_default_lease_ms),
         runner_default_claim_limit: Keyword.fetch!(opts, :runner_default_claim_limit),
         runner_default_retry_delay_ms: Keyword.fetch!(opts, :runner_default_retry_delay_ms),
         runner_target_timeout_ms: Keyword.fetch!(opts, :runner_target_timeout_ms),
         runner_username_env: Keyword.fetch!(opts, :runner_username_env),
         runner_password_env: Keyword.fetch!(opts, :runner_password_env),
         target_allowed_hosts: Keyword.fetch!(opts, :target_allowed_hosts),
         target_require_https: Keyword.fetch!(opts, :target_require_https),
         target_deny_private_networks: Keyword.fetch!(opts, :target_deny_private_networks),
         target_private_network_allowlist:
           Keyword.fetch!(opts, :target_private_network_allowlist),
         target_max_response_bytes: Keyword.fetch!(opts, :target_max_response_bytes),
         tls: tls
       )}
    end
  end

  def new(_invalid), do: {:error, {:invalid_config, :options}}

  defp defaults do
    [
      enabled: false,
      ip: {127, 0, 0, 1},
      port: 8080,
      acceptors: max(10, System.schedulers_online()),
      http2_enabled: false,
      metrics_enabled: true,
      command_batching_enabled: false,
      command_batch_window_ms: 1,
      command_batch_max_commands: 1_024,
      command_batch_shards: System.schedulers_online(),
      request_timeout_ms: 30_000,
      max_body_bytes: 1_048_576,
      max_batch_commands: 1_000,
      max_connections: 1_024,
      max_in_flight_requests: 1_024,
      idle_timeout_ms: 60_000,
      max_keepalive_requests: 1_000,
      request_header_timeout_ms: 5_000,
      max_headers: 100,
      max_header_name_bytes: 64,
      max_header_value_bytes: 8_192,
      max_request_line_bytes: 8_192,
      backend: @default_backend,
      auth_provider: @default_auth_provider,
      auth_options: [],
      trust_context_headers: false,
      auth_cache_enabled: true,
      auth_cache_ttl_ms: 300_000,
      auth_cache_max_entries: 10_000,
      invocations_enabled: false,
      invocation_definitions_file: nil,
      runner_enabled: false,
      runner_id: "ferricstore-http-#{node()}",
      runner_poll_interval_ms: 1_000,
      runner_default_lease_ms: 30_000,
      runner_default_claim_limit: 10,
      runner_default_retry_delay_ms: 1_000,
      runner_target_timeout_ms: 30_000,
      runner_username_env: "FERRICSTORE_HTTP_RUNNER_USERNAME",
      runner_password_env: "FERRICSTORE_HTTP_RUNNER_PASSWORD",
      target_allowed_hosts: ["localhost", "127.0.0.1", "::1"],
      target_require_https: false,
      target_deny_private_networks: true,
      target_private_network_allowlist: ["localhost", "127.0.0.1", "::1"],
      target_max_response_bytes: 1_048_576,
      tls: [enabled: false]
    ]
  end

  defp boolean_option(opts, key) do
    if is_boolean(Keyword.get(opts, key)), do: :ok, else: invalid(key)
  end

  defp non_negative_integer_option(opts, key) do
    value = Keyword.get(opts, key)
    if is_integer(value) and value >= 0, do: :ok, else: invalid(key)
  end

  defp port_option(opts) do
    case Keyword.get(opts, :port) do
      port when is_integer(port) and port >= 0 and port <= 65_535 -> :ok
      _invalid -> invalid(:port)
    end
  end

  defp ip_option(opts) do
    case Keyword.get(opts, :ip) do
      ip when is_tuple(ip) -> if :inet.is_ip_address(ip), do: :ok, else: invalid(:ip)
      _invalid -> invalid(:ip)
    end
  end

  defp positive_integer_options(opts) do
    case Enum.find(@positive_integer_options, fn key ->
           value = Keyword.get(opts, key)
           not (is_integer(value) and value > 0)
         end) do
      nil -> :ok
      invalid_key -> invalid(invalid_key)
    end
  end

  defp keyword_option(opts, key) do
    if Keyword.keyword?(Keyword.get(opts, key)), do: :ok, else: invalid(key)
  end

  defp optional_binary_option(opts, key) do
    case Keyword.get(opts, key) do
      nil -> :ok
      value when is_binary(value) and value != "" -> :ok
      _invalid -> invalid(key)
    end
  end

  defp nonempty_binary_option(opts, key) do
    value = Keyword.get(opts, key)
    if is_binary(value) and value != "", do: :ok, else: invalid(key)
  end

  defp binary_list_option(opts, key) do
    case Keyword.get(opts, key) do
      values when is_list(values) ->
        if Enum.all?(values, &(is_binary(&1) and &1 != "")), do: :ok, else: invalid(key)

      _invalid ->
        invalid(key)
    end
  end

  defp runner_dependency(opts) do
    if Keyword.get(opts, :runner_enabled) and not Keyword.get(opts, :invocations_enabled),
      do: invalid(:runner_enabled),
      else: :ok
  end

  defp callback_module(opts, key, callbacks) do
    module = Keyword.get(opts, key)

    if is_atom(module) and Code.ensure_loaded?(module) and
         Enum.all?(callbacks, fn {callback, arity} ->
           function_exported?(module, callback, arity)
         end) do
      :ok
    else
      invalid(key)
    end
  end

  defp tls_options(opts) when is_list(opts) do
    case Keyword.get(opts, :enabled, false) do
      false ->
        {:ok, %{enabled: false}}

      true ->
        certfile = Keyword.get(opts, :certfile)
        keyfile = Keyword.get(opts, :keyfile)

        if non_empty_binary?(certfile) and non_empty_binary?(keyfile) do
          {:ok, %{enabled: true, certfile: certfile, keyfile: keyfile}}
        else
          invalid(:tls)
        end

      _invalid ->
        invalid(:tls)
    end
  end

  defp tls_options(_invalid), do: invalid(:tls)

  defp non_empty_binary?(value), do: is_binary(value) and value != ""
  defp invalid(key), do: {:error, {:invalid_config, key}}

  defp apply_environment(opts) do
    opts
    |> put_env_boolean(:enabled, "FERRICSTORE_HTTP_ENABLED")
    |> put_env_ip(:ip, "FERRICSTORE_HTTP_BIND")
    |> put_env_integer(:port, "FERRICSTORE_HTTP_PORT")
    |> put_env_integer(:acceptors, "FERRICSTORE_HTTP_ACCEPTORS")
    |> put_env_boolean(:http2_enabled, "FERRICSTORE_HTTP2_ENABLED")
    |> put_env_boolean(:metrics_enabled, "FERRICSTORE_HTTP_METRICS_ENABLED")
    |> put_env_integer(:request_timeout_ms, "FERRICSTORE_HTTP_REQUEST_TIMEOUT_MS")
    |> put_env_integer(:max_body_bytes, "FERRICSTORE_HTTP_MAX_BODY_BYTES")
    |> put_env_integer(:max_batch_commands, "FERRICSTORE_HTTP_MAX_BATCH_COMMANDS")
    |> put_env_integer(:max_connections, "FERRICSTORE_HTTP_MAX_CONNECTIONS")
    |> put_env_integer(:max_in_flight_requests, "FERRICSTORE_HTTP_MAX_IN_FLIGHT_REQUESTS")
    |> put_env_integer(:idle_timeout_ms, "FERRICSTORE_HTTP_IDLE_TIMEOUT_MS")
    |> put_env_integer(:max_keepalive_requests, "FERRICSTORE_HTTP_MAX_KEEPALIVE_REQUESTS")
    |> put_env_integer(:request_header_timeout_ms, "FERRICSTORE_HTTP_HEADER_TIMEOUT_MS")
    |> put_env_integer(:max_headers, "FERRICSTORE_HTTP_MAX_HEADERS")
    |> put_env_integer(:max_header_name_bytes, "FERRICSTORE_HTTP_MAX_HEADER_NAME_BYTES")
    |> put_env_integer(:max_header_value_bytes, "FERRICSTORE_HTTP_MAX_HEADER_VALUE_BYTES")
    |> put_env_integer(:max_request_line_bytes, "FERRICSTORE_HTTP_MAX_REQUEST_LINE_BYTES")
    |> put_env_boolean(:command_batching_enabled, "FERRICSTORE_HTTP_COMMAND_BATCHING_ENABLED")
    |> put_env_integer(:command_batch_window_ms, "FERRICSTORE_HTTP_COMMAND_BATCH_WINDOW_MS")
    |> put_env_integer(:command_batch_max_commands, "FERRICSTORE_HTTP_COMMAND_BATCH_MAX_COMMANDS")
    |> put_env_integer(:command_batch_shards, "FERRICSTORE_HTTP_COMMAND_BATCH_SHARDS")
    |> put_env_boolean(:auth_cache_enabled, "FERRICSTORE_HTTP_AUTH_CACHE_ENABLED")
    |> put_env_boolean(:trust_context_headers, "FERRICSTORE_HTTP_TRUST_CONTEXT_HEADERS")
    |> put_env_integer(:auth_cache_ttl_ms, "FERRICSTORE_HTTP_AUTH_CACHE_TTL_MS")
    |> put_env_integer(:auth_cache_max_entries, "FERRICSTORE_HTTP_AUTH_CACHE_MAX_ENTRIES")
    |> put_env_boolean(:invocations_enabled, "FERRICSTORE_HTTP_INVOCATIONS_ENABLED")
    |> put_env(:invocation_definitions_file, "FERRICSTORE_HTTP_INVOCATION_DEFINITIONS_FILE")
    |> put_env_boolean(:runner_enabled, "FERRICSTORE_HTTP_RUNNER_ENABLED")
    |> put_env(:runner_id, "FERRICSTORE_HTTP_RUNNER_ID")
    |> put_env_integer(:runner_poll_interval_ms, "FERRICSTORE_HTTP_RUNNER_POLL_INTERVAL_MS")
    |> put_env_integer(:runner_default_lease_ms, "FERRICSTORE_HTTP_RUNNER_DEFAULT_LEASE_MS")
    |> put_env_integer(:runner_default_claim_limit, "FERRICSTORE_HTTP_RUNNER_DEFAULT_CLAIM_LIMIT")
    |> put_env_integer(
      :runner_default_retry_delay_ms,
      "FERRICSTORE_HTTP_RUNNER_DEFAULT_RETRY_DELAY_MS"
    )
    |> put_env_integer(:runner_target_timeout_ms, "FERRICSTORE_HTTP_RUNNER_TARGET_TIMEOUT_MS")
    |> put_env(:runner_username_env, "FERRICSTORE_HTTP_RUNNER_USERNAME_ENV")
    |> put_env(:runner_password_env, "FERRICSTORE_HTTP_RUNNER_PASSWORD_ENV")
    |> put_env_csv(:target_allowed_hosts, "FERRICSTORE_HTTP_TARGET_ALLOWED_HOSTS")
    |> put_env_boolean(:target_require_https, "FERRICSTORE_HTTP_TARGET_REQUIRE_HTTPS")
    |> put_env_boolean(
      :target_deny_private_networks,
      "FERRICSTORE_HTTP_TARGET_DENY_PRIVATE_NETWORKS"
    )
    |> put_env_csv(
      :target_private_network_allowlist,
      "FERRICSTORE_HTTP_TARGET_PRIVATE_NETWORK_ALLOWLIST"
    )
    |> put_env_integer(:target_max_response_bytes, "FERRICSTORE_HTTP_TARGET_MAX_RESPONSE_BYTES")
    |> put_tls_environment()
  end

  defp put_env(opts, key, variable) do
    case System.get_env(variable) do
      nil -> opts
      "" -> Keyword.put(opts, key, nil)
      value -> Keyword.put(opts, key, value)
    end
  end

  defp put_env_csv(opts, key, variable) do
    case System.get_env(variable) do
      nil ->
        opts

      value ->
        Keyword.put(opts, key, value |> String.split(",", trim: true) |> Enum.map(&String.trim/1))
    end
  end

  defp put_env_boolean(opts, key, variable) do
    case System.get_env(variable) do
      nil -> opts
      "true" -> Keyword.put(opts, key, true)
      "false" -> Keyword.put(opts, key, false)
      _invalid -> Keyword.put(opts, key, :invalid_environment_value)
    end
  end

  defp put_env_integer(opts, key, variable) do
    case System.get_env(variable) do
      nil -> opts
      value -> Keyword.put(opts, key, parse_integer(value))
    end
  end

  defp put_env_ip(opts, key, variable) do
    case System.get_env(variable) do
      nil ->
        opts

      value ->
        parsed = value |> String.to_charlist() |> :inet.parse_address()

        case parsed do
          {:ok, address} -> Keyword.put(opts, key, address)
          {:error, _reason} -> Keyword.put(opts, key, :invalid_environment_value)
        end
    end
  end

  defp parse_integer(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _invalid -> :invalid_environment_value
    end
  end

  defp put_tls_environment(opts) do
    tls = Keyword.get(opts, :tls, [])

    tls =
      tls
      |> put_nested_env_boolean(:enabled, "FERRICSTORE_HTTP_TLS_ENABLED")
      |> put_nested_env(:certfile, "FERRICSTORE_HTTP_TLS_CERT_FILE")
      |> put_nested_env(:keyfile, "FERRICSTORE_HTTP_TLS_KEY_FILE")

    Keyword.put(opts, :tls, tls)
  end

  defp put_nested_env_boolean(opts, key, variable) do
    case System.get_env(variable) do
      nil -> opts
      "true" -> Keyword.put(opts, key, true)
      "false" -> Keyword.put(opts, key, false)
      _invalid -> Keyword.put(opts, key, :invalid_environment_value)
    end
  end

  defp put_nested_env(opts, key, variable) do
    case System.get_env(variable) do
      nil -> opts
      value -> Keyword.put(opts, key, value)
    end
  end
end
