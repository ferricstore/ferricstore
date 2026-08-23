defmodule FerricstoreHttp.ConfigTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.Config

  defmodule ProviderWithBackendArity do
    def authenticate(_username, _password, _opts), do: {:ok, :session}
  end

  defmodule BackendWithProviderArity do
    def authenticate(_credentials, _opts), do: {:ok, :identity}
    def execute_batch(_session, _commands, _opts), do: {:ok, []}
    def ready?, do: true
  end

  test "builds conservative HTTP/1 defaults" do
    assert {:ok, config} = Config.new([])
    refute config.enabled
    refute config.http2_enabled
    assert config.metrics_enabled
    assert config.port == 8080
    assert config.acceptors >= 10
    assert config.max_batch_commands == 1_000
    refute config.command_batching_enabled
    assert config.command_batch_window_ms == 1
    assert config.command_batch_max_commands == 1_024
    assert config.command_batch_shards == System.schedulers_online()
    assert config.max_in_flight_requests == 1_024
    assert config.auth_cache_enabled
    assert config.auth_cache_ttl_ms == 300_000
    assert config.auth_cache_max_entries == 10_000
    refute config.trust_context_headers
    refute config.invocations_enabled
    refute config.runner_enabled
    assert config.invocation_definitions_file == nil
    assert config.target_allowed_hosts == ["localhost", "127.0.0.1", "::1"]
    assert config.target_deny_private_networks
    refute config.target_require_https
    assert config.tls == %{enabled: false}
  end

  test "rejects invalid limits, callback modules, and incomplete TLS" do
    for key <- [
          :request_timeout_ms,
          :acceptors,
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
        ] do
      assert {:error, {:invalid_config, ^key}} = Config.new([{key, 0}])
    end

    assert {:error, {:invalid_config, :enabled}} = Config.new(enabled: :yes)
    assert {:error, {:invalid_config, :http2_enabled}} = Config.new(http2_enabled: :yes)
    assert {:error, {:invalid_config, :metrics_enabled}} = Config.new(metrics_enabled: :yes)

    assert {:error, {:invalid_config, :command_batching_enabled}} =
             Config.new(command_batching_enabled: :yes)

    assert {:error, {:invalid_config, :command_batch_window_ms}} =
             Config.new(command_batch_window_ms: -1)

    assert {:error, {:invalid_config, :auth_cache_enabled}} =
             Config.new(auth_cache_enabled: :yes)

    assert {:error, {:invalid_config, :trust_context_headers}} =
             Config.new(trust_context_headers: :yes)

    assert {:error, {:invalid_config, :invocations_enabled}} =
             Config.new(invocations_enabled: :yes)

    assert {:error, {:invalid_config, :runner_enabled}} = Config.new(runner_enabled: true)

    assert {:error, {:invalid_config, :target_allowed_hosts}} =
             Config.new(target_allowed_hosts: [""])

    assert {:error, {:invalid_config, :port}} = Config.new(port: 65_536)
    assert {:error, {:invalid_config, :ip}} = Config.new(ip: "127.0.0.1")
    assert {:error, {:invalid_config, :ip}} = Config.new(ip: {999, 0, 0, 1})
    assert {:error, {:invalid_config, :ip}} = Config.new(ip: {0, 0, 0, 0, 0, 0, 0, 65_536})
    assert {:error, {:invalid_config, :auth_options}} = Config.new(auth_options: %{})
    assert {:error, {:invalid_config, :backend}} = Config.new(backend: String)
    assert {:error, {:invalid_config, :auth_provider}} = Config.new(auth_provider: String)

    assert {:error, {:invalid_config, :auth_provider}} =
             Config.new(auth_provider: ProviderWithBackendArity)

    assert {:error, {:invalid_config, :backend}} =
             Config.new(backend: BackendWithProviderArity)

    assert {:error, {:invalid_config, :tls}} = Config.new(tls: [enabled: true])
    assert {:error, {:invalid_config, :tls}} = Config.new(tls: [enabled: :yes])
    assert {:error, {:invalid_config, :options}} = Config.new(%{})
  end

  test "loads supported runtime environment overrides" do
    previous =
      for variable <- [
            "FERRICSTORE_HTTP_ENABLED",
            "FERRICSTORE_HTTP_BIND",
            "FERRICSTORE_HTTP_PORT",
            "FERRICSTORE_HTTP_ACCEPTORS",
            "FERRICSTORE_HTTP2_ENABLED",
            "FERRICSTORE_HTTP_METRICS_ENABLED",
            "FERRICSTORE_HTTP_REQUEST_TIMEOUT_MS",
            "FERRICSTORE_HTTP_MAX_BODY_BYTES",
            "FERRICSTORE_HTTP_MAX_BATCH_COMMANDS",
            "FERRICSTORE_HTTP_MAX_CONNECTIONS",
            "FERRICSTORE_HTTP_MAX_IN_FLIGHT_REQUESTS",
            "FERRICSTORE_HTTP_IDLE_TIMEOUT_MS",
            "FERRICSTORE_HTTP_MAX_KEEPALIVE_REQUESTS",
            "FERRICSTORE_HTTP_HEADER_TIMEOUT_MS",
            "FERRICSTORE_HTTP_MAX_HEADERS",
            "FERRICSTORE_HTTP_MAX_HEADER_NAME_BYTES",
            "FERRICSTORE_HTTP_MAX_HEADER_VALUE_BYTES",
            "FERRICSTORE_HTTP_MAX_REQUEST_LINE_BYTES",
            "FERRICSTORE_HTTP_COMMAND_BATCHING_ENABLED",
            "FERRICSTORE_HTTP_COMMAND_BATCH_WINDOW_MS",
            "FERRICSTORE_HTTP_COMMAND_BATCH_MAX_COMMANDS",
            "FERRICSTORE_HTTP_COMMAND_BATCH_SHARDS",
            "FERRICSTORE_HTTP_AUTH_CACHE_ENABLED",
            "FERRICSTORE_HTTP_AUTH_CACHE_TTL_MS",
            "FERRICSTORE_HTTP_AUTH_CACHE_MAX_ENTRIES",
            "FERRICSTORE_HTTP_TRUST_CONTEXT_HEADERS",
            "FERRICSTORE_HTTP_INVOCATIONS_ENABLED",
            "FERRICSTORE_HTTP_RUNNER_ENABLED",
            "FERRICSTORE_HTTP_TARGET_ALLOWED_HOSTS",
            "FERRICSTORE_HTTP_TARGET_REQUIRE_HTTPS"
          ],
          into: %{} do
        {variable, System.get_env(variable)}
      end

    on_exit(fn ->
      Enum.each(previous, fn {variable, value} -> restore_env(variable, value) end)
    end)

    System.put_env("FERRICSTORE_HTTP_ENABLED", "true")
    System.put_env("FERRICSTORE_HTTP_BIND", "0.0.0.0")
    System.put_env("FERRICSTORE_HTTP_PORT", "9090")
    System.put_env("FERRICSTORE_HTTP_ACCEPTORS", "24")
    System.put_env("FERRICSTORE_HTTP2_ENABLED", "true")
    System.put_env("FERRICSTORE_HTTP_METRICS_ENABLED", "false")
    System.put_env("FERRICSTORE_HTTP_REQUEST_TIMEOUT_MS", "45000")
    System.put_env("FERRICSTORE_HTTP_MAX_BODY_BYTES", "2097152")
    System.put_env("FERRICSTORE_HTTP_MAX_BATCH_COMMANDS", "500")
    System.put_env("FERRICSTORE_HTTP_MAX_CONNECTIONS", "2048")
    System.put_env("FERRICSTORE_HTTP_MAX_IN_FLIGHT_REQUESTS", "1536")
    System.put_env("FERRICSTORE_HTTP_IDLE_TIMEOUT_MS", "70000")
    System.put_env("FERRICSTORE_HTTP_MAX_KEEPALIVE_REQUESTS", "750")
    System.put_env("FERRICSTORE_HTTP_HEADER_TIMEOUT_MS", "6000")
    System.put_env("FERRICSTORE_HTTP_MAX_HEADERS", "80")
    System.put_env("FERRICSTORE_HTTP_MAX_HEADER_NAME_BYTES", "96")
    System.put_env("FERRICSTORE_HTTP_MAX_HEADER_VALUE_BYTES", "4096")
    System.put_env("FERRICSTORE_HTTP_MAX_REQUEST_LINE_BYTES", "4096")
    System.put_env("FERRICSTORE_HTTP_COMMAND_BATCHING_ENABLED", "false")
    System.put_env("FERRICSTORE_HTTP_COMMAND_BATCH_WINDOW_MS", "3")
    System.put_env("FERRICSTORE_HTTP_COMMAND_BATCH_MAX_COMMANDS", "99")
    System.put_env("FERRICSTORE_HTTP_COMMAND_BATCH_SHARDS", "7")
    System.put_env("FERRICSTORE_HTTP_AUTH_CACHE_ENABLED", "false")
    System.put_env("FERRICSTORE_HTTP_AUTH_CACHE_TTL_MS", "1234")
    System.put_env("FERRICSTORE_HTTP_AUTH_CACHE_MAX_ENTRIES", "4321")
    System.put_env("FERRICSTORE_HTTP_TRUST_CONTEXT_HEADERS", "true")
    System.put_env("FERRICSTORE_HTTP_INVOCATIONS_ENABLED", "true")
    System.put_env("FERRICSTORE_HTTP_RUNNER_ENABLED", "true")
    System.put_env("FERRICSTORE_HTTP_TARGET_ALLOWED_HOSTS", "api.example.com,*.example.net")
    System.put_env("FERRICSTORE_HTTP_TARGET_REQUIRE_HTTPS", "true")

    assert {:ok, config} = Config.load()
    assert config.enabled
    assert config.ip == {0, 0, 0, 0}
    assert config.port == 9090
    assert config.acceptors == 24
    assert config.http2_enabled
    refute config.metrics_enabled
    assert config.request_timeout_ms == 45_000
    assert config.max_body_bytes == 2_097_152
    assert config.max_batch_commands == 500
    assert config.max_connections == 2_048
    assert config.max_in_flight_requests == 1_536
    assert config.idle_timeout_ms == 70_000
    assert config.max_keepalive_requests == 750
    assert config.request_header_timeout_ms == 6_000
    assert config.max_headers == 80
    assert config.max_header_name_bytes == 96
    assert config.max_header_value_bytes == 4_096
    assert config.max_request_line_bytes == 4_096
    refute config.command_batching_enabled
    assert config.command_batch_window_ms == 3
    assert config.command_batch_max_commands == 99
    assert config.command_batch_shards == 7
    refute config.auth_cache_enabled
    assert config.auth_cache_ttl_ms == 1_234
    assert config.auth_cache_max_entries == 4_321
    assert config.trust_context_headers
    assert config.invocations_enabled
    assert config.runner_enabled
    assert config.target_allowed_hosts == ["api.example.com", "*.example.net"]
    assert config.target_require_https
  end

  test "rejects an invalid runtime bind address" do
    previous = System.get_env("FERRICSTORE_HTTP_BIND")
    on_exit(fn -> restore_env("FERRICSTORE_HTTP_BIND", previous) end)
    System.put_env("FERRICSTORE_HTTP_BIND", "not-an-ip")

    assert {:error, {:invalid_config, :ip}} = Config.load()
  end

  defp restore_env(variable, nil), do: System.delete_env(variable)
  defp restore_env(variable, value), do: System.put_env(variable, value)
end
