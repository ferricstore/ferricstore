defmodule FerricstoreHttp.PythonSdkLambdaBenchmark.MemoryBackend do
  @moduledoc false

  @behaviour FerricstoreHttp.Backend
  @table __MODULE__

  def reset! do
    if :ets.whereis(@table) != :undefined, do: :ets.delete(@table)

    _table =
      :ets.new(@table, [
        :named_table,
        :public,
        :set,
        {:read_concurrency, true},
        {:write_concurrency, :auto}
      ])

    :ok
  end

  @impl FerricstoreHttp.Backend
  def authenticate("lambda-group-" <> group, password, _opts) do
    if password == "lambda-group-#{group}-password" do
      {:ok, {:memory_session, group}}
    else
      {:error, :unauthenticated}
    end
  end

  def authenticate(_username, _password, _opts), do: {:error, :unauthenticated}

  @impl FerricstoreHttp.Backend
  def execute_batch({:memory_session, group}, commands, _opts) do
    {:ok,
     Enum.map(commands, fn
       ["PING"] ->
         %{status: :ok, value: "PONG"}

       ["SET", key, value] when is_binary(key) ->
         true = :ets.insert(@table, {{group, key}, value})
         %{status: :ok, value: "OK"}

       ["GET", key] when is_binary(key) ->
         value =
           case :ets.lookup(@table, {group, key}) do
             [{{^group, ^key}, value}] -> value
             [] -> nil
           end

         %{status: :ok, value: value}

       _unsupported ->
         %{status: :error, value: "ERR unsupported command"}
     end)}
  end

  @impl FerricstoreHttp.Backend
  def ready?, do: true
end

defmodule FerricstoreHttp.PythonSdkLambdaBenchmark do
  @moduledoc false

  alias FerricstoreServer.Acl.CatalogProjector

  def run do
    runtime = start_isolated_runtime()

    try do
      run_benchmark()
    after
      Enum.each(Enum.reverse(runtime.started_applications), &Application.stop/1)
      File.rm_rf!(runtime.data_directory)
    end
  end

  defp run_benchmark do
    python_sdk_path = System.fetch_env!("FERRICSTORE_PYTHON_SDK_PATH")

    python =
      System.get_env("FERRICSTORE_PYTHON_EXECUTABLE") || System.find_executable("python3") ||
        raise "python3 is required"

    groups = positive_env("FERRICSTORE_LAMBDA_GROUPS", 8)
    environments_per_group = positive_env("FERRICSTORE_LAMBDA_ENVS_PER_GROUP", 8)
    concurrency = groups * environments_per_group
    tls_enabled = boolean_env("FERRICSTORE_HTTP_BENCH_TLS_ENABLED", true)
    backend = benchmark_backend()
    :ok = CatalogProjector.mark_ready()

    if backend == FerricstoreHttp.Backends.Ferricstore,
      do: configure_groups(groups),
      else: FerricstoreHttp.PythonSdkLambdaBenchmark.MemoryBackend.reset!()

    tls_files = tls_files()

    try do
      {:ok, config} =
        FerricstoreHttp.Config.new(
          enabled: true,
          port: 0,
          backend: backend,
          http2_enabled: true,
          max_connections: concurrency + 64,
          max_in_flight_requests: concurrency + 64,
          metrics_enabled: boolean_env("FERRICSTORE_HTTP_BENCH_METRICS_ENABLED", true),
          command_batching_enabled:
            boolean_env("FERRICSTORE_HTTP_BENCH_COMMAND_BATCHING_ENABLED", false),
          command_batch_window_ms:
            non_negative_env("FERRICSTORE_HTTP_BENCH_COMMAND_BATCH_WINDOW_MS", 1),
          tls: tls_options(tls_enabled, tls_files)
        )

      {:ok, server} = FerricstoreHttp.Server.start_link(config)

      try do
        run_python(python, python_sdk_path, tls_enabled, tls_files.cafile)
        IO.puts(Jason.encode!(%{command_batch: FerricstoreHttp.Metrics.command_batch_stats()}))
      after
        :ok = Supervisor.stop(server)
      end
    after
      File.rm_rf!(tls_files.directory)
    end
  end

  defp configure_groups(groups) do
    Enum.each(0..(groups - 1), fn group ->
      username = "lambda-group-#{group}"
      password = "lambda-group-#{group}-password"

      :ok =
        FerricstoreServer.Acl.set_user(username, [
          "on",
          "resetpass",
          ">#{password}",
          "resetkeys",
          "+PING",
          "+GET",
          "+SET",
          "+PIPELINE",
          "+FLOW.CREATE",
          "+FLOW.GET",
          "+FLOW.START_AND_CLAIM",
          "~lambda:#{group}:*"
        ])
    end)
  end

  defp run_python(python, python_sdk_path, tls_enabled, cafile) do
    script = Path.expand("../../scripts/http/python_sdk_lambda_benchmark.py", __DIR__)
    scheme = if tls_enabled, do: "https", else: "http"

    env = [
      {"PYTHONPATH", Path.join(python_sdk_path, "src")},
      {"FERRICSTORE_HTTP_BENCH_URL", "#{scheme}://127.0.0.1:#{FerricstoreHttp.Listener.port()}"},
      {"FERRICSTORE_HTTP_BENCH_CA_FILE", cafile},
      {"FERRICSTORE_HTTP_BENCH_NATIVE_URL",
       "ferric://127.0.0.1:#{FerricstoreServer.Native.Listener.port()}"}
    ]

    {output, status} =
      profile_benchmark(fn ->
        System.cmd(python, [script],
          cd: python_sdk_path,
          env: env,
          stderr_to_stdout: true
        )
      end)

    IO.write(output)

    if status != 0 do
      raise "Python SDK Lambda benchmark failed with exit status #{status}"
    end
  end

  defp profile_benchmark(fun) do
    case System.get_env("FERRICSTORE_HTTP_BENCH_PROFILE", "off") do
      "off" -> fun.()
      "call_time" -> run_profile(:call_time, fun)
      "call_memory" -> run_profile(:call_memory, fun)
      _invalid -> raise "FERRICSTORE_HTTP_BENCH_PROFILE must be off, call_time, or call_memory"
    end
  end

  defp run_profile(type, fun) do
    {:ok, _profiler} = :tprof.start(%{type: type})
    _enabled = :tprof.enable_trace(:all)

    Enum.each(profiled_modules(), fn module ->
      {:module, ^module} = Code.ensure_loaded(module)
      _matched = :tprof.set_pattern(module, :_, :_)
    end)

    try do
      result = fun.()
      _disabled = :tprof.disable_trace(:all)

      :tprof.collect()
      |> :tprof.inspect(:total, {:measurement, :descending})
      |> :tprof.format()

      result
    after
      :tprof.stop()
    end
  end

  defp profiled_modules do
    [
      FerricstoreHttp.Auth,
      FerricstoreHttp.Auth.Cache,
      FerricstoreHttp.Backends.Ferricstore,
      FerricstoreHttp.CommandService,
      FerricstoreHttp.Handlers.Commands,
      FerricstoreHttp.HTTP,
      FerricstoreServer.Acl,
      FerricstoreServer.AuthenticationGateway,
      FerricstoreServer.CommandGateway,
      FerricstoreServer.Connection.Auth,
      FerricstoreServer.Native.Commands,
      FerricstoreServer.Native.ResourceBudget,
      Ferricstore.Commands.Dispatcher,
      Jason
    ]
  end

  defp positive_env(name, default) do
    value = System.get_env(name, Integer.to_string(default))

    case Integer.parse(value) do
      {integer, ""} when integer > 0 -> integer
      _invalid -> raise "#{name} must be a positive integer"
    end
  end

  defp non_negative_env(name, default) do
    value = System.get_env(name, Integer.to_string(default))

    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> integer
      _invalid -> raise "#{name} must be a non-negative integer"
    end
  end

  defp boolean_env(name, default) do
    case System.get_env(name, to_string(default)) do
      "true" -> true
      "false" -> false
      _invalid -> raise "#{name} must be true or false"
    end
  end

  defp tls_options(true, files),
    do: [enabled: true, certfile: files.certfile, keyfile: files.keyfile]

  defp tls_options(false, _files), do: [enabled: false]

  defp benchmark_backend do
    case System.get_env("FERRICSTORE_HTTP_BENCH_BACKEND", "ferricstore") do
      "ferricstore" -> FerricstoreHttp.Backends.Ferricstore
      "memory" -> FerricstoreHttp.PythonSdkLambdaBenchmark.MemoryBackend
      _invalid -> raise "FERRICSTORE_HTTP_BENCH_BACKEND must be ferricstore or memory"
    end
  end

  defp start_isolated_runtime do
    data_directory =
      Path.join(System.tmp_dir!(), "ferricstore_http_lambda_bench_#{unique_id()}")

    concurrent_environments =
      positive_env("FERRICSTORE_LAMBDA_GROUPS", 8) *
        positive_env("FERRICSTORE_LAMBDA_ENVS_PER_GROUP", 8)

    runtime_options = [
      native_port: 0,
      health_port: 0,
      health_probe_port: 0,
      protected_mode: false,
      data_dir: data_directory,
      test_data_dir_auto_cleanup: true,
      shard_count: 4,
      auth_rate_limit_max_attempts: concurrent_environments + 64,
      operational_guard_enabled: false,
      flow_scheduler_enabled: false,
      flow_policy_migration_worker_enabled: false
    ]

    Enum.each(runtime_options, fn {key, value} ->
      Application.put_env(:ferricstore, key, value)
    end)

    {:ok, started_applications} = Application.ensure_all_started(:ferricstore_http)

    %{data_directory: data_directory, started_applications: started_applications}
  end

  defp tls_files do
    root_key = rsa_key()
    root_key_identifier = key_identifier(root_key)

    root =
      :public_key.pkix_test_root_cert(
        ~c"FerricStore HTTP benchmark CA",
        certificate_options(root_key, key_identifier_extensions(root_key_identifier))
      )

    peer_key = rsa_key()
    peer_key_identifier = key_identifier(peer_key)

    tls_config =
      :public_key.pkix_test_data(%{
        root: root,
        intermediates: [],
        peer:
          certificate_options(peer_key, [
            subject_alt_name_extension(),
            subject_key_identifier_extension(peer_key_identifier),
            authority_key_identifier_extension(root_key_identifier)
          ])
      })

    certificate = Keyword.fetch!(tls_config, :cert)
    {key_type, key_der} = Keyword.fetch!(tls_config, :key)
    directory = Path.join(System.tmp_dir!(), "ferricstore_http_bench_#{unique_id()}")
    certfile = Path.join(directory, "server-cert.pem")
    keyfile = Path.join(directory, "server-key.pem")
    cafile = Path.join(directory, "test-ca.pem")

    File.mkdir_p!(directory)
    File.write!(certfile, :public_key.pem_encode([{:Certificate, certificate, :not_encrypted}]))
    File.write!(keyfile, :public_key.pem_encode([{key_type, key_der, :not_encrypted}]))
    File.write!(cafile, encode_certificates(Keyword.fetch!(tls_config, :cacerts)))

    %{directory: directory, certfile: certfile, keyfile: keyfile, cafile: cafile}
  end

  defp certificate_options(key, extensions) do
    [digest: :sha256, key: key, extensions: extensions]
  end

  defp rsa_key, do: :public_key.generate_key({:rsa, 2_048, 65_537})

  defp key_identifier(private_key) do
    public_key = {:RSAPublicKey, elem(private_key, 2), elem(private_key, 3)}
    :crypto.hash(:sha, :public_key.der_encode(:RSAPublicKey, public_key))
  end

  defp key_identifier_extensions(key_identifier) do
    [
      subject_key_identifier_extension(key_identifier),
      authority_key_identifier_extension(key_identifier)
    ]
  end

  defp subject_alt_name_extension do
    {:Extension, {2, 5, 29, 17}, false, [iPAddress: <<127, 0, 0, 1>>, dNSName: ~c"localhost"]}
  end

  defp subject_key_identifier_extension(key_identifier) do
    {:Extension, {2, 5, 29, 14}, false, key_identifier}
  end

  defp authority_key_identifier_extension(key_identifier) do
    value = {:AuthorityKeyIdentifier, key_identifier, :asn1_NOVALUE, :asn1_NOVALUE}
    {:Extension, {2, 5, 29, 35}, false, value}
  end

  defp encode_certificates(certificates) do
    certificates
    |> Enum.map(&{:Certificate, &1, :not_encrypted})
    |> :public_key.pem_encode()
  end

  defp unique_id, do: System.unique_integer([:positive, :monotonic])
end

FerricstoreHttp.PythonSdkLambdaBenchmark.run()
