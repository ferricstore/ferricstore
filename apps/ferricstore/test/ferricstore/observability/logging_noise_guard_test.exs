defmodule Ferricstore.Observability.LoggingNoiseGuardTest do
  use ExUnit.Case, async: false

  @repo_root Path.expand("../../../../../", __DIR__)
  @lib_root Path.join(@repo_root, "apps/ferricstore/lib")

  test "prod and bench configs do not default Logger to debug" do
    for env <- [:prod, :bench] do
      config = Config.Reader.read!(Path.join(@repo_root, "config/config.exs"), env: env)
      level = get_in(config, [:logger, :level])

      assert level in [:info, :notice, :warning, :error],
             "#{env} logger level must suppress debug logs, got #{inspect(level)}"
    end
  end

  test "test config suppresses debug/info log noise by default" do
    config = Config.Reader.read!(Path.join(@repo_root, "config/config.exs"), env: :test)
    level = get_in(config, [:logger, :level])

    assert level in [:warning, :error],
           "test logger level must keep suite output and scheduler work bounded, got #{inspect(level)}"
  end

  test "prod runtime log level defaults to info unless explicitly overridden" do
    with_env("FERRICSTORE_LOG_LEVEL", nil, fn ->
      assert runtime_logger_level() == :info
    end)

    with_env("FERRICSTORE_LOG_LEVEL", "warning", fn ->
      assert runtime_logger_level() == :warning
    end)

    with_env("FERRICSTORE_LOG_LEVEL", "bad-value", fn ->
      assert runtime_logger_level() == :info
    end)
  end

  test "prod runtime protected mode accepts common strict boolean values" do
    for value <- ["true", "TRUE", "1", "yes", "on"] do
      with_env("FERRICSTORE_PROTECTED_MODE", value, fn ->
        assert runtime_ferricstore_config(:protected_mode) == true
      end)
    end

    for value <- ["false", "FALSE", "0", "no", "off"] do
      with_env("FERRICSTORE_PROTECTED_MODE", value, fn ->
        assert runtime_ferricstore_config(:protected_mode) == false
      end)
    end

    with_env("FERRICSTORE_PROTECTED_MODE", "maybe", fn ->
      assert_raise RuntimeError, ~r/FERRICSTORE_PROTECTED_MODE/, fn ->
        runtime_config()
      end
    end)
  end

  test "prod runtime derives keydir RAM from maxmemory unless explicitly overridden" do
    with_env("FERRICSTORE_KEYDIR_MAX_RAM", nil, fn ->
      with_env("FERRICSTORE_MAX_MEMORY", nil, fn ->
        assert runtime_ferricstore_config(:max_memory_bytes) > 0
        assert runtime_ferricstore_config(:keydir_max_ram) >= 268_435_456
        assert runtime_ferricstore_config(:keydir_max_ram) <= 8_589_934_592
      end)

      with_env("FERRICSTORE_MAX_MEMORY", "auto", fn ->
        assert runtime_ferricstore_config(:max_memory_bytes) > 0
        assert runtime_ferricstore_config(:keydir_max_ram) >= 268_435_456
        assert runtime_ferricstore_config(:keydir_max_ram) <= 8_589_934_592
      end)

      with_env("FERRICSTORE_MAX_MEMORY", "0", fn ->
        assert runtime_ferricstore_config(:keydir_max_ram) == 268_435_456
      end)

      with_env("FERRICSTORE_MAX_MEMORY", "100000000000", fn ->
        assert runtime_ferricstore_config(:keydir_max_ram) == 8_589_934_592
      end)

      with_env("FERRICSTORE_MAX_MEMORY", "1073741824", fn ->
        assert runtime_ferricstore_config(:keydir_max_ram) == 268_435_456
      end)
    end)

    with_env("FERRICSTORE_MAX_MEMORY", "100000000000", fn ->
      with_env("FERRICSTORE_KEYDIR_MAX_RAM", "123456789", fn ->
        assert runtime_ferricstore_config(:keydir_max_ram) == 123_456_789
      end)
    end)
  end

  test "flow soak harness exposes and applies derived memory budgets" do
    source = File.read!(Path.join(@repo_root, "bench/flow_state_lmdb_soak.exs"))
    lmdb_writer_source = Ferricstore.Test.SourceFiles.flow_source()
    state_machine_source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "Application.put_env(:ferricstore, :max_memory_bytes, max_memory_bytes)"
    assert source =~ "Application.put_env(:ferricstore, :keydir_max_ram, app_keydir_max_ram_bytes(max_memory_bytes))"
    assert source =~
             ~S|max_memory_bytes=#{Application.get_env(:ferricstore, :max_memory_bytes)}|

    assert source =~ ~S|keydir_max_ram=#{Application.get_env(:ferricstore, :keydir_max_ram)}|
    assert source =~ ~s("auto" -> max_total_mem_mb * 1024 * 1024)
    assert source =~ ~S|optional_cli_arg("CREATE_NOW_MS", "--create-now-ms")|
    assert source =~ ~S|optional_cli_arg("CLAIM_NOW_MS", "--claim-now-ms")|
    assert source =~ ~S|[:ferricstore, :flow, :hibernation, :evict_hot]|
    assert source =~ ~S|[:ferricstore, :flow, :hibernation, :promote]|
    assert source =~ "cold_due_evicted="
    assert source =~ "cold_due_promoted="
    assert lmdb_writer_source =~ ~S|[:ferricstore, :flow, :hibernation, :evict_hot]|
    assert state_machine_source =~ ~S|[:ferricstore, :flow, :hibernation, :promote]|
  end

  test "prod runtime WARaft ETS byte cap treats empty as unset and validates overrides" do
    with_env("FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_BYTES", nil, fn ->
      refute Keyword.has_key?(
               runtime_ferricstore_memory_overrides(),
               :waraft_segment_log_max_ets_bytes
             )
    end)

    with_env("FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_BYTES", "", fn ->
      refute Keyword.has_key?(
               runtime_ferricstore_memory_overrides(),
               :waraft_segment_log_max_ets_bytes
             )
    end)

    with_env("FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_BYTES", "off", fn ->
      assert runtime_ferricstore_memory_overrides()[:waraft_segment_log_max_ets_bytes] ==
               :infinity
    end)

    with_env("FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_BYTES", "1048576", fn ->
      assert runtime_ferricstore_memory_overrides()[:waraft_segment_log_max_ets_bytes] ==
               1_048_576
    end)

    with_env("FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_BYTES", "256MB", fn ->
      assert_raise RuntimeError, ~r/FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_BYTES/, fn ->
        runtime_config()
      end
    end)
  end

  test "runtime library telemetry handlers use named callbacks" do
    offenders =
      @lib_root
      |> Path.join("**/*.ex")
      |> Path.wildcard()
      |> Enum.flat_map(&anonymous_telemetry_attach_sites/1)

    assert offenders == [],
           "anonymous telemetry handlers in runtime library code can emit scheduler/log noise:\n" <>
             Enum.join(offenders, "\n")
  end

  defp anonymous_telemetry_attach_sites(path) do
    source = File.read!(path)

    source
    |> Code.string_to_quoted!(token_metadata: true)
    |> collect_anonymous_telemetry_attach_sites([])
    |> Enum.map(fn line ->
      relative = Path.relative_to(path, @repo_root)
      "#{relative}:#{line}"
    end)
  end

  defp collect_anonymous_telemetry_attach_sites(ast, acc) do
    {_ast, acc} =
      Macro.prewalk(ast, acc, fn
        {{:., meta, [:telemetry, attach_fun]}, _call_meta, args} = node, acc
        when attach_fun in [:attach, :attach_many] ->
          if local_telemetry_handler?(Enum.at(args, 2)) do
            {node, [Keyword.fetch!(meta, :line) | acc]}
          else
            {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(acc)
  end

  defp local_telemetry_handler?({:fn, _meta, _clauses}), do: true

  defp local_telemetry_handler?(
         {:&, _capture_meta, [{:/, _slash_meta, [{name, _name_meta, context}, arity]}]}
       )
       when is_atom(name) and is_atom(context) and is_integer(arity),
       do: true

  defp local_telemetry_handler?(_other), do: false

  defp runtime_logger_level do
    runtime_config()
    |> get_in([:logger, :level])
  end

  defp runtime_ferricstore_config(key) do
    runtime_config()
    |> merged_runtime_ferricstore_config()
    |> Keyword.fetch!(key)
  end

  defp runtime_ferricstore_memory_overrides do
    runtime_config()
    |> merged_runtime_ferricstore_config()
    |> Keyword.take([
      :flow_history_projector_max_pending_entries,
      :flow_lmdb_writer_max_mailbox_messages,
      :flow_lmdb_writer_max_enqueue_ops,
      :waraft_segment_log_max_ets_bytes,
      :waraft_segment_log_max_ets_entries,
      :waraft_segment_log_min_ets_entries,
      :waraft_apply_projection_cache_max_entries
    ])
  end

  defp merged_runtime_ferricstore_config(config) do
    config
    |> Keyword.get_values(:ferricstore)
    |> Enum.reduce([], &Keyword.merge(&2, &1))
  end

  defp runtime_config do
    @repo_root
    |> Path.join("config/runtime.exs")
    |> Config.Reader.read!(env: :prod)
  end

  defp with_env(key, value, fun) do
    old_value = System.get_env(key)

    try do
      if is_nil(value) do
        System.delete_env(key)
      else
        System.put_env(key, value)
      end

      fun.()
    after
      if is_nil(old_value) do
        System.delete_env(key)
      else
        System.put_env(key, old_value)
      end
    end
  end
end
