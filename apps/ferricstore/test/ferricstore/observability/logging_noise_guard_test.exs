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
    @repo_root
    |> Path.join("config/runtime.exs")
    |> Config.Reader.read!(env: :prod)
    |> get_in([:logger, :level])
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
