defmodule FerricstoreHttp.ArchitectureTest do
  use ExUnit.Case, async: true

  alias ArchTest.Collector
  alias FerricstoreHttp.Auth

  setup_all do
    {:ok,
     calls: Collector.calls(:ferricstore_http),
     lower_layer_calls: Collector.calls(:ferricstore) ++ Collector.calls(:ferricstore_server)}
  end

  test "FerricStore internals stay isolated behind the backend adapter", %{calls: calls} do
    assert_no_calls(calls,
      from: fn module ->
        app_module?(module) and module != FerricstoreHttp.Backends.Ferricstore
      end,
      to: &ferricstore_module?/1
    )
  end

  test "source-level gateway references stay confined to the backend adapter" do
    allowed = Path.expand("../../lib/ferricstore_http/backends/ferricstore.ex", __DIR__)

    violations =
      Path.expand("../../lib/ferricstore_http/**/*.ex", __DIR__)
      |> Path.wildcard()
      |> Enum.reject(&(&1 == allowed))
      |> Enum.filter(&references_ferricstore_module?/1)
      |> Enum.map(&Path.relative_to_cwd/1)

    assert violations == []
  end

  test "Cowboy stays isolated to the HTTP transport boundary", %{calls: calls} do
    assert_no_calls(calls,
      from: fn module -> app_module?(module) and not http_transport_boundary?(module) end,
      to: &cowboy_module?/1
    )
  end

  test "authentication providers are independent of Cowboy", %{calls: calls} do
    assert_no_calls(calls,
      from: &module_under?(&1, "Elixir.FerricstoreHttp.Auth."),
      to: &cowboy_module?/1
    )
  end

  test "handlers do not bypass the backend boundary", %{calls: calls} do
    assert_no_calls(calls,
      from: &module_under?(&1, "Elixir.FerricstoreHttp.Handlers."),
      to: fn module -> ferricstore_module?(module) or backend_implementation?(module) end
    )
  end

  test "the engine and protocol server do not depend on HTTP", %{lower_layer_calls: calls} do
    assert_no_calls(calls,
      from: fn module -> ferricstore_module?(module) end,
      to: &app_module?/1
    )
  end

  test "production request paths contain no debug IO or sleeps", %{calls: calls} do
    assert_no_calls(calls, from: &app_module?/1, to: [IO], functions: [:puts, :inspect])
    assert_no_calls(calls, from: &app_module?/1, to: [Process], functions: [:sleep])
  end

  test "HTTP authentication identity remains single-scope" do
    assert %Auth.Context{session: :session}
           |> Map.from_struct()
           |> Map.keys()
           |> Enum.sort() == [:cache_key, :scopes, :session, :subject]

    assert %Auth.Identity{session: :session}
           |> Map.from_struct()
           |> Map.keys()
           |> Enum.sort() == [:scopes, :session, :subject]
  end

  defp assert_no_calls(calls, opts) do
    from = Keyword.fetch!(opts, :from)
    to = Keyword.fetch!(opts, :to)
    functions = Keyword.get(opts, :functions)

    violations =
      Enum.filter(calls, fn call ->
        matches_module?(from, call.caller_module) and matches_module?(to, call.callee_module) and
          matches_function?(functions, call.callee_function)
      end)

    assert violations == [], violation_message(violations)
  end

  defp matches_module?(matcher, module) when is_function(matcher, 1), do: matcher.(module)
  defp matches_module?(modules, module) when is_list(modules), do: module in modules
  defp matches_function?(nil, _function), do: true
  defp matches_function?(functions, function), do: function in functions

  defp module_under?(module, prefix) when is_atom(module) do
    module |> Atom.to_string() |> String.starts_with?(prefix)
  end

  defp app_module?(module), do: module_under?(module, "Elixir.FerricstoreHttp")

  defp http_transport_boundary?(module) do
    module in [
      FerricstoreHttp.Admission.StreamHandler,
      FerricstoreHttp.HTTP,
      FerricstoreHttp.Listener,
      FerricstoreHttp.Router
    ] or module_under?(module, "Elixir.FerricstoreHttp.Handlers.")
  end

  defp backend_implementation?(module),
    do: module_under?(module, "Elixir.FerricstoreHttp.Backends.")

  defp cowboy_module?(module) when is_atom(module) do
    module in [:cowboy, :cowboy_metrics_h, :cowboy_req, :cowboy_router, :cowboy_stream]
  end

  defp ferricstore_module?(module) when is_atom(module) do
    module_under?(module, "Elixir.FerricstoreServer.") or
      module_under?(module, "Elixir.Ferricstore.") or module == FerricStore
  end

  defp references_ferricstore_module?(path) do
    ast = path |> File.read!() |> Code.string_to_quoted!()

    {_, referenced?} =
      Macro.prewalk(ast, false, fn
        {:__aliases__, _, [root | _]} = node, _acc
        when root in [:FerricStore, :Ferricstore, :FerricstoreServer] ->
          {node, true}

        node, acc ->
          {node, acc}
      end)

    referenced?
  end

  defp violation_message([]), do: ""

  defp violation_message(violations) do
    entries =
      Enum.map_join(violations, "\n", fn call ->
        "#{inspect(call.caller_module)}.#{call.caller_function}/#{call.caller_arity} -> " <>
          "#{inspect(call.callee_module)}.#{call.callee_function}/#{call.callee_arity}"
      end)

    "Forbidden architecture calls:\n#{entries}"
  end
end
