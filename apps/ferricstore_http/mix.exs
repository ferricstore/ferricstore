defmodule FerricstoreHttp.MixProject do
  use Mix.Project

  @version "0.11.14"

  def project do
    [
      app: :ferricstore_http,
      version: @version,
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.20",
      description: "In-process HTTP and invocation protocol for FerricStore",
      source_url: "https://github.com/ferricstore/ferricstore",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      docs: docs(),
      dialyzer: [plt_add_apps: [:mix, :ex_unit]],
      test_coverage: [tool: ExCoveralls]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :ssl, :public_key, :inets],
      mod: {FerricstoreHttp.Application, []}
    ]
  end

  defp deps do
    [
      {:ferricstore_server, in_umbrella: true},
      {:cowboy, "~> 2.18.0"},
      {:cowlib,
       github: "ninenines/cowlib", ref: "834c19854c2d6b6b0840d7ff90b0775e9f67f631", override: true},
      {:jason, "~> 1.4"},
      {:mint, "~> 1.9.3"},
      {:msgpax, "~> 2.4.0"},
      {:arch_test, "~> 0.3.1", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.3", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18.5", only: :test, runtime: false}
    ]
  end

  defp docs do
    [
      main: "http-api",
      extras: [
        "../../guides/http-api.md",
        "../../docs/http/api.md",
        "../../docs/http/architecture.md",
        "../../docs/http/testing-and-benchmarks.md"
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]
end
