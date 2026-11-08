defmodule FerricstoreHttp.MixProject do
  use Mix.Project

  @version "0.12.19"

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
      # No Hex release of Cowboy or Cowlib has these fixes yet, so both are
      # pinned to upstream master, which pairs them ("Temporarily depend on
      # Cowlib master"):
      #   Cowboy 3a630c07  EEF-CVE-2026-115701  pipelined HTTP/1.1 data
      #                    buffered without bound while a request runs
      #   Cowlib 4401b858  EEF-CVE-2026-43966   response splitting through
      #                    non-VCHAR bytes in structured headers
      #   Cowlib a3b15e29  EEF-CVE-2026-113431  HPACK amplification through
      #                    an unbounded decoded HTTP/2 header list
      # Return to Hex releases once they include these commits. Cowboy names
      # ranch 1.8; ranch 2.2 replaces it, as with Hex Cowboy.
      {:cowboy,
       github: "ninenines/cowboy", ref: "269595e95cdec10f52236d7026b6bb9031778619", override: true},
      {:cowlib,
       github: "ninenines/cowlib", ref: "68e6a981aa26ef366099d76e54b028616aa8e6df", override: true},
      {:ranch, "~> 2.2.1", override: true},
      {:jason, "~> 1.4"},
      {:mint, "~> 1.10.2"},
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
