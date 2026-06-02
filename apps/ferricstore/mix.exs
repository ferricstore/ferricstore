defmodule Ferricstore.MixProject do
  use Mix.Project

  @version "0.3.7"

  def project do
    [
      app: :ferricstore,
      version: @version,
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger, :ssl, :public_key],
      mod: {Ferricstore.Application, []}
    ]
  end

  defp docs do
    [
      main: "getting-started",
      extras: [
        "../../guides/getting-started.md",
        "../../guides/embedded-mode.md",
        "../../guides/standalone-mode.md",
        "../../guides/commands.md",
        "../../guides/configuration.md",
        "../../guides/architecture.md",
        "../../guides/deployment.md",
        "../../guides/security.md",
        "../../guides/extensions.md",
        "../../guides/best-practices.md",
        "../../guides/comparison.md"
      ],
      groups_for_extras: [
        Guides: Path.wildcard("../../guides/*.md")
      ],
      source_url: "https://github.com/yoavgeva/ferricstore",
      homepage_url: "https://github.com/yoavgeva/ferricstore"
    ]
  end

  defp package do
    [
      description:
        "Distributed persistent key-value cache with Redis wire protocol (RESP3), Raft consensus, and Bitcask storage engine.",
      files: [
        "lib",
        "native/ferricstore_bitcask/.cargo",
        "native/ferricstore_bitcask/src",
        "native/ferricstore_bitcask/Cargo*",
        "native/ferricstore_wal_nif/.cargo",
        "native/ferricstore_wal_nif/src",
        "native/ferricstore_wal_nif/Cargo*",
        "native/resp_parser_nif/.cargo",
        "native/resp_parser_nif/src",
        "native/resp_parser_nif/Cargo*",
        "checksum-*.exs",
        "mix.exs"
      ],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/yoavgeva/ferricstore"
      }
    ]
  end

  defp deps do
    [
      {:rustler_precompiled, "~> 0.8"},
      {:rustler, "~> 0.37", optional: true},
      {:wa_raft, "~> 0.1", hex: :ferricstore_waraft, manager: :rebar3},
      {:libcluster, "3.3.3"},
      {:libcluster_consul, "1.3.0", optional: true},
      {:libcluster_etcd, "1.1.2", optional: true},
      {:telemetry, "~> 1.4"},
      {:jason, "~> 1.4"},
      {:plug, "~> 1.16", optional: true},
      {:ex_doc, "~> 0.35", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:arch_test, "~> 0.1.2", only: [:dev, :test], runtime: false}
    ]
  end
end
