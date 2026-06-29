defmodule FerricstoreServer.MixProject do
  use Mix.Project

  def project do
    [
      app: :ferricstore_server,
      version: "0.5.7",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      package: package(),
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger, :ssl, :public_key, :ranch],
      mod: {FerricstoreServer.Application, []}
    ]
  end

  defp package do
    [
      description: "FerricStore native protocol server for durable KV and FerricFlow.",
      files: [
        "lib",
        "native/native_protocol_nif/.cargo",
        "native/native_protocol_nif/src",
        "native/native_protocol_nif/Cargo*",
        "checksum-*.exs",
        "mix.exs"
      ],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/ferricstore/ferricstore"
      }
    ]
  end

  defp deps do
    [
      {:ferricstore, in_umbrella: true},
      {:ranch, "~> 2.2"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.4"},
      {:rustler_precompiled, "~> 0.8"},
      {:rustler, "~> 0.37", optional: true},
      {:arch_test, "~> 0.3.1", only: [:dev, :test], runtime: false}
    ]
  end
end
