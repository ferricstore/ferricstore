defmodule FerricstoreServer.MixProject do
  use Mix.Project

  def project do
    [
      app: :ferricstore_server,
      version: "0.3.8",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger, :ssl, :public_key],
      mod: {FerricstoreServer.Application, []}
    ]
  end

  defp deps do
    [
      {:ferricstore, in_umbrella: true},
      {:ranch, "~> 2.2"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.4"}
    ]
  end
end
