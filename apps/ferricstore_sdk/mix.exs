defmodule FerricstoreSdk.MixProject do
  use Mix.Project

  def project do
    [
      app: :ferricstore_sdk,
      version: "0.7.1",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :ssl]
    ]
  end

  defp deps do
    [
      {:ferricstore_server, in_umbrella: true, only: :test}
    ]
  end
end
