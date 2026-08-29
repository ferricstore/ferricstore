defmodule FerricstoreClusterConsul.MixProject do
  use Mix.Project

  def project do
    [
      app: :ferricstore_cluster_consul,
      version: "0.11.14",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:libcluster_consul]]
  end

  defp deps do
    [
      {:libcluster, "3.5.0", override: true},
      {:libcluster_consul, "1.3.0"}
    ]
  end
end
