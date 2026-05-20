defmodule Ferricstore.Umbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.3.5",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      releases: [
        ferricstore: [
          applications: [
            ferricstore: :permanent,
            ferricstore_server: :permanent
            # NO ferricstore_ecto — it's a library for Phoenix apps
          ],
          include_executables_for: [:unix],
          rel_templates_path: "rel",
          steps: [:assemble, :tar]
        ]
      ]
    ]
  end

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:junit_formatter, "~> 3.4", only: :test},
      {:benchee, "~> 1.3", only: :bench, runtime: false},
      {:benchee_html, "~> 1.0", only: :bench, runtime: false}
    ]
  end

  defp aliases do
    [
      "bench.resp": "run bench/resp_bench.exs",
      "bench.store": "run bench/store_bench.exs",
      "bench.commands": "run bench/commands_bench.exs",
      "bench.tcp": "run bench/tcp_bench.exs",
      "bench.waraft_resp": "run --no-start bench/waraft_resp_router_bench.exs"
    ]
  end
end
