defmodule Ferricstore.Umbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.5.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      releases: [
        ferricstore: [
          applications: [
            ranch: :permanent,
            ferricstore: :permanent,
            ferricstore_server: :permanent
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
      "bench.commands": "run bench/commands_bench.exs",
      "bench.tcp": "run bench/tcp_bench.exs",
      "bench.flow": "run bench/flow_workflow_bench.exs"
    ]
  end
end
