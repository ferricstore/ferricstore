defmodule Ferricstore.Umbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.11.5",
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
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false},
      {:junit_formatter, "~> 3.4", only: :test},
      {:benchee, "~> 1.3", only: :bench, runtime: false},
      {:benchee_html, "~> 1.0", only: :bench, runtime: false}
    ]
  end

  defp aliases do
    [
      "bench.commands": "run bench/commands_bench.exs",
      "bench.pubsub": "run --no-start bench/pubsub_bench.exs",
      "bench.pubsub_activity_log": "run --no-start bench/pubsub_activity_log_bench.exs",
      "bench.pubsub_channels": "run --no-start bench/pubsub_channels_bench.exs",
      "bench.pubsub_cleanup": "run --no-start bench/pubsub_cleanup_bench.exs",
      "bench.pubsub_pattern_index": "run --no-start bench/pubsub_pattern_index_bench.exs",
      "bench.pubsub_session": "run --no-start bench/pubsub_session_bench.exs",
      "bench.pubsub_snapshot": "run --no-start bench/pubsub_snapshot_bench.exs",
      "bench.pubsub_subscription": "run --no-start bench/pubsub_subscription_bench.exs",
      "bench.native_pubsub": "run --no-start bench/native_pubsub_bench.exs",
      "bench.native_pubsub_load": "run --no-start bench/native_pubsub_load_bench.exs",
      "bench.tcp": "run bench/tcp_bench.exs",
      "bench.flow": "run bench/flow_workflow_bench.exs"
    ]
  end
end
