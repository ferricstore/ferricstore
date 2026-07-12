defmodule Ferricstore.GitHubActionsGuardTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../..", __DIR__)
  @workflow_glob Path.join(@repo_root, ".github/workflows/*.{yml,yaml}")

  @deprecated_action_pins [
    "actions/checkout@v4",
    "actions/cache@v4",
    "actions/upload-artifact@v4",
    "actions/download-artifact@v4",
    "actions/attest-build-provenance@v1"
  ]

  test "workflows use supported JavaScript action runtimes" do
    for path <- workflow_paths(), source = File.read!(path) do
      relative_path = Path.relative_to(path, @repo_root)

      for pin <- @deprecated_action_pins do
        refute source =~ pin, "#{relative_path} still pins deprecated #{pin}"
      end

      refute source =~ "FORCE_JAVASCRIPT_ACTIONS_TO_NODE24",
             "#{relative_path} still forces old JavaScript actions onto Node 24"
    end
  end

  test "literal local test paths referenced by workflows exist" do
    missing_paths =
      workflow_paths()
      |> Enum.flat_map(&literal_test_paths/1)
      |> Enum.uniq()
      |> Enum.reject(&File.exists?(Path.join(@repo_root, &1)))

    assert missing_paths == []
  end

  test "serial WARaft backend tests do not overload a core partition" do
    workflow = File.read!(Path.join(@repo_root, ".github/workflows/test.yml"))

    waraft_suite =
      File.read!(
        Path.join(
          @repo_root,
          "apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs"
        )
      )

    assert waraft_suite =~ "@moduletag :waraft_backend_suite"
    refute workflow =~ "--exclude waraft_backend_suite"

    assert count_occurrences(
             workflow,
             "apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs"
           ) == 4

    assert count_occurrences(
             workflow,
             "mix test apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs"
           ) == 2

    assert workflow =~ "test-waraft-ubuntu:"
    assert workflow =~ "test-waraft-macos:"
    assert workflow =~ "test-results-waraft-ubuntu"
    assert workflow =~ "test-results-waraft-macos"
  end

  test "destructive suites fail on every non-zero test command" do
    workflow = File.read!(Path.join(@repo_root, ".github/workflows/test.yml"))

    refute workflow =~ "max 5 failures allowed"
    refute workflow =~ "set +e"
    refute workflow =~ "count_failures"
    refute workflow =~ "FAILURES="
    assert count_occurrences(workflow, "set -o pipefail") == 6

    assert workflow =~ "Run ferricstore shard-kill tests"
    assert workflow =~ "Run SDK shard-kill tests"
    assert workflow =~ "Run Jepsen tests"
    assert workflow =~ "Run large-allocation tests"
    assert workflow =~ "Run ferricstore cluster tests"
    assert workflow =~ "Run ferricstore_server cluster tests"
  end

  test "core suites use deterministic duration-aware file partitions" do
    workflow = File.read!(Path.join(@repo_root, ".github/workflows/test.yml"))

    assert count_occurrences(workflow, ".github/scripts/core_test_partition.exs") == 2
    assert workflow =~ ".github/test-timings/ferricstore.tsv"
    refute workflow =~ "mix test apps/ferricstore/test --partitions 3"

    assert File.exists?(Path.join(@repo_root, ".github/scripts/core_test_partition.ex"))
    assert File.exists?(Path.join(@repo_root, ".github/scripts/core_test_partition.exs"))
    assert File.exists?(Path.join(@repo_root, ".github/test-timings/ferricstore.tsv"))
  end

  test "lint and dependency security checks are gating" do
    workflow = File.read!(Path.join(@repo_root, ".github/workflows/test.yml"))
    root_mix = File.read!(Path.join(@repo_root, "mix.exs"))

    refute workflow =~ "--mute-exit-status"

    assert count_occurrences(
             workflow,
             "mix credo suggest --only warning --min-priority high --files-excluded 'apps/*/test/**/*'"
           ) == 2

    assert count_occurrences(
             workflow,
             "mix credo suggest --only warning --min-priority high --ignore-checks UnusedListOperation,ExpensiveEmptyEnumCheck"
           ) == 2

    assert workflow =~ "mix hex.audit"
    assert workflow =~ "mix deps.compile --include-children mix_audit"
    assert workflow =~ "mix deps.audit"
    assert workflow =~ "cargo install cargo-audit --version 0.22.2 --locked"

    for lockfile <- [
          "apps/ferricstore/native/ferricstore_bitcask/Cargo.lock",
          "apps/ferricstore/native/ferricstore_wal_nif/Cargo.lock",
          "apps/ferricstore_server/native/native_protocol_nif/Cargo.lock"
        ] do
      assert workflow =~ "cargo audit --file #{lockfile}"
    end

    assert root_mix =~ ~s({:mix_audit, "~> 2.1")
  end

  defp workflow_paths, do: Path.wildcard(@workflow_glob)

  defp count_occurrences(source, pattern) do
    source
    |> String.split(pattern)
    |> length()
    |> Kernel.-(1)
  end

  defp literal_test_paths(path) do
    path
    |> File.read!()
    |> String.split()
    |> Enum.map(&String.trim(&1, "\\\"'"))
    |> Enum.filter(&String.ends_with?(&1, "_test.exs"))
    |> Enum.reject(&String.contains?(&1, ["*", "$", "{"]))
  end
end
