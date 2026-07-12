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
    assert count_occurrences(workflow, "--exclude waraft_backend_suite") == 2

    assert count_occurrences(
             workflow,
             "mix test apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs"
           ) == 2

    assert workflow =~ "test-waraft-ubuntu:"
    assert workflow =~ "test-waraft-macos:"
    assert workflow =~ "test-results-waraft-ubuntu"
    assert workflow =~ "test-results-waraft-macos"
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
