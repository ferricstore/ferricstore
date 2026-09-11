defmodule Ferricstore.ReadmeCapabilityContractTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Catalog

  @readme_path Path.expand("../../../../README.md", __DIR__)
  @command_guide_path Path.expand("../../../../guides/commands.md", __DIR__)

  @required_sections [
    "## OSS Capability Map",
    "## Scope And Boundaries",
    "## Interfaces And Published SDKs",
    "## Command-Line Tools",
    "## Querying Flows With FQL1",
    "## Schedules, Policies, And Governance",
    "## Operations Dashboard And Security"
  ]

  @required_capabilities [
    "FLOW.QUERY.INDEXES",
    "EXPLAIN ANALYZE",
    "flow_query_result_v1",
    "FLOW.SCHEDULE.CREATE",
    "FLOW.POLICY.SET",
    "per-state FIFO",
    "FLOW.EFFECT.*",
    "FLOW.APPROVAL.*",
    "FLOW.CIRCUIT.*",
    "FLOW.BUDGET.*",
    "FLOW.LIMIT.*",
    "/dashboard/login",
    "/dashboard/security",
    "/metrics",
    "not Redis RESP",
    "does not replay handler code",
    "io.github.ferricstore:ferricstore-java"
  ]

  @cli_tasks [
    "mix ferricstore.info",
    "mix ferricstore.keys",
    "mix ferricstore.config",
    "mix ferricstore.merge",
    "mix ferricstore.redis_compat",
    "mix ferricstore.recovery_kill9"
  ]

  @published_sdks [
    "ferricstore/ferricstore-python",
    "ferricstore/ferricstore-go",
    "ferricstore/ferricstore-elixir",
    "ferricstore/ferricstore-typescript",
    "ferricstore/ferricstore-java"
  ]

  @command_summary_additions [
    "FLOW.START_AND_CLAIM",
    "FLOW.STEP_CONTINUE",
    "FLOW.RUN_STEPS_MANY",
    "FLOW.QUERY.INDEXES",
    "FLOW.EFFECT.RESERVE",
    "FLOW.APPROVAL.REQUEST",
    "FLOW.CIRCUIT.OPEN",
    "FLOW.BUDGET.COMMIT",
    "FLOW.LIMIT.LEASE",
    "FLOW.GOVERNANCE.OVERVIEW"
  ]

  test "README opens with a workflow-first mental model and local start" do
    readme = File.read!(@readme_path)
    intro = binary_part(readme, 0, min(byte_size(readme), 2_500))

    assert intro =~ "durable workflow and queue server"
    assert intro =~ "Applications run ordinary handler code in their own services"
    assert intro =~ "create -> claim a state -> run handler"
    assert intro =~ "## Start Locally"
    assert intro =~ "docker run"

    assert heading_offset(readme, "## Start Locally") <
             heading_offset(readme, "## OSS Capability Map")
  end

  test "README identifies the implemented OSS product surface" do
    readme = File.read!(@readme_path)
    normalized_readme = Regex.replace(~r/\s+/, readme, " ")

    for section <- @required_sections do
      assert readme =~ section, "README is missing section #{inspect(section)}"
    end

    for capability <- @required_capabilities do
      assert readme =~ capability, "README is missing capability #{inspect(capability)}"
    end

    for task <- @cli_tasks do
      assert readme =~ task, "README is missing CLI task #{inspect(task)}"
    end

    assert normalized_readme =~
             "query planner, schedules, governance primitives, and operations dashboard"

    assert normalized_readme =~ "included in this OSS repository"
    assert readme =~ "`partition_key` is an application routing"
    assert readme =~ "FQL does not return payload"
  end

  test "README names every published SDK" do
    readme = File.read!(@readme_path)

    for repository <- @published_sdks do
      assert readme =~ repository, "README is missing published SDK #{repository}"
    end
  end

  test "linked command summary agrees with the advertised Flow surface" do
    guide = File.read!(@command_guide_path)

    for command <- @command_summary_additions do
      assert guide =~ "`#{command}`", "command summary is missing #{command}"
    end
  end

  test "advertised exact commands remain in the executable command catalog" do
    catalog_names = Catalog.names() |> MapSet.new()

    for command <- @command_summary_additions do
      assert MapSet.member?(catalog_names, String.downcase(command)),
             "README advertises missing command #{command}"
    end
  end

  defp heading_offset(readme, heading) do
    {offset, _length} = :binary.match(readme, heading)
    offset
  end
end
