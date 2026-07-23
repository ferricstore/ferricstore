defmodule Ferricstore.ReleaseVersionTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../..", __DIR__)
  @release_version "0.10.2"

  @project_files [
    "mix.exs",
    "apps/ferricstore/mix.exs",
    "apps/ferricstore_server/mix.exs"
  ]

  @current_release_docs [
    "README.md",
    "guides/deployment.md",
    "guides/embedded-mode.md",
    "guides/getting-started.md"
  ]

  test "umbrella projects publish one release version" do
    versions =
      Enum.map(@project_files, fn relative_path ->
        source = read!(relative_path)

        case Regex.run(~r/(?:@version\s+|version:\s*)"([^"]+)"/, source, capture: :all_but_first) do
          [version] -> version
          nil -> flunk("#{relative_path} does not declare a project version")
        end
      end)

    assert Enum.uniq(versions) == [@release_version]
  end

  test "current installation docs use the release version" do
    references =
      Enum.flat_map(@current_release_docs, fn relative_path ->
        source = read!(relative_path)

        image_versions =
          Regex.scan(
            ~r{ghcr\.io/ferricstore/ferricstore:(\d+\.\d+\.\d+)},
            source,
            capture: :all_but_first
          )

        package_versions =
          Regex.scan(
            ~r/\{:ferricstore,\s*"~>\s*(\d+\.\d+\.\d+)"\}/,
            source,
            capture: :all_but_first
          )

        release_tag_versions =
          Regex.scan(
            ~r/current\s+release tag is `(\d+\.\d+\.\d+)`/,
            source,
            capture: :all_but_first
          )

        Enum.map(image_versions ++ package_versions ++ release_tag_versions, fn [version] ->
          {relative_path, version}
        end)
      end)

    assert references != []

    assert Enum.all?(references, fn {_path, version} -> version == @release_version end),
           "stale release references: #{inspect(Enum.reject(references, &match?({_, @release_version}, &1)))}"
  end

  test "changelog contains the release section" do
    changelog = read!("CHANGELOG.md")

    assert changelog =~ "## Unreleased"
    assert changelog =~ "## #{@release_version} - "
  end

  test "container release packages the query catalog and smoke-tests startup" do
    dockerfile = read!("Dockerfile")

    assert dockerfile =~
             "COPY apps/ferricstore/priv/flow_query apps/ferricstore/priv/flow_query"

    assert File.exists?(Path.join(@repo_root, "scripts/smoke-docker-image.sh"))

    for workflow <- [
          ".github/workflows/docker-ci.yml",
          ".github/workflows/docker-publish.yml"
        ] do
      assert read!(workflow) =~ "scripts/smoke-docker-image.sh"
    end
  end

  test "container smoke test bounds registry propagation retries" do
    smoke = read!("scripts/smoke-docker-image.sh")

    assert smoke =~ "FERRICSTORE_SMOKE_PULL_ATTEMPTS"
    assert smoke =~ "FERRICSTORE_SMOKE_PULL_INTERVAL_SECONDS"
    assert smoke =~ "docker image inspect"
    assert smoke =~ "docker pull"
  end

  test "container smoke test retries remote images and skips cached images" do
    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-smoke-test-#{System.unique_integer([:positive, :monotonic])}"
      )

    fake_bin = Path.join(tmp_dir, "bin")
    fake_docker = Path.join(fake_bin, "docker")
    File.mkdir_p!(fake_bin)

    File.write!(fake_docker, """
    #!/usr/bin/env bash
    set -euo pipefail

    case "${1:-}" in
      image)
        [[ "${DOCKER_IMAGE_CACHED:-0}" == "1" ]]
        ;;
      pull)
        count=0
        if [[ -f "$DOCKER_PULL_COUNT_FILE" ]]; then
          count="$(<"$DOCKER_PULL_COUNT_FILE")"
        fi
        count=$((count + 1))
        printf '%s' "$count" >"$DOCKER_PULL_COUNT_FILE"
        [[ "$count" -ge "$DOCKER_PULL_SUCCEEDS_AT" ]]
        ;;
      run)
        exit 42
        ;;
      *)
        exit 0
        ;;
    esac
    """)

    File.chmod!(fake_docker, 0o755)
    on_exit(fn -> File.rm_rf!(tmp_dir) end)

    pull_count = Path.join(tmp_dir, "remote-pulls")

    assert {_, 42} =
             run_smoke_with_fake_docker(fake_bin,
               DOCKER_PULL_COUNT_FILE: pull_count,
               DOCKER_PULL_SUCCEEDS_AT: "3"
             )

    assert File.read!(pull_count) == "3"

    cached_pull_count = Path.join(tmp_dir, "cached-pulls")

    assert {_, 42} =
             run_smoke_with_fake_docker(fake_bin,
               DOCKER_IMAGE_CACHED: "1",
               DOCKER_PULL_COUNT_FILE: cached_pull_count,
               DOCKER_PULL_SUCCEEDS_AT: "1"
             )

    refute File.exists?(cached_pull_count)
  end

  test "Hex package inputs contain no removed protocol artifacts" do
    refute File.exists?(
             Path.join(
               @repo_root,
               "apps/ferricstore/checksum-Elixir.Ferricstore.Resp.ParserNif.exs"
             )
           )
  end

  defp read!(relative_path), do: File.read!(Path.join(@repo_root, relative_path))

  defp run_smoke_with_fake_docker(fake_bin, env) do
    env =
      [
        {"PATH", fake_bin <> ":" <> System.fetch_env!("PATH")},
        {"FERRICSTORE_SMOKE_PULL_ATTEMPTS", "4"},
        {"FERRICSTORE_SMOKE_PULL_INTERVAL_SECONDS", "0"}
      ] ++ Enum.map(env, fn {key, value} -> {Atom.to_string(key), value} end)

    System.cmd(
      "bash",
      [
        Path.join(@repo_root, "scripts/smoke-docker-image.sh"),
        "example.invalid/ferricstore:test"
      ],
      env: env,
      stderr_to_stdout: true
    )
  end
end
