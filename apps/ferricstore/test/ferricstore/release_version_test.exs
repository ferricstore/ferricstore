defmodule Ferricstore.ReleaseVersionTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../..", __DIR__)
  @release_version "0.9.0"

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

  defp read!(relative_path), do: File.read!(Path.join(@repo_root, relative_path))
end
