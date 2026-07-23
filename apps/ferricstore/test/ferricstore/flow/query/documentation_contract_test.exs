defmodule Ferricstore.Flow.Query.DocumentationContractTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../../../..", __DIR__)

  test "the command guide documents the complete OSS query surface" do
    guide = File.read!(Path.join(@repo_root, "guides/commands.md"))
    normalized = String.replace(guide, ~r/\s+/, " ")

    assert normalized =~
             "The OSS default includes bounded composite collections, exact `RETURN COUNT`,"

    assert normalized =~ "Enterprise uses the same provider"
    refute normalized =~ "Enterprise installs the composite collection provider"
    refute normalized =~ "OSS rejects unadvertised composite counts"
  end
end
