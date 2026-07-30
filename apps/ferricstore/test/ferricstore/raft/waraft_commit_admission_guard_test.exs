defmodule Ferricstore.Raft.WARaftCommitAdmissionGuardTest do
  @moduledoc false
  use ExUnit.Case, async: true
  @moduletag :raft

  test "leader skips only redundant post-preparation admission" do
    source = Ferricstore.Test.SourceFiles.waraft_backend_source()

    prepare =
      Ferricstore.Test.SourceFiles.private_function_source!(source, "prepare_commit_command")

    transformed =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "admit_prepared_command",
        "ApplyWork.admit_command"
      )

    # Every submitted command is admitted before blob preparation. Reuse that
    # result only when preparation and apply-context wrapping return the exact
    # same term; generated blob refs and sanitized/context-wrapped commands must
    # still cross the post-transformation admission boundary.
    assert prepare =~ "with :ok <- ApplyWork.admit_command(apply_context, command)"

    assert prepare =~
             "admit_prepared_command(apply_context, command, wrapped_command)"

    assert source =~
             "defp admit_prepared_command(_apply_context, command, command), do: :ok"

    assert transformed =~
             "ApplyWork.admit_command(apply_context, prepared_command)"
  end
end
