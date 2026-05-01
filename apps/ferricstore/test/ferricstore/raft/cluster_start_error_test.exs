defmodule Ferricstore.Raft.ClusterStartErrorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.Cluster

  @root Path.expand("../../..", __DIR__)
  @application_path Path.join(@root, "lib/ferricstore/application.ex")
  @cluster_path Path.join(@root, "lib/ferricstore/raft/cluster.ex")

  describe "start error recovery action" do
    test "nested supervisor already_started uses same UID restart" do
      pid = self()

      reason =
        {:shutdown, {:failed_to_start_child, :ferricstore_shard_0, {:already_started, pid}}}

      assert Cluster.start_error_recovery_action(reason) == :same_uid_restart
    end

    test "direct already_started uses same UID restart" do
      assert Cluster.start_error_recovery_action({:already_started, self()}) ==
               :same_uid_restart
    end

    test "existing raft state restarts with the same UID" do
      assert Cluster.start_error_recovery_action(:not_new) == :existing_state_restart
    end

    test "other failures fail closed instead of deleting raft state" do
      assert Cluster.start_error_recovery_action(:shutdown) == :fail_closed
    end

    test "startup code does not use fresh UID recovery" do
      source = File.read!(@cluster_path)

      refute source =~ "fresh_uid"
      refute source =~ "fresh start with unique UID"
      refute source =~ "restart_uid"
    end

    test "application checks ra system startup result" do
      assert ok_match_to_start_system?(@application_path)
    end
  end

  defp ok_match_to_start_system?(path) do
    {:ok, ast} =
      path
      |> File.read!()
      |> Code.string_to_quoted(columns: true)

    {_ast, found?} =
      Macro.prewalk(ast, false, fn
        {:=, _meta, [left, right]} = node, acc ->
          found? =
            acc or
              (Macro.to_string(left) == ":ok" and
                 Macro.to_string(right) == "Ferricstore.Raft.Cluster.start_system(data_dir)")

          {node, found?}

        node, acc ->
          {node, acc}
      end)

    found?
  end
end
