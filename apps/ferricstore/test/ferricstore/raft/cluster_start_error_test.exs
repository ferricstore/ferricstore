defmodule Ferricstore.Raft.ClusterStartErrorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.Cluster

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

    test "other failures keep explicit recovery path" do
      assert Cluster.start_error_recovery_action(:shutdown) == :fresh_uid_recovery
    end
  end
end
