defmodule Ferricstore.Cluster.JoinIdentityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Cluster.JoinIdentity

  @target :"node2@127.0.0.1"

  test "accepts copied data when local and target cluster ids match" do
    assert :ok =
             JoinIdentity.validate(
               {:ok, %{cluster_id: "cluster-a", replication_mode: :raft}},
               {:ok, %{cluster_id: "cluster-a", replication_mode: :raft}},
               @target
             )
  end

  test "rejects unsupported standalone target marker even when cluster ids match" do
    assert {:error, {:target_cluster_state_unusable, @target, :missing_replication_mode}} =
             JoinIdentity.validate(
               {:ok, %{cluster_id: "cluster-a", replication_mode: :raft}},
               {:ok, %{cluster_id: "cluster-a", replication_mode: :standalone}},
               @target
             )
  end

  test "allows legacy local data with no marker" do
    assert :ok =
             JoinIdentity.validate(
               {:error, :enoent},
               {:error, :enoent},
               @target
             )
  end

  test "rejects copied target data when local marker is missing" do
    assert {:error, {:local_cluster_state_missing, @target}} =
             JoinIdentity.validate(
               {:error, :enoent},
               {:ok, %{cluster_id: "foreign", replication_mode: :raft}},
               @target
             )
  end

  test "rejects copied target data without a marker when local has a marker" do
    assert {:error, {:target_cluster_state_missing, @target}} =
             JoinIdentity.validate(
               {:ok, %{cluster_id: "cluster-a"}},
               {:error, :enoent},
               @target
             )
  end

  test "rejects copied target data from another cluster" do
    assert {:error, {:target_cluster_id_mismatch, @target, "cluster-a", "cluster-b"}} =
             JoinIdentity.validate(
               {:ok, %{cluster_id: "cluster-a"}},
               {:ok, %{cluster_id: "cluster-b"}},
               @target
             )
  end

  test "rejects unreadable local marker" do
    assert {:error, {:local_cluster_state_unreadable, :bad_checksum}} =
             JoinIdentity.validate(
               {:error, :bad_checksum},
               {:ok, %{cluster_id: "cluster-a"}},
               @target
             )
  end

  test "rejects unreadable target marker" do
    assert {:error, {:target_cluster_state_unreadable, @target, :bad_checksum}} =
             JoinIdentity.validate(
               {:ok, %{cluster_id: "cluster-a"}},
               {:error, :bad_checksum},
               @target
             )
  end
end
