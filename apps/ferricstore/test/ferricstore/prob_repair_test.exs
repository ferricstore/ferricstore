defmodule Ferricstore.ProbRepairTest do
  @moduledoc """
  The node-local halves of `Ferricstore.ProbRepair`: a replica copying a file
  aside and serving it in chunks, and the checks on what a repair entry names.
  The whole repair, across nodes, is covered by the cluster tests.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.ProbRepair

  setup do
    root = Path.join(System.tmp_dir!(), "prob_repair_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "data/shard_3/prob"))
    on_exit(fn -> File.rm_rf(root) end)
    {:ok, root: root}
  end

  test "a replica copies the file and its receipt aside and serves them in chunks", %{root: root} do
    path = Path.join(root, "data/shard_3/prob/key.bloom")
    data = :crypto.strong_rand_bytes(2_500_000)
    File.write!(path, data)
    File.write!(path <> ".mutation", "receipt")
    id = String.duplicate("ab", 12)

    assert :ok == ProbRepair.stage(root, 3, id, path)

    dir = ProbRepair.staging_dir(root, 3)
    assert File.read!(Path.join(dir, id <> ".data")) == data
    assert File.read!(Path.join(dir, id <> ".receipt")) == "receipt"

    :persistent_term.put({ProbRepair, :data_dir}, root)
    assert {:ready, info} = ProbRepair.staged_info(3, id, 1)
    assert info.data == %{size: byte_size(data), sha256: :crypto.hash(:sha256, data)}
    assert info.receipt == %{size: 7, sha256: :crypto.hash(:sha256, "receipt")}

    assert {:ok, chunk} = ProbRepair.staged_chunk(3, id, :data, 1_048_576, 1_048_576)
    assert chunk == binary_part(data, 1_048_576, 1_048_576)
    assert {:error, :invalid_repair_id} == ProbRepair.staged_chunk(3, "../x", :data, 0, 10)
  end

  test "a replica with nothing to give says so", %{root: root} do
    id = String.duplicate("cd", 12)
    :persistent_term.put({ProbRepair, :data_dir}, root)

    assert :ok == ProbRepair.stage(root, 3, id, Path.join(root, "data/shard_3/prob/gone.cms"))
    assert :none == ProbRepair.staged_info(3, id, 1)

    other = String.duplicate("ef", 12)
    assert :ok == ProbRepair.stage_none(root, 3, other)
    assert :none == ProbRepair.staged_info(3, other, 1)
  end

  test "copies older than the staging TTL are swept", %{root: root} do
    dir = ProbRepair.staging_dir(root, 0)
    File.mkdir_p!(dir)
    old = Path.join(dir, String.duplicate("aa", 12) <> ".data")
    fresh = Path.join(dir, String.duplicate("bb", 12) <> ".data")
    File.write!(old, "old")
    File.write!(fresh, "fresh")
    File.touch!(old, System.os_time(:second) - 3_600)

    pid =
      start_supervised!(
        {ProbRepair,
         name: :prob_repair_sweep_test,
         instance_ctx: %{data_dir: root, shard_count: 0},
         startup_scan_ms: 60_000,
         ttl_ms: 600_000}
      )

    assert Ferricstore.Test.Eventually.eventually(fn -> not File.exists?(old) end)
    assert File.exists?(fresh)
    assert Process.alive?(pid)
  end

  test "a repair entry must name a key, a known type and a well-formed id" do
    id = String.duplicate("01", 12)
    assert ProbRepair.valid_request?("k", :cms, id)
    refute ProbRepair.valid_request?("k", :string, id)
    refute ProbRepair.valid_request?("k", :cms, "../../etc")
    refute ProbRepair.valid_request?(:k, :cms, id)
  end
end
