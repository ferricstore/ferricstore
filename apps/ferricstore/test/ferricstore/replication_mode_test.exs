defmodule Ferricstore.ReplicationModeTest do
  use ExUnit.Case, async: false

  alias Ferricstore.ReplicationMode

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-replication-mode-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(dir)
    File.mkdir_p!(dir)

    on_exit(fn ->
      File.rm_rf!(dir)

      ReplicationMode.put_current(:raft)
    end)

    {:ok, dir: dir}
  end

  test "missing marker resolves to raft", %{dir: dir} do
    assert :raft = ReplicationMode.resolve!(dir, 4)
  end

  test "raft marker resolves to raft", %{dir: dir} do
    ReplicationMode.mark_raft!(dir, 4, 1, %{0 => 7})

    assert :raft = ReplicationMode.resolve!(dir, 4)

    assert {:ok, %{replication_mode: :raft, barrier_indices: %{0 => 7}}} =
             ReplicationMode.read(dir)
  end

  test "unsupported standalone marker fails closed", %{dir: dir} do
    ReplicationMode.write!(dir, %{replication_mode: :standalone, shard_count: 4})

    assert_raise RuntimeError, ~r/standalone promotion mode was removed/, fn ->
      ReplicationMode.resolve!(dir, 4)
    end
  end

  test "raft marker preserves the data-dir cluster identity", %{dir: dir} do
    ReplicationMode.write!(dir, %{replication_mode: :raft, shard_count: 4})
    {:ok, %{cluster_id: cluster_id}} = ReplicationMode.read(dir)

    ReplicationMode.mark_raft!(dir, 4, 11, %{0 => 42})

    assert {:ok, %{cluster_id: ^cluster_id, replication_mode: :raft}} =
             ReplicationMode.read(dir)
  end

  test "corrupted marker fails closed", %{dir: dir} do
    File.write!(ReplicationMode.marker_path(dir), "not a marker")

    assert_raise RuntimeError, ~r/failed to read cluster_state marker/, fn ->
      ReplicationMode.resolve!(dir, 4)
    end
  end

  test "marker decode does not create atoms from corrupt outer term", %{dir: dir} do
    atom_name = "ferricstore_cluster_state_outer_#{System.unique_integer([:positive])}"
    refute existing_atom?(atom_name)

    File.write!(ReplicationMode.marker_path(dir), unknown_atom_payload(atom_name))

    assert {:error, _reason} = ReplicationMode.read(dir)
    refute existing_atom?(atom_name)
  end

  test "marker decode does not create atoms from corrupt checksummed payload", %{dir: dir} do
    atom_name = "ferricstore_cluster_state_payload_#{System.unique_integer([:positive])}"
    refute existing_atom?(atom_name)

    payload = unknown_atom_payload(atom_name)
    checksum = :crypto.hash(:sha256, payload)
    encoded = :erlang.term_to_binary({:ferricstore_cluster_state_v1, payload, checksum})

    File.write!(ReplicationMode.marker_path(dir), encoded)

    assert {:error, _reason} = ReplicationMode.read(dir)
    refute existing_atom?(atom_name)
  end

  test "shard count mismatch fails closed", %{dir: dir} do
    ReplicationMode.mark_raft!(dir, 4, 1, %{})

    assert_raise RuntimeError, ~r/shard_count mismatch/, fn ->
      ReplicationMode.resolve!(dir, 8)
    end
  end

  defp unknown_atom_payload(atom_name) when is_binary(atom_name) and byte_size(atom_name) < 256 do
    <<131, 119, byte_size(atom_name), atom_name::binary>>
  end

  defp existing_atom?(atom_name) do
    _ = String.to_existing_atom(atom_name)
    true
  rescue
    ArgumentError -> false
  end
end
