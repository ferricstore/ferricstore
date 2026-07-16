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

  test "marker stores node identity as data, not as a dynamic atom", %{dir: dir} do
    node_name = :"ferricstore_restart_marker_#{System.unique_integer([:positive])}@host"

    ReplicationMode.write!(dir, %{
      replication_mode: :raft,
      shard_count: 4,
      node: node_name
    })

    assert {:ok, %{node: encoded_node}} = ReplicationMode.read(dir)
    assert encoded_node == Atom.to_string(node_name)
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

  test "marker reader rejects non-canonical outer and inner terms", %{dir: dir} do
    ReplicationMode.write!(dir, %{
      replication_mode: :raft,
      shard_count: 4,
      padding: String.duplicate("marker", 4_096)
    })

    path = ReplicationMode.marker_path(dir)
    encoded = File.read!(path)

    {:ferricstore_cluster_state_v1, payload, _checksum} =
      :erlang.binary_to_term(encoded, [:safe])

    outer_term = :erlang.binary_to_term(encoded, [:safe])
    compressed_outer = :erlang.term_to_binary(outer_term, compressed: 9)
    assert <<131, 80, _rest::binary>> = compressed_outer

    for invalid <- [compressed_outer, encoded <> <<0>>] do
      File.write!(path, invalid)
      assert {:error, _reason} = ReplicationMode.read(dir)
    end

    compressed_payload =
      payload
      |> :erlang.binary_to_term([:safe])
      |> :erlang.term_to_binary(compressed: 9)

    assert <<131, 80, _rest::binary>> = compressed_payload

    for invalid_payload <- [compressed_payload, payload <> <<0>>] do
      checksum = :crypto.hash(:sha256, invalid_payload)

      File.write!(
        path,
        :erlang.term_to_binary({:ferricstore_cluster_state_v1, invalid_payload, checksum})
      )

      assert {:error, _reason} = ReplicationMode.read(dir)
    end
  end

  test "marker reader refuses symlinks and oversized files", %{dir: dir} do
    path = ReplicationMode.marker_path(dir)
    target = Path.join(dir, "marker-target")
    File.write!(target, "not a marker")
    File.ln_s!(target, path)

    assert {:error, {:symlink, _reason}} = ReplicationMode.read(dir)

    File.rm!(path)
    File.write!(path, :binary.copy(<<0>>, 1_048_577))
    assert {:error, {:too_large, _reason}} = ReplicationMode.read(dir)
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
