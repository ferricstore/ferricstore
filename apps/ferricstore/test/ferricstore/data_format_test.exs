defmodule Ferricstore.DataFormatTest do
  use ExUnit.Case, async: false

  alias Ferricstore.DataFormat
  alias Ferricstore.ReplicationMode

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_data_format_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    previous = Application.get_env(:ferricstore, :data_format)

    on_exit(fn ->
      File.rm_rf!(dir)

      if is_nil(previous),
        do: Application.delete_env(:ferricstore, :data_format),
        else: Application.put_env(:ferricstore, :data_format, previous)
    end)

    {:ok, dir: dir}
  end

  test "a new data dir starts at the latest format and records it", %{dir: dir} do
    assert DataFormat.resolve!(dir) == DataFormat.latest()
    assert DataFormat.latest() == 2
    assert File.read!(DataFormat.marker_path(dir)) == "2\n"
    assert DataFormat.resolve!(dir) == 2
  end

  test "a data dir that booted before format markers is format 1", %{dir: dir} do
    :ok = ReplicationMode.write!(dir, %{replication_mode: :raft, shard_count: 4})
    assert DataFormat.resolve!(dir) == 1
    assert File.read!(DataFormat.marker_path(dir)) == "1\n"
  end

  test "a recorded format wins over everything else", %{dir: dir} do
    File.write!(DataFormat.marker_path(dir), "1\n")
    assert DataFormat.resolve!(dir) == 1

    File.write!(DataFormat.marker_path(dir), "2\n")
    :ok = ReplicationMode.write!(dir, %{replication_mode: :raft, shard_count: 4})
    assert DataFormat.resolve!(dir) == 2
  end

  test "the configured format applies only to a new data dir", %{dir: dir} do
    Application.put_env(:ferricstore, :data_format, 1)
    assert DataFormat.resolve!(dir) == 1

    File.write!(DataFormat.marker_path(dir), "2\n")
    error = assert_raise RuntimeError, ~r/data_format/, fn -> DataFormat.resolve!(dir) end
    assert_points_to_export_restore(error)
  end

  test "an unreadable marker stops the boot", %{dir: dir} do
    File.write!(DataFormat.marker_path(dir), "seven\n")
    assert_raise RuntimeError, ~r/data_format/, fn -> DataFormat.resolve!(dir) end
  end

  test "another instance's data dir takes the node's format and must keep it", %{dir: dir} do
    assert :ok = DataFormat.ensure_node_format!(dir)
    assert File.read!(DataFormat.marker_path(dir)) == "#{DataFormat.current()}\n"

    File.write!(DataFormat.marker_path(dir), "#{other_format()}\n")

    error =
      assert_raise RuntimeError, ~r/data_format/, fn -> DataFormat.ensure_node_format!(dir) end

    assert_points_to_export_restore(error)
  end

  # A mismatch error must name the migration path that exists: export from the
  # old cluster and restore into a new one.
  defp assert_points_to_export_restore(%RuntimeError{message: message}) do
    assert message =~ "FerricStore.export/2"
    assert message =~ "FerricStore.restore/2"
    refute message =~ "FerricStore.Upgrade"
  end

  test "an embedded instance does not start on a data dir in another format", %{dir: dir} do
    name = :"data_format_instance_#{System.unique_integer([:positive])}"
    File.write!(DataFormat.marker_path(dir), "#{other_format()}\n")
    Process.flag(:trap_exit, true)
    on_exit(fn -> FerricStore.Instance.cleanup(name) end)

    assert {:error, _reason} =
             FerricStore.Instance.Supervisor.start_link(name,
               data_dir: dir,
               shard_count: 1,
               flow_shared_ref_backfill?: false
             )

    assert File.read!(DataFormat.marker_path(dir)) == "#{other_format()}\n"
  end

  test "a joining node must use the same data format" do
    assert :ok =
             DataFormat.validate_join_target(:"a@127.0.0.1", fn _node ->
               {:ok, DataFormat.current()}
             end)

    other = other_format()
    target = :"b@127.0.0.1"

    assert {:error, {:data_format_mismatch, ^target, ^other, _current}} =
             DataFormat.validate_join_target(target, fn _node -> {:ok, other} end)

    # A release without data formats writes format 1 keys.
    assert DataFormat.validate_join_target(target, fn _node ->
             {:error, {:exception, :undef, []}}
           end) ==
             if(DataFormat.current() == 1,
               do: :ok,
               else: {:error, {:data_format_mismatch, target, 1, DataFormat.current()}}
             )
  end

  defp other_format, do: if(DataFormat.current() == 1, do: 2, else: 1)
end
