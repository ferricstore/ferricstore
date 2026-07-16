defmodule Ferricstore.Raft.WARaftSegmentReaderSecurityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.WARaftSegmentReader

  @moduletag :raft

  test "missing shard reads do not intern ETS table atoms" do
    shard_index = 1_000_000_000 + System.unique_integer([:positive])
    table_name = "raft_log_ferricstore_waraft_backend_#{shard_index + 1}"

    assert_raise ArgumentError, fn -> String.to_existing_atom(table_name) end

    _result =
      WARaftSegmentReader.read_value(
        %{data_dir: System.tmp_dir!()},
        shard_index,
        1,
        "missing"
      )

    assert_raise ArgumentError, fn -> String.to_existing_atom(table_name) end
  end

  test "batch reads reject malformed segment locations instead of reporting misses" do
    assert {:error, :not_waraft_segment_location} =
             WARaftSegmentReader.read_values_from_location(
               %{data_dir: System.tmp_dir!()},
               0,
               {:corrupt_location, 1},
               ["key"]
             )

    assert {:error, :bad_segment_location} =
             WARaftSegmentReader.read_values_from_location(
               %{data_dir: System.tmp_dir!()},
               0,
               {:waraft_segment, 0},
               ["key"]
             )
  end

  test "spilled apply projections preserve expiry semantics" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-expired-projection-#{System.unique_integer([:positive])}"
      )

    index = 73
    file_id = {:waraft_apply_projection, index}
    expired_at_ms = System.system_time(:millisecond) - 1

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {"expired", "must-stay-hidden", expired_at_ms}
             ])

    assert :not_found =
             WARaftSegmentReader.read_value_from_location(
               %{data_dir: data_dir},
               0,
               file_id,
               "expired"
             )

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    assert :not_found =
             WARaftSegmentReader.read_value_from_location(
               %{data_dir: data_dir},
               0,
               file_id,
               "expired"
             )

    assert {:ok, %{}} =
             WARaftSegmentReader.read_values_from_location(
               %{data_dir: data_dir},
               0,
               file_id,
               ["expired"]
             )

    assert {:ok, "must-stay-hidden"} =
             WARaftSegmentReader.read_value_from_location_including_expired(
               %{data_dir: data_dir},
               0,
               file_id,
               "expired"
             )
  end

  test "corrupt durable apply projections never satisfy replay dependencies" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-corrupt-projection-#{System.unique_integer([:positive])}"
      )

    index = 91

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {"flow:value", "payload", 0}
             ])

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    [segment_path] =
      Path.wildcard(
        Path.join([
          data_dir,
          "waraft",
          "ferricstore_waraft_backend.1",
          "apply_projection_log",
          "segment_log",
          "*.seg"
        ])
      )

    File.write!(segment_path, "corrupt")

    refute WARaftSegmentReader.apply_projection_dependency_ready?(data_dir, 0, index)
  end

  test "recorded projection locations fail closed when their record disappears" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-corrupt-recorded-projection-#{System.unique_integer([:positive])}"
      )

    position_index = 117
    projection_index = 1
    projection_root =
      Path.join([
        data_dir,
        "waraft",
        "ferricstore_waraft_backend.1",
        "segment_projection_log"
      ])

    on_exit(fn -> File.rm_rf!(data_dir) end)

    assert :ok =
             :ferricstore_waraft_spike_segment_log.write_projection(
               to_charlist(projection_root),
               {:raft_log_pos, position_index, 0},
               [{"flow:key", "payload", 0}]
             )

    assert {:ok, "payload"} =
             WARaftSegmentReader.read_value_from_location(
               %{data_dir: data_dir},
               0,
               {:waraft_projection, projection_index},
               "flow:key"
             )

    [segment_path] = Path.wildcard(Path.join([projection_root, "segment_log", "*.seg"]))
    File.write!(segment_path, "corrupt")

    assert {:error, :projection_entry_missing_at_recorded_location} =
             WARaftSegmentReader.read_value_from_location(
               %{data_dir: data_dir},
               0,
               {:waraft_projection, projection_index},
               "flow:key"
             )
  end
end
