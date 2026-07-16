defmodule Ferricstore.BitcaskTombstonePageTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF

  @header_size 26

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-tombstone-page-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    path = Path.join(dir, "00000000000000000001.log")
    File.touch!(path)
    on_exit(fn -> File.rm_rf!(dir) end)

    %{path: path}
  end

  test "pages by physical records and returns an exact EOF cursor", %{path: path} do
    assert {:ok,
            [
              {:put, 0, _},
              {:delete, deleted_1_offset, deleted_1_size},
              {:put, second_page_offset, _},
              {:delete, deleted_2_offset, deleted_2_size}
            ]} =
             NIF.v2_append_ops_batch_nosync(path, [
               {:put, "live-1", "value-1", 11},
               {:delete, "deleted-1"},
               {:put, "live-2", "value-2", 22},
               {:delete, "deleted-2"}
             ])

    assert {:ok, [{"deleted-1", ^deleted_1_offset, ^deleted_1_size, 0}],
            ^second_page_offset, false} = NIF.v2_scan_tombstones_page(path, 0, 2)

    file_len = File.stat!(path).size

    assert {:ok, [{"deleted-2", ^deleted_2_offset, ^deleted_2_size, 0}], ^file_len, true} =
             NIF.v2_scan_tombstones_page(path, second_page_offset, 2)

    assert {:ok, [], ^file_len, true} = NIF.v2_scan_tombstones_page(path, file_len, 2)
  end

  test "rejects zero, oversized, and past-EOF page bounds", %{path: path} do
    assert {:error, "max_records must be positive"} =
             NIF.v2_scan_tombstones_page(path, 0, 0)

    assert {:error, "max_records exceeds maximum 65536"} =
             NIF.v2_scan_tombstones_page(path, 0, 65_537)

    assert {:error, reason} = NIF.v2_scan_tombstones_page(path, 1, 1)
    assert reason =~ "start_offset 1 exceeds file length 0"
  end

  test "returns no partial page when a scanned live record is corrupt", %{path: path} do
    assert {:ok, [{:delete, _deleted_offset, _}, {:put, live_offset, _}]} =
             NIF.v2_append_ops_batch_nosync(path, [
               {:delete, "deleted"},
               {:put, "live", "value", 0}
             ])

    value_offset = live_offset + @header_size + byte_size("live")
    {:ok, file} = :file.open(path, [:read, :write, :binary])
    :ok = :file.pwrite(file, value_offset, <<0xFF>>)
    :ok = :file.close(file)

    assert {:error, reason} = NIF.v2_scan_tombstones_page(path, 0, 2)
    assert reason =~ "CRC mismatch"
  end
end
