defmodule Ferricstore.Store.AvailableDiskSpaceGuardTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF

  test "available disk space uses the Bitcask statvfs NIF instead of shelling out in the shard" do
    assert function_exported?(NIF, :v2_available_disk_space, 1)
    assert {:ok, bytes} = NIF.v2_available_disk_space(System.tmp_dir!())
    assert is_integer(bytes) and bytes > 0

    shard_source =
      [
        "../../../lib/ferricstore/store/shard.ex",
        "../../../lib/ferricstore/store/shard/startup.ex",
        "../../../lib/ferricstore/store/shard/calls.ex",
        "../../../lib/ferricstore/store/shard/routing.ex",
        "../../../lib/ferricstore/store/shard/compaction.ex",
        "../../../lib/ferricstore/store/shard/info.ex"
      ]
      |> Enum.map_join("\n", fn path ->
        path
        |> Path.expand(__DIR__)
        |> File.read!()
      end)

    refute shard_source =~ ~s(System.cmd("df"),
           "available_disk_space must not block the shard GenServer by spawning df"
  end
end
