defmodule Ferricstore.Store.IoUringIntegrationTest do
  @moduledoc """
  Linux io_uring integration coverage for the async Bitcask write path.

  The test is excluded from default `mix test` by the `:linux_io_uring` tag.
  CI runs it only after the loaded NIF reports that io_uring is available on
  the Linux runner.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  @moduletag :linux_io_uring

  test "async append and fsync persist records when io_uring is available" do
    assert {:unix, :linux} == :os.type()
    assert NIF.io_uring_available()

    dir =
      Path.join(System.tmp_dir!(), "ferricstore_io_uring_#{System.unique_integer([:positive])}")

    path = Path.join(dir, "00000000000000000001.data")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)

    batch = [{"uring:key:1", "uring:value:1", 0}, {"uring:key:2", "uring:value:2", 0}]

    :ok = NIF.v2_append_batch_async(self(), 1, path, batch)

    assert_receive {:tokio_complete, 1, :ok, locations}, 5_000
    assert [{off1, 13}, {off2, 13}] = locations
    assert off2 > off1

    :ok = NIF.v2_fsync_async(self(), 2, path)
    assert_receive {:tokio_complete, 2, :ok, :ok}, 5_000

    assert {:ok, "uring:value:1"} = NIF.v2_pread_at(path, off1)
    assert {:ok, "uring:value:2"} = NIF.v2_pread_at(path, off2)
  end
end
