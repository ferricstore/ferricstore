defmodule Ferricstore.Test.LMDBFixture do
  @moduledoc false

  @release_timeout_ms 30_000

  @doc false
  def cleanup_data_dir!(data_dir, shard_index \\ 0)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    lmdb_path =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    if File.dir?(lmdb_path) do
      :ok = Ferricstore.Flow.LMDB.release(lmdb_path, @release_timeout_ms)
    end

    File.rm_rf!(data_dir)
    :ok
  end
end
