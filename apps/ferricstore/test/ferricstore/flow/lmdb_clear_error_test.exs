defmodule Ferricstore.Flow.LMDBClearErrorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF

  test "a failed LMDB clear names the step that failed" do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_clear_error_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    # A directory where LMDB expects its data file makes opening the environment fail.
    File.mkdir_p!(Path.join(path, "data.mdb"))

    assert {:error, message} = NIF.lmdb_clear(path, Ferricstore.Flow.LMDB.map_size())
    assert message =~ ~r/^lmdb clear: open environment: /
  end
end
