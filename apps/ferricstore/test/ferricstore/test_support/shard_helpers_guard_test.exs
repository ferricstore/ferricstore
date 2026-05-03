defmodule Ferricstore.TestSupport.ShardHelpersGuardTest do
  use ExUnit.Case, async: true

  test "flush_all_shards uses the ActiveFile registry for the current active log" do
    source = File.read!("test/support/shard_helpers.ex")

    assert source =~ "Ferricstore.Store.ActiveFile.get",
           "flush_all_shards/0 must fsync the currently published active file"

    refute source =~ ":ferricstore_active_file_path",
           "flush_all_shards/0 must not use the retired persistent_term active-file key"
  end
end
