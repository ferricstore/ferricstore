defmodule Ferricstore.Flow.InfoCountReadTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.InfoCountRead

  test "zset_count_many returns empty counts without router access" do
    assert InfoCountRead.zset_count_many(:ctx, []) == {:ok, []}
  end

  test "terminal_lmdb_counts skips cold counts when disabled" do
    assert InfoCountRead.terminal_lmdb_counts(
             :ctx,
             [{"completed", "flow:index:type:partition:completed"}],
             "partition",
             false,
             true,
             ["completed"]
           ) == {:ok, %{}}
  end

  test "terminal_lmdb_counts skips LMDB when no terminal states match" do
    ctx = %{name: :info_count_read_test, shard_count: 1, data_dir: "/unused"}

    assert InfoCountRead.terminal_lmdb_counts(
             ctx,
             [{"queued", "flow:index:type:partition:queued"}],
             "partition",
             true,
             false,
             ["completed"]
           ) == {:ok, %{}}
  end
end
