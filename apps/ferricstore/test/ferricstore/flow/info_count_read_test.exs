defmodule Ferricstore.Flow.InfoCountReadTest do
  use ExUnit.Case, async: false
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

  test "terminal sweep limit rejects invalid configuration instead of disabling cleanup" do
    previous = Application.get_env(:ferricstore, :flow_lmdb_terminal_sweep_limit)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:ferricstore, :flow_lmdb_terminal_sweep_limit)
      else
        Application.put_env(:ferricstore, :flow_lmdb_terminal_sweep_limit, previous)
      end
    end)

    Application.put_env(:ferricstore, :flow_lmdb_terminal_sweep_limit, 17)
    assert InfoCountRead.terminal_lmdb_sweep_limit() == 17

    for invalid <- [0, -1, "invalid", nil] do
      Application.put_env(:ferricstore, :flow_lmdb_terminal_sweep_limit, invalid)
      assert InfoCountRead.terminal_lmdb_sweep_limit() == 10_000
    end
  end
end
