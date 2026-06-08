defmodule Ferricstore.Flow.LMDBWriter.ShardsTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.LMDBWriter.Shards

  test "indexes returns all shard indexes" do
    assert Shards.indexes(0) == []
    assert Enum.to_list(Shards.indexes(4)) == [0, 1, 2, 3]
  end

  test "flush_all_concurrency is bounded by shard count and scheduler cap" do
    assert Shards.flush_all_concurrency(0) == 1
    assert Shards.flush_all_concurrency(1) == 1
    assert Shards.flush_all_concurrency(100) <= 16
    assert Shards.flush_all_concurrency(100) <= System.schedulers_online()
  end

  test "merge_flush_all_result keeps first failure" do
    assert Shards.merge_flush_all_result({:ok, {0, :ok}}, :ok) == :ok
    assert Shards.merge_flush_all_result({:ok, {0, {:error, :failed}}}, :ok) == {:error, :failed}

    assert Shards.merge_flush_all_result({:ok, {1, {:error, :later}}}, {:error, :failed}) ==
             {:error, :failed}

    assert Shards.merge_flush_all_result({:exit, :timeout}, :ok) ==
             {:error, {:flush_task_exit, :timeout}}

    assert Shards.merge_flush_all_result({:exit, :timeout}, {:error, :failed}) ==
             {:error, :failed}
  end
end
