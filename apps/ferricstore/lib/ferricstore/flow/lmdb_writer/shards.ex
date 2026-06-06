defmodule Ferricstore.Flow.LMDBWriter.Shards do
  @moduledoc false

  def indexes(0), do: []
  def indexes(shard_count), do: 0..(shard_count - 1)

  def flush_all_concurrency(0), do: 1

  def flush_all_concurrency(shard_count) do
    min(shard_count, max(1, min(System.schedulers_online(), 16)))
  end

  def flush_all_task_timeout(timeout), do: timeout + 1_000

  def merge_flush_all_result({:ok, {_shard_index, :ok}}, acc), do: acc

  def merge_flush_all_result({:ok, {_shard_index, {:error, _reason} = error}}, :ok),
    do: error

  def merge_flush_all_result({:ok, {_shard_index, {:error, _reason}}}, acc), do: acc

  def merge_flush_all_result({:exit, reason}, :ok), do: {:error, {:flush_task_exit, reason}}

  def merge_flush_all_result({:exit, _reason}, acc), do: acc
end
