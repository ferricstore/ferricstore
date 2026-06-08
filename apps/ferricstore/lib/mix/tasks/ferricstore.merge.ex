defmodule Mix.Tasks.Ferricstore.Merge do
  @moduledoc """
  Triggers a manual merge/compaction check on one or all FerricStore shards.

  ## Usage

      mix ferricstore.merge [shard_index]

  ## Arguments

    * `shard_index` (optional) -- zero-based index of the shard to compact.
      Must be in the range `0..(shard_count - 1)`. When omitted, triggers a
      merge check on all shards.

  ## Behaviour

  Calls `Ferricstore.Merge.Scheduler.trigger_check/1` which runs the merge
  decision logic synchronously: check fragmentation, acquire the semaphore,
  and run compaction if warranted. If the shard does not need compaction
  (fragmentation below threshold, not enough files, etc.), the check
  completes as a no-op.

  ## Examples

      # Trigger merge on shard 0
      mix ferricstore.merge 0

      # Trigger merge on all shards
      mix ferricstore.merge
  """

  use Mix.Task

  @shortdoc "Trigger manual merge/compaction on a shard"

  @doc """
  Runs the merge task.

  ## Parameters

    * `args` -- command-line arguments. If the first element is a valid
      shard index, triggers merge on that shard only. Otherwise triggers
      on all shards.

  """
  @spec run(list()) :: :ok
  @impl Mix.Task
  def run([]) do
    ensure_started()

    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    Enum.each(0..(shard_count - 1), fn i ->
      trigger_shard(i)
    end)

    :ok
  end

  def run([shard_str | _]) do
    ensure_started()

    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    case Integer.parse(shard_str) do
      {index, ""} when index >= 0 and index < shard_count ->
        trigger_shard(index)

      {index, ""} ->
        Mix.shell().info(
          "ERROR: Invalid shard index #{index}. " <>
            "Must be 0..#{shard_count - 1}."
        )

      _ ->
        Mix.shell().info(
          "ERROR: Invalid shard argument \"#{shard_str}\". " <>
            "Expected a numeric shard index (0..#{shard_count - 1})."
        )
    end

    :ok
  end

  defp trigger_shard(index) do
    scheduler = Ferricstore.Merge.Scheduler.scheduler_name(index)

    case Process.whereis(scheduler) do
      pid when is_pid(pid) ->
        Ferricstore.Merge.Scheduler.trigger_check(index)
        status = Ferricstore.Merge.Scheduler.status(index)

        Mix.shell().info(
          "Merge check completed for shard #{index}: " <>
            "mode=#{status.mode} merging=#{status.merging} " <>
            "merge_count=#{status.merge_count}"
        )

      nil ->
        Mix.shell().info("Merge scheduler for shard #{index} is not running.")
    end
  end

  defp ensure_started do
    Mix.Task.run("app.start")
  end
end
