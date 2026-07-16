defmodule Ferricstore.Commands.ProbabilisticCreateRollbackTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}
  alias Ferricstore.Test.MockStore

  @commands [
    {Bloom, "BF.RESERVE", ["bloom", "0.01", "100"], "bloom", "bloom"},
    {CMS, "CMS.INITBYDIM", ["cms", "100", "5"], "cms", "cms"},
    {Cuckoo, "CF.RESERVE", ["cuckoo", "100"], "cuckoo", "cuckoo"},
    {TopK, "TOPK.RESERVE", ["topk", "5"], "topk", "topk"}
  ]

  test "metadata registration failure removes every newly-created sidecar" do
    store = MockStore.make()
    prob_dir = store.prob_dir.()
    failing_store = %{store | put: fn _key, _value, _expiry -> {:error, :metadata_failed} end}

    for {module, command, args, key, extension} <- @commands do
      assert {:error, :metadata_failed} == module.handle(command, args, failing_store)
      refute File.exists?(prob_path(prob_dir, key, extension))
    end
  end

  test "directory fsync failure removes every newly-created sidecar" do
    store = MockStore.make()
    prob_dir = store.prob_dir.()
    Process.put(:ferricstore_prob_command_fsync_dir_hook, fn _path -> {:error, :eio} end)
    on_exit(fn -> Process.delete(:ferricstore_prob_command_fsync_dir_hook) end)

    for {module, command, args, key, extension} <- @commands do
      assert {:error, {:fsync_dir_failed, :prob_file_dir, :eio}} ==
               module.handle(command, args, store)

      refute File.exists?(prob_path(prob_dir, key, extension))
    end
  end

  defp prob_path(prob_dir, key, extension) do
    Ferricstore.ProbFile.path(prob_dir, key, extension)
  end
end
