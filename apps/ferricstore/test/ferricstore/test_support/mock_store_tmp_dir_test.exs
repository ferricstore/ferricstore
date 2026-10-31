defmodule Ferricstore.Test.MockStoreTmpDirTest do
  @moduledoc """
  The mock stores' temp directories go with the test that made them: every
  `MockStore.make/1` and `ProbMockStore.make_*` call used to leave one behind
  in the system temp directory, about 125,000 of them over many runs.
  """

  use ExUnit.Case, async: true

  import Ferricstore.Test.Eventually, only: [eventually: 1]

  alias Ferricstore.Test.{MockStore, ProbMockStore}

  test "a mock store's directories go when the process that made them exits" do
    {pid, dirs} =
      make_in_process(fn ->
        mock = MockStore.make()
        mock_dir = mock.bloom_registry.path.("k") |> Path.dirname() |> Path.dirname()
        prob_dirs = for make <- [:make_cuckoo, :make_cms, :make_bloom], do: prob_dir(make)
        [mock_dir | prob_dirs]
      end)

    assert length(dirs) == 4
    assert Enum.all?(dirs, &File.dir?/1)

    send(pid, :exit)
    assert eventually(fn -> not Enum.any?(dirs, &File.exists?/1) end)
  end

  test "a store a Task makes belongs to the test that started the Task" do
    dir = Task.async(fn -> prob_dir(:make_bloom) end) |> Task.await()

    Process.sleep(100)
    assert File.dir?(dir)
  end

  defp prob_dir(make), do: apply(ProbMockStore, make, []).prob_dir.()

  defp make_in_process(fun) do
    parent = self()

    pid =
      spawn(fn ->
        send(parent, {:dirs, fun.()})

        receive do
          :exit -> :ok
        end
      end)

    receive do
      {:dirs, dirs} -> {pid, dirs}
    after
      5_000 -> flunk("the store was not made")
    end
  end
end
