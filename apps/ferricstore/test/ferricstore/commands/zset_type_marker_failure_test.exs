defmodule Ferricstore.Commands.ZSetTypeMarkerFailureTest do
  @moduledoc """
  On the instance, ZINCRBY (at every cluster feature level) and ZADD with a
  condition (below level 4) claim a new sorted set's type marker and then
  write the member as a separate replicated write. When that write fails,
  deleting the marker they claimed is no undo when the write may have
  committed (a timeout's outcome is unknown) or another client has added
  members under the marker since: those members would be left without one.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  @hook :ferricstore_quorum_write_hook

  setup do
    on_exit(fn -> Process.delete(@hook) end)

    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{zset-type-marker:#{System.unique_integer([:positive])}}"
    }
  end

  # Runs `fun` in place of the first replicated write `match?` accepts.
  defp hook_write(match?, fun) do
    Process.put(@hook, fn command, write ->
      if match?.(command) and Process.get(:hooked_write_done) != true do
        Process.put(:hooked_write_done, true)
        fun.(write)
      else
        write.()
      end
    end)

    on_exit(fn -> Process.delete(:hooked_write_done) end)
  end

  defp committed_then_timed_out(write) do
    _ = write.()
    {:error, {:timeout, :unknown_outcome}}
  end

  test "ZINCRBY whose increment commits but times out keeps the set's marker",
       %{ctx: ctx, key: key} do
    hook_write(&(elem(&1, 0) == :zincrby), &committed_then_timed_out/1)

    assert {:error, {:timeout, :unknown_outcome}} =
             Dispatcher.dispatch("ZINCRBY", [key, "1", "m"], ctx)

    Process.delete(@hook)

    assert "1" == Dispatcher.dispatch("ZSCORE", [key, "m"], ctx)
    assert 1 == Dispatcher.dispatch("EXISTS", [key], ctx)
    assert {:simple, "zset"} == Dispatcher.dispatch("TYPE", [key], ctx)
  end

  test "ZADD NX whose member write commits but times out keeps the set's marker",
       %{ctx: ctx, key: key} do
    hook_write(&(elem(&1, 0) == :compound_batch_put), &committed_then_timed_out/1)

    assert {:error, {:timeout, :unknown_outcome}} =
             Dispatcher.dispatch("ZADD", [key, "NX", "1", "m"], ctx)

    Process.delete(@hook)

    assert "1" == Dispatcher.dispatch("ZSCORE", [key, "m"], ctx)
    assert 1 == Dispatcher.dispatch("EXISTS", [key], ctx)
  end

  test "a ZADD XX that adds nothing keeps the marker another client added a member under",
       %{ctx: ctx, key: key} do
    hook_write(&(elem(&1, 0) == :compound_type_claim), fn claim ->
      result = claim.()

      assert 1 =
               Task.async(fn -> Dispatcher.dispatch("ZADD", [key, "2", "other"], ctx) end)
               |> Task.await()

      result
    end)

    assert 0 == Dispatcher.dispatch("ZADD", [key, "XX", "1", "m"], ctx)

    Process.delete(@hook)

    assert "2" == Dispatcher.dispatch("ZSCORE", [key, "other"], ctx)
    assert 1 == Dispatcher.dispatch("EXISTS", [key], ctx)
  end

  test "a failed ZADD NX keeps the marker another client's members were added under",
       %{ctx: ctx, key: key} do
    hook_write(&(elem(&1, 0) == :compound_batch_put), fn _write ->
      assert 1 =
               Task.async(fn -> Dispatcher.dispatch("ZADD", [key, "2", "other"], ctx) end)
               |> Task.await()

      {:error, "ERR storage write failed"}
    end)

    assert {:error, "ERR storage write failed"} =
             Dispatcher.dispatch("ZADD", [key, "NX", "1", "m"], ctx)

    Process.delete(@hook)

    assert "2" == Dispatcher.dispatch("ZSCORE", [key, "other"], ctx)
    assert 1 == Dispatcher.dispatch("EXISTS", [key], ctx)
    assert {:simple, "zset"} == Dispatcher.dispatch("TYPE", [key], ctx)
  end
end
