defmodule Ferricstore.Commands.ListGeoTypeMarkerFailureTest do
  @moduledoc """
  On the instance, LPUSH and RPUSH (at every cluster feature level) and GEOADD
  with a condition (below level 4) claim a new key's type marker and then
  write its elements as a separate replicated write. A write that timed out
  may have committed, so deleting the claimed marker afterwards left the
  elements without one.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  @hook :ferricstore_quorum_write_hook

  setup do
    on_exit(fn -> Process.delete(@hook) end)

    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{type-marker-failure:#{System.unique_integer([:positive])}}"
    }
  end

  # Lets the first replicated write `match?` accepts commit, then reports an
  # unknown outcome for it, as a timeout does.
  defp commit_then_time_out(match?) do
    Process.put(@hook, fn command, write ->
      if match?.(command) and Process.get(:hooked_write_done) != true do
        Process.put(:hooked_write_done, true)
        _ = write.()
        {:error, {:timeout, :unknown_outcome}}
      else
        write.()
      end
    end)

    on_exit(fn -> Process.delete(:hooked_write_done) end)
  end

  test "LPUSH whose push commits but times out keeps the list's marker", %{ctx: ctx, key: key} do
    commit_then_time_out(&(elem(&1, 0) == :list_op))

    assert {:error, {:timeout, :unknown_outcome}} = Dispatcher.dispatch("LPUSH", [key, "a"], ctx)
    Process.delete(@hook)

    assert ["a"] == Dispatcher.dispatch("LRANGE", [key, "0", "-1"], ctx)
    assert 1 == Dispatcher.dispatch("EXISTS", [key], ctx)
    assert {:simple, "list"} == Dispatcher.dispatch("TYPE", [key], ctx)
  end

  test "GEOADD NX whose member write commits but times out keeps the key's marker",
       %{ctx: ctx, key: key} do
    commit_then_time_out(&(elem(&1, 0) == :compound_batch_put))

    assert {:error, {:timeout, :unknown_outcome}} =
             Dispatcher.dispatch("GEOADD", [key, "NX", "13.361389", "38.115556", "palermo"], ctx)

    Process.delete(@hook)

    assert [[_lng, _lat]] = Dispatcher.dispatch("GEOPOS", [key, "palermo"], ctx)
    assert 1 == Dispatcher.dispatch("EXISTS", [key], ctx)
  end
end
