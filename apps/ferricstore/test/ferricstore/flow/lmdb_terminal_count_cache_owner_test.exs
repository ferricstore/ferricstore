defmodule Ferricstore.Flow.LMDB.TerminalCountCacheOwnerTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDB.TerminalCountCacheOwner
  alias Ferricstore.Flow.LMDB.TerminalCounts

  test "short-lived cache writers cannot become the terminal-count table owner" do
    table = TerminalCountCacheOwner.table_name()
    owner = Process.whereis(TerminalCountCacheOwner)

    assert is_pid(owner)
    assert Process.alive?(owner)
    assert :ets.info(table, :owner) == owner

    path = "terminal-count-owner:" <> Integer.to_string(System.unique_integer([:positive]))
    count_key = "terminal-count-key"

    task =
      Task.async(fn ->
        assert :ok = TerminalCounts.put_cached_count_key(path, count_key, 17)
        :ets.info(table, :owner)
      end)

    assert Task.await(task) == owner

    assert :ets.info(table, :owner) == owner
    assert :ets.lookup(table, {path, count_key}) == [{{path, count_key}, 17}]
  end

  test "concurrent cache misses retain one stable supervised owner" do
    table = TerminalCountCacheOwner.table_name()
    owner = Process.whereis(TerminalCountCacheOwner)
    path = "terminal-count-race:" <> Integer.to_string(System.unique_integer([:positive]))

    1..64
    |> Task.async_stream(
      fn index ->
        TerminalCounts.put_cached_count_key(path, "key-#{index}", index)
      end,
      max_concurrency: 64,
      ordered: false,
      timeout: 5_000
    )
    |> Enum.each(fn result -> assert result == {:ok, :ok} end)

    assert is_pid(owner)
    assert :ets.info(table, :owner) == owner
    assert :ets.info(table, :size) >= 64
  end

  test "supervision recreates the cache after the owner exits" do
    table = TerminalCountCacheOwner.table_name()
    old_owner = Process.whereis(TerminalCountCacheOwner)
    monitor = Process.monitor(old_owner)

    Process.exit(old_owner, :kill)

    assert_receive {:DOWN, ^monitor, :process, ^old_owner, :killed}, 5_000

    restarted_owner = await_restarted_owner(old_owner, 100)
    assert is_pid(restarted_owner)
    assert :ets.info(table, :owner) == restarted_owner

    assert :ok = TerminalCounts.put_cached_count_key("restarted-owner", "count-key", 23)

    assert :ets.lookup(table, {"restarted-owner", "count-key"}) ==
             [{{"restarted-owner", "count-key"}, 23}]
  end

  defp await_restarted_owner(_old_owner, 0), do: nil

  defp await_restarted_owner(old_owner, attempts) do
    case Process.whereis(TerminalCountCacheOwner) do
      owner when is_pid(owner) and owner != old_owner ->
        owner

      _missing_or_old ->
        Process.sleep(10)
        await_restarted_owner(old_owner, attempts - 1)
    end
  end
end
