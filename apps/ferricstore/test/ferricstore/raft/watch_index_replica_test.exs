defmodule Ferricstore.Raft.WatchIndexReplicaTest do
  @moduledoc """
  Every replica applies a watched EXEC and must reach the leader's decision.
  Two state machines apply the same entries here while their storage differs
  the way independent nodes' does: a value one node relocated when it trimmed
  its log sits at another location on the other node.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.Transaction.WatchIndex

  @key "{watch-replica}key"
  @other "{watch-replica}other"

  setup do
    replicas = [start_replica(), start_replica()]
    {:ok, replicas: replicas}
  end

  describe "from feature level 5" do
    setup %{replicas: replicas} do
      apply_all(replicas, 1, {:put, FeatureLevel.key(5), FeatureLevel.marker(), 0})
      apply_all(replicas, 2, {:put, @key, "v", 0})
      [leader, follower] = replicas
      relocate!(leader, @key, {:waraft_segment, 2})
      relocate!(follower, @key, {:waraft_projection, 9})
      :ok
    end

    test "replicas whose stored locations differ agree on a clean EXEC", %{replicas: replicas} do
      token = watch!(replicas, 3, @key)

      assert exec(replicas, 4, %{@key => token}) == [[], []]
    end

    test "a write to another key leaves the EXEC clean on every replica", %{replicas: replicas} do
      token = watch!(replicas, 3, @key)
      apply_all(replicas, 4, {:put, @other, "x", 0})

      assert exec(replicas, 5, %{@key => token}) == [[], []]
    end

    test "a write to the watched key aborts the EXEC on every replica", %{replicas: replicas} do
      token = watch!(replicas, 3, @key)
      apply_all(replicas, 4, {:put, @key, "changed", 0})

      assert exec(replicas, 5, %{@key => token}) == [nil, nil]
    end

    test "rewriting the same value aborts the EXEC on every replica", %{replicas: replicas} do
      token = watch!(replicas, 3, @key)
      apply_all(replicas, 4, {:put, @key, "v", 0})

      assert exec(replicas, 5, %{@key => token}) == [nil, nil]
    end

    test "creating a watched missing key aborts the EXEC", %{replicas: replicas} do
      token = watch!(replicas, 3, @other)
      apply_all(replicas, 4, {:put, @other, "new", 0})

      assert exec(replicas, 5, %{@other => token}) == [nil, nil]
    end

    test "a watched key that expires before EXEC aborts it", %{replicas: replicas} do
      apply_all(replicas, 3, {:put, @other, "short", 5_000})
      token = watch!(replicas, 4, @other)

      assert exec(replicas, 5, %{@other => token}, 6_000) == [nil, nil]
    end

    test "a WATCH older than the index row's lifetime aborts the EXEC", %{replicas: replicas} do
      token = watch!(replicas, 3, @key)

      assert exec(replicas, 4, %{@key => token}, 1_000 + WatchIndex.row_ttl_ms() + 1) ==
               [nil, nil]
    end
  end

  test "below feature level 5 WATCH writes no index rows", %{replicas: [leader | _] = replicas} do
    apply_all(replicas, 1, {:put, @key, "v", 0})
    _token = watch!(replicas, 2, @key)

    assert :ets.lookup(leader.keydir, WatchIndex.row_key(@key)) == []
    assert :ets.lookup(leader.keydir, WatchIndex.in_use_key()) == []
  end

  defp watch!(replicas, index, key) do
    [tokens | _follower_tokens] = apply_all(replicas, index, {:watch_tokens, [key]})
    Map.fetch!(tokens, key)
  end

  defp exec(replicas, index, watched_keys, now_ms \\ 1_000),
    do: apply_all(replicas, index, {:tx_execute, [], nil, watched_keys}, now_ms)

  defp apply_all(replicas, index, command, now_ms \\ 1_000) do
    Enum.map(replicas, fn replica ->
      meta = %{index: index, system_time: now_ms}
      state = Agent.get(replica.state, & &1)

      {new_state, result} =
        case StateMachine.apply_waraft_storage_command(command, meta, state) do
          {new_state, result} -> {new_state, result}
          {new_state, result, _effects} -> {new_state, result}
        end

      Agent.update(replica.state, fn _old -> new_state end)
      unwrap_applied(result)
    end)
  end

  defp unwrap_applied({:applied_at, _index, result}), do: result
  defp unwrap_applied(result), do: result

  # A trimmed log moves a value to a projection file on its own schedule.
  defp relocate!(replica, key, file_id) do
    [{^key, value, expire_at_ms, lfu, _file_id, offset, size}] = :ets.lookup(replica.keydir, key)
    true = :ets.insert(replica.keydir, {key, value, expire_at_ms, lfu, file_id, offset, size})
  end

  defp start_replica do
    shard_index = 40_000 + System.unique_integer([:positive])
    root = Path.join(System.tmp_dir!(), "watch-index-replica-#{shard_index}")
    shard_path = Ferricstore.DataDir.shard_data_path(root, shard_index)
    path = Path.join(shard_path, "00000.log")
    File.mkdir_p!(shard_path)
    File.touch!(path)

    keydir = :ets.new(:watch_index_replica, [:set, :public])

    state =
      StateMachine.init(%{
        shard_index: shard_index,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: path,
        ets: keydir
      })

    {:ok, writer} = Ferricstore.Store.BitcaskWriter.start_link(shard_index: shard_index)
    {:ok, agent} = Agent.start_link(fn -> state end)

    on_exit(fn ->
      for pid <- [writer, agent], Process.alive?(pid) do
        try do
          GenServer.stop(pid)
        catch
          :exit, _reason -> :ok
        end
      end

      _ = LMDB.release(state.flow_lmdb_path)
      File.rm_rf!(root)
    end)

    %{state: agent, keydir: keydir}
  end
end
