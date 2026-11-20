defmodule Ferricstore.Raft.ApplyProjectionBatchDeleteTest do
  @moduledoc """
  An apply that overwrites keys whose previous values sit in the
  apply-projection cache drops those cache entries once per entry, not once
  per key: each drop takes the cache lock and updates its counters.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Store.BitcaskWriter

  setup do
    shard_index = 9000 + :rand.uniform(999)
    root = Path.join(System.tmp_dir!(), "apc_batch_delete_#{:rand.uniform(9_999_999)}")
    dir = Ferricstore.DataDir.shard_data_path(root, shard_index)
    File.mkdir_p!(dir)
    active_file_path = Path.join(dir, "00000.log")
    File.touch!(active_file_path)

    keydir_name = :"apc_batch_delete_keydir_#{:rand.uniform(9_999_999)}"
    :ets.new(keydir_name, [:set, :public, :named_table])

    state =
      StateMachine.init(%{
        shard_index: shard_index,
        shard_data_path: dir,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir_name
      })
      |> Map.put(:data_dir, root)

    Ferricstore.Store.Shard.CompoundMemberIndex.ensure_table!(state.compound_member_index_name)
    {:ok, writer_pid} = BitcaskWriter.start_link(shard_index: shard_index)

    on_exit(fn ->
      try do
        if Process.alive?(writer_pid), do: GenServer.stop(writer_pid, :normal, 5_000)
      catch
        :exit, _reason -> :ok
      end

      WARaftSegmentReader.clear_apply_projection_cache(root, shard_index)

      try do
        :ets.delete(keydir_name)
      rescue
        ArgumentError -> :ok
      end

      Ferricstore.Test.LMDBFixture.cleanup_data_dir!(root, shard_index)
    end)

    %{state: state, root: root, shard_index: shard_index, ets: keydir_name}
  end

  test "overwriting cached keys in one entry drops their cache entries in one call", %{
    state: state,
    root: root,
    shard_index: shard_index,
    ets: ets
  } do
    keys = for i <- 1..20, do: "apc-batch-delete:#{i}"

    assert {state, {:applied_at, 1, _results}, _effects} =
             StateMachine.apply_waraft_segment_command(
               {:batch, Enum.map(keys, &{:put, &1, "v1", 0})},
               %{index: 1, term: 1},
               state,
               projection_writer(1)
             )

    for key <- keys do
      assert [{^key, _value, 0, _lfu, {:waraft_apply_projection, 1}, _offset, _size}] =
               :ets.lookup(ets, key)
    end

    :ok =
      WARaftSegmentReader.put_apply_projection(
        root,
        shard_index,
        1,
        Enum.map(keys, &{&1, "v1", 0})
      )

    assert WARaftSegmentReader.apply_projection_cache_count(root, shard_index) == 20

    mfa = {WARaftSegmentReader, :delete_apply_projection_entries, 3}
    :erlang.trace_pattern(mfa, true, [:call_count])
    :erlang.trace(self(), true, [:call])

    try do
      assert {_state, {:applied_at, 2, _results}, _effects} =
               StateMachine.apply_waraft_segment_command(
                 {:batch, Enum.map(keys, &{:put, &1, "v2", 0})},
                 %{index: 2, term: 1},
                 state,
                 projection_writer(2)
               )

      assert {:call_count, 1} = :erlang.trace_info(mfa, :call_count)
    after
      :erlang.trace(self(), false, [:call])
      :erlang.trace_pattern(mfa, false, [:call_count])
    end

    assert WARaftSegmentReader.apply_projection_cache_count(root, shard_index) == 0

    for key <- keys do
      assert [{^key, _value, 0, _lfu, {:waraft_apply_projection, 2}, _offset, _size}] =
               :ets.lookup(ets, key)
    end
  end

  defp projection_writer(index) do
    fn batch ->
      locations =
        batch
        |> Enum.with_index()
        |> Enum.map(fn {{:put, _key, value, _expire_at_ms}, i} ->
          {:put, i * 64, byte_size(value)}
        end)

      {:ok, {:waraft_apply_projection, index}, locations}
    end
  end
end
