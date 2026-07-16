defmodule Ferricstore.Raft.FlushShardApplyTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.SharedRefBackfill
  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.ServerCatalog
  alias Ferricstore.Store.BitcaskWriter
  alias Ferricstore.Store.LFU

  test "replicated shard flush streams durable tombstones and preserves control-plane rows" do
    %{state: state, keydir: keydir, path: path} = start_state()

    keys = Enum.map(1..1_025, &"flush-shard:key:#{&1}")

    Enum.each(keys, fn key ->
      true = :ets.insert(keydir, {key, "value", 0, LFU.initial(), 0, 0, 5})
    end)

    catalog_key = ServerCatalog.entry_key("acl", "default")
    watermark_key = Keys.shared_value_ref_backfill_key(state.shard_index)
    progress_key = SharedRefBackfill.progress_key(state.shard_index)

    for key <- [catalog_key, watermark_key, progress_key] do
      true = :ets.insert(keydir, {key, "control", 0, LFU.initial(), 0, 0, 7})
    end

    assert {_new_state, {:ok, 1_025}} =
             StateMachine.apply(%{}, {:flush_shard, {1, 0}}, state)

    assert Enum.all?(keys, &(:ets.lookup(keydir, &1) == []))
    assert [{^catalog_key, "control", 0, _, 0, 0, 7}] = :ets.lookup(keydir, catalog_key)
    assert [{^watermark_key, "control", 0, _, 0, 0, 7}] = :ets.lookup(keydir, watermark_key)
    assert [{^progress_key, "control", 0, _, 0, 0, 7}] = :ets.lookup(keydir, progress_key)

    assert {:ok, tombstones} = NIF.v2_scan_tombstones(path)
    assert length(tombstones) == length(keys)
  end

  test "replicated shard flush clears fetch-or-compute locks before deleting rows" do
    %{state: state, keydir: keydir} = start_state()
    key = "flush-shard:locked"
    true = :ets.insert(keydir, {key, "value", 0, LFU.initial(), 0, 0, 5})

    locked_state = %{
      state
      | fetch_or_compute_locks: %{key => {make_ref(), Ferricstore.HLC.now_ms() + 60_000}}
    }

    assert {new_state, {:ok, 1}} =
             StateMachine.apply(%{}, {:flush_shard, {1, 0}}, locked_state)

    assert new_state.applied_count == locked_state.applied_count + 1
    assert new_state.fetch_or_compute_locks == %{}
    assert [] == :ets.lookup(keydir, key)
  end

  defp start_state do
    shard_index = 20_000 + System.unique_integer([:positive])
    root = Path.join(System.tmp_dir!(), "flush-shard-#{shard_index}")
    shard_path = Ferricstore.DataDir.shard_data_path(root, shard_index)
    path = Path.join(shard_path, "00000.log")
    File.mkdir_p!(shard_path)
    File.touch!(path)

    keydir = :ets.new(:flush_shard_apply, [:set, :public])

    state =
      StateMachine.init(%{
        shard_index: shard_index,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: path,
        ets: keydir
      })

    {:ok, writer} = BitcaskWriter.start_link(shard_index: shard_index)

    on_exit(fn ->
      if Process.alive?(writer) do
        try do
          GenServer.stop(writer)
        catch
          :exit, _reason -> :ok
        end
      end

      if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
      File.rm_rf!(root)
    end)

    %{state: state, keydir: keydir, path: path}
  end
end
