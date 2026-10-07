defmodule Ferricstore.Flow.LMDBRebuilder.BatchDurabilityTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, Locator}
  alias Ferricstore.Flow.LMDBRebuilder.ColdState
  alias Ferricstore.Raft.WARaftSegmentReader, as: Reader

  setup do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(8), padding: false)
    root = Path.join(System.tmp_dir!(), "flow-batch-durability-#{System.pid()}-#{suffix}")
    previous = Application.fetch_env(:ferricstore, :waraft_apply_projection_spill_hook)

    on_exit(fn ->
      case previous do
        {:ok, hook} ->
          Application.put_env(:ferricstore, :waraft_apply_projection_spill_hook, hook)

        :error ->
          Application.delete_env(:ferricstore, :waraft_apply_projection_spill_hook)
      end

      Reader.clear_apply_projection_cache(root, 0)
      Reader.clear_apply_projection_cache(root, 1)
      Ferricstore.Test.LMDBFixture.cleanup_data_dir!(root)
    end)

    %{root: root, ctx: %{data_dir: root}}
  end

  test "mixed hot and cold preparation batches complete indexes and keeps other shards hot", %{
    root: root,
    ctx: ctx
  } do
    entries = for index <- 1..64, do: put_source(root, index, rem(index, 2) == 0)
    assert :ok = Reader.put_apply_projection(root, 0, 1_000, [{"unrequested", "keep", 0}])
    assert :ok = Reader.put_apply_projection(root, 1, 1, [{"other-shard", "other", 0}])
    calls = :atomics.new(1, [])

    Application.put_env(:ferricstore, :waraft_apply_projection_spill_hook, fn _batches ->
      :atomics.add(calls, 1, 1)
      :ok
    end)

    decoded = ColdState.read_and_decode(entries, root, 0, ctx)
    assert length(decoded) == 64
    assert :atomics.get(calls, 1) <= 2
    assert Reader.apply_projection_cache_count(root, 0) == 1
    assert Reader.apply_projection_cache_bytes(root, 0) == byte_size("keep")
    assert Reader.apply_projection_cache_count(root, 1) == 1

    for {key, value, _, record, %Locator{} = locator} <- decoded do
      assert Locator.hydration_ready?(locator)
      index = String.replace_prefix(record.id, "batch-", "") |> String.to_integer()

      assert {:ok, ^value} =
               Reader.read_value_from_location_including_expired(
                 ctx,
                 0,
                 {:waraft_apply_projection, index},
                 key
               )

      assert {:ok, "sibling-#{index}"} ==
               Reader.read_value_from_location_including_expired(
                 ctx,
                 0,
                 {:waraft_apply_projection, index},
                 "sibling"
               )
    end
  end

  test "failed batch preparation preserves unchanged-source failures and valid groups", %{
    root: root,
    ctx: ctx
  } do
    good = put_source(root, 10, true)
    bad = put_source(root, 11, false)
    keydir = :ets.new(:batch_source_failure, [:set])
    :ets.insert(keydir, [good, bad])
    fail_index(11)
    Process.put(:flow_lmdb_rebuild_cold_read_errors, 0)

    {decoded, changed} = ColdState.read_and_decode_with_status([good, bad], root, 0, ctx, keydir)
    assert [{_, _, _, %{id: "batch-10"}, %Locator{}}] = decoded
    assert changed == []
    assert Process.get(:flow_lmdb_rebuild_cold_read_errors) == 1
    assert Reader.apply_projection_cache_count(root, 0) == 2
  end

  test "a source changed during a failed batch is refreshed without hiding stable failures", %{
    root: root,
    ctx: ctx
  } do
    good = put_source(root, 20, true)
    bad = put_source(root, 21, true)
    keydir = :ets.new(:batch_source_replaced, [:set])
    :ets.insert(keydir, [good, bad])
    bad_key = elem(bad, 0)

    Application.put_env(:ferricstore, :waraft_apply_projection_spill_hook, fn batches ->
      if Enum.any?(batches, fn {{:raft_log_pos, index, _}, _} -> index == 21 end) do
        :ets.insert(keydir, {bad_key, nil, 0, :flow_state_deleted, :deleted, 0, 0})
        {:error, :synthetic_eio}
      else
        :ok
      end
    end)

    Process.put(:flow_lmdb_rebuild_cold_read_errors, 0)
    {decoded, changed} = ColdState.read_and_decode_with_status([good, bad], root, 0, ctx, keydir)
    assert [{_, _, _, %{id: "batch-20"}, %Locator{}}] = decoded
    assert changed == [bad_key]
    assert Process.get(:flow_lmdb_rebuild_cold_read_errors) == 0
  end

  defp fail_index(bad_index) do
    Application.put_env(:ferricstore, :waraft_apply_projection_spill_hook, fn batches ->
      if Enum.any?(batches, fn {{:raft_log_pos, index, _}, _} -> index == bad_index end),
        do: {:error, :synthetic_eio},
        else: :ok
    end)
  end

  defp put_source(root, index, hot?) do
    id = "batch-#{index}"
    key = Keys.state_key(id)

    record = %{
      id: id,
      type: "batch",
      state: "queued",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: 1,
      next_run_at_ms: 0,
      priority: 0,
      partition_key: nil,
      state_enter_seq: 1,
      root_flow_id: id
    }

    value = Ferricstore.Flow.encode_record(record)

    assert :ok =
             Reader.put_apply_projection(root, 0, index, [
               {key, value, 0},
               {"sibling", "sibling-#{index}", 0}
             ])

    {key, if(hot?, do: value), 0, 0, {:waraft_apply_projection, index}, 0, byte_size(value)}
  end
end
