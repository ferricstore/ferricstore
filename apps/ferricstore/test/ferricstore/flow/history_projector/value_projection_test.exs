defmodule Ferricstore.Flow.HistoryProjector.ValueProjectionTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector.ValueProjection

  test "projected value work is split into bounded batches" do
    refs = Enum.map(1..513, &"ref-#{&1}")
    batches = ValueProjection.projected_flow_value_ref_batches(refs)

    assert Enum.map(batches, &length/1) == [256, 256, 1]
    assert List.flatten(batches) == refs
  end

  test "projected value discovery fails closed on an unreadable keydir locator" do
    keydir = :ets.new(:history_projector_invalid_value_locator, [:set, :public])
    ref = Ferricstore.Flow.Keys.value_key("flow", :payload, 1)
    row = {ref, nil, 0, Ferricstore.Store.LFU.initial(), :invalid_file, 0, 10}
    :ets.insert(keydir, row)

    assert ValueProjection.projected_flow_value_keydir_items_result(keydir, [ref]) ==
             {:error, {:invalid_projected_flow_value_locator, ref, row}}
  end

  test "projected value reads propagate source failures instead of skipping the ref" do
    unique = System.unique_integer([:positive])
    keydir = :ets.new(:"history_projector_missing_value_source_#{unique}", [:set, :public])
    ref = Ferricstore.Flow.Keys.value_key("missing-source", :payload, 1)
    row = {ref, nil, 0, Ferricstore.Store.LFU.initial(), 0, 0, 10}
    :ets.insert(keydir, row)

    shard_data_path =
      Path.join(System.tmp_dir!(), "ferricstore_missing_value_source_#{unique}")

    assert {:error, {:projected_flow_value_read_failed, ^ref, _reason}} =
             ValueProjection.projected_flow_value_source(
               nil,
               0,
               shard_data_path,
               keydir,
               ref
             )
  end

  test "projected value reads retain expired WARaft apply values for history materialization" do
    unique = System.unique_integer([:positive])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_expired_projection_#{unique}")
    keydir = :ets.new(:"history_projector_expired_value_source_#{unique}", [:set, :public])
    shard_index = 0
    index = unique
    ref = Ferricstore.Flow.Keys.value_key("expired-source", :result, 1)
    value = "expired-value"
    ctx = %{data_dir: data_dir}

    on_exit(fn ->
      Ferricstore.Raft.WARaftSegmentReader.clear_apply_projection_cache(
        data_dir,
        shard_index
      )
    end)

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
               data_dir,
               shard_index,
               index,
               [{ref, value, 1}]
             )

    :ets.insert(
      keydir,
      {ref, nil, 1, Ferricstore.Store.LFU.initial(), {:waraft_apply_projection, index}, 0,
       byte_size(value)}
    )

    assert {:ok, %{key: ^ref, value: ^value}} =
             ValueProjection.projected_flow_value_source(
               ctx,
               shard_index,
               data_dir,
               keydir,
               ref
             )
  end
end
