defmodule Ferricstore.Store.RouterRestartFallbackTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Router

  test "get returns nil instead of exiting when ETS and shard are temporarily unavailable" do
    ctx = unavailable_ctx()

    assert nil == Router.get(ctx, "restart:missing")
  end

  test "batch_get returns nil for unavailable shard fallback instead of exiting" do
    ctx = unavailable_ctx()

    assert [nil, nil] == Router.batch_get(ctx, ["restart:a", "restart:b"])
  end

  test "get_meta returns nil instead of exiting when ETS and shard are temporarily unavailable" do
    ctx = unavailable_ctx()

    assert nil == Router.get_meta(ctx, "restart:meta")
  end

  test "keys skips unavailable shard and reports telemetry" do
    ctx = unavailable_ctx()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :store, :shard_unavailable],
        &__MODULE__.handle_telemetry/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert [] == Router.keys(ctx)
    assert_unavailable_event(:keys)
  end

  test "dbsize reports unavailable keydir fallback" do
    ctx = unavailable_ctx()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :store, :shard_unavailable],
        &__MODULE__.handle_telemetry/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert 0 == Router.dbsize(ctx)

    assert_receive {:telemetry_event, [:ferricstore, :store, :shard_unavailable], %{count: 1},
                    %{request: :dbsize, reason: :keydir_unavailable, shard_index: 0}}
  end

  test "unavailable shard fallbacks emit telemetry" do
    ctx = unavailable_ctx()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :store, :shard_unavailable],
        &__MODULE__.handle_telemetry/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert nil == Router.get(ctx, "restart:get")
    assert [nil, nil] == Router.batch_get(ctx, ["restart:batch:a", "restart:batch:b"])
    assert nil == Router.get_meta(ctx, "restart:meta")

    assert_unavailable_event(:get)
    assert_unavailable_event(:get)
    assert_unavailable_event(:get)
    assert_unavailable_event(:get_meta)
  end

  test "compound reads fallback and report unavailable shards" do
    ctx = unavailable_ctx()
    redis_key = "restart:compound"
    compound_key = "H:" <> redis_key <> <<0>> <> "field"
    prefix = "H:" <> redis_key <> <<0>>
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :store, :shard_unavailable],
        &__MODULE__.handle_telemetry/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert nil == Router.compound_get(ctx, redis_key, compound_key)
    assert [nil] == Router.compound_batch_get(ctx, redis_key, [compound_key])
    assert nil == Router.compound_get_meta(ctx, redis_key, compound_key)
    assert [nil] == Router.compound_batch_get_meta(ctx, redis_key, [compound_key])
    assert [] == Router.compound_scan(ctx, redis_key, prefix)
    assert 0 == Router.compound_count(ctx, redis_key, prefix)

    assert_unavailable_event(:compound_get)
    assert_unavailable_event(:compound_batch_get)
    assert_unavailable_event(:compound_get_meta)
    assert_unavailable_event(:compound_batch_get_meta)
    assert_unavailable_event(:compound_scan)
    assert_unavailable_event(:compound_count)
  end

  def handle_telemetry(event, measurements, metadata, parent) do
    send(parent, {:telemetry_event, event, measurements, metadata})
  end

  defp unavailable_ctx do
    keydir = :ets.new(:"router_restart_fallback_#{System.unique_integer([:positive])}", [:set])
    :ets.delete(keydir)

    %FerricStore.Instance{
      name: :"router_restart_fallback_#{System.unique_integer([:positive])}",
      data_dir: System.tmp_dir!(),
      data_dir_expanded: System.tmp_dir!(),
      shard_count: 1,
      slot_map: Tuple.duplicate(0, 1024),
      shard_names: {:"missing_router_restart_shard_#{System.unique_integer([:positive])}"},
      keydir_refs: {keydir},
      stats_counter: :counters.new(16, []),
      write_version: :counters.new(1, []),
      hot_cache_max_value_size: 1024,
      read_sample_rate: 0
    }
  end

  defp assert_unavailable_event(request) do
    assert_receive {:telemetry_event, [:ferricstore, :store, :shard_unavailable], %{count: 1},
                    %{request: ^request, reason: :noproc, shard_index: 0}}
  end
end
