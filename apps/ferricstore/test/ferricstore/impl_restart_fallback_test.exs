defmodule Ferricstore.ImplRestartFallbackTest do
  use ExUnit.Case, async: true

  test "compound reads through Impl fallback instead of exiting while shard is unavailable" do
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

    assert {:ok, nil} = FerricStore.Impl.hget(ctx, "impl_restart_hash", "field")

    assert_receive {:telemetry_event, [:ferricstore, :store, :shard_unavailable], %{count: 1},
                    %{
                      instance: :impl_restart_fallback,
                      request: :compound_get,
                      reason: :noproc,
                      shard_index: 0
                    }}
  end

  def handle_telemetry(event, measurements, metadata, parent) do
    send(parent, {:telemetry_event, event, measurements, metadata})
  end

  defp unavailable_ctx do
    keydir = :ets.new(:"impl_restart_fallback_#{System.unique_integer([:positive])}", [:set])
    :ets.delete(keydir)

    %FerricStore.Instance{
      name: :impl_restart_fallback,
      data_dir: System.tmp_dir!(),
      data_dir_expanded: System.tmp_dir!(),
      shard_count: 1,
      slot_map: Tuple.duplicate(0, 1024),
      shard_names: {:"missing_impl_restart_shard_#{System.unique_integer([:positive])}"},
      keydir_refs: {keydir},
      stats_counter: :counters.new(16, []),
      write_version: :counters.new(1, []),
      hot_cache_max_value_size: 1024,
      read_sample_rate: 0
    }
  end
end
