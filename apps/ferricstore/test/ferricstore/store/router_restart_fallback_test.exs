defmodule Ferricstore.Store.RouterRestartFallbackTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.DataDir
  alias Ferricstore.Store.Shard
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.Router

  test "get fails closed when ETS and shard are temporarily unavailable" do
    ctx = unavailable_ctx()

    assert ReadResult.failure(:keydir_unavailable) == Router.get(ctx, "restart:missing")
  end

  test "batch_get fails closed for an unavailable shard" do
    ctx = unavailable_ctx()

    failure = ReadResult.failure(:shard_unavailable)
    assert [^failure, ^failure] = Router.batch_get(ctx, ["restart:a", "restart:b"])
  end

  test "get_meta fails closed when ETS and shard are temporarily unavailable" do
    ctx = unavailable_ctx()

    assert ReadResult.failure(:keydir_unavailable) == Router.get_meta(ctx, "restart:meta")
  end

  test "GETRANGE propagates an unavailable keydir instead of treating the failure as a value" do
    ctx = unavailable_ctx()

    assert {:error, "ERR storage read failed"} ==
             FerricStore.Impl.getrange(ctx, "restart:range", 0, -1)
  end

  test "keys fails closed for an unavailable shard and reports telemetry" do
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

    assert ReadResult.failure(:shard_unavailable) == Router.keys(ctx)
    assert_unavailable_event(:keys)
  end

  test "dbsize fails closed for an unavailable keydir" do
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

    assert ReadResult.failure(:keydir_unavailable) == Router.dbsize(ctx)

    assert_receive {:telemetry_event, [:ferricstore, :store, :shard_unavailable], %{count: 1},
                    %{request: :dbsize, reason: :keydir_unavailable, shard_index: 0}}
  end

  test "public enumeration APIs never turn storage outages into empty success" do
    ctx = unavailable_ctx()

    assert ReadResult.failure(:shard_unavailable) == FerricStore.Impl.keys(ctx)
    assert ReadResult.failure(:keydir_unavailable) == FerricStore.Impl.dbsize(ctx)

    assert {:error, "ERR storage read failed"} =
             Ferricstore.Commands.Server.handle("KEYS", ["*"], ctx)

    assert {:error, "ERR storage read failed"} =
             Ferricstore.Commands.Server.handle("DBSIZE", [], ctx)
  end

  test "metadata-only reads report unavailable keydir fallbacks" do
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

    assert nil == Router.expire_at_ms(ctx, "restart:ttl")
    assert nil == Router.value_size(ctx, "restart:size")
    assert false == Router.exists?(ctx, "restart:exists")

    assert_keydir_unavailable_event(:expire_at_ms)
    assert_keydir_unavailable_event(:value_size)
    assert_keydir_unavailable_event(:exists)
  end

  test "get_version falls back to shared counter and reports unavailable shard" do
    ctx = unavailable_ctx()
    handler_id = {__MODULE__, make_ref()}

    :counters.add(ctx.write_version, 1, 7)

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :store, :shard_unavailable],
        &__MODULE__.handle_telemetry/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert 7 == Router.get_version(ctx, "restart:version")

    assert_unavailable_event(:get_version)
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

    assert ReadResult.failure(:keydir_unavailable) == Router.get(ctx, "restart:get")

    failure = ReadResult.failure(:shard_unavailable)
    assert [^failure, ^failure] = Router.batch_get(ctx, ["restart:batch:a", "restart:batch:b"])
    assert ReadResult.failure(:keydir_unavailable) == Router.get_meta(ctx, "restart:meta")

    assert_unavailable_event(:get)
    assert_unavailable_event(:get)
    assert_unavailable_event(:get)
    assert_unavailable_event(:get_meta)
  end

  test "get does not treat rebuilding keydir as an authoritative miss" do
    name = :"router_recover_keydir_#{System.unique_integer([:positive])}"
    tmp_dir = Path.join(System.tmp_dir!(), Atom.to_string(name))
    DataDir.ensure_layout!(tmp_dir, 1)

    ctx =
      FerricStore.Instance.build(name,
        data_dir: tmp_dir,
        shard_count: 1,
        hot_cache_max_value_size: 0,
        read_sample_rate: 0
      )

    shard_path = DataDir.shard_data_path(tmp_dir, 0)
    log_path = ShardETS.file_path(shard_path, 0)
    key = "restart:recover:last-key"

    value = :binary.copy("x", 64 * 1024)

    records =
      for(i <- 1..511, do: {"restart:recover:filler:#{i}", value, 0}) ++
        [{key, "value", 0}]

    assert {:ok, _locations} = NIF.v2_append_batch(log_path, records)

    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :store, :shard_unavailable],
        &__MODULE__.handle_telemetry/4,
        self()
      )

    start_task =
      Task.async(fn ->
        case Shard.start_link(
               index: 0,
               data_dir: tmp_dir,
               instance_ctx: ctx,
               flush_interval_ms: 60_000
             ) do
          {:ok, pid} = result ->
            Process.unlink(pid)
            result

          other ->
            other
        end
      end)

    try do
      refute observe_authoritative_miss_while_rebuilding(ctx, key, start_task)
      assert {:ok, _pid} = Task.await(start_task, 10_000)
      assert "value" == Router.get(ctx, key)
    after
      case Process.whereis(Router.resolve_shard(ctx, 0)) do
        nil -> :ok
        pid -> GenServer.stop(pid)
      end

      :telemetry.detach(handler_id)
      FerricStore.Instance.cleanup(name)
      File.rm_rf(tmp_dir)
    end
  end

  test "compound reads fail closed and report unavailable shards" do
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

    failure = ReadResult.failure(:shard_unavailable)

    assert ^failure = Router.compound_get(ctx, redis_key, compound_key)
    assert [^failure] = Router.compound_batch_get(ctx, redis_key, [compound_key])
    assert ^failure = Router.compound_get_meta(ctx, redis_key, compound_key)
    assert [^failure] = Router.compound_batch_get_meta(ctx, redis_key, [compound_key])
    assert ^failure = Router.compound_scan(ctx, redis_key, prefix)
    assert ^failure = Router.compound_count(ctx, redis_key, prefix)

    assert_unavailable_event(:compound_get)
    assert_unavailable_event(:compound_batch_get)
    assert_unavailable_event(:compound_get_meta)
    assert_unavailable_event(:compound_batch_get_meta)
    assert_unavailable_event(:compound_scan)
    assert_unavailable_event(:compound_count)
  end

  test "custom compound writes return errors and report unavailable shards" do
    ctx = unavailable_ctx()
    redis_key = "restart:compound_write"
    compound_key = "H:" <> redis_key <> <<0>> <> "field"
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :store, :shard_unavailable],
        &__MODULE__.handle_telemetry/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:error, "ERR shard not available"} ==
             Router.compound_put(ctx, redis_key, compound_key, "value", 0)

    assert {:error, "ERR shard not available"} ==
             Router.compound_delete(ctx, redis_key, compound_key)

    assert_unavailable_event(:compound_put)
    assert_unavailable_event(:compound_delete)
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

  defp assert_keydir_unavailable_event(request) do
    assert_receive {:telemetry_event, [:ferricstore, :store, :shard_unavailable], %{count: 1},
                    %{request: ^request, reason: :keydir_unavailable, shard_index: 0}}
  end

  defp observe_authoritative_miss_while_rebuilding(ctx, key, start_task) do
    keydir = elem(ctx.keydir_refs, 0)
    deadline = System.monotonic_time(:millisecond) + 5_000
    observe_authoritative_miss_while_rebuilding(ctx, key, keydir, start_task, deadline)
  end

  defp observe_authoritative_miss_while_rebuilding(ctx, key, keydir, start_task, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      false
    else
      if Process.alive?(start_task.pid) do
        if final_keydir_missing_key?(keydir, key) do
          drain_unavailable_events()
          _ = Router.get(ctx, key)
          !received_unavailable_event?()
        else
          Process.sleep(1)
          observe_authoritative_miss_while_rebuilding(ctx, key, keydir, start_task, deadline)
        end
      else
        false
      end
    end
  end

  defp final_keydir_missing_key?(keydir, key) do
    case :ets.whereis(keydir) do
      :undefined -> false
      _tid -> :ets.lookup(keydir, key) == []
    end
  rescue
    ArgumentError -> false
  end

  defp drain_unavailable_events do
    receive do
      {:telemetry_event, [:ferricstore, :store, :shard_unavailable], _measurements, _metadata} ->
        drain_unavailable_events()
    after
      0 -> :ok
    end
  end

  defp received_unavailable_event? do
    receive do
      {:telemetry_event, [:ferricstore, :store, :shard_unavailable], _measurements, _metadata} ->
        true
    after
      20 -> false
    end
  end
end
