defmodule Ferricstore.Flow.Query.StatisticsWorkerTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, StorageScope}
  alias Ferricstore.Flow.Query.{CompositeRange, IndexDefinition}
  alias Ferricstore.Store.{Router, SlotMap}

  alias Ferricstore.Flow.Query.{
    IndexStatistics,
    StatisticsStore,
    StatisticsWorker
  }

  test "coalesces requests and records an exact prefix only when a capped probe exhausts" do
    {ctx, store, worker} = context()
    parent = self()

    read_fun = fn _path, range, max_items, max_bytes ->
      send(parent, {:probe, range.prefix, max_items, max_bytes})

      {:ok,
       %{
         entries: [%{id: "a"}, %{id: "b"}, %{id: "c"}],
         cursor: nil,
         exhausted: true,
         scanned_entries: 3,
         scanned_bytes: 300
       }}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: worker,
       statistics_store: store,
       read_fun: read_fun,
       probe_interval_ms: 0}
    )

    definition = definition()

    for _duplicate <- 1..10 do
      assert :ok =
               StatisticsWorker.probe(worker, 0, definition, "tenant-a", ["tenant-a", "failed"])
    end

    assert_receive {:probe, _prefix, 257, 1_048_576}, 1_000
    refute_receive {:probe, _prefix, _items, _bytes}, 50

    eventually(fn ->
      with {:ok, stat} <-
             StatisticsStore.lookup(ctx, definition.id, definition.version, "tenant-a") do
        assert {:ok, 3} =
                 IndexStatistics.prefix_count(
                   stat,
                   ["tenant-a", "failed"],
                   stat.collected_at_ms
                 )

        assert stat.average_entry_bytes == 100
        true
      else
        :not_found -> false
      end
    end)
  end

  test "an exact prefix probe does not refresh stale histogram evidence" do
    {ctx, store, worker} = context()
    definition = definition()
    stale_at_ms = System.system_time(:millisecond) - 5 * 60 * 1_000 - 1

    stale =
      IndexStatistics.new!(%{
        index_id: definition.id,
        index_version: definition.version,
        scope_digest: IndexStatistics.scope_digest("tenant-a"),
        collected_at_ms: stale_at_ms,
        source_watermark: 1,
        total_entries: 3,
        distinct_runs: 3,
        prefix_counts: %{},
        prefix_observed_at_ms: %{},
        histograms: %{
          updated_at_ms: [%{lower: 0, upper: 100, count: 3}]
        },
        null_counts: %{},
        missing_counts: %{},
        average_entry_bytes: 100,
        average_row_bytes: 500,
        sample_rate_ppm: 1_000_000,
        confidence: :high
      })

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      {:ok,
       %{
         entries: [%{id: "fresh"}],
         cursor: nil,
         exhausted: true,
         scanned_entries: 1,
         scanned_bytes: 100
       }}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})
    assert :ok = StatisticsStore.put(store, stale)

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: worker,
       statistics_store: store,
       read_fun: read_fun,
       probe_interval_ms: 0}
    )

    assert :ok =
             StatisticsWorker.probe(worker, 0, definition, "tenant-a", ["tenant-a", "failed"])

    eventually(fn ->
      with {:ok, stat} <-
             StatisticsStore.lookup(ctx, definition.id, definition.version, "tenant-a"),
           true <- stat.collected_at_ms > stale_at_ms do
        assert stat.histograms == stale.histograms

        assert :unknown =
                 IndexStatistics.histogram_fraction_ppm(
                   stat,
                   :updated_at_ms,
                   0,
                   100,
                   true
                 )

        true
      else
        _not_updated -> false
      end
    end)
  end

  test "a non-exhausted probe never turns a lower bound into an exact count" do
    {ctx, store, worker} = context()

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      {:ok,
       %{
         entries: List.duplicate(%{id: "candidate"}, 257),
         cursor: "more",
         exhausted: false,
         scanned_entries: 257,
         scanned_bytes: 25_700
       }}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: worker,
       statistics_store: store,
       read_fun: read_fun,
       probe_interval_ms: 0}
    )

    definition = definition()
    assert :ok = StatisticsWorker.probe(worker, 0, definition, "tenant-a", ["tenant-a"])

    Process.sleep(50)
    assert :not_found = StatisticsStore.lookup(ctx, definition.id, definition.version, "tenant-a")
  end

  test "an exhausted callback cannot exceed the bounded probe request" do
    {ctx, store, worker} = context()

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      {:ok,
       %{
         entries: List.duplicate(%{id: "candidate"}, 258),
         cursor: nil,
         exhausted: true,
         scanned_entries: 258,
         scanned_bytes: 25_800
       }}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: worker,
       statistics_store: store,
       read_fun: read_fun,
       probe_interval_ms: 0}
    )

    definition = definition()
    assert :ok = StatisticsWorker.probe(worker, 0, definition, "tenant-a", ["tenant-a"])
    eventually(fn -> MapSet.size(:sys.get_state(worker).pending) == 0 end)
    assert :not_found = StatisticsStore.lookup(ctx, definition.id, definition.version, "tenant-a")
  end

  test "asynchronous probes never wait for LMDB sampling" do
    {ctx, store, worker} = context()
    parent = self()

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      send(parent, {:probe_started, self()})

      receive do
        :finish_probe ->
          {:ok,
           %{
             entries: [],
             cursor: nil,
             exhausted: true,
             scanned_entries: 0,
             scanned_bytes: 0
           }}
      end
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    worker_pid =
      start_supervised!(
        {StatisticsWorker,
         instance_ctx: ctx,
         name: worker,
         statistics_store: store,
         read_fun: read_fun,
         probe_interval_ms: 0}
      )

    assert :ok =
             StatisticsWorker.probe_async(
               ctx,
               worker,
               0,
               prepared_probe("tenant-a", ["tenant-a", "failed"])
             )

    assert_receive {:probe_started, ^worker_pid}, 1_000
    send(worker_pid, :finish_probe)
  end

  test "shared prepared probes validate hidden scope and physical shard routing" do
    {ctx, store, worker} = context()
    parent = self()
    probe = shared_prepared_probe(11, "logical", "failed")

    read_fun = fn _path, range, _max_items, _max_bytes ->
      send(parent, {:shared_probe, range})

      {:ok,
       %{
         entries: [%{id: "one"}, %{id: "two"}],
         cursor: nil,
         exhausted: true,
         scanned_entries: 2,
         scanned_bytes: 200
       }}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: worker,
       statistics_store: store,
       read_fun: read_fun,
       probe_interval_ms: 0}
    )

    assert :ok = StatisticsWorker.probe_async(ctx, worker, 0, probe)
    assert_receive {:shared_probe, range}, 1_000
    assert range == probe.range

    eventually(fn ->
      case StatisticsStore.lookup(
             ctx,
             probe.definition.id,
             probe.definition.version,
             probe.statistics_key
           ) do
        {:ok, stat} ->
          IndexStatistics.prefix_count(stat, probe.equality_values, stat.collected_at_ms) ==
            {:ok, 2}

        :not_found ->
          false
      end
    end)

    refute StatisticsWorker.probe_async(ctx, worker, 0, %{
             probe
             | physical_partition_key: "logical"
           }) == :ok
  end

  test "batched probe admission is bounded and validates prepared probes before I/O" do
    {ctx, store, worker} = context()
    parent = self()

    read_fun = fn _path, range, _max_items, _max_bytes ->
      send(parent, {:batched_probe_read, range})

      {:ok,
       %{
         entries: [],
         cursor: nil,
         exhausted: true,
         scanned_entries: 0,
         scanned_bytes: 0
       }}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: worker,
       statistics_store: store,
       read_fun: read_fun,
       probe_interval_ms: 0}
    )

    valid = prepared_probe("tenant-a", ["tenant-a", "failed"])

    forged =
      "tenant-a"
      |> prepared_probe(["tenant-a", "completed"])
      |> Map.put(:physical_partition_key, "wrong-partition")

    assert :ok = StatisticsWorker.probe_many_async(ctx, worker, 0, [valid, forged])
    assert_receive {:batched_probe_read, range}, 1_000
    assert range == valid.range
    refute_receive {:batched_probe_read, _range}, 50

    assert {:error, :invalid_query_statistics_probe} =
             StatisticsWorker.probe_many_async(ctx, worker, 0, List.duplicate(valid, 33))
  end

  test "asynchronous probe admission bounds the worker mailbox while I/O is blocked" do
    {ctx, store, worker} = context()
    parent = self()
    calls = :counters.new(1, [])

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      call = :counters.get(calls, 1)
      :counters.add(calls, 1, 1)

      if call == 0 do
        send(parent, {:probe_started, self()})
        receive do: (:finish_probe -> :ok)
      end

      {:ok,
       %{
         entries: [],
         cursor: nil,
         exhausted: true,
         scanned_entries: 0,
         scanned_bytes: 0
       }}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    worker_pid =
      start_supervised!(
        {StatisticsWorker,
         instance_ctx: ctx,
         name: worker,
         statistics_store: store,
         read_fun: read_fun,
         probe_interval_ms: 0}
      )

    assert :ok =
             StatisticsWorker.probe_async(
               ctx,
               worker,
               0,
               prepared_probe("tenant-a", ["tenant-a", "first"])
             )

    assert_receive {:probe_started, ^worker_pid}, 1_000

    results =
      1..2_048
      |> Task.async_stream(
        fn ordinal ->
          StatisticsWorker.probe_async(
            ctx,
            worker,
            0,
            prepared_probe("tenant-a", ["tenant-a", "state-#{ordinal}"])
          )
        end,
        max_concurrency: 64,
        ordered: false,
        timeout: 5_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == :ok)) == 1_024
    assert Enum.count(results, &(&1 == {:error, :query_statistics_probe_queue_full})) == 1_024
    {:message_queue_len, queued} = Process.info(worker_pid, :message_queue_len)
    assert queued <= 1
    send(worker_pid, :finish_probe)
  end

  test "duplicate asynchronous probes do not consume admission capacity" do
    {ctx, store, worker} = context()
    parent = self()

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      send(parent, {:probe_started, self()})
      receive do: (:finish_probe -> :ok)

      {:ok,
       %{
         entries: [],
         cursor: nil,
         exhausted: true,
         scanned_entries: 0,
         scanned_bytes: 0
       }}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    worker_pid =
      start_supervised!(
        {StatisticsWorker,
         instance_ctx: ctx,
         name: worker,
         statistics_store: store,
         read_fun: read_fun,
         probe_interval_ms: 0}
      )

    probe = prepared_probe("tenant-a", ["tenant-a", "failed"])
    assert :ok = StatisticsWorker.probe_async(ctx, worker, 0, probe)
    assert_receive {:probe_started, ^worker_pid}, 1_000

    assert Enum.all?(1..2_048, fn _duplicate ->
             StatisticsWorker.probe_async(ctx, worker, 0, probe) == :ok
           end)

    admission_table = :"#{ctx.name}.Flow.Query.StatisticsAdmission"

    staged_slots =
      Enum.count(:ets.tab2list(admission_table), fn
        {{:"$async_probe_slot", _slot}, _ticket, _payload} -> true
        _metadata -> false
      end)

    assert staged_slots == 1
    send(worker_pid, :finish_probe)
  end

  test "asynchronous admission does not expose raw scope or predicate values" do
    {ctx, store, worker} = context()
    parent = self()

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      send(parent, {:probe_started, self()})
      receive do: (:finish_probe -> :ok)

      {:ok,
       %{
         entries: [],
         cursor: nil,
         exhausted: true,
         scanned_entries: 0,
         scanned_bytes: 0
       }}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    worker_pid =
      start_supervised!(
        {StatisticsWorker,
         instance_ctx: ctx,
         name: worker,
         statistics_store: store,
         read_fun: read_fun,
         probe_interval_ms: 0}
      )

    assert :ok =
             StatisticsWorker.probe_async(
               ctx,
               worker,
               0,
               prepared_probe("tenant-blocking-probe", [
                 "tenant-blocking-probe",
                 "blocked-state"
               ])
             )

    assert_receive {:probe_started, ^worker_pid}, 1_000

    secret_scope = "sensitive-tenant-value-that-must-not-be-staged"
    secret_predicate = "sensitive-state-value-that-must-not-be-staged"

    assert :ok =
             StatisticsWorker.probe_async(
               ctx,
               worker,
               0,
               prepared_probe(secret_scope, [secret_scope, secret_predicate])
             )

    admission_table = :"#{ctx.name}.Flow.Query.StatisticsAdmission"
    staged = admission_table |> :ets.tab2list() |> :erlang.term_to_binary()
    assert :binary.match(staged, secret_scope) == :nomatch
    assert :binary.match(staged, secret_predicate) == :nomatch
    send(worker_pid, :finish_probe)
  end

  test "malformed asynchronous admission entries cannot crash the worker" do
    {ctx, store, worker} = context()
    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    worker_pid =
      start_supervised!(
        {StatisticsWorker,
         instance_ctx: ctx, name: worker, statistics_store: store, probe_interval_ms: 0}
      )

    admission_table = :"#{ctx.name}.Flow.Query.StatisticsAdmission"
    slot_key = {:"$async_probe_slot", 9_999}
    assert :ets.insert(admission_table, {slot_key, 1, :malformed})
    monitor = Process.monitor(worker_pid)
    send(worker_pid, :drain_async_probes)

    _state = :sys.get_state(worker)
    assert Process.whereis(worker) == worker_pid
    refute_receive {:DOWN, ^monitor, :process, ^worker_pid, _reason}
    assert :ets.lookup(admission_table, slot_key) == []
    Process.demonitor(monitor, [:flush])
  end

  test "rejects malformed or cross-shard probe requests before I/O" do
    {ctx, store, worker} = context()
    parent = self()

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      send(parent, :read)
      {:error, :read}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: worker,
       statistics_store: store,
       read_fun: read_fun,
       probe_interval_ms: 0}
    )

    assert {:error, :invalid_query_statistics_probe} =
             StatisticsWorker.probe(worker, 2, definition(), "tenant-a", ["tenant-a"])

    assert {:error, :invalid_query_statistics_probe} =
             StatisticsWorker.probe(worker, 0, definition(), "tenant-a", ["tenant-b"])

    forged = %{definition() | fingerprint: :crypto.strong_rand_bytes(32)}

    assert {:error, :invalid_query_statistics_probe} =
             StatisticsWorker.probe(worker, 0, forged, "tenant-a", ["tenant-a"])

    assert {:error, :invalid_query_statistics_probe} =
             StatisticsWorker.probe(
               worker,
               0,
               definition(),
               "tenant-a",
               ["tenant-a", String.duplicate("x", 1_025)]
             )

    refute_receive :read, 50
  end

  test "rejects a valid scope submitted to the wrong shard" do
    {ctx, store, worker} = context()
    ctx = %{ctx | shard_count: 2, slot_map: SlotMap.build_uniform(2)}
    parent = self()

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      send(parent, :read)
      {:error, :unexpected_read}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: worker,
       statistics_store: store,
       read_fun: read_fun,
       probe_interval_ms: 0}
    )

    scope =
      Enum.find_value(1..100, fn ordinal ->
        candidate = "tenant-#{ordinal}"
        shard = Router.shard_for(ctx, Keys.state_key("", candidate))
        if shard in [0, 1], do: {candidate, shard}
      end)

    {scope, routed_shard} = scope
    wrong_shard = 1 - routed_shard

    assert {:error, :invalid_query_statistics_probe} =
             StatisticsWorker.probe(
               worker,
               wrong_shard,
               definition(),
               scope,
               [scope, "failed"]
             )

    assert {:error, :invalid_query_statistics_probe} =
             StatisticsWorker.probe_async(
               ctx,
               worker,
               wrong_shard,
               prepared_probe(scope, [scope, "failed"])
             )

    refute_receive :read, 50
  end

  test "rejects malformed asynchronous contexts without raising" do
    assert {:error, :invalid_query_statistics_probe} =
             StatisticsWorker.probe_async(
               %{},
               :missing_worker,
               0,
               prepared_probe("tenant-a", ["tenant-a"])
             )
  end

  test "rejects prepared probes whose digests or range do not match their values" do
    {ctx, store, worker} = context()
    parent = self()

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      send(parent, :read)
      {:error, :unexpected_read}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: worker,
       statistics_store: store,
       read_fun: read_fun,
       probe_interval_ms: 0}
    )

    probe = prepared_probe("tenant-a", ["tenant-a", "failed"])

    assert {:error, :invalid_query_statistics_probe} =
             StatisticsWorker.probe_async(
               ctx,
               worker,
               0,
               %{probe | scope_digest: IndexStatistics.scope_digest("tenant-b")}
             )

    assert {:error, :invalid_query_statistics_probe} =
             StatisticsWorker.probe_async(
               ctx,
               worker,
               0,
               %{probe | prefix_digest: IndexStatistics.prefix_digest(["tenant-a", "running"])}
             )

    other = prepared_probe("tenant-a", ["tenant-a", "running"])

    assert {:error, :invalid_query_statistics_probe} =
             StatisticsWorker.probe_async(ctx, worker, 0, %{probe | range: other.range})

    refute_receive :read, 50
  end

  test "fails startup cleanly when the context admission table is already owned" do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)
    {ctx, store, worker} = context()
    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx, name: worker, statistics_store: store, probe_interval_ms: 0}
    )

    assert {:error, :invalid_query_statistics_worker_options} =
             StatisticsWorker.start_link(
               instance_ctx: ctx,
               name: :"#{worker}.duplicate",
               statistics_store: store,
               probe_interval_ms: 0
             )
  end

  test "bounds the recently-seen probe cache under high-cardinality traffic" do
    {ctx, store, worker} = context()
    parent = self()

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      send(parent, :probe_finished)

      {:ok,
       %{
         entries: [],
         cursor: nil,
         exhausted: true,
         scanned_entries: 0,
         scanned_bytes: 0
       }}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: worker,
       statistics_store: store,
       read_fun: read_fun,
       probe_interval_ms: 0,
       max_seen_entries: 2}
    )

    for state <- ["one", "two", "three"] do
      assert :ok =
               StatisticsWorker.probe(worker, 0, definition(), "tenant-a", ["tenant-a", state])

      assert_receive :probe_finished, 1_000
      eventually(fn -> MapSet.size(:sys.get_state(worker).pending) == 0 end)
    end

    assert map_size(:sys.get_state(worker).seen) == 2
  end

  test "evicts an old exact-prefix observation when a statistics row reaches capacity" do
    {ctx, store, worker} = context()

    read_fun = fn _path, _range, _max_items, _max_bytes ->
      {:ok,
       %{
         entries: [],
         cursor: nil,
         exhausted: true,
         scanned_entries: 0,
         scanned_bytes: 0
       }}
    end

    start_supervised!({StatisticsStore, instance_ctx: ctx, name: store, max_entries: 8})

    start_supervised!(
      {StatisticsWorker,
       instance_ctx: ctx,
       name: worker,
       statistics_store: store,
       read_fun: read_fun,
       probe_interval_ms: 0}
    )

    for ordinal <- 1..257 do
      assert :ok =
               StatisticsWorker.probe(
                 worker,
                 0,
                 definition(),
                 "tenant-a",
                 ["tenant-a", "state-#{ordinal}"]
               )
    end

    eventually(fn -> MapSet.size(:sys.get_state(worker).pending) == 0 end, 200)
    assert {:ok, stat} = StatisticsStore.lookup(ctx, definition().id, 1, "tenant-a")
    assert map_size(stat.prefix_counts) == 256

    assert {:ok, 0} =
             IndexStatistics.prefix_count(
               stat,
               ["tenant-a", "state-257"],
               stat.collected_at_ms
             )
  end

  defp context do
    suffix = System.unique_integer([:positive, :monotonic])

    ctx = %{
      name: :"statistics_worker_instance_#{suffix}",
      data_dir: Path.join(System.tmp_dir!(), "statistics_worker_#{suffix}"),
      shard_count: 1,
      slot_map: SlotMap.build_uniform(1)
    }

    on_exit(fn -> File.rm_rf!(ctx.data_dir) end)

    {ctx, :"statistics_store_#{suffix}", :"statistics_worker_#{suffix}"}
  end

  defp definition do
    IndexDefinition.new!(%{
      id: "flow_runs_tenant_state_updated",
      version: 1,
      fields: [
        {:partition_key, :asc},
        {:state, :asc},
        {:updated_at_ms, :desc}
      ]
    })
  end

  defp shared_definition do
    IndexDefinition.new!(%{
      id: "flow_runs_tenant_state_updated_shared",
      version: 1,
      scope_bytes: 8,
      fields: [
        {:partition_key, :asc},
        {:state, :asc},
        {:updated_at_ms, :desc}
      ]
    })
  end

  defp prepared_probe(scope, equality_values) do
    definition = definition()
    {:ok, range} = CompositeRange.prefix(definition, equality_values)

    %{
      definition: definition,
      equality_values: equality_values,
      range: range,
      scope_prefix: nil,
      physical_partition_key: scope,
      statistics_key: scope,
      scope_digest: IndexStatistics.scope_digest(scope),
      prefix_digest: IndexStatistics.prefix_digest(equality_values)
    }
  end

  defp shared_prepared_probe(tenant_ref, logical_partition, state) do
    definition = shared_definition()
    scope_prefix = <<tenant_ref::unsigned-big-64>>
    equality_values = [logical_partition, state]
    statistics_key = :crypto.hash(:sha256, ["tenant-statistics", scope_prefix])

    {:ok, physical_partition_key} =
      StorageScope.physical_partition_key(logical_partition, scope_prefix)

    {:ok, range} = CompositeRange.prefix(definition, scope_prefix, equality_values)

    %{
      definition: definition,
      equality_values: equality_values,
      range: range,
      scope_prefix: scope_prefix,
      physical_partition_key: physical_partition_key,
      statistics_key: statistics_key,
      scope_digest: IndexStatistics.scope_digest(statistics_key),
      prefix_digest: IndexStatistics.prefix_digest(equality_values)
    }
  end

  defp eventually(fun, attempts \\ 50)
  defp eventually(fun, 0), do: assert(fun.())

  defp eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end
