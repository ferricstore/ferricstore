defmodule FerricstoreServer.Bugs.PipelineAsyncBatchErrorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.ClientTracking
  alias FerricstoreServer.Connection
  alias FerricstoreServer.Connection.Pipeline

  @ns "pipeline_quorum_fast_path"
  @pipeline_source Path.expand("../../../lib/ferricstore_server/connection/pipeline.ex", __DIR__)

  setup do
    ClientTracking.init_tables()
    :ets.delete_all_objects(:ferricstore_tracking)
    :ets.delete_all_objects(:ferricstore_tracking_connections)

    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()

    on_exit(fn ->
      NamespaceConfig.reset_all()
      ShardHelpers.flush_all_keys()
      ClientTracking.cleanup(self())
    end)

    :ok
  end

  test "SET fast path writes through quorum" do
    ctx = FerricStore.Instance.get(:default)
    key = "#{@ns}:stale_async:#{System.unique_integer([:positive])}"

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("SET pipeline fast path")

    commands = [
      {:command, "SET", [key, "v1"], {:set, key, "v1"}, [key]},
      set_ast("#{@ns}:same_batch:#{System.unique_integer([:positive])}", "v2")
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, response}
    assert response == "+OK\r\n+OK\r\n"
    assert Router.get(ctx, key) == "v1"
  end

  test "mixed GET and SET fast path writes through quorum" do
    ctx = FerricStore.Instance.get(:default)
    set_key = "#{@ns}:mixed_stale_async:#{System.unique_integer([:positive])}"

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("mixed GET/SET pipeline fast path")

    commands = [
      get_ast("#{@ns}:missing:#{System.unique_integer([:positive])}"),
      {:command, "SET", [set_key, "v1"], {:set, set_key, "v1"}, [set_key]}
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, response}
    assert response == "_\r\n+OK\r\n"
    assert Router.get(ctx, set_key) == "v1"
  end

  test "FLOW write pipeline batches same-shard commands without losing order" do
    ctx = FerricStore.Instance.get(:default)
    id = "pipe_flow_order_#{System.unique_integer([:positive])}"
    partition_key = "tenant-pipe-flow-order"
    handler_id = {:pipeline_flow_write_batch, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :pipeline_write, :stop],
        fn _event, measurements, metadata, test_pid ->
          send(test_pid, {:pipeline_write_batch, measurements, metadata})
        end,
        self()
      )

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("FLOW write pipeline fast path")

    commands = [
      {:command, "FLOW.CREATE", [],
       {:flow_create, id,
        [
          type: "pipeline-flow",
          state: "queued",
          partition_key: partition_key,
          now_ms: 1,
          run_at_ms: 1
        ]}, []},
      {:command, "FLOW.TRANSITION", [],
       {:flow_transition, id, "queued", "ready",
        [partition_key: partition_key, fencing_token: 0, now_ms: 2, run_at_ms: 2]}, []}
    ]

    try do
      assert {:continue, ^state} =
               Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

      assert_receive {:pipeline_response, response}
      refute response =~ "-ERR"

      assert {:ok, %{state: "ready", version: 2}} =
               FerricStore.flow_get(id, partition_key: partition_key)

      assert_receive {:pipeline_write_batch, %{count: count}, _metadata}, 1_000
      assert count >= 2
    after
      :telemetry.detach(handler_id)
    end
  end

  test "mixed pipeline batches only consecutive FLOW write segments" do
    ctx = FerricStore.Instance.get(:default)
    id = "pipe_flow_mixed_#{System.unique_integer([:positive])}"
    partition_key = "tenant-pipe-flow-mixed"
    handler_id = {:pipeline_flow_mixed_write_batch, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :pipeline_write, :stop],
        fn _event, measurements, metadata, test_pid ->
          send(test_pid, {:pipeline_write_batch, measurements, metadata})
        end,
        self()
      )

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("mixed FLOW segment pipeline fast path")

    commands = [
      {:command, "PING", [], :ping, []},
      flow_create_ast(id, partition_key, now_ms: 1, run_at_ms: 1),
      flow_transition_ast(id, partition_key, "queued", "ready", now_ms: 2, run_at_ms: 2),
      {:command, "FLOW.GET", [], {:flow_get, id, [partition_key: partition_key]}, []},
      {:command, "PING", [], :ping, []}
    ]

    try do
      assert {:continue, ^state} =
               Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

      assert_receive {:pipeline_response, response}
      refute response =~ "-ERR"
      assert response =~ "+PONG\r\n"
      assert response =~ "$5\r\nready\r\n"

      assert {:ok, %{state: "ready", version: 2}} =
               FerricStore.flow_get(id, partition_key: partition_key)

      assert_receive {:pipeline_write_batch, %{count: count}, _metadata}, 1_000
      assert count >= 2
    after
      :telemetry.detach(handler_id)
    end
  end

  test "mixed pipeline batches consecutive FLOW read segments" do
    ctx = FerricStore.Instance.get(:default)
    partition_key = "tenant-pipe-flow-read-mixed"
    id_a = "pipe_flow_read_a_#{System.unique_integer([:positive])}"
    id_b = "pipe_flow_read_b_#{System.unique_integer([:positive])}"
    handler_id = {:pipeline_flow_read_batch, self(), make_ref()}

    assert :ok =
             FerricStore.flow_create(id_a,
               type: "pipeline-flow-read",
               state: "queued",
               partition_key: partition_key,
               now_ms: 1,
               run_at_ms: 1
             )

    assert :ok =
             FerricStore.flow_create(id_b,
               type: "pipeline-flow-read",
               state: "queued",
               partition_key: partition_key,
               now_ms: 2,
               run_at_ms: 2
             )

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :pipeline_read_batch],
        fn _event, measurements, metadata, test_pid ->
          send(test_pid, {:flow_read_batch, measurements, metadata})
        end,
        self()
      )

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("mixed FLOW read segment pipeline fast path")

    commands = [
      {:command, "PING", [], :ping, []},
      {:command, "FLOW.GET", [], {:flow_get, id_a, [partition_key: partition_key]}, []},
      {:command, "FLOW.GET", [], {:flow_get, id_b, [partition_key: partition_key]}, []},
      {:command, "FLOW.HISTORY", [],
       {:flow_history, id_a, [partition_key: partition_key, count: 10]}, []},
      {:command, "PING", [], :ping, []}
    ]

    try do
      assert {:continue, ^state} =
               Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

      assert_receive {:pipeline_response, response}
      refute response =~ "-ERR"
      assert response =~ "+PONG\r\n"
      assert response =~ id_a
      assert response =~ id_b
      assert response =~ "created"

      assert_receive {:flow_read_batch, %{count: 3, gets: 2, histories: 1}, %{source: :pipeline}}
    after
      :telemetry.detach(handler_id)
    end
  end

  test "FLOW terminal commands in pipeline use cross-shard router path" do
    parent = "pipe_flow_parent_#{System.unique_integer([:positive])}"
    child = "pipe_flow_child_#{System.unique_integer([:positive])}"
    {parent_partition, child_partition} = different_flow_partitions()

    assert :ok =
             FerricStore.flow_create(parent,
               type: "pipeline-parent",
               state: "dispatch",
               partition_key: parent_partition
             )

    assert {:ok, created_parent} = FerricStore.flow_get(parent, partition_key: parent_partition)

    assert :ok =
             FerricStore.flow_spawn_children(
               parent,
               [%{id: child, type: "pipeline-child", partition_key: child_partition}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: parent_partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert {:ok, waiting} = FerricStore.flow_get(parent, partition_key: parent_partition)

    assert waiting.state == "waiting_children"

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("pipeline-child",
               partition_key: child_partition,
               worker: "pipeline-worker",
               limit: 1,
               now_ms: 9_000_000_000_000
             )

    state = connection_state(FerricStore.Instance.get(:default))
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("FLOW terminal pipeline router path")

    commands = [
      {:command, "FLOW.COMPLETE", [],
       {:flow_complete, child, claimed.lease_token,
        [partition_key: child_partition, fencing_token: claimed.fencing_token]}, []},
      {:command, "PING", [], :ping, []}
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, response}
    refute response =~ "-ERR"
    assert response =~ "+PONG\r\n"

    assert {:ok, done_parent} = FerricStore.flow_get(parent, partition_key: parent_partition)
    assert done_parent.state == "children_done"
    assert done_parent.child_groups["fanout"]["children"][child] == "completed"
  end

  test "FLOW terminal pipeline reaches Flow write coalescer" do
    ctx = FerricStore.Instance.get(:default)
    type = "pipeline-terminal-#{System.unique_integer([:positive])}"
    partition_key = "tenant-pipe-flow-terminal"
    complete_id = "pipe_flow_complete_#{System.unique_integer([:positive])}"
    cancel_id = "pipe_flow_cancel_#{System.unique_integer([:positive])}"
    handler_id = {:pipeline_flow_terminal_write, self(), make_ref()}

    assert :ok =
             FerricStore.flow_create(complete_id,
               type: type,
               state: "queued",
               partition_key: partition_key,
               now_ms: 1,
               run_at_ms: 1
             )

    assert :ok =
             FerricStore.flow_create(cancel_id,
               type: type,
               state: "queued",
               partition_key: partition_key,
               now_ms: 2,
               run_at_ms: 2
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition_key,
               worker: "pipeline-terminal-worker",
               limit: 1,
               now_ms: 9_000_000_000_000
             )

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :pipeline_write, :stop],
        fn _event, measurements, metadata, test_pid ->
          send(test_pid, {:flow_pipeline_write, measurements, metadata})
        end,
        self()
      )

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("FLOW terminal pipeline write coalescer")

    commands = [
      {:command, "FLOW.COMPLETE", [],
       {:flow_complete, claimed.id, claimed.lease_token,
        [partition_key: partition_key, fencing_token: claimed.fencing_token]}, []},
      {:command, "FLOW.CANCEL", [],
       {:flow_cancel, cancel_id,
        [partition_key: partition_key, fencing_token: 0, now_ms: 9_000_000_000_001]}, []}
    ]

    try do
      assert {:continue, ^state} =
               Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

      assert_receive {:pipeline_response, response}
      refute response =~ "-ERR"
      assert_receive {:flow_pipeline_write, _measurements, %{result: :ok}}

      assert {:ok, completed} = FerricStore.flow_get(claimed.id, partition_key: partition_key)
      assert {:ok, cancelled} = FerricStore.flow_get(cancel_id, partition_key: partition_key)
      assert completed.state == "completed"
      assert cancelled.state == "cancelled"
    after
      :telemetry.detach(handler_id)
    end
  end

  test "SET pipeline fast path writes through sandbox namespace" do
    ctx = FerricStore.Instance.get(:default)
    sandbox = "sandbox_pipe:" <> Integer.to_string(System.unique_integer([:positive])) <> ":"
    key1 = "set1"
    key2 = "set2"

    state = connection_state(ctx) |> Map.put(:sandbox_namespace, sandbox)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("sandbox SET pipeline fast path")

    commands = [
      set_ast(key1, "v1"),
      set_ast(key2, "v2")
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, "+OK\r\n+OK\r\n"}
    assert Router.get(ctx, key1) == nil
    assert Router.get(ctx, key2) == nil
    assert Router.get(ctx, sandbox <> key1) == "v1"
    assert Router.get(ctx, sandbox <> key2) == "v2"
  end

  test "mixed GET and SET fast path writes SETs through sandbox namespace" do
    ctx = FerricStore.Instance.get(:default)
    sandbox = "sandbox_mixed:" <> Integer.to_string(System.unique_integer([:positive])) <> ":"
    get_key = "existing"
    set_key = "new"

    :ok = Router.put(ctx, sandbox <> get_key, "inside", 0)

    state = connection_state(ctx) |> Map.put(:sandbox_namespace, sandbox)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("sandbox mixed GET/SET pipeline fast path")

    commands = [
      get_ast(get_key),
      set_ast(set_key, "written")
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, "$6\r\ninside\r\n+OK\r\n"}
    assert Router.get(ctx, set_key) == nil
    assert Router.get(ctx, sandbox <> set_key) == "written"
  end

  test "GET pipeline fast path records client tracking reads" do
    ctx = FerricStore.Instance.get(:default)
    key1 = "#{@ns}:tracked_get_1:#{System.unique_integer([:positive])}"
    key2 = "#{@ns}:tracked_get_2:#{System.unique_integer([:positive])}"

    :ok = Router.put(ctx, key1, "v1", 0)
    :ok = Router.put(ctx, key2, "v2", 0)

    state = enabled_tracking_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("tracked GET pipeline fast path")

    commands = [
      get_ast(key1),
      get_ast(key2)
    ]

    assert {:continue, new_state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, "$2\r\nv1\r\n$2\r\nv2\r\n"}
    assert new_state.tracking.enabled
    assert :ets.lookup(:ferricstore_tracking, key1) == [{key1, self()}]
    assert :ets.lookup(:ferricstore_tracking, key2) == [{key2, self()}]
  end

  test "mixed GET and SET fast path records only GET reads for client tracking" do
    ctx = FerricStore.Instance.get(:default)
    get_key = "#{@ns}:tracked_mixed_get:#{System.unique_integer([:positive])}"
    set_key = "#{@ns}:tracked_mixed_set:#{System.unique_integer([:positive])}"

    :ok = Router.put(ctx, get_key, "v1", 0)

    state = enabled_tracking_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("tracked mixed GET/SET pipeline fast path")

    commands = [
      get_ast(get_key),
      set_ast(set_key, "written")
    ]

    assert {:continue, new_state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, "$2\r\nv1\r\n+OK\r\n"}
    assert new_state.tracking.enabled
    assert :ets.lookup(:ferricstore_tracking, get_key) == [{get_key, self()}]
    assert :ets.lookup(:ferricstore_tracking, set_key) == []
  end

  test "general pure pipeline records client tracking reads" do
    ctx = FerricStore.Instance.get(:default)
    key = "#{@ns}:tracked_general_get:#{System.unique_integer([:positive])}"

    :ok = Router.put(ctx, key, "v1", 0)

    state = enabled_tracking_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("tracked general pure pipeline")

    commands = [
      get_ast(key),
      {:command, "PING", [], :ping, []}
    ]

    assert {:continue, new_state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, "$2\r\nv1\r\n+PONG\r\n"}
    assert new_state.tracking.enabled
    assert :ets.lookup(:ferricstore_tracking, key) == [{key, self()}]
  end

  test "SET pipeline fast path sends client tracking invalidations" do
    ctx = FerricStore.Instance.get(:default)
    key = "#{@ns}:tracked_set_invalidate:#{System.unique_integer([:positive])}"
    other_key = "#{@ns}:tracked_set_other:#{System.unique_integer([:positive])}"

    track_key_for_current_process(key)

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("tracked SET pipeline fast path")

    commands = [
      set_ast(key, "v2"),
      set_ast(other_key, "other")
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, "+OK\r\n+OK\r\n"}
    assert_receive {:tracking_invalidation, _payload, [^key]}
    assert :ets.lookup(:ferricstore_tracking, key) == []
  end

  test "mixed GET and SET fast path sends client tracking invalidations for SET keys" do
    ctx = FerricStore.Instance.get(:default)
    get_key = "#{@ns}:tracked_mixed_read:#{System.unique_integer([:positive])}"
    set_key = "#{@ns}:tracked_mixed_invalidate:#{System.unique_integer([:positive])}"

    :ok = Router.put(ctx, get_key, "v1", 0)
    track_key_for_current_process(set_key)

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("tracked mixed invalidation pipeline fast path")

    commands = [
      get_ast(get_key),
      set_ast(set_key, "v2")
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, "$2\r\nv1\r\n+OK\r\n"}
    assert_receive {:tracking_invalidation, _payload, [^set_key]}
    assert :ets.lookup(:ferricstore_tracking, set_key) == []
  end

  test "mixed GET and SET fast path assembles replies without index maps" do
    body =
      @pipeline_source
      |> File.read!()
      |> String.split("  defp do_mixed_fast_path", parts: 2)
      |> List.last()
      |> String.split("  defp mixed_set_results", parts: 2)
      |> hd()

    refute body =~ "Map.new("
    refute body =~ "Map.get("
  end

  test "general pure pipeline converts command raises into Redis error replies" do
    ctx = FerricStore.Instance.get(:default)
    raw_store_key = {:ferricstore_raw_store, ctx.name}
    old_raw_store = :persistent_term.get(raw_store_key, :missing)

    raising_store =
      ctx
      |> FerricstoreServer.Connection.Store.build_raw_store()
      |> Map.put(:incr, fn _key, _delta -> raise "write key latch timeout after 5ms" end)

    :persistent_term.put(raw_store_key, raising_store)

    on_exit(fn ->
      case old_raw_store do
        :missing -> :persistent_term.erase(raw_store_key)
        store -> :persistent_term.put(raw_store_key, store)
      end
    end)

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("general pure pipeline error containment")

    commands = [
      {:command, "INCR", ["raise"], {:incr, "raise"}, ["raise"]},
      {:command, "PING", [], :ping, []}
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, response}
    assert response =~ "-ERR internal error"
    refute response =~ "write key latch timeout"
    refute response =~ "RuntimeError"
    assert response =~ "+PONG\r\n"
  end

  defp connection_state(ctx) do
    %Connection{
      socket: :socket,
      transport: :test_transport,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access
    }
  end

  defp enabled_tracking_state(ctx) do
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    connection_state(ctx) |> Map.put(:tracking, tracking)
  end

  defp track_key_for_current_process(key) do
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    ClientTracking.track_key(self(), key, tracking)
    :ok
  end

  defp capture_response_fn do
    fn :socket, :test_transport, response ->
      send(self(), {:pipeline_response, IO.iodata_to_binary(response)})
      :ok
    end
  end

  defp flunking_handle_fn(path) do
    fn command, _state ->
      flunk("expected #{path}, got fallback for #{inspect(command)}")
    end
  end

  defp get_ast(key), do: {:command, "GET", [key], {:get, key}, [key]}
  defp set_ast(key, value), do: {:command, "SET", [key, value], {:set, key, value}, [key]}

  defp flow_create_ast(id, partition_key, opts) do
    {:command, "FLOW.CREATE", [],
     {:flow_create, id,
      Keyword.merge(
        [type: "pipeline-flow", state: "queued", partition_key: partition_key],
        opts
      )}, []}
  end

  defp flow_transition_ast(id, partition_key, from_state, to_state, opts) do
    {:command, "FLOW.TRANSITION", [],
     {:flow_transition, id, from_state, to_state,
      Keyword.merge([partition_key: partition_key, fencing_token: 0], opts)}, []}
  end

  defp different_flow_partitions do
    base = "pipeline-flow-tenant:" <> Integer.to_string(System.unique_integer([:positive]))
    ctx = FerricStore.Instance.get(:default)

    first = "#{base}:parent"
    first_shard = flow_partition_shard(ctx, first)

    second =
      1..256
      |> Enum.map(&"#{base}:child:#{&1}")
      |> Enum.find(fn candidate -> flow_partition_shard(ctx, candidate) != first_shard end)

    assert is_binary(second)
    {first, second}
  end

  defp flow_partition_shard(ctx, partition_key) do
    key = Ferricstore.Flow.Keys.state_key("probe", partition_key)
    Router.shard_for(ctx, key)
  end

end
