defmodule FerricstoreServer.Commands.BlockingBugHuntTest.Sections.Part01 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Blocking, List, Stream}
      alias Ferricstore.Store.Router
      alias FerricstoreServer.Acl
      alias FerricstoreServer.Connection
      alias FerricstoreServer.Connection.Auth, as: ConnAuth
      alias FerricstoreServer.Connection.Blocking, as: ConnBlocking
      alias FerricstoreServer.Connection.Store, as: ConnStore
      alias FerricstoreServer.Resp.{Encoder, Parser}
      alias FerricstoreServer.Listener
      alias Ferricstore.Test.MockStore
      alias Ferricstore.Waiters

  test "blocking list dispatch converts storage raises into error replies" do
    ctx = FerricStore.Instance.get(:default)
    key = "blocking_raise"
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    meta_key = Ferricstore.Store.CompoundKey.list_meta_key(key)

    with_raw_store(ctx, fn raw ->
      raw
      |> Map.put(:compound_get, fn
        ^key, ^type_key -> "list"
        ^key, ^meta_key -> :erlang.term_to_binary({1, 0, 0})
        _redis_key, _compound_key -> nil
      end)
      |> Map.put(:compound_scan, fn ^key, _prefix -> raise "write key latch timeout after 5ms" end)
      |> Map.put(:list_op, fn ^key, {:lpop, 1} -> raise "write key latch timeout after 5ms" end)
    end)

    try do
      assert {:continue, response, ^state} =
               ConnBlocking.dispatch_blpop_ast([key], 1, state)

      response = IO.iodata_to_binary(response)
      assert response =~ "-ERR internal error"
      refute response =~ "write key latch timeout"
      refute response =~ "RuntimeError"
    after
      restore_raw_store(ctx)
    end
  end

  test "BLPOP on a non-list key returns WRONGTYPE instead of blocking" do
    ctx = FerricStore.Instance.get(:default)
    key = ukey("blpop_wrongtype")
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}

    assert :ok = Router.put(ctx, key, "string-value", 0)

    assert {:continue, response, ^state} = ConnBlocking.dispatch_blpop_ast([key], 1, state)
    assert {:ok, [{:error, msg}], ""} = Parser.parse(IO.iodata_to_binary(response))
    assert msg =~ "WRONGTYPE"
  end

  test "BLMPOP on a non-list key returns WRONGTYPE instead of treating it as empty" do
    ctx = FerricStore.Instance.get(:default)
    key = ukey("blmpop_wrongtype")
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}

    assert :ok = Router.put(ctx, key, "string-value", 0)

    assert {:continue, response, ^state} =
             ConnBlocking.dispatch_blmpop_ast([key], :left, 1, 1, state)

    assert {:ok, [{:error, msg}], ""} = Parser.parse(IO.iodata_to_binary(response))
    assert msg =~ "WRONGTYPE"
  end

  test "spurious BLPOP wake keeps waiting until real work or timeout" do
    ctx = FerricStore.Instance.get(:default)
    key = ukey("spurious_wake")
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}
    store = FerricstoreServer.Connection.Store.build_store(ctx, nil)

    task =
      Task.async(fn ->
        ConnBlocking.dispatch_blpop_ast([key], 1_000, state)
      end)

    wait_until(fn -> Waiters.count(key) == 1 end)
    send(task.pid, {:waiter_notify, key})

    refute Task.yield(task, 150),
           "a notify that does not pop a value must not complete the blocking command"

    assert 1 = List.handle("RPUSH", [key, "v1"], store)
    send(task.pid, {:waiter_notify, key})

    assert {:ok, {:continue, response, ^state}} = Task.yield(task, 500)
    assert Parser.parse(IO.iodata_to_binary(response)) == {:ok, [[key, "v1"]], ""}
  end

  test "stale waiter notification cannot make BLPOP pop an unrelated key" do
    ctx = FerricStore.Instance.get(:default)
    stale_key = ukey("stale_notify_blpop_old")
    blocked_key = ukey("stale_notify_blpop_new")
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}
    store = FerricstoreServer.Connection.Store.build_store(ctx, nil)

    assert 1 = List.handle("RPUSH", [stale_key, "old-value"], store)
    send(self(), {:waiter_notify, stale_key})

    assert {:continue, response, ^state} =
             ConnBlocking.dispatch_blpop_ast([blocked_key], 25, state)

    assert Parser.parse(IO.iodata_to_binary(response)) == {:ok, [nil], ""}
    assert "old-value" = List.handle("LPOP", [stale_key], store)
  end

  test "stale waiter notification cannot make BLMPOP pop an unrelated key" do
    ctx = FerricStore.Instance.get(:default)
    stale_key = ukey("stale_notify_blmpop_old")
    blocked_key = ukey("stale_notify_blmpop_new")
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}
    store = FerricstoreServer.Connection.Store.build_store(ctx, nil)

    assert 1 = List.handle("RPUSH", [stale_key, "old-value"], store)
    send(self(), {:waiter_notify, stale_key})

    assert {:continue, response, ^state} =
             ConnBlocking.dispatch_blmpop_ast([blocked_key], :left, 1, 25, state)

    assert Parser.parse(IO.iodata_to_binary(response)) == {:ok, [nil], ""}
    assert "old-value" = List.handle("LPOP", [stale_key], store)
  end

  test "contended BLPOP wake re-registers waiter before the next push" do
    ctx = FerricStore.Instance.get(:default)
    key = ukey("blpop_contended_reregister")
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}
    store = FerricstoreServer.Connection.Store.build_store(ctx, nil)

    task =
      Task.async(fn ->
        ConnBlocking.dispatch_blpop_ast([key], 500, state)
      end)

    wait_until(fn -> Waiters.count(key) == 1 end)

    Waiters.unregister(key, task.pid)
    send(task.pid, {:waiter_notify, key})
    wait_until(fn -> Waiters.count(key) == 1 end)

    assert 1 = List.handle("RPUSH", [key, "fresh-value"], store)

    assert {:ok, {:continue, response, ^state}} = Task.yield(task, 500)
    assert Parser.parse(IO.iodata_to_binary(response)) == {:ok, [[key, "fresh-value"]], ""}
  end

  test "BLPOP rechecks after waiter registration to avoid missed list wake" do
    ctx = FerricStore.Instance.get(:default)
    key = ukey("blpop_register_gap")
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}
    store = ConnStore.build_store(ctx, nil)

    Process.put(:ferricstore_list_block_before_register_hook, fn ->
      assert 1 = List.handle("RPUSH", [key, "v1"], store)
    end)

    try do
      assert {:continue, response, ^state} = ConnBlocking.dispatch_blpop_ast([key], 50, state)
      assert Parser.parse(IO.iodata_to_binary(response)) == {:ok, [[key, "v1"]], ""}
    after
      Process.delete(:ferricstore_list_block_before_register_hook)
    end
  end

  test "BLMOVE rechecks after waiter registration to avoid missed list wake" do
    ctx = FerricStore.Instance.get(:default)
    source = ukey("blmove_register_gap_src")
    destination = ukey("blmove_register_gap_dst")
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}
    store = ConnStore.build_store(ctx, nil)

    Process.put(:ferricstore_list_block_before_register_hook, fn ->
      assert 1 = List.handle("RPUSH", [source, "v1"], store)
    end)

    try do
      assert {:continue, response, ^state} =
               ConnBlocking.dispatch_blmove_ast(source, destination, :left, :right, 50, state)

      assert Parser.parse(IO.iodata_to_binary(response)) == {:ok, ["v1"], ""}
      assert "v1" = List.handle("RPOP", [destination], store)
    after
      Process.delete(:ferricstore_list_block_before_register_hook)
    end
  end

  test "BLMPOP rechecks after waiter registration to avoid missed list wake" do
    ctx = FerricStore.Instance.get(:default)
    key = ukey("blmpop_register_gap")
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}
    store = ConnStore.build_store(ctx, nil)

    Process.put(:ferricstore_list_block_before_register_hook, fn ->
      assert 1 = List.handle("RPUSH", [key, "v1"], store)
    end)

    try do
      assert {:continue, response, ^state} =
               ConnBlocking.dispatch_blmpop_ast([key], :left, 1, 50, state)

      assert Parser.parse(IO.iodata_to_binary(response)) == {:ok, [[key, ["v1"]]], ""}
    after
      Process.delete(:ferricstore_list_block_before_register_hook)
    end
  end

  test "blocked BLPOP stops when the authenticated user is revoked", context do
    require_tcp!(context)
    FerricstoreServer.Acl.reset!()
    on_exit(fn -> FerricstoreServer.Acl.reset!() end)

    key = ukey("acl_block")
    admin = connect_and_hello(context.port)
    worker = connect_and_hello(context.port)

    send_cmd(admin, ["ACL", "SETUSER", "worker", "on", ">pass", "~#{key}", "+@all"])
    assert {:ok, {:simple, "OK"}} = recv_response(admin)

    send_cmd(worker, ["AUTH", "worker", "pass"])
    assert {:ok, {:simple, "OK"}} = recv_response(worker)

    send_cmd(worker, ["BLPOP", key, "5"])
    wait_until(fn -> Waiters.count(key) == 1 end)

    send_cmd(admin, ["ACL", "SETUSER", "worker", "off"])
    assert {:ok, {:simple, "OK"}} = recv_response(admin)

    send_cmd(admin, ["RPUSH", key, "secret-work"])
    assert {:ok, 1} = recv_response(admin)

    assert {:ok, {:error, msg}} = recv_response(worker)
    assert msg =~ "NOPERM"

    send_cmd(admin, ["LPOP", key])
    assert {:ok, "secret-work"} = recv_response(admin)

    :gen_tcp.close(admin)
    :gen_tcp.close(worker)
  end

  test "FLOW.CLAIM_DUE BLOCK refreshes ACL before post-register claim" do
    Acl.reset!()
    on_exit(fn -> Acl.reset!() end)

    ctx = FerricStore.Instance.get(:default)
    type = ukey("flow_acl_block")
    partition = ukey("tenant")
    id = ukey("flow")

    create_result =
      FerricStore.flow_create(id,
        type: type,
        partition_key: partition,
        state: "queued",
        run_at_ms: 1
      )

    assert create_result == :ok or match?({:ok, _record}, create_result)

    assert :ok = Acl.set_user("worker", ["on", ">pass", "~#{partition}", "+FLOW.CLAIM_DUE"])
    stale_cache = ConnAuth.build_acl_cache("worker")
    assert :ok = Acl.set_user("worker", ["off"])

    state = %Connection{
      instance_ctx: ctx,
      sandbox_namespace: nil,
      username: "worker",
      acl_cache: stale_cache,
      authenticated: true,
      require_auth: false
    }

    assert {:continue, response, _state} =
             ConnBlocking.dispatch_flow_claim_due_ast(
               type,
               [
                 partition_key: partition,
                 worker: "w1",
                 limit: 1,
                 now_ms: 1,
                 block_ms: 50
               ],
               state
             )

    assert {:ok, [{:error, msg}], ""} = Parser.parse(IO.iodata_to_binary(response))
    assert msg =~ "NOPERM"

    assert {:ok, [%{id: ^id}]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "admin",
               limit: 1,
               now_ms: 1
             )
  end

  test "BLPOP refreshes ACL before post-register pop" do
    Acl.reset!()
    on_exit(fn -> Acl.reset!() end)

    ctx = FerricStore.Instance.get(:default)
    key = ukey("blpop_acl_register_gap")
    store = ConnStore.build_store(ctx, nil)

    assert :ok = Acl.set_user("worker", ["on", ">pass", "~#{key}", "+BLPOP"])
    stale_cache = ConnAuth.build_acl_cache("worker")

    Process.put(:ferricstore_list_block_before_register_hook, fn ->
      assert :ok = Acl.set_user("worker", ["off"])
      assert 1 = List.handle("RPUSH", [key, "secret-work"], store)
    end)

    state = %Connection{
      instance_ctx: ctx,
      sandbox_namespace: nil,
      username: "worker",
      acl_cache: stale_cache,
      authenticated: true,
      require_auth: false
    }

    try do
      assert {:continue, response, _state} = ConnBlocking.dispatch_blpop_ast([key], 50, state)
      assert {:ok, [{:error, msg}], ""} = Parser.parse(IO.iodata_to_binary(response))
      assert msg =~ "NOPERM"
      assert "secret-work" = List.handle("LPOP", [key], store)
    after
      Process.delete(:ferricstore_list_block_before_register_hook)
    end
  end

  test "XREAD BLOCK refreshes ACL before post-register stream read" do
    Acl.reset!()
    on_exit(fn -> Acl.reset!() end)

    ctx = FerricStore.Instance.get(:default)
    stream = ukey("xread_acl_register_gap")
    store = ConnStore.build_store(ctx, nil)

    assert :ok = Acl.set_user("worker", ["on", ">pass", "~#{stream}", "+XREAD"])
    stale_cache = ConnAuth.build_acl_cache("worker")

    Process.put(:ferricstore_stream_block_before_register_hook, fn ->
      assert :ok = Acl.set_user("worker", ["off"])
      assert is_binary(Stream.handle("XADD", [stream, "1-0", "f", "v"], store))
    end)

    state = %Connection{
      instance_ctx: ctx,
      sandbox_namespace: nil,
      username: "worker",
      acl_cache: stale_cache,
      authenticated: true,
      require_auth: false
    }

    try do
      ast = {:xread, :infinity, {:block, 50}, [{stream, "0-0"}]}
      args = ["BLOCK", "50", "STREAMS", stream, "0-0"]

      assert {:continue, response, _state} =
               ConnBlocking.dispatch_stream_read_ast("XREAD", ast, args, state)

      assert {:ok, [{:error, msg}], ""} = Parser.parse(IO.iodata_to_binary(response))
      assert msg =~ "NOPERM"
    after
      Process.delete(:ferricstore_stream_block_before_register_hook)
    end
  end

  test "XREAD BLOCK rechecks after waiter registration to avoid missed stream wake" do
    ctx = FerricStore.Instance.get(:default)
    stream = ukey("xread_register_gap")
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}
    store = ConnStore.build_store(ctx, nil)

    Process.put(:ferricstore_stream_block_before_register_hook, fn ->
      assert is_binary(Stream.handle("XADD", [stream, "1-0", "f", "v"], store))
    end)

    try do
      ast = {:xread, :infinity, {:block, 50}, [{stream, "0-0"}]}
      args = ["BLOCK", "50", "STREAMS", stream, "0-0"]

      assert {:continue, response, ^state} = ConnBlocking.dispatch_xread_ast(ast, args, state)

      assert {:ok, [[[^stream, [["1-0", "f", "v"]]]]], ""} =
               Parser.parse(IO.iodata_to_binary(response))
    after
      Process.delete(:ferricstore_stream_block_before_register_hook)
    end
  end

  test "XREADGROUP BLOCK loser remains registered after contended wake" do
    ctx = FerricStore.Instance.get(:default)
    stream = ukey("xreadgroup_contended")
    state = %FerricstoreServer.Connection{instance_ctx: ctx, sandbox_namespace: nil}
    store = ConnStore.build_store(ctx, nil)
    parent = self()

    assert is_binary(Stream.handle("XADD", [stream, "1-0", "seed", "v0"], store))
    assert :ok = Stream.handle("XGROUP", ["CREATE", stream, "grp", "$"], store)

    task_fun = fn consumer ->
      ast = {:xreadgroup, "grp", consumer, {:infinity, {:block, 500}, [{stream, ">"}]}}
      args = ["GROUP", "grp", consumer, "BLOCK", "500", "STREAMS", stream, ">"]
      result = ConnBlocking.dispatch_xread_ast(ast, args, state)
      send(parent, {:xreadgroup_done, consumer, result})
      result
    end

    t1 = Task.async(fn -> task_fun.("c1") end)
    t2 = Task.async(fn -> task_fun.("c2") end)

    wait_until(fn -> Stream.stream_waiter_count(stream) == 2 end)

    assert is_binary(Stream.handle("XADD", [stream, "2-0", "f", "v1"], store))
    assert_receive {:xreadgroup_done, first_consumer, first_result}, 500
    assert first_consumer in ["c1", "c2"]
    assert_stream_entry(first_result, stream, "2-0")

    assert is_binary(Stream.handle("XADD", [stream, "3-0", "f", "v2"], store))
    assert_receive {:xreadgroup_done, second_consumer, second_result}, 800
    assert second_consumer in ["c1", "c2"]
    assert second_consumer != first_consumer
    assert_stream_entry(second_result, stream, "3-0")

    Task.shutdown(t1, :brutal_kill)
    Task.shutdown(t2, :brutal_kill)
  end

  # Raw helpers used in setup_all (before context is available)
    end
  end
end
