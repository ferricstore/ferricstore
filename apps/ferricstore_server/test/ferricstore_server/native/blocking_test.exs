defmodule FerricstoreServer.Native.BlockingTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias FerricstoreServer.Native.{Blocking, OutboundBudget, ResourceBudget, Session}

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    :ok
  end

  test "blocking workers are monitored by their connection owner" do
    assert {:ok, prepared} =
             Session.prepare_command(%{
               "command" => "BLPOP",
               "args" => ["native:blocking:monitored", "5"]
             })

    state = %{
      instance_ctx: FerricStore.Instance.get(:default),
      acl_cache: :full_access,
      require_auth: false,
      authenticated: true
    }

    assert {:ok, pid, monitor_ref} = Blocking.start_prepared(prepared, state, %{})
    assert is_pid(pid)
    assert is_reference(monitor_ref)

    Process.exit(pid, :shutdown)
    assert_receive {:DOWN, ^monitor_ref, :process, ^pid, :shutdown}, 1_000
  end

  test "blocking workers terminate when their connection owner dies" do
    test_pid = self()

    owner =
      spawn(fn ->
        {:ok, prepared} =
          Session.prepare_command(%{
            "command" => "BLPOP",
            "args" => ["native:blocking:owner-death", "0"]
          })

        state = %{
          instance_ctx: FerricStore.Instance.get(:default),
          acl_cache: :full_access,
          require_auth: false,
          authenticated: true
        }

        {:ok, worker, _monitor_ref} = Blocking.start_prepared(prepared, state, %{})
        send(test_pid, {:blocking_worker, worker})
        Process.sleep(:infinity)
      end)

    assert_receive {:blocking_worker, worker}, 1_000
    worker_ref = Process.monitor(worker)
    Process.exit(owner, :kill)

    assert_receive {:DOWN, ^worker_ref, :process, ^worker, _reason}, 1_000
  end

  test "blocking workers share a server-wide waiter budget" do
    budget = :"native_blocking_budget_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: budget,
       limits: %{executions: 1, lanes: 1, blocking_requests: 1, chunk_streams: 1, chunk_bytes: 1}}
    )

    assert {:ok, prepared} =
             Session.prepare_command(%{
               "command" => "BLPOP",
               "args" => ["native:blocking:global-budget", "0"]
             })

    state = %{
      instance_ctx: FerricStore.Instance.get(:default),
      acl_cache: :full_access,
      require_auth: false,
      authenticated: true,
      resource_budget: budget
    }

    assert {:ok, worker, monitor_ref} = Blocking.start_prepared(prepared, state, %{})
    assert %{blocking_requests: 1} = ResourceBudget.usage(budget)

    assert {:error, :busy, "ERR native global blocking request limit exceeded"} =
             Blocking.start_prepared(prepared, state, %{})

    Process.exit(worker, :kill)
    assert_receive {:DOWN, ^monitor_ref, :process, ^worker, :killed}, 1_000
    assert wait_until(fn -> ResourceBudget.usage(budget).blocking_requests == 0 end) == :ok
  end

  test "finite blocking timeouts are not starved by queued notifications" do
    key = "native:blocking:deadline:#{System.unique_integer([:positive])}"

    assert {:ok, prepared} =
             Session.prepare_command(%{
               "command" => "BLPOP",
               "args" => [key, "0.01"]
             })

    state = %{
      instance_ctx: FerricStore.Instance.get(:default),
      acl_cache: :full_access,
      require_auth: false,
      authenticated: true
    }

    meta = %{request_id: make_ref()}
    assert {:ok, worker, monitor_ref} = Blocking.start_prepared(prepared, state, meta)

    for _ <- 1..20_000, do: send(worker, {:waiter_notify, key})

    assert_receive {:native_blocking_response, ^meta, ^worker, :ok, nil}, 500
    assert_receive {:DOWN, ^monitor_ref, :process, ^worker, :normal}, 500
  end

  test "blocking results retain outbound capacity after the worker exits" do
    budget = start_outbound_budget(10_000)
    counter = OutboundBudget.new_counter()
    key = "native:blocking:outbound:#{System.unique_integer([:positive, :monotonic])}"
    value = :binary.copy("v", 256)
    assert {:ok, 1} = FerricStore.rpush(key, [value])
    on_exit(fn -> FerricStore.del(key) end)

    assert {:ok, prepared} =
             Session.prepare_command(%{"command" => "BLPOP", "args" => [key, "1"]})

    state = %{
      instance_ctx: FerricStore.Instance.get(:default),
      acl_cache: :full_access,
      require_auth: false,
      authenticated: true,
      resource_budget: budget,
      outbound_counter: counter,
      max_outbound_bytes: 10_000
    }

    meta = %{request_id: make_ref()}
    assert {:ok, worker, monitor_ref} = Blocking.start_prepared(prepared, state, meta)

    assert_receive {:native_blocking_response_budgeted, ^meta, ^worker, :ok, [^key, ^value],
                    %OutboundBudget{} = lease},
                   1_000

    refute_receive {:DOWN, ^monitor_ref, :process, ^worker, _reason}
    assert OutboundBudget.usage(counter) > 0
    assert ResourceBudget.usage(budget).outbound_bytes > 0

    assert :ok = OutboundBudget.release(lease)
    send(worker, {:native_blocking_outbound_released, lease.resource_token})
    assert_receive {:DOWN, ^monitor_ref, :process, ^worker, :normal}, 1_000
    assert OutboundBudget.usage(counter) == 0
    assert ResourceBudget.usage(budget).outbound_bytes == 0
  end

  test "blocking result overflow emits a bounded close signal without the result" do
    budget = start_outbound_budget(10_000)
    counter = OutboundBudget.new_counter()
    key = "native:blocking:outbound-overflow:#{System.unique_integer([:positive, :monotonic])}"
    assert {:ok, 1} = FerricStore.rpush(key, [:binary.copy("v", 256)])
    on_exit(fn -> FerricStore.del(key) end)

    assert {:ok, prepared} =
             Session.prepare_command(%{"command" => "BLPOP", "args" => [key, "1"]})

    state = %{
      instance_ctx: FerricStore.Instance.get(:default),
      acl_cache: :full_access,
      require_auth: false,
      authenticated: true,
      resource_budget: budget,
      outbound_counter: counter,
      max_outbound_bytes: 16
    }

    meta = %{request_id: make_ref()}
    assert {:ok, worker, monitor_ref} = Blocking.start_prepared(prepared, state, meta)

    assert_receive {:native_blocking_outbound_overflow, ^meta, ^worker}, 1_000
    refute_receive {:native_blocking_response, ^meta, ^worker, _status, _value}
    refute_receive {:native_blocking_response_budgeted, ^meta, ^worker, _status, _value, _lease}
    assert_receive {:DOWN, ^monitor_ref, :process, ^worker, :normal}, 1_000
    assert OutboundBudget.usage(counter) == 0
    assert ResourceBudget.usage(budget).outbound_bytes == 0
  end

  test "XREAD remains wakeable when a stream is deleted and immediately recreated" do
    key = "native:blocking:stream-recreate:#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.get(:default)
    assert {:ok, old_id} = FerricStore.xadd(key, ["field", "old"])

    assert {:ok, prepared} =
             Session.prepare_command(%{
               "command" => "XREAD",
               "args" => ["BLOCK", "0", "STREAMS", key, old_id]
             })

    state = %{
      instance_ctx: ctx,
      acl_cache: :full_access,
      require_auth: false,
      authenticated: true
    }

    meta = %{request_id: make_ref()}
    assert {:ok, worker, monitor_ref} = Blocking.start_prepared(prepared, state, meta)

    on_exit(fn ->
      if Process.alive?(worker), do: Process.exit(worker, :kill)
      FerricStore.del(key)
    end)

    assert wait_until(fn ->
             Ferricstore.Commands.Stream.stream_waiter_count(key, ctx) == 1
           end) == :ok

    assert {:ok, 1} = FerricStore.del(key)
    assert {:ok, new_id} = FerricStore.xadd(key, ["field", "new"])

    assert_receive {:native_blocking_response, ^meta, ^worker, :ok, result}, 1_000
    assert new_id in List.flatten(result)
    assert_receive {:DOWN, ^monitor_ref, :process, ^worker, :normal}, 500
  end

  test "XREAD wakes with WRONGTYPE when SET replaces its stream" do
    key = "native:blocking:stream-overwrite:#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.get(:default)
    assert {:ok, old_id} = FerricStore.xadd(key, ["field", "old"])

    assert {:ok, prepared} =
             Session.prepare_command(%{
               "command" => "XREAD",
               "args" => ["BLOCK", "0", "STREAMS", key, old_id]
             })

    state = %{
      instance_ctx: ctx,
      acl_cache: :full_access,
      require_auth: false,
      authenticated: true
    }

    meta = %{request_id: make_ref()}
    assert {:ok, worker, monitor_ref} = Blocking.start_prepared(prepared, state, meta)

    on_exit(fn ->
      if Process.alive?(worker), do: Process.exit(worker, :kill)
      FerricStore.del(key)
    end)

    assert wait_until(fn ->
             Ferricstore.Commands.Stream.stream_waiter_count(key, ctx) == 1
           end) == :ok

    assert :ok = FerricStore.set(key, "replacement")

    assert_receive {:native_blocking_response, ^meta, ^worker, :error, reason}, 1_000
    assert reason =~ "WRONGTYPE"
    assert_receive {:DOWN, ^monitor_ref, :process, ^worker, :normal}, 500
    assert {:ok, "replacement"} = FerricStore.get(key)
    assert Router.compound_count(ctx, key, Ferricstore.Store.CompoundKey.stream_prefix(key)) == 0
  end

  test "XREAD remains wakeable across FLUSHDB and immediate recreation" do
    key = "native:blocking:stream-flush:#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.get(:default)
    assert {:ok, old_id} = FerricStore.xadd(key, ["field", "old"])

    assert {:ok, prepared} =
             Session.prepare_command(%{
               "command" => "XREAD",
               "args" => ["BLOCK", "0", "STREAMS", key, old_id]
             })

    state = %{
      instance_ctx: ctx,
      acl_cache: :full_access,
      require_auth: false,
      authenticated: true
    }

    meta = %{request_id: make_ref()}
    assert {:ok, worker, monitor_ref} = Blocking.start_prepared(prepared, state, meta)

    on_exit(fn ->
      if Process.alive?(worker), do: Process.exit(worker, :kill)
      FerricStore.del(key)
    end)

    assert wait_until(fn ->
             Ferricstore.Commands.Stream.stream_waiter_count(key, ctx) == 1
           end) == :ok

    assert :ok = FerricStore.flushdb()
    assert {:ok, new_id} = FerricStore.xadd(key, ["field", "new"])

    assert_receive {:native_blocking_response, ^meta, ^worker, :ok, result}, 1_000
    assert new_id in List.flatten(result)
    assert_receive {:DOWN, ^monitor_ref, :process, ^worker, :normal}, 500
  end

  test "BRPOPLPUSH is not exposed as a blocking compatibility command" do
    refute Blocking.blocking_command?("BRPOPLPUSH")

    assert {:error, "ERR unknown command 'brpoplpush', with args beginning with: "} =
             Session.prepare_command(%{
               "command" => "BRPOPLPUSH",
               "args" => ["source", "destination", "1"]
             })
  end

  defp start_outbound_budget(outbound_bytes) do
    name = :"native_blocking_outbound_budget_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       limits: %{
         executions: 1,
         lanes: 1,
         blocking_requests: 1,
         chunk_streams: 1,
         chunk_bytes: 1,
         inbound_bytes: 1,
         subscription_bytes: 1,
         session_bytes: 1,
         outbound_bytes: outbound_bytes
       }}
    )

    name
  end

  defp wait_until(fun, attempts \\ 100)

  defp wait_until(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      wait_until(fun, attempts - 1)
    end
  end

  defp wait_until(_fun, 0), do: flunk("condition was not met before timeout")
end
