defmodule FerricstoreServer.Native.BlockingTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Native.{Blocking, ResourceBudget, Session}

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

  test "BRPOPLPUSH is not exposed as a blocking compatibility command" do
    refute Blocking.blocking_command?("BRPOPLPUSH")

    assert {:error, "ERR unknown command 'brpoplpush', with args beginning with: "} =
             Session.prepare_command(%{
               "command" => "BRPOPLPUSH",
               "args" => ["source", "destination", "1"]
             })
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
