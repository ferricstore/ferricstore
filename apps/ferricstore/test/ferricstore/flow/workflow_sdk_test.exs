defmodule FerricStore.Flow.WorkflowSDKTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  defmodule Store do
    def flow_create(id, opts) do
      send(test_pid(), {:flow_create, id, opts})

      {:ok,
       %{id: id, type: opts[:type], state: opts[:state], partition_key: opts[:partition_key]}}
    end

    def flow_create_many(partition_key, items, opts) do
      send(test_pid(), {:flow_create_many, partition_key, items, opts})
      {:ok, Enum.map(items, &Map.take(&1, [:id, :partition_key]))}
    end

    def flow_spawn_children(parent_id, children, opts) do
      send(test_pid(), {:flow_spawn_children, parent_id, children, opts})
      {:ok, %{id: parent_id, state: opts[:wait_state] || opts[:success]}}
    end

    def flow_policy_set(type, opts) do
      send(test_pid(), {:flow_policy_set, type, opts})
      {:ok, %{type: type, policy: opts}}
    end

    def flow_claim_due(type, opts) do
      send(test_pid(), {:flow_claim_due, type, opts})

      {:ok,
       [
         %{
           id: "f1",
           type: type,
           state: "created",
           partition_key: "tenant-a:invoice-1",
           lease_token: "lease-1",
           fencing_token: 7,
           payload: %{amount: 42}
         }
       ]}
    end

    def flow_transition(id, from_state, to_state, opts) do
      send(test_pid(), {:flow_transition, id, from_state, to_state, opts})

      if Process.get(:flow_sdk_raise_transition) do
        raise "transition storage failure"
      end

      {:ok, %{id: id, state: to_state}}
    end

    def flow_complete(id, lease_token, opts) do
      send(test_pid(), {:flow_complete, id, lease_token, opts})
      {:ok, %{id: id, state: "completed"}}
    end

    def flow_retry(id, lease_token, opts) do
      send(test_pid(), {:flow_retry, id, lease_token, opts})
      {:ok, %{id: id, state: "created"}}
    end

    def flow_get(id, opts) do
      send(test_pid(), {:flow_get, id, opts})
      {:ok, %{id: id}}
    end

    def flow_history(id, opts) do
      send(test_pid(), {:flow_history, id, opts})
      {:ok, []}
    end

    def flow_list(type, opts), do: send_ok({:flow_list, type, opts}, [])

    def flow_by_parent(parent_flow_id, opts) do
      send_ok(
        {:flow_by_parent, parent_flow_id, opts},
        [
          %{id: "child-running", state: "created"},
          %{id: "child-done", state: "completed"}
        ]
      )
    end

    def flow_by_root(root_flow_id, opts), do: send_ok({:flow_by_root, root_flow_id, opts}, [])

    def flow_by_correlation(correlation_id, opts),
      do: send_ok({:flow_by_correlation, correlation_id, opts}, [])

    def flow_info(type, opts), do: send_ok({:flow_info, type, opts}, %{})
    def flow_stuck(type, opts), do: send_ok({:flow_stuck, type, opts}, [])

    def flow_reclaim(type, opts),
      do:
        send_ok({:flow_reclaim, type, opts}, [
          %{id: "reclaimed-1", type: type, state: "running", lease_token: "lease-r"}
        ])

    def flow_extend_lease(id, lease_token, opts),
      do: send_ok({:flow_extend_lease, id, lease_token, opts}, %{})

    def flow_cancel(id, opts), do: send_ok({:flow_cancel, id, opts}, %{})
    def flow_rewind(id, opts), do: send_ok({:flow_rewind, id, opts}, %{})

    defp test_pid, do: Process.get(:flow_sdk_test_pid)

    defp send_ok(message, result) do
      send(test_pid(), message)
      {:ok, result}
    end
  end

  defmodule BillingFlow do
    use FerricStore.Flow.Workflow,
      type: "billing",
      store: FerricStore.Flow.WorkflowSDKTest.Store,
      partition_by: [:tenant_id, :invoice_id],
      initial_state: :created

    state :created do
      lease_ms(60_000)
      claim_payload(true, max_bytes: 32_000)
      retry(max_retries: 8, backoff: [kind: :fixed, base_ms: 1_000, max_ms: 60_000])
      on_ok(:charged)
      on_error(retry_or: :failed)
    end

    state :charged do
      on_ok(complete())
      on_error(fail())
    end
  end

  defmodule OrderedFlow do
    use FerricStore.Flow.Workflow,
      type: "ordered",
      store: FerricStore.Flow.WorkflowSDKTest.Store,
      partition_by: [:tenant_id],
      initial_state: :queued

    state :queued do
      mode(:fifo)
      retry(max_retries: 2)
    end

    state :review do
      mode(:parallel)
    end
  end

  defmodule WorkerWorkflow do
    def claim_due(state, opts) do
      send(test_pid(), {:worker_claim_due, state, opts})

      {:ok,
       [
         FerricStore.Flow.Job.new(__MODULE__, %{
           id: "f1",
           type: "billing",
           state: "created",
           lease_token: "lease-1",
           fencing_token: 7
         })
       ]}
    end

    def ok(job, result) do
      send(test_pid(), {:worker_ok, job.id, result})
      {:ok, %{id: job.id, state: "charged"}}
    end

    def error(job, reason) do
      send(test_pid(), {:worker_error, job.id, reason})
      {:ok, %{id: job.id, state: "failed"}}
    end

    defp test_pid, do: :persistent_term.get({__MODULE__, :test_pid})
  end

  setup do
    Process.put(:flow_sdk_test_pid, self())
    :persistent_term.put({WorkerWorkflow, :test_pid}, self())
    :ok
  end

  test "create derives type, state and partition key from attrs" do
    assert {:ok, record} =
             BillingFlow.create(%{
               id: "f1",
               tenant_id: "tenant-a",
               invoice_id: "invoice-1",
               payload: %{amount: 42},
               correlation_id: "order-1"
             })

    assert record.partition_key == "fpk:8:tenant-a9:invoice-1"

    assert_received {:flow_create, "f1", create_opts}
    assert create_opts[:type] == "billing"
    assert create_opts[:state] == "created"
    assert create_opts[:partition_key] == "fpk:8:tenant-a9:invoice-1"
    assert create_opts[:payload] == %{amount: 42}
    assert create_opts[:correlation_id] == "order-1"
  end

  test "derived partition keys cannot collide through component delimiters" do
    assert {:ok, first} =
             BillingFlow.create(%{
               id: "f-partition-first",
               tenant_id: "a:b",
               invoice_id: "c"
             })

    assert {:ok, second} =
             BillingFlow.create(%{
               id: "f-partition-second",
               tenant_id: "a",
               invoice_id: "b:c"
             })

    refute first.partition_key == second.partition_key
  end

  test "create_many groups by shard in core by passing per-item partition keys" do
    assert {:ok, _} =
             BillingFlow.create_many([
               %{id: "f1", tenant_id: "tenant-a", invoice_id: "1"},
               %{id: "f2", tenant_id: "tenant-b", invoice_id: "2"}
             ])

    assert_received {:flow_create_many, nil, items, opts}

    assert items == [
             %{id: "f1", partition_key: "fpk:8:tenant-a1:1"},
             %{id: "f2", partition_key: "fpk:8:tenant-b1:2"}
           ]

    assert opts[:type] == "billing"
    assert opts[:state] == "created"
  end

  test "workflow commands cannot override the declared flow type" do
    assert {:ok, _record} =
             BillingFlow.create(
               %{id: "f-type", tenant_id: "tenant-a", invoice_id: "1"},
               type: "forged"
             )

    assert_received {:flow_create, "f-type", create_opts}
    assert create_opts[:type] == "billing"

    assert {:ok, _records} =
             BillingFlow.create_many(
               [%{id: "f-type-many", tenant_id: "tenant-a", invoice_id: "2"}],
               type: "forged"
             )

    assert_received {:flow_create_many, nil, _items, create_many_opts}
    assert create_many_opts[:type] == "billing"

    assert %{type: "billing"} =
             BillingFlow.child(
               %{id: "f-type-child", tenant_id: "tenant-a", invoice_id: "3"},
               type: "forged"
             )
  end

  test "partition builders reject non-scalar values without raising" do
    error =
      {:error, "ERR flow partition key fields must be strings, atoms, integers, or floats"}

    attrs = %{id: "f-invalid-partition", tenant_id: %{}, invoice_id: "1"}

    assert ^error = BillingFlow.create(attrs)
    assert ^error = BillingFlow.create_many([attrs])
    assert ^error = BillingFlow.child(attrs)
  end

  test "child builds child specs with workflow defaults" do
    assert %{
             id: "child-1",
             type: "billing",
             state: "created",
             partition_key: "fpk:8:tenant-a9:invoice-1",
             payload: %{amount: 10}
           } =
             BillingFlow.child(%{
               id: "child-1",
               tenant_id: "tenant-a",
               invoice_id: "invoice-1",
               payload: %{amount: 10}
             })
  end

  test "fanout wraps flow_spawn_children with parent guard defaults and aliases" do
    parent =
      FerricStore.Flow.Job.new(BillingFlow, %{
        id: "parent-1",
        state: "created",
        partition_key: "tenant-a:invoice-1",
        lease_token: "lease-1",
        fencing_token: 0
      })

    child = BillingFlow.child(%{id: "child-1", tenant_id: "tenant-a", invoice_id: "invoice-1"})

    assert {:ok, %{state: "waiting_children"}} =
             BillingFlow.fanout(parent, [child],
               group_id: "charge-fanout",
               wait: :all,
               on_all_ok: :charged,
               on_any_error: :failed
             )

    assert_received {:flow_spawn_children, "parent-1", [^child], fanout_opts}
    assert fanout_opts[:group_id] == "charge-fanout"
    assert fanout_opts[:wait] == :all
    assert fanout_opts[:wait_state] == "waiting_children"
    assert fanout_opts[:success] == "charged"
    assert fanout_opts[:failure] == "failed"
    assert fanout_opts[:partition_key] == "tenant-a:invoice-1"
    assert fanout_opts[:from_state] == "created"
    assert fanout_opts[:lease_token] == "lease-1"
    assert fanout_opts[:fencing_token] == 0
  end

  test "children and waiting_children query by parent with partition inherited from job" do
    parent =
      FerricStore.Flow.Job.new(BillingFlow, %{
        id: "parent-1",
        state: "created",
        type: "billing",
        partition_key: "tenant-a:invoice-1",
        fencing_token: 7
      })

    assert {:ok, children} = BillingFlow.children(parent, count: 10)
    assert Enum.map(children, & &1.id) == ["child-running", "child-done"]

    assert_received {:flow_by_parent, "parent-1", child_opts}
    assert child_opts[:partition_key] == "tenant-a:invoice-1"
    assert child_opts[:count] == 10

    assert {:ok, waiting} = BillingFlow.waiting_children(parent)
    assert Enum.map(waiting, & &1.id) == ["child-running"]
  end

  test "claim_due applies state defaults and wraps returned records as jobs" do
    assert {:ok, [job]} = BillingFlow.claim_due(:created, worker: "worker-1", limit: 10)

    assert %FerricStore.Flow.Job{} = job
    assert job.id == "f1"
    assert job.workflow == BillingFlow
    assert job.payload == %{amount: 42}

    assert_received {:flow_claim_due, "billing", claim_opts}
    assert claim_opts[:state] == "created"
    assert claim_opts[:lease_ms] == 60_000
    assert claim_opts[:payload] == true
    assert claim_opts[:payload_max_bytes] == 32_000
    assert claim_opts[:worker] == "worker-1"
    assert claim_opts[:limit] == 10
  end

  test "ok follows on_ok transition and carries lease/fencing guards" do
    job =
      FerricStore.Flow.Job.new(BillingFlow, %{
        id: "f1",
        state: "created",
        partition_key: "tenant-a:invoice-1",
        lease_token: "lease-1",
        fencing_token: 7
      })

    assert {:ok, %{state: "charged"}} = BillingFlow.ok(job, %{charged: true})

    assert_received {:flow_transition, "f1", "created", "charged", transition_opts}
    assert transition_opts[:lease_token] == "lease-1"
    assert transition_opts[:fencing_token] == 7
    assert transition_opts[:partition_key] == "tenant-a:invoice-1"
    assert transition_opts[:payload] == %{charged: true}
  end

  test "ok can complete terminal states" do
    job =
      FerricStore.Flow.Job.new(BillingFlow, %{
        id: "f1",
        state: "charged",
        partition_key: "tenant-a:invoice-1",
        lease_token: "lease-1",
        fencing_token: 7
      })

    assert {:ok, %{state: "completed"}} = BillingFlow.ok(job, "receipt")

    assert_received {:flow_complete, "f1", "lease-1", complete_opts}
    assert complete_opts[:fencing_token] == 7
    assert complete_opts[:partition_key] == "tenant-a:invoice-1"
    assert complete_opts[:result] == "receipt"
  end

  test "error applies state retry policy and exhausted target" do
    job =
      FerricStore.Flow.Job.new(BillingFlow, %{
        id: "f1",
        state: "created",
        partition_key: "tenant-a:invoice-1",
        lease_token: "lease-1",
        fencing_token: 7
      })

    assert {:ok, %{state: "created"}} = BillingFlow.error(job, "declined")

    assert_received {:flow_retry, "f1", "lease-1", retry_opts}
    assert retry_opts[:fencing_token] == 7
    assert retry_opts[:partition_key] == "tenant-a:invoice-1"
    assert retry_opts[:error] == "declined"
    assert retry_opts[:retry][:max_retries] == 8
    assert retry_opts[:retry][:backoff] == [kind: :fixed, base_ms: 1_000, max_ms: 60_000]
    assert retry_opts[:retry][:exhausted_to] == "failed"
  end

  test "install_policy writes DSL retry policy into embedded API" do
    assert {:ok, _} = BillingFlow.install_policy()

    assert_received {:flow_policy_set, "billing",
                     [
                       states: %{
                         "created" => [
                           mode: :parallel,
                           retry: [
                             max_retries: 8,
                             backoff: [kind: :fixed, base_ms: 1_000, max_ms: 60_000],
                             exhausted_to: "failed"
                           ]
                         ],
                         "charged" => [mode: :parallel]
                       },
                       replace: true
                     ]}
  end

  test "install_policy writes each declared state execution mode" do
    assert {:ok, _} = OrderedFlow.install_policy()

    assert_received {:flow_policy_set, "ordered",
                     [
                       states: %{
                         "queued" => [
                           mode: :fifo,
                           retry: [max_retries: 2]
                         ],
                         "review" => [mode: :parallel]
                       },
                       replace: true
                     ]}
  end

  test "install_policy can explicitly patch an existing policy" do
    assert {:ok, _} = OrderedFlow.install_policy(replace: false, max_active_ms: 30_000)

    assert_received {:flow_policy_set, "ordered", opts}
    assert opts[:replace] == false
    assert opts[:max_active_ms] == 30_000
    assert opts[:states]["queued"][:mode] == :fifo
  end

  test "worker polls claim_due and applies ok result" do
    test_pid = self()

    handler = fn job ->
      send(test_pid, {:handler_seen, job.id})
      {:ok, "done"}
    end

    assert {:ok, pid} =
             FerricStore.Flow.Worker.start_link(
               workflow: WorkerWorkflow,
               state: :created,
               worker: "worker-1",
               limit: 10,
               interval_ms: 60_000,
               handler: handler
             )

    assert_receive {:worker_claim_due, :created, opts}
    assert opts[:worker] == "worker-1"
    assert opts[:limit] == 10
    assert_receive {:handler_seen, "f1"}
    assert_receive {:worker_ok, "f1", "done"}

    GenServer.stop(pid)
  end

  test "run_once and handle convert handler return values to ok or error commands" do
    test_pid = self()

    assert {:ok, [ok_result]} =
             BillingFlow.run_once(:created,
               worker: "worker-1",
               handler: fn job ->
                 send(test_pid, {:run_once_seen, job.id})
                 {:ok, "charged"}
               end
             )

    assert {:ok, %{state: "charged"}} = ok_result
    assert_receive {:run_once_seen, "f1"}
    assert_received {:flow_transition, "f1", "created", "charged", ok_opts}
    assert ok_opts[:payload] == "charged"

    job =
      FerricStore.Flow.Job.new(BillingFlow, %{
        id: "f2",
        state: "created",
        type: "billing",
        partition_key: "tenant-a:invoice-1",
        lease_token: "lease-2",
        fencing_token: 8
      })

    assert {:ok, %{state: "created"}} =
             BillingFlow.handle(job, fn _job -> {:error, "boom"} end)

    assert_received {:flow_retry, "f2", "lease-2", retry_opts}
    assert retry_opts[:error] == "boom"
  end

  test "handle does not convert storage mutation failures into a second command" do
    Process.put(:flow_sdk_raise_transition, true)

    job =
      FerricStore.Flow.Job.new(BillingFlow, %{
        id: "f-storage-failure",
        state: "created",
        type: "billing",
        partition_key: "tenant-a:invoice-1",
        lease_token: "lease-storage-failure",
        fencing_token: 9
      })

    assert_raise RuntimeError, "transition storage failure", fn ->
      BillingFlow.handle(job, fn _job -> {:ok, "charged"} end)
    end

    assert_received {:flow_transition, "f-storage-failure", "created", "charged", _opts}
    refute_received {:flow_retry, "f-storage-failure", "lease-storage-failure", _opts}
  after
    Process.delete(:flow_sdk_raise_transition)
  end

  test "reclaim_once wraps reclaimed records as jobs" do
    assert {:ok, jobs} = BillingFlow.reclaim_once(:running, worker: "worker-1", limit: 10)

    assert [%FerricStore.Flow.Job{}] = jobs
    assert_received {:flow_reclaim, "billing", opts}
    assert opts[:state] == "running"
    assert opts[:reclaim_expired] == false
    assert opts[:worker] == "worker-1"
  end
end
