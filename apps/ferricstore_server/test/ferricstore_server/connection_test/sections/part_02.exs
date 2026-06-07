defmodule FerricstoreServer.ConnectionTest.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Encoder
      alias FerricstoreServer.Resp.Parser
      alias FerricstoreServer.Listener
      alias FerricstoreServer.Connection.Pipeline

  test "pipeline prefetch does not read through keyless write barrier", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    key = "prefetch-flushdb-barrier:" <> Integer.to_string(System.unique_integer([:positive]))

    send_command(sock, ["SET", key, "old"])
    assert [{:simple, "OK"}] = recv_values(sock, 1)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["FLUSHDB"]),
        Encoder.encode(["GET", key])
      ])

    send_raw(sock, pipeline)
    assert [{:simple, "OK"}, nil] = recv_values(sock, 2, "", 60)

    :gen_tcp.close(sock)
  end

  test "multi-key DEL stays on existing command path and remains correct", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    tag = "{phase1-del:" <> Integer.to_string(System.unique_integer([:positive])) <> "}"
    key_a = "#{tag}:a"
    key_b = "#{tag}:b"

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["SET", key_a, "a"]),
        Encoder.encode(["SET", key_b, "b"]),
        Encoder.encode(["DEL", key_a, key_b]),
        Encoder.encode(["GET", key_a]),
        Encoder.encode(["GET", key_b])
      ])

    send_raw(sock, pipeline)
    assert [{:simple, "OK"}, {:simple, "OK"}, 2, nil, nil] = recv_values(sock, 5)

    :gen_tcp.close(sock)
  end

  test "pipelined FLOW.CLAIM_DUE commands coalesce compatible claims", %{port: port} do
    handler_id =
      {__MODULE__, self(), :flow_claim_due_pipeline, System.unique_integer([:positive])}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :pipeline_claim_due_batch],
        &__MODULE__.handle_pipeline_claim_due_batch/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))
    type = "pipeline-claim:" <> Integer.to_string(System.unique_integer([:positive]))

    ids =
      for idx <- 1..3 do
        id = "#{type}:#{idx}:#{System.unique_integer([:positive])}"

        assert :ok =
                 FerricStore.flow_create(id,
                   type: type,
                   partition_key: partition,
                   run_at_ms: 1_000,
                   now_ms: 1_000
                 )

        id
      end

    claim =
      Encoder.encode([
        "FLOW.CLAIM_DUE",
        type,
        "STATE",
        "queued",
        "WORKER",
        "worker-a",
        "PARTITION",
        partition,
        "LIMIT",
        "1",
        "NOW",
        "2000"
      ])

    send_raw(sock, IO.iodata_to_binary([claim, claim, claim]))

    results = recv_values(sock, 3)
    claimed_ids = results |> Enum.flat_map(& &1) |> Enum.map(&Map.fetch!(&1, "id"))

    assert Enum.all?(results, &(length(&1) == 1))
    assert MapSet.new(claimed_ids) == MapSet.new(ids)

    assert_receive {:pipeline_claim_due_batch, %{commands: 3, groups: 1, coalesced_calls: 1},
                    %{source: :resp_pipeline}},
                   1_000

    :gen_tcp.close(sock)
  end

  test "pipelined FLOW.CLAIM_DUE preserves partition lists and named value hydration", %{
    port: port
  } do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    suffix = Integer.to_string(System.unique_integer([:positive]))
    type = "pipeline-claim-values:" <> suffix
    partition_a = "tenant-a:" <> suffix
    partition_b = "tenant-b:" <> suffix
    partition_c = "tenant-c:" <> suffix

    for {partition, idx} <- [{partition_a, 1}, {partition_b, 2}, {partition_c, 3}] do
      assert :ok =
               FerricStore.flow_create("#{type}:#{idx}",
                 type: type,
                 partition_key: partition,
                 values: %{"payment" => "payment-#{idx}", "ignored" => "ignored-#{idx}"},
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )
    end

    claim =
      Encoder.encode([
        "FLOW.CLAIM_DUE",
        type,
        "STATE",
        "queued",
        "WORKER",
        "worker-a",
        "PARTITIONS",
        "2",
        partition_a,
        partition_b,
        "LIMIT",
        "2",
        "NOW",
        "2000",
        "NOPAYLOAD",
        "VALUE",
        "payment"
      ])

    send_raw(sock, IO.iodata_to_binary([claim, claim]))

    assert [claimed, []] = recv_values(sock, 2)
    assert length(claimed) == 2

    claimed_by_partition = Map.new(claimed, &{Map.fetch!(&1, "partition_key"), &1})

    assert Map.keys(claimed_by_partition) |> MapSet.new() ==
             MapSet.new([partition_a, partition_b])

    refute Map.has_key?(claimed_by_partition, partition_c)

    assert %{"payment" => "payment-1"} = claimed_by_partition[partition_a]["values"]
    assert %{"payment" => "payment-2"} = claimed_by_partition[partition_b]["values"]
    refute Map.has_key?(claimed_by_partition[partition_a]["values"], "ignored")

    :gen_tcp.close(sock)
  end

  test "FLOW.CLAIM_DUE BLOCK waits and wakes on a due create", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))
    type = "block-claim:" <> Integer.to_string(System.unique_integer([:positive]))
    id = "#{type}:#{System.unique_integer([:positive])}"

    claim =
      Encoder.encode([
        "FLOW.CLAIM_DUE",
        type,
        "WORKER",
        "worker-a",
        "PARTITION",
        partition,
        "LIMIT",
        "1",
        "RETURN",
        "JOBS_COMPACT",
        "BLOCK",
        "1000"
      ])

    send_raw(sock, claim)

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> flow_claim_waiter_registered?(type, partition) end,
      "RESP claim_due waiter registered",
      100,
      5
    )

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               partition_key: partition,
               state: "queued",
               now_ms: 1_000,
               run_at_ms: 1_000
             )

    assert [[[^id, ^partition, lease_token, fencing_token]]] = recv_values(sock, 1)
    assert is_binary(lease_token)
    assert is_integer(fencing_token)

    :gen_tcp.close(sock)
  end

  test "pipelined FLOW.CLAIM_DUE BLOCK 0 waits forever and holds later commands", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))
    type = "block-forever-claim:" <> Integer.to_string(System.unique_integer([:positive]))
    id = "#{type}:#{System.unique_integer([:positive])}"

    claim =
      Encoder.encode([
        "FLOW.CLAIM_DUE",
        type,
        "WORKER",
        "worker-a",
        "PARTITION",
        partition,
        "LIMIT",
        "1",
        "RETURN",
        "JOBS_COMPACT",
        "BLOCK",
        "0"
      ])

    send_raw(sock, IO.iodata_to_binary([claim, Encoder.encode(["PING"])]))

    assert {:error, :timeout} = :gen_tcp.recv(sock, 0, 80)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               partition_key: partition,
               state: "queued",
               now_ms: 1_000,
               run_at_ms: 1_000
             )

    assert [[[^id, ^partition, lease_token, fencing_token]], {:simple, "PONG"}] =
             recv_values(sock, 2)

    assert is_binary(lease_token)
    assert is_integer(fencing_token)

    :gen_tcp.close(sock)
  end

  test "FLOW.CLAIM_DUE BLOCK returns an error when waiter row cap is reached", %{port: port} do
    previous_max = Application.get_env(:ferricstore, :flow_claim_due_max_waiter_rows)
    Application.put_env(:ferricstore, :flow_claim_due_max_waiter_rows, 1)

    occupying_keys = Ferricstore.Flow.ClaimWaiters.wait_keys("occupied", "queued", 0, "p1")
    deadline = System.monotonic_time(:millisecond) + 5_000

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    try do
      assert :ok = Ferricstore.Flow.ClaimWaiters.register(occupying_keys, self(), deadline)

      send_command(sock, [
        "FLOW.CLAIM_DUE",
        "blocked-cap:" <> Integer.to_string(System.unique_integer([:positive])),
        "WORKER",
        "worker-a",
        "PARTITION",
        "tenant:" <> Integer.to_string(System.unique_integer([:positive])),
        "LIMIT",
        "1",
        "BLOCK",
        "1000"
      ])

      assert [{:error, "ERR max blocked claim_due waiters reached"}] = recv_values(sock, 1)
    after
      :gen_tcp.close(sock)
      Ferricstore.Flow.ClaimWaiters.unregister(occupying_keys, self())

      case previous_max do
        nil -> Application.delete_env(:ferricstore, :flow_claim_due_max_waiter_rows)
        value -> Application.put_env(:ferricstore, :flow_claim_due_max_waiter_rows, value)
      end
    end
  end

  test "FLOW.CLAIM_DUE BLOCK re-registers after a spurious wake", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))
    type = "block-reregister-claim:" <> Integer.to_string(System.unique_integer([:positive]))
    id = "#{type}:#{System.unique_integer([:positive])}"

    try do
      send_command(sock, [
        "FLOW.CLAIM_DUE",
        type,
        "STATE",
        "queued",
        "WORKER",
        "worker-a",
        "PARTITION",
        partition,
        "LIMIT",
        "1",
        "RETURN",
        "JOBS_COMPACT",
        "BLOCK",
        "10000"
      ])

      Ferricstore.Test.ShardHelpers.eventually(
        fn -> flow_claim_waiter_registered?(type, partition) end,
        "RESP claim_due waiter registered",
        100,
        5
      )

      assert 1 = Ferricstore.Flow.ClaimWaiters.notify_ready(type, "queued", 0, partition, 1)

      Ferricstore.Test.ShardHelpers.eventually(
        fn -> flow_claim_waiter_registered?(type, partition) end,
        "RESP claim_due waiter re-registered after empty wake",
        4_000,
        20
      )

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 partition_key: partition,
                 state: "queued",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      assert [[[^id, ^partition, lease_token, fencing_token]]] = recv_values(sock, 1)
      assert is_binary(lease_token)
      assert is_integer(fencing_token)
    after
      :gen_tcp.close(sock)
    end
  end

  test "FLOW.CLAIM_DUE BLOCK performs one empty claim attempt before waiting", %{port: port} do
    handler_id =
      {__MODULE__, self(), :flow_claim_due_block_once, System.unique_integer([:positive])}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :claim_due, :stop],
        &__MODULE__.handle_flow_claim_due_stop/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))
    type = "block-empty-claim:" <> Integer.to_string(System.unique_integer([:positive]))

    send_command(sock, [
      "FLOW.CLAIM_DUE",
      type,
      "WORKER",
      "worker-a",
      "PARTITION",
      partition,
      "LIMIT",
      "1",
      "BLOCK",
      "20"
    ])

    assert [[]] = recv_values(sock, 1)

    assert_receive {:flow_claim_due_stop, %{count: 0}, %{flow_type: ^type}}, 1_000
    refute_receive {:flow_claim_due_stop, _measurements, %{flow_type: ^type}}, 100

    :gen_tcp.close(sock)
  end

  test "FLOW.CLAIM_DUE BLOCK stays idle until wake instead of polling", %{port: port} do
    handler_id =
      {__MODULE__, self(), :flow_claim_due_block_idle, System.unique_integer([:positive])}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :claim_due, :stop],
        &__MODULE__.handle_flow_claim_due_stop/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))
    type = "block-idle-claim:" <> Integer.to_string(System.unique_integer([:positive]))
    id = "#{type}:#{System.unique_integer([:positive])}"

    try do
      send_command(sock, [
        "FLOW.CLAIM_DUE",
        type,
        "WORKER",
        "worker-a",
        "PARTITION",
        partition,
        "LIMIT",
        "1",
        "RETURN",
        "JOBS_COMPACT",
        "BLOCK",
        "2000"
      ])

      assert_receive {:flow_claim_due_stop, %{count: 0}, %{flow_type: ^type}}, 1_000

      Ferricstore.Test.ShardHelpers.eventually(
        fn -> flow_claim_waiter_registered?(type, partition) end,
        "RESP claim_due waiter registered before idle wake",
        100,
        5
      )

      refute_receive {:flow_claim_due_stop, _measurements, %{flow_type: ^type}}, 90

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 partition_key: partition,
                 state: "queued",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      assert [[[^id, ^partition, _lease_token, _fencing_token]]] = recv_values(sock, 1)
    after
      :gen_tcp.close(sock)
    end
  end

  test "FLOW.CLAIM_DUE BLOCK schedules an existing delayed job without polling", %{port: port} do
    handler_id =
      {__MODULE__, self(), :flow_claim_due_block_delayed, System.unique_integer([:positive])}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :claim_due, :stop],
        &__MODULE__.handle_flow_claim_due_stop/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))
    type = "block-delayed-claim:" <> Integer.to_string(System.unique_integer([:positive]))
    id = "#{type}:#{System.unique_integer([:positive])}"
    now = Ferricstore.CommandTime.now_ms()
    run_at = now + 500

    try do
      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 partition_key: partition,
                 state: "queued",
                 now_ms: now,
                 run_at_ms: run_at
               )

      send_command(sock, [
        "FLOW.CLAIM_DUE",
        type,
        "STATE",
        "queued",
        "WORKER",
        "worker-a",
        "PARTITION",
        partition,
        "LIMIT",
        "1",
        "RETURN",
        "JOBS_COMPACT",
        "BLOCK",
        "2000"
      ])

      assert_receive {:flow_claim_due_stop, %{count: 0}, %{flow_type: ^type}}, 1_000
      refute_receive {:flow_claim_due_stop, _measurements, %{flow_type: ^type}}, 100
      assert [[[^id, ^partition, _lease_token, _fencing_token]]] = recv_values(sock, 1)
    after
      :gen_tcp.close(sock)
    end
  end

  test "FLOW.CLAIM_DUE BLOCK reschedules delayed job after empty wake", %{port: port} do
    handler_id =
      {__MODULE__, self(), :flow_claim_due_block_delayed_after_empty_wake,
       System.unique_integer([:positive])}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :claim_due, :stop],
        &__MODULE__.handle_flow_claim_due_stop/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))
    type = "block-delayed-empty-wake:" <> Integer.to_string(System.unique_integer([:positive]))
    id = "#{type}:#{System.unique_integer([:positive])}"
    now = Ferricstore.CommandTime.now_ms()
    run_at = now + 5_000

    try do
      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 partition_key: partition,
                 state: "queued",
                 now_ms: now,
                 run_at_ms: run_at
               )

      send_command(sock, [
        "FLOW.CLAIM_DUE",
        type,
        "STATE",
        "queued",
        "WORKER",
        "worker-a",
        "PARTITION",
        partition,
        "LIMIT",
        "1",
        "RETURN",
        "JOBS_COMPACT",
        "BLOCK",
        "10000"
      ])

      assert_receive {:flow_claim_due_stop, %{count: 0}, %{flow_type: ^type}}, 1_000

      Ferricstore.Test.ShardHelpers.eventually(
        fn -> flow_claim_waiter_registered?(type, partition) end,
        "RESP claim_due waiter registered for delayed job",
        1_000,
        10
      )

      assert 1 = Ferricstore.Flow.ClaimWaiters.notify_ready(type, "queued", 0, partition, 1)

      Ferricstore.Test.ShardHelpers.eventually(
        fn -> flow_claim_waiter_registered?(type, partition) end,
        "RESP claim_due waiter re-registered after empty delayed wake",
        4_000,
        20
      )

      assert_receive {:flow_claim_due_stop, %{count: 0}, %{flow_type: ^type}}, 1_000
      refute_receive {:flow_claim_due_stop, _measurements, %{flow_type: ^type}}, 40
      Process.sleep(max(run_at - Ferricstore.CommandTime.now_ms(), 0) + 100)
      assert [[[^id, ^partition, _lease_token, _fencing_token]]] = recv_values(sock, 1)
    after
      :gen_tcp.close(sock)
    end
  end

  test "FLOW.CLAIM_DUE BLOCK schedules existing delayed jobs for any partition", %{port: port} do
    handler_id =
      {__MODULE__, self(), :flow_claim_due_block_delayed_any, System.unique_integer([:positive])}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :claim_due, :stop],
        &__MODULE__.handle_flow_claim_due_stop/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))
    type = "block-delayed-any-partition:" <> Integer.to_string(System.unique_integer([:positive]))
    id = "#{type}:#{System.unique_integer([:positive])}"
    now = Ferricstore.CommandTime.now_ms()
    run_at = now + 1_500

    try do
      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 partition_key: partition,
                 state: "queued",
                 now_ms: now,
                 run_at_ms: run_at
               )

      send_command(sock, [
        "FLOW.CLAIM_DUE",
        type,
        "STATE",
        "queued",
        "WORKER",
        "worker-a",
        "PARTITION",
        "ANY",
        "LIMIT",
        "1",
        "RETURN",
        "JOBS_COMPACT",
        "BLOCK",
        "3000"
      ])

      assert_receive {:flow_claim_due_stop, %{count: 0}, %{flow_type: ^type}}, 1_000
      refute_receive {:flow_claim_due_stop, _measurements, %{flow_type: ^type}}, 150
      assert [[[^id, ^partition, _lease_token, _fencing_token]]] = recv_values(sock, 1)
    after
      :gen_tcp.close(sock)
    end
  end

  test "empty RESP command frame returns protocol error before later commands", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_raw(sock, "\r\n*0\r\nPING\r\n")

    data = recv(sock)
    assert data =~ "-ERR protocol error"
    assert closed_or_eof?(sock)
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # QUIT
  # ---------------------------------------------------------------------------

  test "QUIT closes the connection after +OK", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["QUIT"])
    data = recv(sock)
    assert data == "+OK\r\n"

    # Connection should be closed shortly after
    assert closed_or_eof?(sock)
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # RESET
  # ---------------------------------------------------------------------------

  test "RESET returns +RESET and keeps connection open", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["RESET"])
    data = recv(sock)
    assert data == "+RESET\r\n"

    # Should still accept commands after RESET
    send_command(sock, ["PING"])
    data2 = recv(sock)
    assert data2 == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Unknown command
  # ---------------------------------------------------------------------------

  test "unknown command returns error", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["UNKNOWNCMD"])
    data = recv(sock)
    assert String.starts_with?(data, "-")
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Partial / split reads (TCP fragmentation)
  # ---------------------------------------------------------------------------

  test "command split across multiple TCP packets is handled", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    # Send "*1\r\n$4\r\nPING\r\n" in two fragments
    send_raw(sock, "*1\r\n")
    Process.sleep(10)
    send_raw(sock, "$4\r\nPING\r\n")

    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  test "multiple commands packed in one TCP segment all receive responses", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    packed =
      IO.iodata_to_binary([
        Encoder.encode(["PING"]),
        Encoder.encode(["PING", "check"])
      ])

    send_raw(sock, packed)

    data = recv_at_least(sock, 20, 500)
    {:ok, responses, ""} = Parser.parse(data)
    assert length(responses) == 2
    assert Enum.at(responses, 0) == {:simple, "PONG"}
    assert Enum.at(responses, 1) == "check"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Connection close without QUIT (abrupt close)
  # ---------------------------------------------------------------------------

  test "server handles abrupt client disconnect gracefully", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)
    :gen_tcp.close(sock)
    # Give the server process time to handle the close — no crash expected
    Process.sleep(50)
  end

  # ---------------------------------------------------------------------------
  # HELLO command edge cases
  # ---------------------------------------------------------------------------

  test "HELLO with no version argument returns greeting", %{port: port} do
    sock = connect(port)
    hello_no_ver = IO.iodata_to_binary(Encoder.encode(["HELLO"]))
    send_raw(sock, hello_no_ver)
    data = recv(sock)

    assert String.starts_with?(data, "%")
    :gen_tcp.close(sock)
  end
    end
  end
end
