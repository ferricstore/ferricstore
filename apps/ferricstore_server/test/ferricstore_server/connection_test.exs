defmodule FerricstoreServer.ConnectionTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener
  alias FerricstoreServer.Connection.Pipeline

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp connect(port) do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    sock
  end

  defp send_raw(sock, data), do: :gen_tcp.send(sock, data)

  defp recv(sock, timeout \\ 500) do
    {:ok, data} = :gen_tcp.recv(sock, 0, timeout)
    data
  end

  defp hello3 do
    IO.iodata_to_binary(Encoder.encode(["HELLO", "3"]))
  end

  defp send_command(sock, args) do
    :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(args)))
  end

  defp flow_claim_waiter_registered?(type, partition) do
    [nil, "queued"]
    |> Enum.any?(fn state ->
      type
      |> Ferricstore.Flow.ClaimWaiters.wait_keys(state, nil, partition)
      |> Enum.any?(&(Ferricstore.Flow.ClaimWaiters.count(&1) > 0))
    end)
  end

  # ---------------------------------------------------------------------------
  # Setup / teardown
  # ---------------------------------------------------------------------------

  setup do
    # The application supervisor manages the Ranch listener.
    # Use the actual bound port (ephemeral in test env).
    {:ok, port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # TCP connection
  # ---------------------------------------------------------------------------

  test "server accepts TCP connection", %{port: port} do
    sock = connect(port)
    assert is_port(sock)
    :gen_tcp.close(sock)
  end

  test "server accepts multiple simultaneous connections", %{port: port} do
    socks = for _ <- 1..5, do: connect(port)
    Enum.each(socks, &:gen_tcp.close/1)
  end

  # ---------------------------------------------------------------------------
  # HELLO 3 handshake
  # ---------------------------------------------------------------------------

  test "HELLO 3 returns a map greeting", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    # The greeting must be a RESP3 map response (starts with %)
    assert String.starts_with?(data, "%")
    :gen_tcp.close(sock)
  end

  test "HELLO 3 greeting contains required fields: server, version, proto", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert is_map(greeting)
    assert Map.has_key?(greeting, "server")
    assert Map.has_key?(greeting, "version")
    assert Map.has_key?(greeting, "proto")
    assert greeting["proto"] == 3
    :gen_tcp.close(sock)
  end

  test "HELLO 3 greeting server name is ferricstore", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert greeting["server"] == "ferricstore"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # RESP2 rejection
  # ---------------------------------------------------------------------------

  test "HELLO 2 is rejected with an error", %{port: port} do
    sock = connect(port)
    hello2 = IO.iodata_to_binary(Encoder.encode(["HELLO", "2"]))
    send_raw(sock, hello2)
    data = recv(sock)

    # Must be an error response
    assert String.starts_with?(data, "-") or String.starts_with?(data, "!")
    :gen_tcp.close(sock)
  end

  test "HELLO with unsupported version returns error", %{port: port} do
    sock = connect(port)
    hello_bad = IO.iodata_to_binary(Encoder.encode(["HELLO", "99"]))
    send_raw(sock, hello_bad)
    data = recv(sock)

    assert String.starts_with?(data, "-") or String.starts_with?(data, "!")
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # PING
  # ---------------------------------------------------------------------------

  test "PING without argument returns +PONG", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["PING"])
    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  test "PING with argument returns bulk string echo", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["PING", "hello world"])
    data = recv(sock)
    {:ok, [response], ""} = Parser.parse(data)
    assert response == "hello world"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Inline commands
  # ---------------------------------------------------------------------------

  test "inline PING returns +PONG", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_raw(sock, "PING\r\n")
    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Pipelined commands
  # ---------------------------------------------------------------------------

  test "pipelined PING commands all receive responses", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["PING"]),
        Encoder.encode(["PING"]),
        Encoder.encode(["PING"])
      ])

    send_raw(sock, pipeline)

    # Collect until we get 3 pong responses
    data = recv_all(sock, "+PONG\r\n", 3)
    count = count_occurrences(data, "+PONG\r\n")
    assert count == 3
    :gen_tcp.close(sock)
  end

  test "pipelined GET preserves non-binary string values and keeps connection open", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    key = "pipeline-get-non-binary:" <> Integer.to_string(System.unique_integer([:positive]))
    missing_key = key <> ":missing"

    send_command(sock, ["INCR", key])
    assert [1] = recv_values(sock, 1)

    send_command(sock, ["GET", key])
    [single_get] = recv_values(sock, 1)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["GET", key]),
        Encoder.encode(["GET", missing_key])
      ])

    send_raw(sock, pipeline)
    assert [^single_get, nil] = recv_values(sock, 2)

    send_command(sock, ["PING"])
    assert [{:simple, "PONG"}] = recv_values(sock, 1)

    :gen_tcp.close(sock)
  end

  test "BLPOP timeout preserves PING sent while the connection is blocked", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    key = "blocked-ping:" <> Integer.to_string(System.unique_integer([:positive]))

    send_command(sock, ["BLPOP", key, "0.1"])
    Process.sleep(20)
    send_command(sock, ["PING"])

    assert [nil, {:simple, "PONG"}] = recv_values(sock, 2)

    :gen_tcp.close(sock)
  end

  test "blocked command buffer overflow returns error and closes connection", %{port: port} do
    old_max = Application.get_env(:ferricstore_server, :blocked_command_buffer_max_bytes)
    Application.put_env(:ferricstore_server, :blocked_command_buffer_max_bytes, 16)

    on_exit(fn ->
      case old_max do
        nil ->
          Application.delete_env(:ferricstore_server, :blocked_command_buffer_max_bytes)

        value ->
          Application.put_env(:ferricstore_server, :blocked_command_buffer_max_bytes, value)
      end
    end)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    key = "blocked-overflow:" <> Integer.to_string(System.unique_integer([:positive]))

    send_command(sock, ["BLPOP", key, "2"])
    Process.sleep(20)
    send_command(sock, ["ECHO", String.duplicate("x", 64)])

    assert [{:error, reason}] = recv_values(sock, 1)
    assert reason =~ "blocked command buffer overflow"
    assert closed_or_eof?(sock)
  end

  test "active once blocked BLPOP client disconnect unregisters waiter promptly", %{port: port} do
    old_mode = Application.get_env(:ferricstore, :socket_active_mode)
    Application.put_env(:ferricstore, :socket_active_mode, :once)

    on_exit(fn ->
      case old_mode do
        nil -> Application.delete_env(:ferricstore, :socket_active_mode)
        value -> Application.put_env(:ferricstore, :socket_active_mode, value)
      end
    end)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    key = "blocked-active-once:" <> Integer.to_string(System.unique_integer([:positive]))
    send_command(sock, ["BLPOP", key, "5"])

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> Ferricstore.Waiters.count(key) == 1 end,
      "active once waiter registered",
      1_000,
      10
    )

    :gen_tcp.close(sock)

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> Ferricstore.Waiters.count(key) == 0 end,
      "active once waiter unregistered after disconnect",
      500,
      10
    )
  end

  test "active once blocked BLPOP re-arms socket and observes disconnect before timeout", %{
    port: port
  } do
    old_mode = Application.get_env(:ferricstore, :socket_active_mode)
    Application.put_env(:ferricstore, :socket_active_mode, :once)

    on_exit(fn ->
      case old_mode do
        nil -> Application.delete_env(:ferricstore, :socket_active_mode)
        value -> Application.put_env(:ferricstore, :socket_active_mode, value)
      end
    end)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    key = "blocked-active-once-rearm:" <> Integer.to_string(System.unique_integer([:positive]))
    send_command(sock, ["BLPOP", key, "30"])

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> Ferricstore.Waiters.count(key) == 1 end,
      "active once waiter registered",
      1_000,
      10
    )

    :ok = send_command(sock, ["PING"])
    :gen_tcp.close(sock)

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> Ferricstore.Waiters.count(key) == 0 end,
      "active once waiter unregistered promptly after queued input and disconnect",
      30,
      10
    )
  end

  test "CLIENT KILL ID interrupts a blocked BLPOP connection", %{port: port} do
    blocked = connect(port)
    send_raw(blocked, hello3())
    _greeting = recv(blocked)

    send_command(blocked, ["CLIENT", "ID"])
    assert [blocked_id] = recv_values(blocked, 1)

    key = "blocked-kill:" <> Integer.to_string(System.unique_integer([:positive]))
    send_command(blocked, ["BLPOP", key, "5"])

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> Ferricstore.Waiters.total_count() > 0 end,
      "BLPOP waiter registered",
      100,
      5
    )

    killer = connect(port)
    send_raw(killer, hello3())
    _greeting = recv(killer)

    send_command(killer, ["CLIENT", "KILL", "ID", Integer.to_string(blocked_id)])
    assert [{:simple, "OK"}] = recv_values(killer, 1)

    assert closed_or_eof?(blocked)

    :gen_tcp.close(killer)
  end

  test "pure pipeline segments prepend encoded entries without list concatenation" do
    source = File.read!("lib/ferricstore_server/connection/pipeline.ex")

    assert source =~ "prepend_pipeline_entries(entries, acc)"

    refute source =~ "Enum.reverse(entries) ++ acc",
           "pipeline segments should prepend responses with one reducer instead of reverse-plus-concat"
  end

  test "pipeline fallback path does not spawn per-command tasks" do
    pipeline_source = File.read!("lib/ferricstore_server/connection/pipeline.ex")
    connection_source = File.read!("lib/ferricstore_server/connection.ex")

    refute pipeline_source =~ "Task.",
           "TCP pipeline fallback must stay in the connection process; per-command tasks add scheduler pressure"

    refute connection_source =~ "as `Task`s",
           "connection documentation must not advertise the old per-command Task pipeline model"
  end

  test "sequential pipeline fallback does not TCP-cork blocking commands" do
    pipeline_source = File.read!("lib/ferricstore_server/connection/pipeline.ex")

    assert pipeline_source =~ "sequential_cork_safe?(commands)",
           "restricted-ACL sequential fallback must not cork a batch containing a blocking command"

    assert pipeline_source =~ "pipeline_contains_blocking_command?(commands)",
           "blocking commands need an explicit uncork boundary so earlier replies flush before the wait"
  end

  test "generic pure fallback coalesces a pipeline into one socket send" do
    ctx = FerricStore.Instance.get(:default)

    state = %FerricstoreServer.Connection{
      socket: :fake_socket,
      transport: :fake_transport,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access
    }

    commands = [
      {:command, "PING", [], :ping, []},
      {:command, "PING", [], :ping, []},
      {:command, "PING", [], :ping, []}
    ]

    parent = self()

    send_response = fn socket, transport, iodata ->
      send(parent, {:send_response, socket, transport, IO.iodata_to_binary(iodata)})
      :ok
    end

    handle_command = fn _cmd, _state ->
      flunk("pure fallback pipeline should not dispatch through sequential handle_command/2")
    end

    assert {:continue, _state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command, send_response)

    assert_received {:send_response, :fake_socket, :fake_transport, "+PONG\r\n+PONG\r\n+PONG\r\n"}
    refute_received {:send_response, _, _, _}
  end

  test "generic pure fallback stops when coalesced socket send fails" do
    ctx = FerricStore.Instance.get(:default)

    state = %FerricstoreServer.Connection{
      socket: :fake_socket,
      transport: :fake_transport,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access
    }

    commands = [
      {:command, "PING", [], :ping, []},
      {:command, "PING", [], :ping, []}
    ]

    send_response = fn _socket, _transport, _iodata -> {:error, :closed} end

    handle_command = fn _cmd, _state ->
      flunk("pure fallback pipeline should not dispatch through sequential handle_command/2")
    end

    assert {:quit, _state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command, send_response)
  end

  test "single FLOW.CREATE command dispatches through typed AST", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    id = "single-flow:" <> Integer.to_string(System.unique_integer([:positive]))
    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))

    send_command(sock, [
      "FLOW.CREATE",
      id,
      "TYPE",
      "single-flow",
      "PARTITION",
      partition,
      "RUN_AT",
      "1000"
    ])

    assert ["OK"] = recv_values(sock, 1)

    send_command(sock, ["FLOW.GET", id, "PARTITION", partition])

    assert [%{"id" => ^id, "type" => "single-flow", "partition_key" => ^partition}] =
             recv_values(sock, 1)

    :gen_tcp.close(sock)
  end

  test "single FLOW.SIGNAL command dispatches through typed AST", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    id = "signal-flow:" <> Integer.to_string(System.unique_integer([:positive]))
    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))

    send_command(sock, [
      "FLOW.CREATE",
      id,
      "TYPE",
      "signal-flow",
      "STATE",
      "waiting_payment",
      "PARTITION",
      partition,
      "RUN_AT",
      "1000"
    ])

    assert ["OK"] = recv_values(sock, 1)

    send_command(sock, [
      "FLOW.SIGNAL",
      id,
      "PARTITION",
      partition,
      "SIGNAL",
      "payment_received",
      "IF_STATE",
      "waiting_payment",
      "TRANSITION_TO",
      "verify_payment",
      "VALUE",
      "payment_event",
      "ref:payment"
    ])

    assert ["OK"] = recv_values(sock, 1)

    send_command(sock, ["FLOW.HISTORY", id, "PARTITION", partition, "COUNT", "10"])

    assert [history] = recv_values(sock, 1)

    assert Enum.any?(history, fn
             [_event_id, %{"event" => "signaled", "signal" => "payment_received"}] -> true
             _entry -> false
           end)

    :gen_tcp.close(sock)
  end

  test "single FLOW terminal query commands dispatch through typed AST", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    type = "empty-terminal-flow:" <> Integer.to_string(System.unique_integer([:positive]))

    send_command(sock, ["FLOW.TERMINALS", type, "COUNT", "10"])
    send_command(sock, ["FLOW.FAILURES", type, "COUNT", "10"])

    assert [[], []] = recv_values(sock, 2)

    :gen_tcp.close(sock)
  end

  test "pipelined FETCH_OR_COMPUTE flushes compute owner before a same-key waiter blocks", %{
    port: port
  } do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    key = "pipeline-fetch-compute:" <> Integer.to_string(System.unique_integer([:positive]))

    on_exit(fn ->
      _ = Ferricstore.FetchOrCompute.fetch_or_compute_error(key, "test cleanup")
    end)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["FETCH_OR_COMPUTE", key, "5000", "hint"]),
        Encoder.encode(["FETCH_OR_COMPUTE", key, "5000", "hint"])
      ])

    send_raw(sock, pipeline)

    assert [["compute", "hint"]] = recv_values(sock, 1)

    assert :ok = Ferricstore.FetchOrCompute.fetch_or_compute_result(key, "computed-value", 5_000)
    assert [["hit", "computed-value"]] = recv_values(sock, 1)

    :gen_tcp.close(sock)
  end

  test "pipelined FLOW.CREATE commands batch internally with independent replies", %{port: port} do
    handler_id = {__MODULE__, self(), :flow_create_pipeline, System.unique_integer([:positive])}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :batcher, :quorum_submit],
        &__MODULE__.handle_quorum_submit/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    partition = "tenant:" <> Integer.to_string(System.unique_integer([:positive]))
    type = "pipeline-flow:" <> Integer.to_string(System.unique_integer([:positive]))
    id_a = "pipeline-flow-a:" <> Integer.to_string(System.unique_integer([:positive]))
    id_b = "pipeline-flow-b:" <> Integer.to_string(System.unique_integer([:positive]))

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode([
          "FLOW.CREATE",
          id_a,
          "TYPE",
          type,
          "PARTITION",
          partition,
          "RUN_AT",
          "1000"
        ]),
        Encoder.encode([
          "FLOW.CREATE",
          id_a,
          "TYPE",
          type,
          "PARTITION",
          partition,
          "RUN_AT",
          "1000"
        ]),
        Encoder.encode([
          "FLOW.CREATE",
          id_b,
          "TYPE",
          type,
          "PARTITION",
          partition,
          "RUN_AT",
          "1000"
        ])
      ])

    send_raw(sock, pipeline)

    assert ["OK", {:error, "ERR flow already exists"}, "OK"] = recv_values(sock, 3)

    assert_receive {:quorum_submit, [:ferricstore, :batcher, :quorum_submit],
                    %{batch_size: batch_size}, %{kind: :batch}},
                   1_000

    assert batch_size >= 3
    :gen_tcp.close(sock)
  end

  test "pipelined phase-1 single-key writes batch internally and preserve replies", %{
    port: port
  } do
    handler_id = {__MODULE__, self(), :phase1_write_pipeline, System.unique_integer([:positive])}
    attach_quorum_submit(handler_id)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    tag = "{phase1:" <> Integer.to_string(System.unique_integer([:positive])) <> "}"
    counter_key = "#{tag}:counter"
    value_key = "#{tag}:value"
    delete_key = "#{tag}:delete"

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["SET", delete_key, "gone"]),
        Encoder.encode(["INCR", counter_key]),
        Encoder.encode(["APPEND", value_key, "a"]),
        Encoder.encode(["SETRANGE", value_key, "1", "b"]),
        Encoder.encode(["DEL", delete_key])
      ])

    send_raw(sock, pipeline)

    assert [{:simple, "OK"}, 1, 1, 2, 1] = recv_values(sock, 5)

    send_command(sock, ["GET", value_key])
    assert ["ab"] = recv_values(sock, 1)

    send_command(sock, ["GET", counter_key])
    assert ["1"] = recv_values(sock, 1)

    sends = drain_quorum_submits()
    assert Enum.any?(sends, &quorum_batch_size_at_least?(&1, 4))

    :gen_tcp.close(sock)
  end

  test "pipelined GETSET is not part of phase-1 write batching", %{port: port} do
    handler_id =
      {__MODULE__, self(), :phase1_write_pipeline_getset, System.unique_integer([:positive])}

    attach_quorum_submit(handler_id)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    tag = "{phase1-getset:" <> Integer.to_string(System.unique_integer([:positive])) <> "}"
    key = "#{tag}:key"
    counter_key = "#{tag}:counter"

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["SET", key, "old"]),
        Encoder.encode(["GETSET", key, "new"]),
        Encoder.encode(["INCR", counter_key])
      ])

    send_raw(sock, pipeline)

    assert [{:simple, "OK"}, "old", 1] = recv_values(sock, 3)

    send_command(sock, ["GET", key])
    assert ["new"] = recv_values(sock, 1)

    sends = drain_quorum_submits()
    refute Enum.any?(sends, &quorum_batch_size_at_least?(&1, 2))

    :gen_tcp.close(sock)
  end

  test "pipelined phase-1 direct compound and JSON writes batch safely", %{port: port} do
    handler_id =
      {__MODULE__, self(), :phase1_direct_write_pipeline, System.unique_integer([:positive])}

    attach_quorum_submit(handler_id)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    tag = "{phase1-direct:" <> Integer.to_string(System.unique_integer([:positive])) <> "}"

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["HINCRBY", "#{tag}:hash", "count", "2"]),
        Encoder.encode(["HINCRBYFLOAT", "#{tag}:hashf", "price", "1.5"]),
        Encoder.encode(["ZINCRBY", "#{tag}:zset", "2.5", "member"]),
        Encoder.encode(["PFADD", "#{tag}:hll", "a", "b"]),
        Encoder.encode(["SETBIT", "#{tag}:bits", "7", "1"]),
        Encoder.encode(["JSON.SET", "#{tag}:doc", "$", ~s({"n":1})]),
        Encoder.encode(["JSON.NUMINCRBY", "#{tag}:doc", "$.n", "1"])
      ])

    send_raw(sock, pipeline)

    assert [
             2,
             hincrbyfloat,
             zincrby,
             1,
             0,
             {:simple, "OK"},
             "2"
           ] = recv_values(sock, 7)

    assert_float_string(hincrbyfloat, 1.5)
    assert_float_string(zincrby, 2.5)

    sends = drain_quorum_submits()
    assert Enum.any?(sends, &quorum_batch_size_at_least?(&1, 7))

    :gen_tcp.close(sock)
  end

  test "pipelined out-of-range SETRANGE and SETBIT fall back to normal validation", %{
    port: port
  } do
    handler_id =
      {__MODULE__, self(), :phase1_invalid_range_pipeline, System.unique_integer([:positive])}

    attach_quorum_submit(handler_id)

    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    tag = "{phase1-invalid-range:" <> Integer.to_string(System.unique_integer([:positive])) <> "}"
    counter_key = "#{tag}:counter"

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["SETRANGE", "#{tag}:range", "536870912", "x"]),
        Encoder.encode(["SETBIT", "#{tag}:bits", "4294967296", "1"]),
        Encoder.encode(["INCR", counter_key])
      ])

    send_raw(sock, pipeline)

    assert [
             {:error, setrange_error},
             {:error, setbit_error},
             1
           ] = recv_values(sock, 3)

    assert setrange_error =~ "ERR string exceeds maximum allowed size"
    assert setbit_error =~ "ERR bit offset is not an integer or out of range"

    sends = drain_quorum_submits()
    refute Enum.any?(sends, &quorum_batch_size_at_least?(&1, 2))

    :gen_tcp.close(sock)
  end

  test "pipeline read boundary sees prior phase-1 write before later write", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    key = "phase1-read-boundary:" <> Integer.to_string(System.unique_integer([:positive]))

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["SET", key, "1"]),
        Encoder.encode(["GET", key]),
        Encoder.encode(["INCR", key])
      ])

    send_raw(sock, pipeline)
    assert [{:simple, "OK"}, "1", 2] = recv_values(sock, 3)

    send_command(sock, ["GET", key])
    assert ["2"] = recv_values(sock, 1)

    :gen_tcp.close(sock)
  end

  test "mixed GET and SET pipeline preserves read before write on same key", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    key = "mixed-read-before-write:" <> Integer.to_string(System.unique_integer([:positive]))

    send_command(sock, ["SET", key, "old"])
    assert [{:simple, "OK"}] = recv_values(sock, 1)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["GET", key]),
        Encoder.encode(["SET", key, "new"]),
        Encoder.encode(["GET", key])
      ])

    send_raw(sock, pipeline)
    assert ["old", {:simple, "OK"}, "new"] = recv_values(sock, 3)

    :gen_tcp.close(sock)
  end

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
    assert [{:simple, "OK"}, nil] = recv_values(sock, 2)

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
        "3000"
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
        200,
        10
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
    run_at = now + 80

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
        "1000"
      ])

      assert_receive {:flow_claim_due_stop, %{count: 0}, %{flow_type: ^type}}, 1_000
      refute_receive {:flow_claim_due_stop, _measurements, %{flow_type: ^type}}, 40
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
    run_at = now + 2_000

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
        "3000"
      ])

      assert_receive {:flow_claim_due_stop, %{count: 0}, %{flow_type: ^type}}, 1_000

      Ferricstore.Test.ShardHelpers.eventually(
        fn -> flow_claim_waiter_registered?(type, partition) end,
        "RESP claim_due waiter registered for delayed job",
        100,
        5
      )

      assert 1 = Ferricstore.Flow.ClaimWaiters.notify_ready(type, "queued", 0, partition, 1)

      Ferricstore.Test.ShardHelpers.eventually(
        fn -> flow_claim_waiter_registered?(type, partition) end,
        "RESP claim_due waiter re-registered after empty delayed wake",
        100,
        30
      )

      assert_receive {:flow_claim_due_stop, %{count: 0}, %{flow_type: ^type}}, 1_000
      refute_receive {:flow_claim_due_stop, _measurements, %{flow_type: ^type}}, 40
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

  test "HELLO 3 mid-session returns greeting again", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["PING"])
    _pong = recv(sock)

    send_raw(sock, hello3())
    data = recv(sock)

    assert is_binary(data)
    {:ok, [greeting], ""} = Parser.parse(data)
    assert is_map(greeting)
    :gen_tcp.close(sock)
  end

  test "HELLO 3 greeting contains mode field", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert Map.has_key?(greeting, "mode")
    :gen_tcp.close(sock)
  end

  test "HELLO 3 greeting id is an integer", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert is_integer(greeting["id"])
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # PING edge cases
  # ---------------------------------------------------------------------------

  test "PING command is case-insensitive", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["ping"])
    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  test "PING with empty string argument returns empty bulk string", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["PING", ""])
    data = recv(sock)
    {:ok, [response], ""} = Parser.parse(data)
    assert response == ""
    :gen_tcp.close(sock)
  end

  test "multiple PING with arguments in pipeline all respond", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["PING", "a"]),
        Encoder.encode(["PING", "b"]),
        Encoder.encode(["PING", "c"])
      ])

    send_raw(sock, pipeline)

    data = recv_at_least(sock, 20, 500)
    {:ok, responses, ""} = Parser.parse(data)
    assert length(responses) == 3
    assert Enum.at(responses, 0) == "a"
    assert Enum.at(responses, 1) == "b"
    assert Enum.at(responses, 2) == "c"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Large payloads
  # ---------------------------------------------------------------------------

  test "large binary value in PING argument (10KB) echoes correctly", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    large_payload = :binary.copy(<<0x42>>, 10_000)
    send_command(sock, ["PING", large_payload])

    data = recv_at_least(sock, 10_009, 2000)
    {:ok, [response], ""} = Parser.parse(data)
    assert byte_size(response) == 10_000
    assert response == large_payload
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Connection state after RESET
  # ---------------------------------------------------------------------------

  test "RESET after pipelining leaves connection functional", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["PING"]),
        Encoder.encode(["PING"]),
        Encoder.encode(["PING"])
      ])

    send_raw(sock, pipeline)
    _responses = recv_all(sock, "+PONG\r\n", 3)

    send_command(sock, ["RESET"])
    _reset = recv(sock)

    send_command(sock, ["PING"])
    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # QUIT in pipeline
  # ---------------------------------------------------------------------------

  test "QUIT in a pipeline sends OK and closes", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["PING"]),
        Encoder.encode(["QUIT"]),
        Encoder.encode(["PING"])
      ])

    send_raw(sock, pipeline)

    data = recv_at_least(sock, 14, 500)
    {:ok, responses, ""} = Parser.parse(data)
    assert Enum.at(responses, 0) == {:simple, "PONG"}
    assert Enum.at(responses, 1) == {:simple, "OK"}

    assert closed_or_eof?(sock)
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Error responses
  # ---------------------------------------------------------------------------

  test "wrong arity PING returns a response without crashing", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["PING", "a", "b"])
    data = recv(sock)
    assert is_binary(data)
    assert byte_size(data) > 0
    :gen_tcp.close(sock)
  end

  test "empty RESP command list closes with protocol error", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_raw(sock, "*0\r\n")
    data = recv(sock)
    assert data =~ "-ERR protocol error"
    assert closed_or_eof?(sock)
    :gen_tcp.close(sock)
  end

  test "blank inline command is skipped and connection remains usable", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_raw(sock, "\r\n")
    assert {:error, :timeout} = :gen_tcp.recv(sock, 0, 50)

    send_command(sock, ["PING"])
    assert recv(sock) == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  test "empty bulk command name returns unknown command error without closing", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_raw(sock, "*1\r\n$0\r\n\r\n")
    data = recv(sock)
    assert String.contains?(data, "unknown command ''")

    send_command(sock, ["PING"])
    assert recv(sock) == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Greeting map fields — all expected key/value pairs
  # ---------------------------------------------------------------------------

  test "HELLO 3 greeting contains all expected fields with correct values", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert greeting["server"] == "ferricstore"
    assert greeting["proto"] == 3
    assert greeting["mode"] == "standalone"
    assert greeting["role"] == "master"
    assert greeting["modules"] == []
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # PING before HELLO
  # ---------------------------------------------------------------------------

  test "PING before HELLO returns +PONG without crashing", %{port: port} do
    sock = connect(port)
    # Do NOT send HELLO first — go straight to PING
    send_command(sock, ["PING"])
    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  test "PING with message before HELLO returns bulk string", %{port: port} do
    sock = connect(port)
    send_command(sock, ["PING", "early bird"])
    data = recv(sock)

    {:ok, [response], ""} = Parser.parse(data)
    assert response == "early bird"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Large pipeline (100+ commands)
  # ---------------------------------------------------------------------------

  test "pipeline of 100 PING commands returns 100 PONG responses", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    pipeline =
      1..100
      |> Enum.map(fn _ -> Encoder.encode(["PING"]) end)
      |> IO.iodata_to_binary()

    send_raw(sock, pipeline)

    data = recv_all(sock, "+PONG\r\n", 100)
    count = count_occurrences(data, "+PONG\r\n")
    assert count == 100
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # CLIENT HELLO dispatch (two-token form)
  # ---------------------------------------------------------------------------

  test "CLIENT HELLO 3 returns greeting map", %{port: port} do
    sock = connect(port)
    # Build the three-element RESP array: CLIENT HELLO 3
    client_hello = "*3\r\n$6\r\nCLIENT\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"
    send_raw(sock, client_hello)
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert is_map(greeting)
    assert greeting["server"] == "ferricstore"
    assert greeting["proto"] == 3
    assert greeting["mode"] == "standalone"
    assert greeting["role"] == "master"
    assert greeting["modules"] == []
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Half-close / zero-byte recv — clean exit
  # ---------------------------------------------------------------------------

  test "server connection exits cleanly on half-close (shutdown :write)", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    # Half-close the write side of the client socket.
    # This causes the server's recv to return {:ok, <<>>} or {:error, :closed}.
    :gen_tcp.shutdown(sock, :write)

    # The server should close its side cleanly without spinning.
    # Verify by confirming the socket reaches a closed state within a bounded time.
    assert closed_or_eof?(sock)
    :gen_tcp.close(sock)
  end

  test "server does not spin on half-close — connection process exits promptly", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    # Capture current BEAM process count before triggering half-close
    procs_before = length(Process.list())

    :gen_tcp.shutdown(sock, :write)
    # Give the server a moment to handle the close
    Process.sleep(100)

    procs_after = length(Process.list())

    # If the server spun infinitely, we'd see a process leak. The count should
    # stay the same or decrease (the connection process exits).
    assert procs_after <= procs_before
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Private test helpers
  # ---------------------------------------------------------------------------

  def handle_quorum_submit(event, measurements, metadata, test_pid) do
    send(test_pid, {:quorum_submit, event, measurements, metadata})
  end

  defp attach_quorum_submit(handler_id) do
    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :batcher, :quorum_submit],
        &__MODULE__.handle_quorum_submit/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp drain_quorum_submits(acc \\ []) do
    receive do
      {:quorum_submit, _event, _measurements, _metadata} = msg ->
        drain_quorum_submits([msg | acc])
    after
      150 -> Enum.reverse(acc)
    end
  end

  defp quorum_batch_size_at_least?(
         {:quorum_submit, _event, %{batch_size: batch_size}, %{kind: :batch}},
         min_size
       ),
       do: batch_size >= min_size

  defp quorum_batch_size_at_least?(_event, _min_size), do: false

  defp assert_float_string(value, expected) when is_binary(value) do
    {parsed, ""} = Float.parse(value)
    assert_in_delta parsed, expected, 0.001
  end

  def handle_pipeline_claim_due_batch(_event, measurements, metadata, test_pid) do
    send(test_pid, {:pipeline_claim_due_batch, measurements, metadata})
  end

  def handle_flow_claim_due_stop(_event, measurements, metadata, test_pid) do
    send(test_pid, {:flow_claim_due_stop, measurements, metadata})
  end

  defp recv_all(_sock, _pattern, 0), do: ""

  defp recv_all(sock, pattern, count) do
    data = recv(sock)

    occurrences = count_occurrences(data, pattern)

    if occurrences >= count do
      data
    else
      data <> recv_all(sock, pattern, count - occurrences)
    end
  end

  defp recv_at_least(sock, min_bytes, timeout) do
    recv_at_least(sock, min_bytes, timeout, "")
  end

  defp recv_at_least(_sock, min_bytes, _timeout, acc) when byte_size(acc) >= min_bytes, do: acc

  defp recv_at_least(sock, min_bytes, timeout, acc) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, chunk} -> recv_at_least(sock, min_bytes, timeout, acc <> chunk)
      {:error, _} -> acc
    end
  end

  defp recv_values(sock, count), do: recv_values(sock, count, "", 20)

  defp recv_values(_sock, count, acc, attempts) when attempts <= 0 do
    case Parser.parse(acc) do
      {:ok, values, _rest} when length(values) >= count -> Enum.take(values, count)
      other -> flunk("expected #{count} RESP values, got #{inspect(other)} from #{inspect(acc)}")
    end
  end

  defp recv_values(sock, count, acc, attempts) do
    case Parser.parse(acc) do
      {:ok, values, _rest} when length(values) >= count ->
        Enum.take(values, count)

      _ ->
        case :gen_tcp.recv(sock, 0, 500) do
          {:ok, chunk} ->
            recv_values(sock, count, acc <> chunk, attempts - 1)

          {:error, :timeout} ->
            recv_values(sock, count, acc, attempts - 1)

          {:error, reason} ->
            flunk("socket closed before #{count} RESP values: #{inspect(reason)}")
        end
    end
  end

  defp count_occurrences(data, pattern) do
    data
    |> :binary.matches(pattern)
    |> length()
  end

  defp closed_or_eof?(sock) do
    case :gen_tcp.recv(sock, 0, 500) do
      {:error, :closed} -> true
      {:error, :econnreset} -> true
      {:error, :einval} -> true
      {:error, :enotconn} -> true
      {:ok, ""} -> true
      {:ok, _data} -> false
    end
  end
end
