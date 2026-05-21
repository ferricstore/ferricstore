defmodule FerricstoreServer.ConnectionTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener

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

  test "empty command frames are skipped and do not poison the connection buffer", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_raw(sock, "\r\n*0\r\nPING\r\n")

    data = recv(sock)
    assert data == "+PONG\r\n"
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

  test "empty command list is handled gracefully", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_raw(sock, "*0\r\n")
    assert {:error, :timeout} = :gen_tcp.recv(sock, 0, 50)

    send_command(sock, ["PING"])
    pong = recv(sock)
    assert pong == "+PONG\r\n"
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
