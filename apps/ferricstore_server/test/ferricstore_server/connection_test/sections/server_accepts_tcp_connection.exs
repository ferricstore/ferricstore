defmodule FerricstoreServer.ConnectionTest.Sections.ServerAcceptsTcpConnection do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Encoder
      alias FerricstoreServer.Resp.Parser
      alias FerricstoreServer.Listener
      alias FerricstoreServer.Connection.Pipeline

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

      test "pipelined GET preserves non-binary string values and keeps connection open", %{
        port: port
      } do
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

      test "active once blocked BLPOP client disconnect unregisters waiter promptly", %{
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

        key =
          "blocked-active-once-rearm:" <> Integer.to_string(System.unique_integer([:positive]))

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
        source = pipeline_source()

        assert source =~ "prepend_pipeline_entries(entries, acc)"

        refute source =~ "Enum.reverse(entries) ++ acc",
               "pipeline segments should prepend responses with one reducer instead of reverse-plus-concat"
      end

      test "pipeline fallback path does not spawn per-command tasks" do
        pipeline_source = pipeline_source()
        connection_source = File.read!("lib/ferricstore_server/connection.ex")

        refute pipeline_source =~ "Task.",
               "TCP pipeline fallback must stay in the connection process; per-command tasks add scheduler pressure"

        refute connection_source =~ "as `Task`s",
               "connection documentation must not advertise the old per-command Task pipeline model"
      end

      test "sequential pipeline fallback does not TCP-cork blocking commands" do
        pipeline_source = pipeline_source()

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

        assert_received {:send_response, :fake_socket, :fake_transport,
                         "+PONG\r\n+PONG\r\n+PONG\r\n"}

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

        assert :ok =
                 Ferricstore.FetchOrCompute.fetch_or_compute_result(key, "computed-value", 5_000)

        assert [["hit", "computed-value"]] = recv_values(sock, 1)

        :gen_tcp.close(sock)
      end

      test "pipelined FLOW.CREATE commands batch internally with independent replies", %{
        port: port
      } do
        handler_id =
          {__MODULE__, self(), :flow_create_pipeline, System.unique_integer([:positive])}

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
        handler_id =
          {__MODULE__, self(), :phase1_write_pipeline, System.unique_integer([:positive])}

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
            Encoder.encode(["SETBIT", "#{tag}:bits", "7", "1"])
          ])

        send_raw(sock, pipeline)

        assert [
                 2,
                 hincrbyfloat,
                 zincrby,
                 1,
                 0
               ] = recv_values(sock, 5)

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

        tag =
          "{phase1-invalid-range:" <> Integer.to_string(System.unique_integer([:positive])) <> "}"

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
    end
  end
end
