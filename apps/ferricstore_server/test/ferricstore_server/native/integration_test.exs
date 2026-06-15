defmodule FerricstoreServer.Native.IntegrationTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Ferricstore.Config
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Native.{Codec, Listener}

  @op_auth 0x0002
  @op_ping 0x0003
  @op_client_set_name 0x0004
  @op_client_info 0x0005
  @op_startup 0x000C
  @op_route_batch 0x000F
  @op_window_update 0x000D
  @op_pipeline 0x000E
  @op_options 0x000B
  @op_subscribe_events 0x0011
  @op_get 0x0101
  @op_set 0x0102
  @op_mget 0x0104
  @op_zadd 0x0140
  @op_zrange 0x0142

  setup do
    Config.set("requirepass", "")
    old_response_chunk_bytes = Application.get_env(:ferricstore, :native_response_chunk_bytes)

    old_response_coalesce_bytes =
      Application.get_env(:ferricstore, :native_response_coalesce_bytes)

    old_max_pending_chunks = Application.get_env(:ferricstore, :native_max_pending_chunks)
    old_idle_timeout_ms = Application.get_env(:ferricstore, :native_idle_timeout_ms)

    old_max_pending_chunk_bytes =
      Application.get_env(:ferricstore, :native_max_pending_chunk_bytes)

    old_trace_enabled = Application.get_env(:ferricstore, :native_trace_enabled)

    old_request_compression_enabled =
      Application.get_env(:ferricstore, :native_request_compression_enabled)

    old_max_inflight_per_connection =
      Application.get_env(:ferricstore, :native_max_inflight_per_connection)

    old_max_inflight_per_lane = Application.get_env(:ferricstore, :native_max_inflight_per_lane)

    unless Listener.running?() do
      {:ok, _pid} = Listener.start(0)

      on_exit(fn ->
        if Listener.running?(), do: Listener.stop()
      end)
    end

    on_exit(fn ->
      Config.set("requirepass", "")
      restore_env(:native_response_chunk_bytes, old_response_chunk_bytes)
      restore_env(:native_response_coalesce_bytes, old_response_coalesce_bytes)
      restore_env(:native_max_pending_chunks, old_max_pending_chunks)
      restore_env(:native_idle_timeout_ms, old_idle_timeout_ms)
      restore_env(:native_max_pending_chunk_bytes, old_max_pending_chunk_bytes)
      restore_env(:native_trace_enabled, old_trace_enabled)
      restore_env(:native_request_compression_enabled, old_request_compression_enabled)
      restore_env(:native_max_inflight_per_connection, old_max_inflight_per_connection)
      restore_env(:native_max_inflight_per_lane, old_max_inflight_per_lane)
    end)

    %{port: Listener.port()}
  end

  test "OPTIONS returns native capabilities over TCP", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_options, 0, 1, %{})
    assert {0, payload} = recv_response(sock)
    assert payload["protocol_versions"] == [1]
    assert payload["multiplexing"]["ordered_per_lane"] == true

    :gen_tcp.close(sock)
  end

  test "idle native clients are closed after configured timeout", %{port: port} do
    Application.put_env(:ferricstore, :native_idle_timeout_ms, 25)
    sock = connect(port)

    assert {:error, :closed} = :gen_tcp.recv(sock, 0, 500)
  end

  test "PING heartbeat keeps native idle timeout alive", %{port: port} do
    Application.put_env(:ferricstore, :native_idle_timeout_ms, 80)
    sock = connect(port)

    Process.sleep(40)
    send_request(sock, @op_ping, 0, 101, %{})
    assert {0, "PONG"} = recv_response(sock)

    Process.sleep(40)
    send_request(sock, @op_ping, 0, 102, %{})
    assert {0, "PONG"} = recv_response(sock)

    :gen_tcp.close(sock)
  end

  test "data commands are rejected on control lane", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_get, 0, 2, %{"key" => "native-control-lane"})
    assert {6, payload} = recv_response(sock)
    assert payload["code"] == "bad_request"
    assert payload["message"] =~ "control lane 0"

    :gen_tcp.close(sock)
  end

  test "AUTH gates data commands over TCP when requirepass is configured", %{port: port} do
    Config.set("requirepass", "native-secret")
    sock = connect(port)
    key = "native:auth:#{System.unique_integer([:positive])}"

    send_request(sock, @op_get, 1, 20, %{"key" => key})
    assert {2, noauth} = recv_response(sock)
    assert noauth["code"] == "auth"
    assert noauth["message"] =~ "NOAUTH"

    send_request(sock, @op_auth, 0, 21, %{"username" => "default", "password" => "wrong"})
    assert {2, wrongpass} = recv_response(sock)
    assert wrongpass["code"] == "auth"
    assert wrongpass["message"] =~ "WRONGPASS"

    send_request(sock, @op_auth, 0, 22, %{"username" => "default", "password" => "native-secret"})
    assert {0, "OK"} = recv_response(sock)

    send_request(sock, @op_set, 1, 23, %{"key" => key, "value" => "value"})
    assert {0, "OK"} = recv_response(sock)

    :gen_tcp.close(sock)
  end

  test "CLIENT.SETNAME is visible through CLIENT.INFO over native TCP", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_client_set_name, 0, 24, %{"name" => "native-client"})
    assert {0, "OK"} = recv_response(sock)

    send_request(sock, @op_client_info, 0, 25, %{})
    assert {0, payload} = recv_response(sock)
    assert payload["client_name"] == "native-client"
    assert payload["protocol"] == "native"
    assert payload["username"] == "default"

    :gen_tcp.close(sock)
  end

  test "standard KV read opcodes return compact custom payloads over TCP", %{port: port} do
    sock = connect(port)
    key = "native:compact-kv:#{System.unique_integer([:positive])}"
    missing = key <> ":missing"

    send_request(sock, @op_set, 1, 501, %{"key" => key, "value" => "v"})
    assert {0, "OK"} = recv_response(sock)

    send_request(sock, @op_get, 1, 502, %{"key" => key})
    assert {:raw, 0, <<0x82, 1, 1::unsigned-32, "v">>} = recv_raw_response(sock)

    send_request(sock, @op_get, 1, 503, %{"key" => missing})
    assert {:raw, 0, <<0x82, 0>>} = recv_raw_response(sock)

    send_request(sock, @op_mget, 1, 504, %{"keys" => [key, missing]})

    assert {:raw, 0, <<0x83, 2::unsigned-32, 1, 1::unsigned-32, "v", 0>>} =
             recv_raw_response(sock)

    send_request(sock, @op_mget, 1, 505, %{"keys" => [key, key]})

    assert {:raw, 0, <<0x89, 2::unsigned-32, 1::unsigned-32, "v", "v">>} =
             recv_raw_response(sock)

    :gen_tcp.close(sock)
  end

  test "ROUTE_BATCH returns shard and lane hints for client-side routing", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_route_batch, 0, 10, %{"keys" => ["native-route-a", "native-route-b"]})
    assert {0, [route_a, route_b]} = recv_response(sock)

    assert route_a["key"] == "native-route-a"
    assert is_integer(route_a["slot"])
    assert is_integer(route_a["shard"])
    assert route_a["lane_id"] == route_a["shard"] + 1
    assert route_b["key"] == "native-route-b"
    assert is_integer(route_b["route_epoch"])

    :gen_tcp.close(sock)
  end

  test "WINDOW_UPDATE reports current native flow-control limits", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_window_update, 0, 11, %{"lane_id" => 3, "credits" => 128})
    assert {0, payload} = recv_response(sock)

    assert payload["accepted"] == true
    assert payload["lane_id"] == 3
    assert payload["credits"] == 128
    assert payload["limits"]["window_update"] == true
    assert is_integer(payload["limits"]["max_inflight_per_lane"])
    assert is_integer(payload["limits"]["response_coalesce_bytes"])
    assert payload["limits"]["response_coalesce_bytes"] > 0

    :gen_tcp.close(sock)
  end

  test "WINDOW_UPDATE can close the request window and reject new data frames", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_window_update, 0, 32, %{
      "max_inflight_per_connection" => 0,
      "max_inflight_per_lane" => 0
    })

    assert {0, payload} = recv_response(sock)
    assert payload["accepted"] == true
    assert payload["limits"]["max_inflight_per_connection"] == 0
    assert payload["limits"]["max_inflight_per_lane"] == 0

    send_request(sock, @op_set, 1, 33, %{"key" => "native:window", "value" => "blocked"})
    assert {4, busy} = recv_response(sock)
    assert busy["code"] == "flow_control_window_exhausted"
    assert busy["scope"] == "connection"

    :gen_tcp.close(sock)
  end

  test "WINDOW_UPDATE cannot raise request windows above server configured limits", %{port: _port} do
    Application.put_env(:ferricstore, :native_max_inflight_per_connection, 7)
    Application.put_env(:ferricstore, :native_max_inflight_per_lane, 3)
    port = restart_listener([])
    sock = connect(port)

    send_request(sock, @op_window_update, 0, 46, %{
      "max_inflight_per_connection" => 10_000,
      "max_inflight_per_lane" => 10_000
    })

    assert {0, payload} = recv_response(sock)
    assert payload["limits"]["max_inflight_per_connection"] == 7
    assert payload["limits"]["max_inflight_per_lane"] == 3

    :gen_tcp.close(sock)
  end

  test "PIPELINE executes data commands and returns per-command results", %{port: port} do
    sock = connect(port)
    key = "native:pipeline:#{System.unique_integer([:positive])}"

    send_request(sock, @op_pipeline, 1, 12, %{
      "atomicity" => "none",
      "commands" => [
        %{
          "opcode" => @op_set,
          "lane_id" => 1,
          "request_id" => 13,
          "body" => %{"key" => key, "value" => "v"}
        },
        %{"opcode" => @op_get, "lane_id" => 1, "request_id" => 14, "body" => %{"key" => key}}
      ]
    })

    assert {0, [set_result, get_result]} = recv_response(sock)
    assert set_result["opcode"] == @op_set
    assert set_result["request_id"] == 13
    assert set_result["status"] == "ok"
    assert set_result["value"] == "OK"
    assert get_result["opcode"] == @op_get
    assert get_result["request_id"] == 14
    assert get_result["status"] == "ok"
    assert get_result["value"] == "v"

    :gen_tcp.close(sock)
  end

  test "compressed flag is rejected until compression is negotiated", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_get, 1, 5, %{"key" => "native-compressed"}, Codec.flags().compressed)
    assert {6, payload} = recv_response(sock)
    assert payload["message"] =~ "compressed"

    :gen_tcp.close(sock)
  end

  test "STARTUP rejects zlib request compression by default", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_startup, 0, 47, %{"compression" => "zlib"})
    assert {6, payload} = recv_response(sock)
    assert payload["message"] =~ "compression is disabled"

    :gen_tcp.close(sock)
  end

  test "STARTUP negotiates zlib and accepts compressed request bodies", %{port: port} do
    Application.put_env(:ferricstore, :native_request_compression_enabled, true)
    sock = connect(port)
    key = "native:zlib:#{System.unique_integer([:positive])}"

    send_request(sock, @op_startup, 0, 27, %{"compression" => "zlib"})
    assert {0, startup} = recv_response(sock)
    assert startup["compression"] == "zlib"

    compressed_body =
      %{"key" => key, "value" => "compressed-value"}
      |> Codec.encode_value()
      |> :zlib.compress()

    frame =
      @op_set
      |> Codec.encode_frame(1, 28, compressed_body, Codec.flags().compressed)
      |> IO.iodata_to_binary()

    :ok = :gen_tcp.send(sock, frame)
    assert {0, "OK"} = recv_response(sock)

    send_request(sock, @op_get, 1, 29, %{"key" => key})
    assert {0, "compressed-value"} = recv_response(sock)

    :gen_tcp.close(sock)
  end

  test "compressed request is rejected if decompressed body exceeds max frame bytes", %{
    port: _port
  } do
    Application.put_env(:ferricstore, :native_request_compression_enabled, true)
    port = restart_listener(max_frame_bytes: 128)
    sock = connect(port)

    send_request(sock, @op_startup, 0, 43, %{"compression" => "zlib"})
    assert {0, _startup} = recv_response(sock)

    compressed_body =
      %{"key" => "native:zlib-too-large", "value" => String.duplicate("x", 1_000)}
      |> Codec.encode_value()
      |> :zlib.compress()

    assert byte_size(compressed_body) <= 128

    frame =
      @op_set
      |> Codec.encode_frame(1, 44, compressed_body, Codec.flags().compressed)
      |> IO.iodata_to_binary()

    :ok = :gen_tcp.send(sock, frame)
    assert {6, payload} = recv_response(sock)
    assert payload["message"] =~ "exceeds max_frame_bytes"

    :gen_tcp.close(sock)
  end

  test "unknown request flags are rejected", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_get, 1, 26, %{"key" => "native-reserved-flag"}, 0x80)
    assert {6, payload} = recv_response(sock)
    assert payload["message"] =~ "unsupported flags"

    :gen_tcp.close(sock)
  end

  test "request id 0 is reserved for server events", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_get, 1, 0, %{"key" => "native-reserved-request-id"})
    assert {6, payload} = recv_response(sock)
    assert payload["message"] =~ "request_id 0"

    :gen_tcp.close(sock)
  end

  test "trace flag is rejected unless enabled by server config", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_get, 1, 48, %{"key" => "native-trace-disabled"}, Codec.flags().trace)
    assert {6, payload} = recv_response(sock)
    assert payload["message"] =~ "trace flag is disabled"

    :gen_tcp.close(sock)
  end

  test "trace flag returns native server stage timings", %{port: port} do
    Application.put_env(:ferricstore, :native_trace_enabled, true)
    sock = connect(port)
    key = "native:trace:#{System.unique_integer([:positive])}"

    send_request(
      sock,
      @op_set,
      1,
      27,
      %{"key" => key, "value" => "value"},
      Codec.flags().trace
    )

    meta = recv_response_meta(sock)
    assert meta.status == 0
    assert Bitwise.band(meta.flags, Codec.flags().trace) != 0
    assert meta.value["value"] == "OK"

    trace = meta.value["trace"]

    for key <- [
          "server_decode_us",
          "server_route_us",
          "server_lane_queue_wait_us",
          "server_body_decode_us",
          "server_command_execute_us",
          "server_response_encode_us"
        ] do
      assert is_integer(trace[key])
      assert trace[key] >= 0
    end

    refute Map.has_key?(trace, "server_lane_enqueue_us")

    :gen_tcp.close(sock)
  end

  test "no_reply flag suppresses response but still executes command", %{port: port} do
    sock = connect(port)
    key = "native:noreply:#{System.unique_integer([:positive])}"

    send_request(sock, @op_set, 1, 6, %{"key" => key, "value" => "value"}, Codec.flags().no_reply)
    assert {:error, :timeout} = :gen_tcp.recv(sock, 0, 100)

    send_request(sock, @op_get, 1, 7, %{"key" => key})
    assert {0, "value"} = recv_response(sock)

    :gen_tcp.close(sock)
  end

  test "chunked request bodies are reassembled before command execution", %{port: port} do
    sock = connect(port)
    key = "native:chunked-request:#{System.unique_integer([:positive])}"
    body = Codec.encode_value(%{"key" => key, "value" => "chunked-value"})
    split_at = div(byte_size(body), 2)
    <<first::binary-size(split_at), second::binary>> = body

    first_frame =
      @op_set
      |> Codec.encode_frame(1, 34, first, Codec.flags().more_chunks)
      |> IO.iodata_to_binary()

    second_frame =
      @op_set
      |> Codec.encode_frame(1, 34, second)
      |> IO.iodata_to_binary()

    :ok = :gen_tcp.send(sock, first_frame)
    assert {:error, :timeout} = :gen_tcp.recv(sock, 0, 100)

    :ok = :gen_tcp.send(sock, second_frame)
    assert {0, "OK"} = recv_response(sock)

    send_request(sock, @op_get, 1, 35, %{"key" => key})
    assert {0, "chunked-value"} = recv_response(sock)

    :gen_tcp.close(sock)
  end

  test "incomplete chunk stream count is bounded per connection", %{port: port} do
    Application.put_env(:ferricstore, :native_max_pending_chunks, 1)
    sock = connect(port)

    body1 = Codec.encode_value(%{"key" => "native:pending-chunk-1", "value" => "a"})
    body2 = Codec.encode_value(%{"key" => "native:pending-chunk-2", "value" => "b"})

    frame1 =
      @op_set
      |> Codec.encode_frame(1, 45, body1, Codec.flags().more_chunks)
      |> IO.iodata_to_binary()

    frame2 =
      @op_set
      |> Codec.encode_frame(1, 46, body2, Codec.flags().more_chunks)
      |> IO.iodata_to_binary()

    :ok = :gen_tcp.send(sock, frame1)
    assert {:error, :timeout} = :gen_tcp.recv(sock, 0, 100)

    :ok = :gen_tcp.send(sock, frame2)
    assert {6, payload} = recv_response(sock)
    assert payload["message"] =~ "pending chunk"

    :gen_tcp.close(sock)
  end

  test "incomplete chunk streams are bounded by total pending bytes", %{port: port} do
    Application.put_env(:ferricstore, :native_max_pending_chunk_bytes, 16)
    sock = connect(port)

    body = Codec.encode_value(%{"key" => "native:pending-chunk-bytes", "value" => "too-large"})

    frame =
      @op_set
      |> Codec.encode_frame(1, 49, body, Codec.flags().more_chunks)
      |> IO.iodata_to_binary()

    :ok = :gen_tcp.send(sock, frame)
    assert {6, payload} = recv_response(sock)
    assert payload["message"] =~ "pending chunk bytes"

    :gen_tcp.close(sock)
  end

  test "large responses are chunked when response chunk limit is configured", %{port: port} do
    Application.put_env(:ferricstore, :native_response_chunk_bytes, 16)
    sock = connect(port)
    key = "native:chunked-response:#{System.unique_integer([:positive])}"
    value = String.duplicate("x", 128)

    send_request(sock, @op_set, 1, 36, %{"key" => key, "value" => value})
    assert {0, "OK"} = recv_response(sock)

    send_request(sock, @op_get, 1, 37, %{"key" => key})
    assert %{status: 0, value: ^value, chunks: chunks} = recv_response_meta(sock)
    assert chunks > 1

    :gen_tcp.close(sock)
  end

  test "expired deadline rejects command before execution", %{port: port} do
    sock = connect(port)
    key = "native:deadline:#{System.unique_integer([:positive])}"

    send_request(sock, @op_set, 1, 8, %{"key" => key, "value" => "value", "deadline_ms" => 1})
    assert {1, payload} = recv_response(sock)
    assert payload["code"] == "deadline_exceeded"

    send_request(sock, @op_get, 1, 9, %{"key" => key})
    assert {0, nil} = recv_response(sock)

    :gen_tcp.close(sock)
  end

  test "SET and GET execute over a data lane", %{port: port} do
    sock = connect(port)
    key = "native:#{System.unique_integer([:positive])}"

    send_request(sock, @op_set, 1, 3, %{"key" => key, "value" => "value"})
    assert {0, "OK"} = recv_response(sock)

    send_request(sock, @op_get, 1, 4, %{"key" => key})
    assert {0, "value"} = recv_response(sock)

    :gen_tcp.close(sock)
  end

  test "pipelined commands preserve response order within one lane", %{port: port} do
    sock = connect(port)
    key = "native:ordered:#{System.unique_integer([:positive])}"

    frame1 =
      @op_set
      |> Codec.encode_frame(2, 30, Codec.encode_value(%{"key" => key, "value" => "value"}))
      |> IO.iodata_to_binary()

    frame2 =
      @op_get
      |> Codec.encode_frame(2, 31, Codec.encode_value(%{"key" => key}))
      |> IO.iodata_to_binary()

    :ok = :gen_tcp.send(sock, [frame1, frame2])

    assert %{lane_id: 2, request_id: 30, status: 0, value: "OK"} = recv_response_meta(sock)
    assert %{lane_id: 2, request_id: 31, status: 0, value: "value"} = recv_response_meta(sock)

    :gen_tcp.close(sock)
  end

  test "closing native connection terminates active lane processes", %{port: port} do
    sock = connect(port)
    key = "native:close-lane:#{System.unique_integer([:positive])}"

    send_request(sock, @op_set, 1, 51, %{"key" => key, "value" => "value"})
    assert {0, "OK"} = recv_response(sock)

    send_request(sock, @op_client_info, 0, 52, %{})
    assert {0, info} = recv_response(sock)
    {:ok, connection_pid} = ConnRegistry.lookup(info["client_id"])
    lane_pid = linked_native_lane!(connection_pid)
    ref = Process.monitor(lane_pid)

    :gen_tcp.close(sock)

    assert_receive {:DOWN, ^ref, :process, ^lane_pid, :shutdown}, 500
  end

  test "closing native connection terminates lane with queued large ZRANGE responses", %{
    port: port
  } do
    sock = connect(port)
    key = "native:close-zrange:#{System.unique_integer([:positive])}"

    items = for score <- 1..512, do: [score, "m#{score}"]

    send_request(sock, @op_zadd, 1, 60, %{"key" => key, "items" => items})
    assert {0, 512} = recv_response(sock)

    send_request(sock, @op_client_info, 0, 61, %{})
    assert {0, info} = recv_response(sock)
    {:ok, connection_pid} = ConnRegistry.lookup(info["client_id"])
    lane_pid = linked_native_lane!(connection_pid)
    ref = Process.monitor(lane_pid)

    frames =
      for request_id <- 62..81 do
        @op_zrange
        |> Codec.encode_frame(
          1,
          request_id,
          Codec.encode_value(%{"key" => key, "start" => 0, "stop" => -1})
        )
        |> IO.iodata_to_binary()
      end

    :ok = :gen_tcp.send(sock, frames)
    :gen_tcp.close(sock)

    assert_receive {:DOWN, ^ref, :process, ^lane_pid, :shutdown}, 1_000
  end

  test "subscribed native clients receive topology change events", %{port: port} do
    sock = connect(port)

    send_request(sock, @op_subscribe_events, 0, 41, %{"events" => ["topology_changed"]})
    assert {0, subscribe} = recv_response(sock)
    assert subscribe["subscribed"] == ["TOPOLOGY_CHANGED"]

    send_request(sock, @op_client_info, 0, 42, %{})
    assert {0, info} = recv_response(sock)
    {:ok, pid} = ConnRegistry.lookup(info["client_id"])

    send(pid, {:native_topology_changed, %{"reason" => "test"}})

    assert %{opcode: 0x0010, lane_id: 0, request_id: 0, status: 0, value: event} =
             recv_response_meta(sock)

    assert event["event"] == "TOPOLOGY_CHANGED"
    assert event["payload"]["reason"] == "test"
    assert is_integer(event["payload"]["route_epoch"])

    :gen_tcp.close(sock)
  end

  defp connect(port) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [
        :binary,
        active: false,
        packet: :raw,
        nodelay: true
      ])

    sock
  end

  defp restart_listener(opts) do
    if Listener.running?(), do: Listener.stop()
    {:ok, _pid} = Listener.start(0, opts)
    Listener.port()
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp send_request(sock, opcode, lane_id, request_id, payload, flags \\ 0) do
    frame =
      opcode
      |> Codec.encode_frame(lane_id, request_id, Codec.encode_value(payload), flags)
      |> IO.iodata_to_binary()

    :ok = :gen_tcp.send(sock, frame)
  end

  defp recv_response(sock) do
    %{status: status, value: value} = recv_response_meta(sock)
    {status, value}
  end

  defp recv_raw_response(sock) do
    %{status: status, flags: flags, raw_value_body: raw_value_body} = recv_response_meta(sock)
    assert Bitwise.band(flags, Codec.flags().custom_payload) != 0
    {:raw, status, raw_value_body}
  end

  defp recv_response_meta(sock) do
    {:ok, header} = :gen_tcp.recv(sock, 24, 5_000)

    <<"FSNP", version, flags, lane_id::unsigned-32, opcode::unsigned-16, request_id::unsigned-64,
      body_len::unsigned-32>> = header

    assert version == 0x81
    {:ok, body} = :gen_tcp.recv(sock, body_len, 5_000)

    {flags, body, chunks} =
      recv_response_chunks(sock, lane_id, opcode, request_id, flags, body, 1)

    body = maybe_uncompress(flags, body)
    <<status::unsigned-16, value_body::binary>> = body

    value = decode_response_value(flags, opcode, value_body)

    %{
      lane_id: lane_id,
      opcode: opcode,
      request_id: request_id,
      flags: flags,
      status: status,
      value: value,
      chunks: chunks,
      raw_value_body: value_body
    }
  end

  defp recv_response_chunks(sock, lane_id, opcode, request_id, flags, body, chunks) do
    if Bitwise.band(flags, Codec.flags().more_chunks) == 0 do
      {flags, body, chunks}
    else
      {:ok, header} = :gen_tcp.recv(sock, 24, 5_000)

      <<"FSNP", 0x81, next_flags, ^lane_id::unsigned-32, ^opcode::unsigned-16,
        ^request_id::unsigned-64, body_len::unsigned-32>> = header

      {:ok, next_body} = :gen_tcp.recv(sock, body_len, 5_000)

      recv_response_chunks(
        sock,
        lane_id,
        opcode,
        request_id,
        next_flags,
        body <> next_body,
        chunks + 1
      )
    end
  end

  defp maybe_uncompress(flags, body) do
    if Bitwise.band(flags, Codec.flags().compressed) != 0 do
      :zlib.uncompress(body)
    else
      body
    end
  end

  defp decode_response_value(flags, opcode, value_body) do
    if Bitwise.band(flags, Codec.flags().custom_payload) != 0 do
      case decode_custom_response_value(opcode, value_body) do
        {:ok, value} ->
          value

        :error ->
          value_body
      end
    else
      {:ok, decoded} = Codec.decode_body(value_body)
      decoded
    end
  end

  defp decode_custom_response_value(@op_get, <<0x82, 0>>), do: {:ok, nil}

  defp decode_custom_response_value(@op_get, <<0x82, 1, size::unsigned-32, value::binary>>)
       when byte_size(value) == size,
       do: {:ok, value}

  defp decode_custom_response_value(@op_mget, <<0x83, count::unsigned-32, rest::binary>>),
    do: decode_compact_mget_values(count, rest, [])

  defp decode_custom_response_value(
         @op_mget,
         <<0x89, count::unsigned-32, size::unsigned-32, rest::binary>>
       ) do
    if byte_size(rest) == count * size do
      {:ok, for(<<value::binary-size(size) <- rest>>, do: value)}
    else
      :error
    end
  end

  defp decode_custom_response_value(_opcode, <<0x81, 1::unsigned-32>>), do: {:ok, "OK"}

  defp decode_custom_response_value(_opcode, <<0x81, count::unsigned-32>>),
    do: {:ok, List.duplicate("OK", count)}

  defp decode_custom_response_value(_opcode, _value_body), do: :error

  defp decode_compact_mget_values(0, "", acc), do: {:ok, Enum.reverse(acc)}

  defp decode_compact_mget_values(count, <<0, rest::binary>>, acc) when count > 0,
    do: decode_compact_mget_values(count - 1, rest, [nil | acc])

  defp decode_compact_mget_values(count, <<1, size::unsigned-32, rest::binary>>, acc)
       when count > 0 and byte_size(rest) >= size do
    <<value::binary-size(size), rest::binary>> = rest
    decode_compact_mget_values(count - 1, rest, [value | acc])
  end

  defp decode_compact_mget_values(_count, _rest, _acc), do: :error

  defp linked_native_lane!(connection_pid) do
    {:links, links} = Process.info(connection_pid, :links)

    Enum.find_value(links, fn linked_pid ->
      case Process.info(linked_pid, :current_function) do
        {:current_function, {FerricstoreServer.Native.Lane, :loop, 3}} -> linked_pid
        _ -> nil
      end
    end) || flunk("expected native connection to link an active lane")
  end
end
