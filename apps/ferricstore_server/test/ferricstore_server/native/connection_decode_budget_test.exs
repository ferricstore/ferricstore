defmodule FerricstoreServer.Native.ConnectionDecodeBudgetTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Native.{Codec, Listener, ResourceBudget}
  alias FerricstoreServer.Native.Connection.{FrameBuffer, Responses}
  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry

  @hello_opcode 0x0001
  @startup_opcode 0x000C
  @ping_opcode 0x0003
  @client_set_name_opcode 0x0004
  @options_opcode 0x000B
  @command_exec_opcode 0x0100
  @get_opcode 0x0101
  @compressed_flag 0x08
  @no_reply_flag 0x10
  @more_chunks_flag 0x20
  @frame_count 129
  @socket_chunk_bytes 64 * 1024
  @large_frame_body_bytes 4 * 1024 * 1024
  @max_frame_bytes 16 * 1024 * 1024
  @max_buffer_bytes 128 * 1024 * 1024
  @receive_timeout 5_000

  setup do
    Acl.reset!()
    on_exit(fn -> Acl.reset!() end)
    :ok
  end

  @tag :native_command_peek
  test "session classification does not fully decode command payloads" do
    source_path =
      Path.expand("../../../lib/ferricstore_server/native/connection.ex", __DIR__)

    source = File.read!(source_path)

    [_prefix, classifier_and_rest] =
      String.split(source, "defp native_session_payload?", parts: 2)

    [classifier | _rest] = String.split(classifier_and_rest, "\n  defp ", parts: 2)

    assert classifier =~ "Codec.peek_command_name"
    refute classifier =~ "Codec.decode_body"
  end

  test "drains budgeted frame continuations without waiting for more socket data" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      1..@frame_count
      |> Enum.map(&Codec.encode_frame(@ping_opcode, 0, &1, ""))
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert receive_response_ids(socket, @frame_count) == Enum.to_list(1..@frame_count)
  end

  test "preserves FIFO when a data frame precedes a session frame on one lane" do
    key = "native:lane:session-barrier:#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = FerricStore.set(key, "value")
    on_exit(fn -> FerricStore.del(key) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        Codec.encode_frame(
          @get_opcode,
          1,
          100,
          Codec.encode_value(%{"key" => key})
        ),
        Codec.encode_frame(
          @command_exec_opcode,
          1,
          101,
          Codec.encode_value(%{"command" => "MULTI", "args" => []})
        )
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert receive_response_statuses(socket, 2) == [{100, 0}, {101, 0}]
  end

  test "flushes a synchronous session response before following data on one lane" do
    key = "native:lane:session-followed-by-data:#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = FerricStore.set(key, "value")
    on_exit(fn -> FerricStore.del(key) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame_on_lane(1, 101, "WATCH", [key]),
        Codec.encode_frame(
          @get_opcode,
          1,
          102,
          Codec.encode_value(%{"key" => key})
        )
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert receive_response_statuses(socket, 2) == [{101, 0}, {102, 0}]
  end

  test "orders multiple session and control frames around one lane batch" do
    key =
      "native:lane:session-control-interleave:#{System.unique_integer([:positive, :monotonic])}"

    assert :ok = FerricStore.set(key, "value")
    on_exit(fn -> FerricStore.del(key) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        Codec.encode_frame(
          @get_opcode,
          1,
          103,
          Codec.encode_value(%{"key" => key})
        ),
        command_exec_frame_on_lane(1, 104, "MULTI", []),
        command_exec_frame_on_lane(1, 105, "EXEC", []),
        # CLIENT.SETNAME is an ordered control frame; PING answers immediately.
        Codec.encode_frame(
          @client_set_name_opcode,
          0,
          106,
          Codec.encode_value(%{"name" => "ordered-control"})
        ),
        Codec.encode_frame(
          @get_opcode,
          1,
          107,
          Codec.encode_value(%{"key" => key})
        )
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)

    assert receive_response_statuses(socket, 5) == [
             {103, 0},
             {104, 0},
             {105, 0},
             {106, 0},
             {107, 0}
           ]
  end

  @tag :native_lane_barrier
  test "waits for a blocking session command before later same-lane work" do
    list_key = "native:lane:blocking-fifo:list:#{System.unique_integer([:positive, :monotonic])}"

    counter_key =
      "native:lane:blocking-fifo:counter:#{System.unique_integer([:positive, :monotonic])}"

    get_key = "native:lane:blocking-fifo:get:#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = FerricStore.set(counter_key, "0")
    assert :ok = FerricStore.set(get_key, "value")

    on_exit(fn ->
      FerricStore.del(list_key)
      FerricStore.del(counter_key)
      FerricStore.del(get_key)
    end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame_on_lane(1, 200, "BLPOP", [list_key, "5"]),
        Codec.encode_frame(
          @get_opcode,
          1,
          202,
          Codec.encode_value(%{"key" => get_key})
        ),
        command_exec_frame_on_lane(2, 201, "INCR", [counter_key]),
        command_exec_frame_on_lane(2, 204, "RPUSH", [list_key, "item"]),
        command_exec_frame_on_lane(1, 203, "MULTI", [])
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    responses = receive_response_statuses(socket, 5)

    assert Enum.sort(Enum.map(responses, &elem(&1, 0))) == [200, 201, 202, 203, 204]

    assert Enum.filter(responses, fn {request_id, _status} -> request_id in [200, 202, 203] end) ==
             [{200, 0}, {202, 0}, {203, 0}]

    assert FerricStore.get(counter_key) == {:ok, "1"}
    assert FerricStore.llen(list_key) == {:ok, 0}
  end

  @tag :native_deferred_admission
  test "deferred frames honor queued request byte caps" do
    previous_connection_limit =
      Application.get_env(:ferricstore, :native_max_queued_request_bytes_per_connection)

    previous_lane_limit =
      Application.get_env(:ferricstore, :native_max_queued_request_bytes_per_lane)

    get_body = Codec.encode_value(%{"key" => "native:deferred-admission:key"})
    get_bytes = FrameBuffer.retained_frame_bytes(byte_size(get_body))
    cap = get_bytes * 2
    generous_cap = get_bytes * 8

    on_exit(fn ->
      restore_env(:native_max_queued_request_bytes_per_connection, previous_connection_limit)
      restore_env(:native_max_queued_request_bytes_per_lane, previous_lane_limit)
    end)

    for {connection_cap, lane_cap} <- [{cap, generous_cap}, {generous_cap, cap}] do
      Application.put_env(
        :ferricstore,
        :native_max_queued_request_bytes_per_connection,
        connection_cap
      )

      Application.put_env(:ferricstore, :native_max_queued_request_bytes_per_lane, lane_cap)

      list_key =
        "native:deferred-admission:list:#{System.unique_integer([:positive, :monotonic])}"

      socket = connect()
      on_exit(fn -> :gen_tcp.close(socket) end)

      requests =
        [
          command_exec_frame_on_lane(1, 510, "BLPOP", [list_key, "5"]),
          Codec.encode_frame(@get_opcode, 1, 511, get_body),
          Codec.encode_frame(@get_opcode, 1, 512, get_body),
          Codec.encode_frame(@get_opcode, 1, 513, get_body)
        ]
        |> IO.iodata_to_binary()

      assert :ok = :gen_tcp.send(socket, requests)
      assert_socket_closed(socket)
    end
  end

  @tag :native_deferred_admission
  test "replayed deferred frames transfer queued byte reservations once" do
    previous_connection_limit =
      Application.get_env(:ferricstore, :native_max_queued_request_bytes_per_connection)

    previous_lane_limit =
      Application.get_env(:ferricstore, :native_max_queued_request_bytes_per_lane)

    list_key = "native:deferred-replay:list:#{System.unique_integer([:positive, :monotonic])}"
    get_key = "native:deferred-replay:value:#{System.unique_integer([:positive, :monotonic])}"
    get_body = Codec.encode_value(%{"key" => get_key})
    get_bytes = FrameBuffer.retained_frame_bytes(byte_size(get_body))
    cap = get_bytes * 2

    Application.put_env(:ferricstore, :native_max_queued_request_bytes_per_connection, cap)
    Application.put_env(:ferricstore, :native_max_queued_request_bytes_per_lane, cap)

    on_exit(fn ->
      restore_env(:native_max_queued_request_bytes_per_connection, previous_connection_limit)
      restore_env(:native_max_queued_request_bytes_per_lane, previous_lane_limit)
    end)

    assert :ok = FerricStore.set(get_key, "value")
    on_exit(fn -> FerricStore.del(get_key) end)

    existing_connections = connection_pids()
    socket = connect()
    connection_pid = wait_for_new_connection(existing_connections)
    pusher = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)
    on_exit(fn -> :gen_tcp.close(pusher) end)

    requests =
      [
        command_exec_frame_on_lane(1, 514, "BLPOP", [list_key, "5"]),
        Codec.encode_frame(@get_opcode, 1, 515, get_body),
        Codec.encode_frame(@get_opcode, 1, 516, get_body)
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert eventually(fn -> Ferricstore.Waiters.count(list_key) == 1 end)

    assert :ok =
             :gen_tcp.send(
               pusher,
               command_exec_frame_on_lane(1, 517, "RPUSH", [list_key, "item"])
             )

    assert [{517, 0}] = receive_response_statuses(pusher, 1)
    assert receive_response_statuses(socket, 3) == [{514, 0}, {515, 0}, {516, 0}]

    expected = %{
      inflight_total: 0,
      lane_inflight: %{},
      queued_request_bytes: 0,
      lane_queued_request_bytes: %{},
      deferred_inflight_total: 0,
      deferred_lane_inflight: %{},
      deferred_queued_request_bytes: 0,
      deferred_lane_queued_request_bytes: %{},
      deferred_frame_metadata_bytes: 0,
      deferred_frames: %{}
    }

    snapshot = fn ->
      {:dictionary, dictionary} = Process.info(connection_pid, :dictionary)

      dictionary
      |> Keyword.fetch!(:native_connection_cleanup_state)
      |> Map.take(Map.keys(expected))
    end

    eventually(fn -> snapshot.() == expected end)
    assert snapshot.() == expected
  end

  @tag :native_deferred_admission
  test "same-lane blocking work at the inflight cap cannot overtake" do
    previous_lane_limit = Application.get_env(:ferricstore, :native_max_inflight_per_lane)
    Application.put_env(:ferricstore, :native_max_inflight_per_lane, 1)

    on_exit(fn ->
      restore_env(:native_max_inflight_per_lane, previous_lane_limit)
    end)

    suffix = System.unique_integer([:positive, :monotonic])
    first_list = "native:deferred-inflight:first:#{suffix}"
    second_list = "native:deferred-inflight:second:#{suffix}"

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame_on_lane(1, 520, "BLPOP", [first_list, "5"]),
        command_exec_frame_on_lane(1, 521, "BLPOP", [second_list, "5"])
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert_socket_closed(socket)
  end

  @tag :native_deferred_admission
  test "deferred session frames honor queued request byte caps" do
    previous_connection_limit =
      Application.get_env(:ferricstore, :native_max_queued_request_bytes_per_connection)

    previous_lane_limit =
      Application.get_env(:ferricstore, :native_max_queued_request_bytes_per_lane)

    watch_body =
      Codec.encode_value(%{
        "command" => "WATCH",
        "args" => [String.duplicate("native:deferred-watch:", 8)]
      })

    watch_bytes = FrameBuffer.retained_frame_bytes(byte_size(watch_body))

    Application.put_env(
      :ferricstore,
      :native_max_queued_request_bytes_per_connection,
      watch_bytes
    )

    Application.put_env(:ferricstore, :native_max_queued_request_bytes_per_lane, watch_bytes)

    on_exit(fn ->
      restore_env(:native_max_queued_request_bytes_per_connection, previous_connection_limit)
      restore_env(:native_max_queued_request_bytes_per_lane, previous_lane_limit)
    end)

    list_key = "native:deferred-watch:list:#{System.unique_integer([:positive, :monotonic])}"

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame_on_lane(1, 530, "BLPOP", [list_key, "5"]),
        Codec.encode_frame(@command_exec_opcode, 1, 531, watch_body),
        Codec.encode_frame(@command_exec_opcode, 1, 532, watch_body)
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert_socket_closed(socket)
  end

  @tag :native_inflight_gate
  test "orders a synchronous inflight error after earlier lane work" do
    previous_lane_limit = Application.get_env(:ferricstore, :native_max_inflight_per_lane)
    Application.put_env(:ferricstore, :native_max_inflight_per_lane, 1)

    on_exit(fn ->
      restore_env(:native_max_inflight_per_lane, previous_lane_limit)
    end)

    key = "native:lane:sync-error:#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = FerricStore.set(key, "value")
    on_exit(fn -> FerricStore.del(key) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        Codec.encode_frame(
          @get_opcode,
          1,
          110,
          Codec.encode_value(%{"key" => key})
        ),
        Codec.encode_frame(
          @get_opcode,
          1,
          111,
          Codec.encode_value(%{"key" => key})
        )
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert receive_response_statuses(socket, 2) == [{110, 0}, {111, 4}]
  end

  test "waits only for the session lane while draining another lane" do
    key = "native:lane:independent:#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = FerricStore.set(key, "value")
    on_exit(fn -> FerricStore.del(key) end)

    existing_pids = connection_pids()
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)
    connection_pid = wait_for_new_connection(existing_pids)

    assert :ok =
             :gen_tcp.send(
               socket,
               Codec.encode_frame(
                 @get_opcode,
                 1,
                 120,
                 Codec.encode_value(%{"key" => key})
               )
             )

    assert [{120, 0}] = receive_response_statuses(socket, 1)
    lane_pid = wait_for_lane(connection_pid, 1)
    :erlang.suspend_process(lane_pid)

    on_exit(fn ->
      if Process.alive?(lane_pid) and Process.info(lane_pid, :status) == {:status, :suspended} do
        :erlang.resume_process(lane_pid)
      end
    end)

    requests =
      [
        Codec.encode_frame(
          @get_opcode,
          1,
          121,
          Codec.encode_value(%{"key" => key})
        ),
        Codec.encode_frame(
          @get_opcode,
          2,
          122,
          Codec.encode_value(%{"key" => key})
        ),
        command_exec_frame_on_lane(1, 123, "WATCH", [key])
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert [{122, 0}] = receive_response_statuses(socket, 1)

    :erlang.resume_process(lane_pid)
    assert receive_response_statuses(socket, 2) == [{121, 0}, {123, 0}]
  end

  @tag :native_lane_barrier
  test "buffers a later pipelined write while waiting on a lane barrier" do
    key = "native:lane:pipelined-barrier:#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = FerricStore.set(key, "value")
    on_exit(fn -> FerricStore.del(key) end)

    existing_pids = connection_pids()
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)
    connection_pid = wait_for_new_connection(existing_pids)

    assert :ok =
             :gen_tcp.send(
               socket,
               Codec.encode_frame(
                 @get_opcode,
                 1,
                 150,
                 Codec.encode_value(%{"key" => key})
               )
             )

    assert [{150, 0}] = receive_response_statuses(socket, 1)
    lane_pid = wait_for_lane(connection_pid, 1)
    :erlang.suspend_process(lane_pid)

    on_exit(fn ->
      if Process.alive?(lane_pid) and Process.info(lane_pid, :status) == {:status, :suspended} do
        :erlang.resume_process(lane_pid)
      end
    end)

    assert :ok =
             :gen_tcp.send(
               socket,
               [
                 Codec.encode_frame(
                   @get_opcode,
                   1,
                   151,
                   Codec.encode_value(%{"key" => key})
                 ),
                 command_exec_frame_on_lane(1, 152, "WATCH", [key])
               ]
             )

    Process.sleep(20)

    assert :ok =
             :gen_tcp.send(
               socket,
               Codec.encode_frame(
                 @get_opcode,
                 1,
                 153,
                 Codec.encode_value(%{"key" => key})
               )
             )

    Process.sleep(20)
    :erlang.resume_process(lane_pid)
    assert receive_response_statuses(socket, 3) == [{151, 0}, {152, 0}, {153, 0}]
  end

  @tag :native_lane_barrier
  test "closes when a lane terminates before its barrier" do
    key = "native:lane:barrier-death:#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = FerricStore.set(key, "value")
    on_exit(fn -> FerricStore.del(key) end)

    existing_pids = connection_pids()
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)
    connection_pid = wait_for_new_connection(existing_pids)

    assert :ok =
             :gen_tcp.send(
               socket,
               Codec.encode_frame(
                 @get_opcode,
                 1,
                 160,
                 Codec.encode_value(%{"key" => key})
               )
             )

    assert [{160, 0}] = receive_response_statuses(socket, 1)
    lane_pid = wait_for_lane(connection_pid, 1)
    :erlang.suspend_process(lane_pid)

    on_exit(fn ->
      if Process.alive?(lane_pid) and Process.info(lane_pid, :status) == {:status, :suspended} do
        :erlang.resume_process(lane_pid)
      end
    end)

    assert :ok =
             :gen_tcp.send(
               socket,
               [
                 Codec.encode_frame(
                   @get_opcode,
                   1,
                   161,
                   Codec.encode_value(%{"key" => key})
                 ),
                 command_exec_frame_on_lane(1, 162, "WATCH", [key])
               ]
             )

    Process.sleep(20)
    Process.exit(lane_pid, :kill)
    assert_socket_closed(socket)
    assert eventually(fn -> not Process.alive?(connection_pid) end)
  end

  @tag :native_lane_barrier
  test "bounds a stalled lane barrier and cleans up its resources" do
    previous_timeout = Application.get_env(:ferricstore, :native_lane_barrier_timeout_ms)
    Application.put_env(:ferricstore, :native_lane_barrier_timeout_ms, 50)

    on_exit(fn ->
      restore_env(:native_lane_barrier_timeout_ms, previous_timeout)
    end)

    key = "native:lane:barrier-timeout:#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = FerricStore.set(key, "value")
    on_exit(fn -> FerricStore.del(key) end)

    existing_pids = connection_pids()
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)
    connection_pid = wait_for_new_connection(existing_pids)

    assert :ok =
             :gen_tcp.send(
               socket,
               Codec.encode_frame(
                 @get_opcode,
                 1,
                 130,
                 Codec.encode_value(%{"key" => key})
               )
             )

    assert [{130, 0}] = receive_response_statuses(socket, 1)
    lane_pid = wait_for_lane(connection_pid, 1)
    :erlang.suspend_process(lane_pid)

    on_exit(fn ->
      if Process.alive?(lane_pid) and Process.info(lane_pid, :status) == {:status, :suspended} do
        :erlang.resume_process(lane_pid)
      end
    end)

    assert :ok =
             :gen_tcp.send(
               socket,
               [
                 Codec.encode_frame(
                   @get_opcode,
                   1,
                   131,
                   Codec.encode_value(%{"key" => key})
                 ),
                 # Lane-local session frames defer behind the lane without
                 # blocking the connection; ordered control frames keep a
                 # bounded barrier.
                 Codec.encode_frame(
                   @client_set_name_opcode,
                   0,
                   132,
                   Codec.encode_value(%{"name" => "stalled-barrier"})
                 )
               ]
             )

    assert_socket_closed(socket)
    assert eventually(fn -> not Process.alive?(connection_pid) end)
    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).lanes == 0 end)
  end

  @tag :native_lane_barrier
  test "stops a pending lane barrier when the client disconnects" do
    previous_timeout = Application.get_env(:ferricstore, :native_lane_barrier_timeout_ms)
    Application.put_env(:ferricstore, :native_lane_barrier_timeout_ms, 5_000)

    on_exit(fn ->
      restore_env(:native_lane_barrier_timeout_ms, previous_timeout)
    end)

    key = "native:lane:barrier-disconnect:#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = FerricStore.set(key, "value")
    on_exit(fn -> FerricStore.del(key) end)

    existing_pids = connection_pids()
    socket = connect()
    connection_pid = wait_for_new_connection(existing_pids)
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok =
             :gen_tcp.send(
               socket,
               Codec.encode_frame(
                 @get_opcode,
                 1,
                 140,
                 Codec.encode_value(%{"key" => key})
               )
             )

    assert [{140, 0}] = receive_response_statuses(socket, 1)
    lane_pid = wait_for_lane(connection_pid, 1)
    :erlang.suspend_process(lane_pid)

    on_exit(fn ->
      if Process.alive?(lane_pid) and Process.info(lane_pid, :status) == {:status, :suspended} do
        :erlang.resume_process(lane_pid)
      end
    end)

    assert :ok =
             :gen_tcp.send(
               socket,
               [
                 Codec.encode_frame(
                   @get_opcode,
                   1,
                   141,
                   Codec.encode_value(%{"key" => key})
                 ),
                 command_exec_frame_on_lane(1, 142, "WATCH", [key])
               ]
             )

    Process.sleep(20)
    assert :ok = :gen_tcp.close(socket)
    assert eventually(fn -> not Process.alive?(connection_pid) end)
  end

  test "preserves an incomplete frame until the remaining socket bytes arrive" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    request = Codec.encode_frame(@ping_opcode, 0, 42, "")
    split_at = byte_size(request) - 2
    <<partial::binary-size(^split_at), final_bytes::binary>> = request

    assert :ok = :gen_tcp.send(socket, partial)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 25)
    assert :ok = :gen_tcp.send(socket, final_bytes)
    assert receive_response_ids(socket, 1) == [42]
  end

  @tag :frame_assembly_deadline
  test "partial frame assembly has an absolute deadline" do
    previous_timeout = Application.get_env(:ferricstore, :native_frame_assembly_timeout_ms)
    Application.put_env(:ferricstore, :native_frame_assembly_timeout_ms, 40)

    on_exit(fn ->
      restore_env(:native_frame_assembly_timeout_ms, previous_timeout)
    end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    frame = Codec.encode_frame(@ping_opcode, 0, 47, String.duplicate("x", 128))
    assert :ok = :gen_tcp.send(socket, binary_part(frame, 0, 25))

    Process.sleep(80)
    assert_socket_closed(socket)
  end

  @tag :chunk_assembly_deadline
  test "chunked request assembly has an absolute deadline across complete wire frames" do
    previous_timeout = Application.get_env(:ferricstore, :native_frame_assembly_timeout_ms)
    Application.put_env(:ferricstore, :native_frame_assembly_timeout_ms, 500)

    on_exit(fn ->
      restore_env(:native_frame_assembly_timeout_ms, previous_timeout)
    end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    partial_request =
      Codec.encode_frame(@ping_opcode, 1, 48, "partial", @more_chunks_flag)

    assert :ok = :gen_tcp.send(socket, partial_request)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 20)

    Process.sleep(35)
    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@ping_opcode, 0, 49, ""))
    assert receive_response_ids(socket, 1) == [49]

    Process.sleep(500)
    assert_socket_closed(socket)
  end

  test "blocking commands inside MULTI are rejected by the session path" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame(201, "MULTI", []),
        command_exec_frame(202, "BLPOP", ["transaction:blocking:key", "0.01"]),
        command_exec_frame(203, "EXEC", [])
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)

    assert socket |> receive_response_statuses(3) |> Map.new() == %{
             201 => 0,
             202 => 1,
             203 => 1
           }
  end

  test "blocking session commands consume the connection inflight window" do
    previous_connection_limit =
      Application.get_env(:ferricstore, :native_max_inflight_per_connection)

    previous_lane_limit = Application.get_env(:ferricstore, :native_max_inflight_per_lane)
    Application.put_env(:ferricstore, :native_max_inflight_per_connection, 1)
    Application.put_env(:ferricstore, :native_max_inflight_per_lane, 1)

    on_exit(fn ->
      restore_env(:native_max_inflight_per_connection, previous_connection_limit)
      restore_env(:native_max_inflight_per_lane, previous_lane_limit)
    end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame(211, "BLPOP", ["native:blocking:held", "5"]),
        command_exec_frame(212, "BLPOP", ["native:blocking:rejected", "0.01"])
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert [{212, 4}] = receive_response_statuses(socket, 1)
  end

  @tag :session_execution_budget
  test "EXEC preserves its transaction when server-wide execution capacity is exhausted" do
    execution_limit =
      Application.get_env(
        :ferricstore,
        :native_max_global_executions,
        max(System.schedulers_online(), 1) * 8
      )

    assert {:ok, budget_token} =
             ResourceBudget.acquire(ResourceBudget, :executions, self(), execution_limit)

    on_exit(fn -> ResourceBudget.release(ResourceBudget, budget_token) end)

    key = "native:session-execution-budget:#{System.unique_integer([:positive])}"
    on_exit(fn -> FerricStore.del(key) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok =
             :gen_tcp.send(
               socket,
               [
                 command_exec_frame(216, "MULTI", []),
                 command_exec_frame(217, "SET", [key, "value"])
               ]
             )

    assert socket |> receive_response_statuses(2) |> Map.new() == %{216 => 0, 217 => 0}

    assert :ok = :gen_tcp.send(socket, command_exec_frame(218, "EXEC", []))
    assert [{218, 4}] = receive_response_statuses(socket, 1)

    assert :ok = ResourceBudget.release(ResourceBudget, budget_token)
    assert :ok = :gen_tcp.send(socket, command_exec_frame(219, "EXEC", []))
    assert [{219, 0}] = receive_response_statuses(socket, 1)
    assert FerricStore.get(key) == {:ok, "value"}
  end

  test "NO_REPLY suppresses a data command rejected by the inflight gate" do
    previous_connection_limit =
      Application.get_env(:ferricstore, :native_max_inflight_per_connection)

    previous_lane_limit = Application.get_env(:ferricstore, :native_max_inflight_per_lane)
    Application.put_env(:ferricstore, :native_max_inflight_per_connection, 1)
    Application.put_env(:ferricstore, :native_max_inflight_per_lane, 1)

    on_exit(fn ->
      restore_env(:native_max_inflight_per_connection, previous_connection_limit)
      restore_env(:native_max_inflight_per_lane, previous_lane_limit)
    end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame(213, "BLPOP", ["native:blocking:no-reply-gate", "5"]),
        Codec.encode_frame(
          @get_opcode,
          1,
          214,
          Codec.encode_value(%{"key" => "native:rejected:no-reply"}),
          @no_reply_flag
        )
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 75)

    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@ping_opcode, 0, 215, ""))
    assert receive_response_ids(socket, 1) == [215]
  end

  test "NO_REPLY suppresses native session command responses" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame(221, "MULTI", [], @no_reply_flag),
        command_exec_frame(222, "DISCARD", [], @no_reply_flag)
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 75)

    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@ping_opcode, 0, 223, ""))
    assert receive_response_ids(socket, 1) == [223]
  end

  test "NO_REPLY control commands skip response encoding" do
    existing_pids = connection_pids()
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)
    connection_pid = wait_for_new_connection(existing_pids)

    :erlang.trace_pattern({Responses, :encode_response, 6}, true, [])
    :erlang.trace(connection_pid, true, [:call])

    on_exit(fn ->
      disable_call_trace(connection_pid)
      :erlang.trace_pattern({Responses, :encode_response, 6}, false, [])
    end)

    requests =
      [
        Codec.encode_frame(@options_opcode, 0, 224, "", @no_reply_flag),
        command_exec_frame(227, "MULTI", [], @no_reply_flag),
        command_exec_frame(228, "DISCARD", [], @no_reply_flag),
        Codec.encode_frame(@ping_opcode, 0, 225, "")
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert receive_response_ids(socket, 1) == [225]

    assert_receive {:trace, ^connection_pid, :call,
                    {Responses, :encode_response, [_state, @ping_opcode, 0, 225, :ok, _value]}}

    refute_receive {:trace, ^connection_pid, :call,
                    {Responses, :encode_response,
                     [_state, @options_opcode, 0, 224, _status, _value]}}

    for request_id <- [227, 228] do
      refute_receive {:trace, ^connection_pid, :call,
                      {Responses, :encode_response,
                       [_state, @command_exec_opcode, 0, ^request_id, _status, _value]}}
    end

    connection_monitor = Process.monitor(connection_pid)
    :ok = :gen_tcp.close(socket)
    assert_receive {:DOWN, ^connection_monitor, :process, ^connection_pid, _reason}
  end

  @tag :control_execution_budget
  test "control commands consume global execution capacity" do
    execution_limit =
      Application.get_env(
        :ferricstore,
        :native_max_global_executions,
        max(System.schedulers_online(), 1) * 8
      )

    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).executions == 0 end)

    assert {:ok, budget_token} =
             ResourceBudget.acquire(ResourceBudget, :executions, self(), execution_limit)

    on_exit(fn -> ResourceBudget.release(ResourceBudget, budget_token) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok =
             :gen_tcp.send(socket, Codec.encode_frame(@options_opcode, 0, 226, ""))

    assert [{226, 4}] = receive_response_statuses(socket, 1)
  end

  test "NO_REPLY suppresses delayed native blocking responses" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok =
             :gen_tcp.send(
               socket,
               command_exec_frame(
                 231,
                 "BLPOP",
                 ["native:blocking:no-reply", "0.01"],
                 @no_reply_flag
               )
             )

    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 75)

    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@ping_opcode, 0, 232, ""))
    assert receive_response_ids(socket, 1) == [232]
  end

  @tag :native_outbound_byte_budget
  test "blocking results release outbound capacity after socket send" do
    previous_limit =
      Application.get_env(:ferricstore, :native_max_outbound_bytes_per_connection)

    Application.put_env(:ferricstore, :native_max_outbound_bytes_per_connection, 1_024 * 1_024)

    on_exit(fn ->
      restore_env(:native_max_outbound_bytes_per_connection, previous_limit)
    end)

    key = "native:blocking:outbound-send:#{System.unique_integer([:positive, :monotonic])}"
    assert {:ok, 1} = FerricStore.rpush(key, [:binary.copy("v", 256)])
    on_exit(fn -> FerricStore.del(key) end)
    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).outbound_bytes == 0 end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok = :gen_tcp.send(socket, command_exec_frame(241, "BLPOP", [key, "1"]))
    assert [{241, 0}] = receive_response_statuses(socket, 1)
    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).outbound_bytes == 0 end)
  end

  @tag :native_outbound_byte_budget
  test "blocking result overflow closes the connection" do
    previous_limit =
      Application.get_env(:ferricstore, :native_max_outbound_bytes_per_connection)

    Application.put_env(:ferricstore, :native_max_outbound_bytes_per_connection, 200)

    on_exit(fn ->
      restore_env(:native_max_outbound_bytes_per_connection, previous_limit)
    end)

    key = "native:blocking:outbound-close:#{System.unique_integer([:positive, :monotonic])}"
    assert {:ok, 1} = FerricStore.rpush(key, [:binary.copy("v", 256)])
    on_exit(fn -> FerricStore.del(key) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok = :gen_tcp.send(socket, command_exec_frame(242, "BLPOP", [key, "1"]))
    assert_socket_closed(socket)
  end

  @tag :inflight_idle_timeout
  test "idle timeout does not terminate an active blocking request" do
    previous_timeout = Application.get_env(:ferricstore, :native_idle_timeout_ms)
    Application.put_env(:ferricstore, :native_idle_timeout_ms, 40)

    on_exit(fn ->
      restore_env(:native_idle_timeout_ms, previous_timeout)
    end)

    key = "native:blocking:idle-timeout:#{System.unique_integer([:positive])}"
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok =
             :gen_tcp.send(socket, command_exec_frame(232, "BLPOP", [key, "0.12"]))

    assert [{232, 0}] = receive_response_statuses(socket, 1)
  end

  @tag :native_response_byte_budget
  test "all native command forms enforce the connection response byte budget" do
    previous_limit = Application.get_env(:ferricstore, :native_max_response_bytes)
    Application.put_env(:ferricstore, :native_max_response_bytes, 64)

    on_exit(fn ->
      restore_env(:native_max_response_bytes, previous_limit)
    end)

    key = "native:response-budget:#{System.unique_integer([:positive])}"
    assert :ok = FerricStore.set(key, String.duplicate("x", 128))
    on_exit(fn -> FerricStore.del(key) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        Codec.encode_frame(@get_opcode, 1, 233, Codec.encode_value(%{"key" => key})),
        Codec.encode_frame(
          @command_exec_opcode,
          1,
          234,
          Codec.encode_value(%{"command" => "GET", "args" => [key]})
        )
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)

    assert socket |> receive_response_statuses(2) |> Map.new() == %{
             233 => 6,
             234 => 6
           }
  end

  @tag :native_outbound_byte_budget
  test "lane responses close a connection before crossing its outbound byte ceiling" do
    previous_limit =
      Application.get_env(:ferricstore, :native_max_outbound_bytes_per_connection)

    Application.put_env(:ferricstore, :native_max_outbound_bytes_per_connection, 16)

    on_exit(fn ->
      restore_env(:native_max_outbound_bytes_per_connection, previous_limit)
    end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    body = Codec.encode_value(%{"key" => "native:outbound:connection-limit"})
    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@get_opcode, 1, 237, body))
    assert_socket_closed(socket)
  end

  @tag :native_outbound_byte_budget
  test "lane responses close when global outbound capacity is exhausted" do
    global_limit =
      Application.get_env(
        :ferricstore,
        :native_max_global_outbound_bytes,
        512 * 1024 * 1024
      )

    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).outbound_bytes == 0 end)

    assert {:ok, holder} =
             ResourceBudget.acquire(ResourceBudget, :outbound_bytes, self(), global_limit)

    on_exit(fn -> ResourceBudget.release(ResourceBudget, holder) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    body = Codec.encode_value(%{"key" => "native:outbound:global-limit"})
    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@get_opcode, 1, 238, body))
    assert_socket_closed(socket)
  end

  @tag :native_outbound_byte_budget
  test "pubsub events release guarded outbound capacity after socket send" do
    previous_limit =
      Application.get_env(:ferricstore, :native_max_outbound_bytes_per_connection)

    Application.put_env(:ferricstore, :native_max_outbound_bytes_per_connection, 1_024 * 1_024)

    on_exit(fn ->
      restore_env(:native_max_outbound_bytes_per_connection, previous_limit)
    end)

    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).outbound_bytes == 0 end)

    channel = "native:outbound:pubsub:#{System.unique_integer([:positive, :monotonic])}"
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok = :gen_tcp.send(socket, command_exec_frame(239, "SUBSCRIBE", [channel]))
    assert [{239, 0}] = receive_response_statuses(socket, 1)

    assert Ferricstore.PubSub.publish(channel, :binary.copy("p", 256)) == 1
    assert {:ok, event} = :gen_tcp.recv(socket, 0, @receive_timeout)
    assert byte_size(event) > 256

    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).outbound_bytes == 0 end)
  end

  @tag :native_outbound_byte_budget
  test "pubsub batches use one guarded reservation and still emit one frame per message" do
    previous_limit =
      Application.get_env(:ferricstore, :native_max_outbound_bytes_per_connection)

    Application.put_env(:ferricstore, :native_max_outbound_bytes_per_connection, 1_024 * 1_024)

    on_exit(fn ->
      restore_env(:native_max_outbound_bytes_per_connection, previous_limit)
    end)

    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).outbound_bytes == 0 end)

    channel = "native:outbound:pubsub-batch:#{System.unique_integer([:positive, :monotonic])}"
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok = :gen_tcp.send(socket, command_exec_frame(242, "SUBSCRIBE", [channel]))
    assert [{242, 0}] = receive_response_statuses(socket, 1)

    assert Ferricstore.PubSub.publish_many([{channel, "one"}, {channel, "two"}]) == [1, 1]
    assert [_first, _second] = receive_native_frames(socket, 2)

    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).outbound_bytes == 0 end)
  end

  @tag :native_outbound_byte_budget
  test "negotiated pubsub batches emit one ordered batch event frame" do
    previous_limit =
      Application.get_env(:ferricstore, :native_max_outbound_bytes_per_connection)

    Application.put_env(:ferricstore, :native_max_outbound_bytes_per_connection, 1_024 * 1_024)

    on_exit(fn ->
      restore_env(:native_max_outbound_bytes_per_connection, previous_limit)
    end)

    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).outbound_bytes == 0 end)

    channel =
      "native:outbound:pubsub-negotiated-batch:#{System.unique_integer([:positive, :monotonic])}"

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    startup_body = Codec.encode_value(%{"compact_response_codecs" => ["pubsub_batch_v1"]})
    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@startup_opcode, 0, 244, startup_body))
    assert [{244, 0}] = receive_response_statuses(socket, 1)

    assert :ok = :gen_tcp.send(socket, command_exec_frame(245, "SUBSCRIBE", [channel]))
    assert [{245, 0}] = receive_response_statuses(socket, 1)

    assert Ferricstore.PubSub.publish_many([{channel, "one"}, {channel, "two"}]) == [1, 1]
    assert [body] = receive_native_frames(socket, 1)
    assert <<0::unsigned-16, value_body::binary>> = body
    assert {:ok, value} = Codec.decode_body(value_body)

    assert value == %{
             "event" => "PUBSUB_MESSAGE",
             "payload" => %{
               "kind" => "message_batch",
               "channel" => channel,
               "messages" => ["one", "two"]
             },
             "at_ms" => value["at_ms"]
           }

    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 25)
    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).outbound_bytes == 0 end)
  end

  test "HELLO refreshes prepared batch delivery metadata for existing subscriptions" do
    existing_pids = connection_pids()
    channel = "native:pubsub:renegotiated-batch:#{System.unique_integer([:positive, :monotonic])}"
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)
    connection_pid = wait_for_new_connection(existing_pids)

    assert :ok = :gen_tcp.send(socket, command_exec_frame(248, "SUBSCRIBE", [channel]))
    assert [{248, 0}] = receive_response_statuses(socket, 1)

    assert [{^channel, [{^connection_pid, _guard}], 1}] =
             :ets.lookup(:ferricstore_pubsub_channel_cache, channel)

    enabled_body = Codec.encode_value(%{"compact_response_codecs" => ["pubsub_batch_v1"]})
    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@hello_opcode, 0, 249, enabled_body))
    assert [{249, 0}] = receive_response_statuses(socket, 1)

    assert eventually(fn ->
             match?(
               [{^channel, [{^connection_pid, _guard, :prepared_batches}], 1}],
               :ets.lookup(:ferricstore_pubsub_channel_cache, channel)
             )
           end)

    disabled_body = Codec.encode_value(%{})
    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@hello_opcode, 0, 250, disabled_body))
    assert [{250, 0}] = receive_response_statuses(socket, 1)

    assert eventually(fn ->
             match?(
               [{^channel, [{^connection_pid, _guard}], 1}],
               :ets.lookup(:ferricstore_pubsub_channel_cache, channel)
             )
           end)
  end

  @tag :native_outbound_byte_budget
  test "negotiated pubsub batches split before the response limit" do
    previous_response_limit = Application.get_env(:ferricstore, :native_max_response_bytes)

    previous_outbound_limit =
      Application.get_env(:ferricstore, :native_max_outbound_bytes_per_connection)

    Application.put_env(:ferricstore, :native_max_response_bytes, 1_024 * 1_024)

    Application.put_env(
      :ferricstore,
      :native_max_outbound_bytes_per_connection,
      4 * 1_024 * 1_024
    )

    on_exit(fn ->
      restore_env(:native_max_response_bytes, previous_response_limit)
      restore_env(:native_max_outbound_bytes_per_connection, previous_outbound_limit)
    end)

    channel = "c"
    messages = [String.duplicate("x", 700_000), String.duplicate("y", 700_000)]
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    hello_body =
      Codec.encode_value(%{
        "compact_flow_responses" => false,
        "compact_response_codecs" => ["pubsub_batch_v1"]
      })

    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@hello_opcode, 0, 246, hello_body))
    assert [{246, 0}] = receive_response_statuses(socket, 1)

    assert :ok = :gen_tcp.send(socket, command_exec_frame(247, "SUBSCRIBE", [channel]))
    assert [{247, 0}] = receive_response_statuses(socket, 1)

    assert Ferricstore.PubSub.publish_many(Enum.map(messages, &{channel, &1})) == [1, 1]

    assert decoded =
             socket
             |> receive_native_frames(2)
             |> Enum.map(fn <<0::unsigned-16, value_body::binary>> ->
               {:ok, value} = Codec.decode_body(value_body)
               value
             end)

    assert Enum.flat_map(decoded, fn %{"payload" => payload} ->
             case payload do
               %{"kind" => "message_batch", "messages" => batch} -> batch
               %{"kind" => "message", "message" => message} -> [message]
             end
           end) == messages
  end

  test "pubsub coalescing processes an ACL barrier before later events" do
    existing_pids = connection_pids()
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)
    connection_pid = wait_for_new_connection(existing_pids)

    send(connection_pid, {:pubsub_message, "ordered", "first"})
    send(connection_pid, {:acl_invalidate, :all, 1})
    send(connection_pid, {:pubsub_message, "ordered", "second"})

    assert [first_body] = receive_native_frames(socket, 1)
    assert <<0::unsigned-16, first_value_body::binary>> = first_body
    assert {:ok, first_value} = Codec.decode_body(first_value_body)

    assert first_value["payload"] == %{
             "kind" => "message",
             "channel" => "ordered",
             "message" => "first"
           }

    assert_socket_closed_without_more_data(socket)
  end

  @tag :native_outbound_byte_budget
  test "pubsub closes a slow subscriber before queueing an over-limit event" do
    previous_limit =
      Application.get_env(:ferricstore, :native_max_outbound_bytes_per_connection)

    Application.put_env(:ferricstore, :native_max_outbound_bytes_per_connection, 200)

    on_exit(fn ->
      restore_env(:native_max_outbound_bytes_per_connection, previous_limit)
    end)

    channel =
      "native:outbound:pubsub-overflow:#{System.unique_integer([:positive, :monotonic])}"

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok = :gen_tcp.send(socket, command_exec_frame(240, "SUBSCRIBE", [channel]))
    assert [{240, 0}] = receive_response_statuses(socket, 1)

    assert Ferricstore.PubSub.publish(channel, :binary.copy("p", 256)) == 0
    assert_socket_closed(socket)
  end

  @tag :native_outbound_byte_budget
  test "pubsub batches cannot bypass the slow-subscriber outbound limit" do
    previous_limit =
      Application.get_env(:ferricstore, :native_max_outbound_bytes_per_connection)

    Application.put_env(:ferricstore, :native_max_outbound_bytes_per_connection, 200)

    on_exit(fn ->
      restore_env(:native_max_outbound_bytes_per_connection, previous_limit)
    end)

    channel =
      "native:outbound:pubsub-batch-overflow:#{System.unique_integer([:positive, :monotonic])}"

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok = :gen_tcp.send(socket, command_exec_frame(243, "SUBSCRIBE", [channel]))
    assert [{243, 0}] = receive_response_statuses(socket, 1)

    payload = :binary.copy("p", 256)
    assert Ferricstore.PubSub.publish_many([{channel, payload}, {channel, payload}]) == [0, 0]
    assert_socket_closed(socket)
  end

  test "keeps fragmented multi-megabyte frames chunked until the frame is complete" do
    frame = large_ping_frame(43)
    chunks = binary_chunks(frame, @socket_chunk_bytes)
    {partial_chunks, [final_chunk]} = Enum.split(chunks, -1)

    accumulator =
      Enum.reduce(partial_chunks, FrameBuffer.new(), fn chunk, accumulator ->
        assert {:incomplete, next} =
                 FrameBuffer.append(
                   accumulator,
                   chunk,
                   @max_frame_bytes,
                   @max_buffer_bytes
                 )

        next
      end)

    assert FrameBuffer.stats(accumulator) == %{
             buffered_bytes: Enum.sum(Enum.map(partial_chunks, &byte_size/1)),
             chunk_count: length(partial_chunks),
             complete?: false,
             header_bytes: 24,
             storage: :iodata
           }

    assert {:ready, complete} =
             FrameBuffer.append(
               accumulator,
               final_chunk,
               @max_frame_bytes,
               @max_buffer_bytes
             )

    assert FrameBuffer.materialize(complete) == frame
  end

  test "buffer accounting work stays constant as the fragment list grows" do
    reductions = fn fragments ->
      Task.async(fn ->
        # Keep collection of the fixture itself outside this work-complexity
        # measurement; the assertion does not depend on wall-clock timings.
        Process.flag(:min_heap_size, 1_000_000)
        body = :binary.copy("x", fragments + 1)
        header = binary_part(Codec.encode_frame(@ping_opcode, 0, 47, body), 0, 24)

        buffer =
          Enum.reduce(1..fragments, FrameBuffer.from_binary(header, fragments + 1), fn _, acc ->
            {:incomplete, next} = FrameBuffer.append(acc, "x", fragments + 1, fragments + 25)
            next
          end)

        assert FrameBuffer.stats(buffer).chunk_count == fragments + 1

        assert {:incomplete, ^buffer} =
                 FrameBuffer.append(buffer, "", fragments + 1, fragments + 25)

        :erlang.garbage_collect()
        {:reductions, before} = Process.info(self(), :reductions)
        Enum.each(1..1_000, fn _ -> FrameBuffer.stats(buffer) end)
        {:reductions, after_count} = Process.info(self(), :reductions)
        after_count - before
      end)
      |> Task.await(5_000)
    end

    small = reductions.(64)
    large = reductions.(16_384)

    assert large <= small * 2,
           "accounting cost grew with fragments: #{small} reductions versus #{large}"
  end

  test "frame buffer preserves nonuniform fragment order across many fragments" do
    fragments = for index <- 1..4_097, do: :binary.copy(<<rem(index, 251)>>, rem(index, 5) + 1)
    body = IO.iodata_to_binary(fragments)
    header = Codec.encode_frame(@ping_opcode, 0, 49, body)

    {:incomplete, buffer} =
      FrameBuffer.append(
        FrameBuffer.new(),
        binary_part(header, 0, 24),
        byte_size(body),
        byte_size(header)
      )

    buffer =
      Enum.reduce(fragments, buffer, fn fragment, buffer ->
        case FrameBuffer.append(buffer, fragment, byte_size(body), byte_size(header)) do
          {:incomplete, next} -> next
          {:ready, next} -> next
        end
      end)

    stats = FrameBuffer.stats(buffer)
    assert stats.chunk_count == length(fragments) + 1
    assert FrameBuffer.materialize(buffer) == header
  end

  test "frame buffer keeps already-large fragments independent" do
    fragment = String.duplicate("x", 8 * 1024)
    body = String.duplicate(fragment, 64)
    header = Codec.encode_frame(@ping_opcode, 0, 50, body)

    {:incomplete, buffer} =
      FrameBuffer.append(
        FrameBuffer.new(),
        binary_part(header, 0, 24),
        byte_size(body),
        byte_size(header)
      )

    buffer =
      Enum.reduce(1..64, buffer, fn _, buffer ->
        case FrameBuffer.append(buffer, fragment, byte_size(body), byte_size(header)) do
          {:incomplete, next} -> next
          {:ready, next} -> next
        end
      end)

    stats = FrameBuffer.stats(buffer)
    assert stats.chunk_count == 65
    assert FrameBuffer.materialize(buffer) == header
  end

  test "accepts a complete maximum-size frame coalesced with continuation bytes" do
    max_frame_bytes = 32
    max_buffer_bytes = max_frame_bytes + 24
    body = String.duplicate("x", max_frame_bytes)
    frame_and_continuation = Codec.encode_frame(@ping_opcode, 0, 45, body) <> "N"

    assert {:ready, buffer} =
             FrameBuffer.append(
               FrameBuffer.new(),
               frame_and_continuation,
               max_frame_bytes,
               max_buffer_bytes
             )

    assert {:ok, [{0, @ping_opcode, 45, 0, ^body}], "N", :done} =
             buffer
             |> FrameBuffer.materialize()
             |> Codec.decode_frames(max_frame_bytes)
  end

  test "rejects an oversized coalesced read even when its first frame is complete" do
    max_frame_bytes = 32
    max_buffer_bytes = max_frame_bytes + 24
    body = String.duplicate("x", max_frame_bytes)
    oversized_continuation = :binary.copy("N", 64 * 1024 + 1)

    assert {:error, :buffer_limit} =
             FrameBuffer.append(
               FrameBuffer.new(),
               Codec.encode_frame(@ping_opcode, 0, 46, body) <> oversized_continuation,
               max_frame_bytes,
               max_buffer_bytes
             )
  end

  test "rejects a declared frame larger than the current buffer budget at its header" do
    body = String.duplicate("x", 128)
    header = binary_part(Codec.encode_frame(@ping_opcode, 0, 48, body), 0, 24)

    assert {:error, :buffer_limit} =
             FrameBuffer.append(FrameBuffer.new(), header, 1_024, 64)
  end

  @tag :preauth_frame_budget
  test "unauthenticated connections apply the smaller frame budget" do
    previous_limit =
      Application.get_env(:ferricstore, :native_unauthenticated_max_frame_bytes)

    Application.put_env(:ferricstore, :native_unauthenticated_max_frame_bytes, 64)

    on_exit(fn ->
      restore_env(:native_unauthenticated_max_frame_bytes, previous_limit)
    end)

    username = "preauth-budget-#{System.unique_integer([:positive])}"
    assert :ok = Acl.set_user(username, ["on", ">secret", "+@all", "~*"])
    assert Acl.has_configured_users?()

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    oversized = Codec.encode_frame(@ping_opcode, 0, 49, String.duplicate("x", 128))
    assert :ok = :gen_tcp.send(socket, binary_part(oversized, 0, 24))
    assert_socket_closed(socket)
  end

  @tag :preauth_frame_budget
  test "unauthenticated chunk streams cannot exceed the logical frame budget" do
    previous_limit =
      Application.get_env(:ferricstore, :native_unauthenticated_max_frame_bytes)

    Application.put_env(:ferricstore, :native_unauthenticated_max_frame_bytes, 64)

    on_exit(fn ->
      restore_env(:native_unauthenticated_max_frame_bytes, previous_limit)
    end)

    username = "preauth-chunks-#{System.unique_integer([:positive])}"
    assert :ok = Acl.set_user(username, ["on", ">secret", "+@all", "~*"])

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    body =
      Codec.encode_value(%{
        "message" => "PONG",
        "padding" => String.duplicate("x", 80)
      })

    assert byte_size(body) > 64
    split_at = div(byte_size(body), 2)
    <<first::binary-size(^split_at), second::binary>> = body
    assert byte_size(first) <= 64
    assert byte_size(second) <= 64

    assert :ok =
             :gen_tcp.send(
               socket,
               Codec.encode_frame(@ping_opcode, 1, 50, first, @more_chunks_flag)
             )

    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 25)
    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@ping_opcode, 1, 50, second))
    assert [{50, 6}] = receive_response_statuses(socket, 1)
  end

  @tag :preauth_frame_budget
  test "unauthenticated compressed frames cannot inflate past the logical frame budget" do
    previous_limit =
      Application.get_env(:ferricstore, :native_unauthenticated_max_frame_bytes)

    previous_compression =
      Application.get_env(:ferricstore, :native_request_compression_enabled)

    Application.put_env(:ferricstore, :native_unauthenticated_max_frame_bytes, 64)
    Application.put_env(:ferricstore, :native_request_compression_enabled, true)

    on_exit(fn ->
      restore_env(:native_unauthenticated_max_frame_bytes, previous_limit)
      restore_env(:native_request_compression_enabled, previous_compression)
    end)

    username = "preauth-compression-#{System.unique_integer([:positive])}"
    assert :ok = Acl.set_user(username, ["on", ">secret", "+@all", "~*"])

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    hello_body = Codec.encode_value(%{"compression" => "zlib"})

    assert :ok =
             :gen_tcp.send(
               socket,
               Codec.encode_frame(@hello_opcode, 0, 51, hello_body, @no_reply_flag)
             )

    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 25)

    body =
      Codec.encode_value(%{
        "message" => "PONG",
        "padding" => String.duplicate("x", 256)
      })

    compressed = :zlib.compress(body)
    assert byte_size(body) > 64
    assert byte_size(compressed) <= 64

    assert :ok =
             :gen_tcp.send(
               socket,
               Codec.encode_frame(@ping_opcode, 0, 52, compressed, @compressed_flag)
             )

    assert [{52, 6}] = receive_response_statuses(socket, 1)
  end

  test "accepts a multi-megabyte frame sent as 64 KiB socket chunks" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    frame = large_ping_frame(44)
    chunks = binary_chunks(frame, @socket_chunk_bytes)

    assert length(chunks) > 64
    Enum.each(chunks, fn chunk -> assert :ok = :gen_tcp.send(socket, chunk) end)
    assert receive_response_ids(socket, 1) == [44]
  end

  @tag :global_inbound_budget
  test "closes a one-byte fragment batch when metadata exhausts inbound budget" do
    raw_limit = 1_024
    frame_count = 32

    global_limit =
      Application.get_env(
        :ferricstore,
        :native_max_global_inbound_buffer_bytes,
        256 * 1024 * 1024
      )

    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).inbound_bytes == 0 end)

    usage = ResourceBudget.usage(ResourceBudget)
    reserve_bytes = global_limit - usage.inbound_bytes - raw_limit
    assert reserve_bytes > 0

    assert {:ok, reservation} =
             ResourceBudget.acquire(ResourceBudget, :inbound_bytes, self(), reserve_bytes)

    on_exit(fn -> ResourceBudget.release(ResourceBudget, reservation) end)
    reserved_usage = ResourceBudget.usage(ResourceBudget)

    batch =
      1..frame_count
      |> Enum.map(fn _ ->
        Codec.encode_frame(@ping_opcode, 1, 501, "x", @more_chunks_flag)
      end)
      |> IO.iodata_to_binary()

    assert byte_size(batch) < raw_limit
    assert frame_count * 64 > raw_limit

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok = :gen_tcp.send(socket, batch)
    assert_socket_closed(socket)

    assert eventually(fn ->
             usage = ResourceBudget.usage(ResourceBudget)

             usage.inbound_bytes == reserved_usage.inbound_bytes and
               usage.chunk_streams == reserved_usage.chunk_streams and
               usage.chunk_bytes == reserved_usage.chunk_bytes
           end)
  end

  @tag :queued_request_byte_budget
  test "tiny received fragments exhaust metadata admission before the raw payload budget" do
    budget = :"native_fragment_metadata_#{System.unique_integer([:positive])}"
    start_supervised!({ResourceBudget, name: budget, limits: %{inbound_bytes: 256}})
    listener = :"native_fragment_listener_#{System.unique_integer([:positive])}"

    start_supervised!(
      :ranch.child_spec(
        listener,
        :ranch_tcp,
        %{socket_opts: [port: 0], num_acceptors: 1},
        FerricstoreServer.Native.Connection,
        %{resource_budget: budget}
      )
    )

    port = :ranch.get_port(listener)

    {:ok, socket} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, nodelay: true], 1_000)

    on_exit(fn -> :gen_tcp.close(socket) end)
    frame = Codec.encode_frame(@ping_opcode, 0, 446, String.duplicate("x", 128))
    assert :ok = :gen_tcp.send(socket, binary_part(frame, 0, 24))
    assert eventually(fn -> ResourceBudget.usage(budget).inbound_bytes > 0 end)

    Enum.reduce_while(1..4, ResourceBudget.usage(budget).inbound_bytes, fn _, before ->
      case :gen_tcp.send(socket, "x") do
        :ok ->
          # Wait for each receive to be accounted before sending the next byte,
          # so the test models retained fragments rather than TCP coalescing.
          assert eventually(fn -> ResourceBudget.usage(budget).inbound_bytes != before end)
          after_bytes = ResourceBudget.usage(budget).inbound_bytes
          if after_bytes == 0, do: {:halt, 0}, else: {:cont, after_bytes}

        {:error, :closed} ->
          {:halt, 0}
      end
    end)

    assert_socket_closed(socket)
    assert eventually(fn -> ResourceBudget.usage(budget).inbound_bytes == 0 end)
    # The released capacity must remain usable for an ordinary complete frame.
    {:ok, healthy} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    on_exit(fn -> :gen_tcp.close(healthy) end)
    assert :ok = :gen_tcp.send(healthy, Codec.encode_frame(@ping_opcode, 0, 447, ""))
    assert receive_response_ids(healthy, 1) == [447]
  end

  @tag :queued_request_byte_budget
  test "decoded requests remain charged to the inbound byte budget while queued in a lane" do
    execution_limit =
      Application.get_env(
        :ferricstore,
        :native_max_global_executions,
        max(System.schedulers_online(), 1) * 8
      )

    assert {:ok, execution_token} =
             ResourceBudget.acquire(ResourceBudget, :executions, self(), execution_limit)

    on_exit(fn -> ResourceBudget.release(ResourceBudget, execution_token) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    body =
      Codec.encode_value(%{
        "key" => String.duplicate("q", @large_frame_body_bytes)
      })

    request = Codec.encode_frame(@get_opcode, 1, 441, body)
    assert :ok = :gen_tcp.send(socket, request)

    assert eventually(fn ->
             usage = ResourceBudget.usage(ResourceBudget)
             usage.lanes >= 1 and usage.inbound_bytes >= byte_size(request)
           end)

    assert :ok = ResourceBudget.release(ResourceBudget, execution_token)
    assert [{441, _status}] = receive_response_statuses(socket, 1)

    assert eventually(fn ->
             ResourceBudget.usage(ResourceBudget).inbound_bytes == 0
           end)
  end

  test "validates configured frame bodies against the wire buffer limit" do
    max_frame_body_bytes = @max_buffer_bytes - 24

    assert FrameBuffer.validate_max_frame_bytes!(max_frame_body_bytes) == max_frame_body_bytes
    assert FrameBuffer.validate_frame_body_bytes!(0) == 0
    assert FrameBuffer.validate_frame_body_bytes!(max_frame_body_bytes) == max_frame_body_bytes

    assert_raise ArgumentError, ~r/native frame body must be between 0 and/, fn ->
      FrameBuffer.validate_frame_body_bytes!(max_frame_body_bytes + 1)
    end

    assert_raise ArgumentError, ~r/native_max_frame_bytes must be an integer between 1 and/, fn ->
      FrameBuffer.validate_max_frame_bytes!(max_frame_body_bytes + 1)
    end

    assert_raise ArgumentError, ~r/native_max_frame_bytes must be an integer between 1 and/, fn ->
      FrameBuffer.validate_max_frame_bytes!(4_294_967_296)
    end

    assert_raise ArgumentError, ~r/native_max_frame_bytes must be an integer between 1 and/, fn ->
      Codec.decode_frames("", 4_294_967_296)
    end
  end

  test "outbound responses never exceed the connection frame limit" do
    max_frame_bytes = 64
    value = String.duplicate("x", max_frame_bytes * 3)

    for configured_chunk_bytes <- [0, max_frame_bytes * 10] do
      state = %{
        compression: :none,
        compact_flow_responses: false,
        max_frame_bytes: max_frame_bytes,
        response_chunk_bytes: configured_chunk_bytes
      }

      frames = Responses.encode_response(state, @ping_opcode, 1, 99, :ok, value)

      assert length(frames) > 1

      bodies =
        for <<"FSNP", 0x81, _flags, _lane_id::unsigned-32, _opcode::unsigned-16,
              _request_id::unsigned-64, body_len::unsigned-32, body::binary>> <- frames do
          assert body_len == byte_size(body)
          assert body_len <= max_frame_bytes
          body
        end

      <<0::unsigned-16, value_body::binary>> = IO.iodata_to_binary(bodies)
      assert {:ok, ^value} = Codec.decode_body(value_body)
    end
  end

  test "compact MGET responses use the connection frame limit when chunking is disabled" do
    max_frame_bytes = 64

    state = %{
      compression: :none,
      compact_flow_responses: false,
      max_frame_bytes: max_frame_bytes,
      response_chunk_bytes: 0
    }

    frames =
      Responses.encode_response(
        state,
        0x0104,
        1,
        100,
        :ok,
        List.duplicate(String.duplicate("x", 32), 8)
      )

    assert length(frames) > 1

    for encoded <- frames do
      frame = IO.iodata_to_binary(encoded)

      assert <<"FSNP", 0x81, _flags, _lane_id::unsigned-32, _opcode::unsigned-16,
               _request_id::unsigned-64, body_len::unsigned-32, body::binary>> = frame

      assert body_len == byte_size(body)
      assert body_len <= max_frame_bytes
    end
  end

  test "large response encoders run on dirty CPU schedulers" do
    source_path =
      Path.expand("../../../native/native_protocol_nif/src/lib.rs", __DIR__)

    source = File.read!(source_path)

    for function <- [
          "encode_frame",
          "encode_compact_claim_jobs_response_frame",
          "encode_compact_ok_list_response_frame",
          "encode_compact_kv_get_response_frame",
          "encode_compact_kv_mget_response_frame",
          "encode_compact_kv_mget"
        ] do
      assert source =~
               ~r/#\[rustler::nif\(schedule = "DirtyCpu"\)\]\s+fn #{function}\b/,
             "expected #{function} to stay off normal BEAM schedulers"
    end
  end

  defp connect do
    {:ok, socket} =
      :gen_tcp.connect(
        {127, 0, 0, 1},
        Listener.port(),
        [:binary, active: false, packet: :raw],
        @receive_timeout
      )

    socket
  end

  defp connection_pids do
    ConnRegistry.snapshot(10_000).clients
    |> MapSet.new(& &1.pid)
  end

  defp wait_for_new_connection(existing_pids, attempts \\ 100)

  defp wait_for_new_connection(_existing_pids, 0),
    do: flunk("native connection did not register")

  defp wait_for_new_connection(existing_pids, attempts) do
    case Enum.find(connection_pids(), &(not MapSet.member?(existing_pids, &1))) do
      nil ->
        Process.sleep(10)
        wait_for_new_connection(existing_pids, attempts - 1)

      pid ->
        pid
    end
  end

  defp large_ping_frame(request_id) do
    body =
      Codec.encode_value(%{
        "message" => "PONG",
        "padding" => String.duplicate("x", @large_frame_body_bytes)
      })

    Codec.encode_frame(@ping_opcode, 0, request_id, body)
  end

  defp command_exec_frame(request_id, command, args, flags \\ 0) do
    body = Codec.encode_value(%{"command" => command, "args" => args})
    Codec.encode_frame(@command_exec_opcode, 0, request_id, body, flags)
  end

  defp command_exec_frame_on_lane(lane_id, request_id, command, args, flags \\ 0) do
    body = Codec.encode_value(%{"command" => command, "args" => args})
    Codec.encode_frame(@command_exec_opcode, lane_id, request_id, body, flags)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp disable_call_trace(pid) do
    :erlang.trace(pid, false, [:call])
  rescue
    ArgumentError -> false
  end

  defp eventually(fun, attempts \\ 100)

  defp eventually(fun, attempts) do
    cond do
      fun.() ->
        true

      attempts == 0 ->
        false

      true ->
        Process.sleep(10)
        eventually(fun, attempts - 1)
    end
  end

  defp wait_for_lane(connection_pid, lane_id, attempts \\ 100)

  defp wait_for_lane(_connection_pid, _lane_id, 0),
    do: flunk("native lane did not start")

  defp wait_for_lane(connection_pid, lane_id, attempts) do
    case Process.info(connection_pid, :dictionary) do
      {:dictionary, dictionary} ->
        case Keyword.get(dictionary, :native_connection_cleanup_state) do
          %{lanes: lanes} when is_map(lanes) ->
            case Map.get(lanes, lane_id) do
              pid when is_pid(pid) -> pid
              _missing -> retry_wait_for_lane(connection_pid, lane_id, attempts)
            end

          _missing ->
            retry_wait_for_lane(connection_pid, lane_id, attempts)
        end

      _missing ->
        retry_wait_for_lane(connection_pid, lane_id, attempts)
    end
  end

  defp retry_wait_for_lane(connection_pid, lane_id, attempts) do
    Process.sleep(10)
    wait_for_lane(connection_pid, lane_id, attempts - 1)
  end

  defp assert_socket_closed(socket, attempts \\ 20)

  defp assert_socket_closed(_socket, 0), do: flunk("native connection remained open")

  defp assert_socket_closed(socket, attempts) do
    case :gen_tcp.recv(socket, 0, 25) do
      {:error, :closed} -> :ok
      {:error, :timeout} -> assert_socket_closed(socket, attempts - 1)
      {:ok, _data} -> assert_socket_closed(socket, attempts - 1)
    end
  end

  defp assert_socket_closed_without_more_data(socket, attempts \\ 20)

  defp assert_socket_closed_without_more_data(_socket, 0),
    do: flunk("native connection remained open after the ACL barrier")

  defp assert_socket_closed_without_more_data(socket, attempts) do
    case :gen_tcp.recv(socket, 0, 25) do
      {:error, :closed} -> :ok
      {:error, :timeout} -> assert_socket_closed_without_more_data(socket, attempts - 1)
      {:ok, data} -> flunk("received #{byte_size(data)} bytes after the ACL barrier")
    end
  end

  defp binary_chunks(binary, chunk_bytes) do
    full_chunks = for <<chunk::binary-size(^chunk_bytes) <- binary>>, do: chunk
    consumed = length(full_chunks) * chunk_bytes

    case binary_part(binary, consumed, byte_size(binary) - consumed) do
      "" -> full_chunks
      remainder -> full_chunks ++ [remainder]
    end
  end

  defp receive_response_ids(socket, expected_count) do
    receive_response_ids(socket, expected_count, "", [])
  end

  defp receive_response_ids(_socket, expected_count, _buffer, ids)
       when length(ids) >= expected_count,
       do: Enum.reverse(ids)

  defp receive_response_ids(socket, expected_count, buffer, ids) do
    {decoded_ids, rest} = decode_response_ids(buffer, [])
    ids = decoded_ids ++ ids

    if length(ids) >= expected_count do
      Enum.reverse(ids)
    else
      assert {:ok, data} = :gen_tcp.recv(socket, 0, @receive_timeout)
      receive_response_ids(socket, expected_count, rest <> data, ids)
    end
  end

  defp decode_response_ids(
         <<"FSNP", 0x81, _flags, 0::unsigned-32, @ping_opcode::unsigned-16,
           request_id::unsigned-64, body_len::unsigned-32, body_and_rest::binary>> = buffer,
         ids
       ) do
    if byte_size(body_and_rest) >= body_len do
      <<_body::binary-size(^body_len), rest::binary>> = body_and_rest
      decode_response_ids(rest, [request_id | ids])
    else
      {ids, buffer}
    end
  end

  defp decode_response_ids(buffer, ids), do: {ids, buffer}

  defp receive_response_statuses(socket, expected_count) do
    receive_response_statuses(socket, expected_count, "", [])
  end

  defp receive_response_statuses(_socket, expected_count, _buffer, responses)
       when length(responses) >= expected_count,
       do: Enum.reverse(responses)

  defp receive_response_statuses(socket, expected_count, buffer, responses) do
    {decoded, rest} = decode_response_statuses(buffer, [])
    responses = decoded ++ responses

    if length(responses) >= expected_count do
      Enum.reverse(responses)
    else
      assert {:ok, data} = :gen_tcp.recv(socket, 0, @receive_timeout)
      receive_response_statuses(socket, expected_count, rest <> data, responses)
    end
  end

  defp decode_response_statuses(
         <<"FSNP", 0x81, flags, _lane_id::unsigned-32, _opcode::unsigned-16,
           request_id::unsigned-64, body_len::unsigned-32, body_and_rest::binary>> = buffer,
         responses
       ) do
    if byte_size(body_and_rest) >= body_len do
      <<body::binary-size(^body_len), rest::binary>> = body_and_rest
      body = if Bitwise.band(flags, @compressed_flag) != 0, do: :zlib.uncompress(body), else: body
      <<status::unsigned-16, _value::binary>> = body
      decode_response_statuses(rest, [{request_id, status} | responses])
    else
      {responses, buffer}
    end
  end

  defp decode_response_statuses(buffer, responses), do: {responses, buffer}

  defp receive_native_frames(socket, expected_count),
    do: receive_native_frames(socket, expected_count, "", [])

  defp receive_native_frames(_socket, expected_count, _buffer, frames)
       when length(frames) >= expected_count,
       do: Enum.reverse(frames)

  defp receive_native_frames(socket, expected_count, buffer, frames) do
    {decoded, rest} = decode_native_frames(buffer, [])
    frames = decoded ++ frames

    if length(frames) >= expected_count do
      Enum.reverse(frames)
    else
      assert {:ok, data} = :gen_tcp.recv(socket, 0, @receive_timeout)
      receive_native_frames(socket, expected_count, rest <> data, frames)
    end
  end

  defp decode_native_frames(
         <<"FSNP", 0x81, _flags, _lane_id::unsigned-32, _opcode::unsigned-16,
           _request_id::unsigned-64, body_len::unsigned-32, body_and_rest::binary>> = buffer,
         frames
       ) do
    if byte_size(body_and_rest) >= body_len do
      <<body::binary-size(^body_len), rest::binary>> = body_and_rest
      decode_native_frames(rest, [body | frames])
    else
      {frames, buffer}
    end
  end

  defp decode_native_frames(buffer, frames), do: {frames, buffer}
end
