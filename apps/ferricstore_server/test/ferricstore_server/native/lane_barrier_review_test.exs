defmodule FerricstoreServer.Native.LaneBarrierReviewTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Native.{Codec, Connection, Lane, Listener}
  alias FerricstoreServer.Native.Connection.FrameBuffer

  @command_exec_opcode 0x0100
  @ping_opcode 0x0003
  @client_set_name_opcode 0x0004
  @window_update_opcode 0x000D
  @subscribe_events_opcode 0x0011
  @event_opcode 0x0010

  setup do
    Acl.reset!()
    previous_timeout = Application.get_env(:ferricstore, :native_lane_barrier_timeout_ms)

    on_exit(fn ->
      Acl.reset!()
      restore_env(:native_lane_barrier_timeout_ms, previous_timeout)
    end)

    suffix = System.unique_integer([:positive, :monotonic])
    keys = Enum.map(1..3, &"native:barrier-review:#{suffix}:#{&1}")
    on_exit(fn -> Enum.each(keys, &FerricStore.del/1) end)
    %{keys: keys}
  end

  test "a control frame does not wait for a blocking command on a lane with an actor", %{
    keys: [list, key, _]
  } do
    Application.put_env(:ferricstore, :native_lane_barrier_timeout_ms, 300)
    assert :ok = FerricStore.set(key, "value")
    socket = connect()

    # GET starts the lane-1 actor; BLPOP then blocks indefinitely on lane 1.
    send_frames(socket, [command(1, 1, "GET", [key]), command(1, 2, "BLPOP", [list, "0"])])
    assert response(socket) == {1, 1, 0}

    send_frames(socket, [set_name(0, 3)])
    assert response(socket) == {0, 3, 0}

    send_frames(socket, [command(2, 4, "GET", [key])])
    assert response(socket) == {2, 4, 0}
  end

  test "a blocking response cannot overtake an earlier session response on its lane", %{
    keys: [list, key, _]
  } do
    assert :ok = FerricStore.set(key, "value")
    assert {:ok, 1} = FerricStore.rpush(list, ["item"])
    socket = connect()

    send_frames(socket, [
      command(1, 10, "GET", [key]),
      command(1, 11, "WATCH", [key]),
      command(1, 12, "BLPOP", [list, "5"]),
      set_name(0, 13)
    ])

    responses = Enum.map(1..4, fn _ -> response(socket) end)
    assert lane_ids(responses, 1) == [10, 11, 12]
    assert lane_ids(responses, 0) == [13]
  end

  test "a barrier does not read the socket while decode backpressure paused input", %{
    keys: [_, key, _]
  } do
    assert :ok = FerricStore.set(key, "value")
    existing = connection_pids()
    socket = connect()
    connection_pid = wait_for_new_connection(existing)

    :erlang.trace_pattern({Connection, :activate_input, 1}, true, [:local])
    :erlang.trace_pattern({Connection, :pause_input, 1}, true, [:local])
    :erlang.trace(connection_pid, true, [:call])

    on_exit(fn ->
      :erlang.trace_pattern({Connection, :activate_input, 1}, false, [:local])
      :erlang.trace_pattern({Connection, :pause_input, 1}, false, [:local])
    end)

    # More than one native decode batch (128 frames), with a session barrier in
    # the first batch so the barrier runs while decode input is paused.
    fillers = for id <- 100..239, do: command(2, id, "GET", [key])
    send_frames(socket, [command(1, 1, "GET", [key]), set_name(0, 2) | fillers])
    Enum.each(1..142, fn _ -> response(socket) end)
    disable_call_trace(connection_pid)

    calls = collect_calls(connection_pid)
    assert Enum.any?(calls, &match?({:pause_input, _}, &1))

    paused_activations =
      Enum.filter(calls, fn
        {:activate_input, %{decode_paused: true}} -> true
        _other -> false
      end)

    assert paused_activations == []
  end

  test "a control barrier across lanes is bounded by one barrier timeout" do
    Application.put_env(:ferricstore, :native_lane_barrier_timeout_ms, 1_500)
    socket = connect()

    send_frames(socket, [
      command(1, 20, "DEBUG", ["SLEEP", "1"]),
      command(2, 21, "DEBUG", ["SLEEP", "2"])
    ])

    started = System.monotonic_time(:millisecond)
    send_frames(socket, [set_name(0, 22)])
    drain_until_ping_or_close(socket, 22)
    elapsed = System.monotonic_time(:millisecond) - started

    assert elapsed < 1_900,
           "barrier waited #{elapsed}ms although native_lane_barrier_timeout_ms is 1500"
  end

  test "a control frame skips barrier round trips for idle lanes", %{keys: [_, key, _]} do
    assert :ok = FerricStore.set(key, "value")
    existing = connection_pids()
    socket = connect()
    connection_pid = wait_for_new_connection(existing)

    send_frames(socket, for(lane <- 1..4, do: command(lane, lane, "GET", [key])))
    Enum.each(1..4, fn _ -> response(socket) end)

    :erlang.trace_pattern({Lane, :barrier, 1}, true, [:global])
    :erlang.trace(connection_pid, true, [:call])
    on_exit(fn -> :erlang.trace_pattern({Lane, :barrier, 1}, false, [:global]) end)

    send_frames(socket, [set_name(0, 30)])
    assert response(socket) == {0, 30, 0}
    disable_call_trace(connection_pid)

    assert Enum.count(collect_calls(connection_pid), &match?({:barrier, _}, &1)) == 0
  end

  test "an ACL invalidation during a barrier still emits AUTH_INVALIDATED" do
    existing = connection_pids()
    socket = connect()
    connection_pid = wait_for_new_connection(existing)

    send_frames(socket, [
      Codec.encode_frame(
        @subscribe_events_opcode,
        0,
        40,
        Codec.encode_value(%{"events" => ["AUTH_INVALIDATED"]})
      )
    ])

    assert response(socket) == {0, 40, 0}

    send_frames(socket, [
      command(1, 41, "DEBUG", ["SLEEP", "1"]),
      set_name(0, 42)
    ])

    Process.sleep(200)
    send(connection_pid, {:acl_invalidate, :all, 1})

    frames = receive_until_closed(socket)
    assert Enum.any?(frames, &match?({:event, "AUTH_INVALIDATED"}, &1)), inspect(frames)
  end

  test "a blocking command behind a busy lane does not stall another lane", %{
    keys: [list, key, _]
  } do
    assert :ok = FerricStore.set(key, "value")
    assert {:ok, 1} = FerricStore.rpush(list, ["item"])
    socket = connect()

    send_frames(socket, [
      command(1, 50, "DEBUG", ["SLEEP", "1"]),
      command(1, 51, "BLPOP", [list, "5"]),
      command(2, 52, "GET", [key])
    ])

    started = System.monotonic_time(:millisecond)
    first = response(socket)
    elapsed = System.monotonic_time(:millisecond) - started

    assert first == {2, 52, 0}
    assert elapsed < 500, "lane 2 waited #{elapsed}ms behind a lane-1 barrier"
  end

  test "a deferred blocking command keeps lane order around it", %{keys: [list, key, _]} do
    assert :ok = FerricStore.set(key, "value")
    assert {:ok, 1} = FerricStore.rpush(list, ["item"])
    socket = connect()

    send_frames(socket, [
      command(1, 70, "DEBUG", ["SLEEP", "1"]),
      command(1, 71, "BLPOP", [list, "5"]),
      command(1, 72, "GET", [key]),
      command(2, 73, "GET", [key])
    ])

    responses = Enum.map(1..4, fn _ -> response(socket) end)
    assert hd(responses) == {2, 73, 0}
    assert lane_ids(responses, 1) == [70, 71, 72]
    assert Enum.all?(responses, fn {_lane, _id, status} -> status == 0 end)
  end

  test "MULTI/EXEC behind a slow command still executes in lane order", %{
    keys: [_, key, _]
  } do
    socket = connect()

    send_frames(socket, [
      command(1, 80, "DEBUG", ["SLEEP", "1"]),
      command(1, 81, "MULTI", []),
      command(1, 82, "SET", [key, "from-multi"]),
      command(1, 83, "EXEC", [])
    ])

    responses = Enum.map(1..4, fn _ -> response(socket) end)
    assert lane_ids(responses, 1) == [80, 81, 82, 83]
    assert Enum.all?(responses, fn {_lane, _id, status} -> status == 0 end)
    assert {:ok, "from-multi"} = FerricStore.get(key)
  end

  test "a lane busy rejection stays ordered without stalling other lanes", %{
    keys: [_, key, _]
  } do
    # Each frame fits a lane alone, but not behind the queued DEBUG SLEEP, so
    # the next lane-1 frame is rejected with an ordered busy reply.
    sleep_body = Codec.encode_value(%{"command" => "DEBUG", "args" => ["SLEEP", "1"]})
    get_body = Codec.encode_value(%{"command" => "GET", "args" => [key]})

    lane_cap =
      max(
        FrameBuffer.retained_frame_bytes(byte_size(sleep_body)),
        FrameBuffer.retained_frame_bytes(byte_size(get_body))
      )

    previous = Application.get_env(:ferricstore, :native_max_queued_request_bytes_per_lane)
    Application.put_env(:ferricstore, :native_max_queued_request_bytes_per_lane, lane_cap)
    on_exit(fn -> restore_env(:native_max_queued_request_bytes_per_lane, previous) end)
    assert :ok = FerricStore.set(key, "value")
    socket = connect()

    send_frames(socket, [
      command(1, 60, "DEBUG", ["SLEEP", "1"]),
      command(1, 61, "GET", [key]),
      command(2, 62, "GET", [key])
    ])

    started = System.monotonic_time(:millisecond)
    first = response(socket)
    elapsed = System.monotonic_time(:millisecond) - started
    rest = Enum.map(1..2, fn _ -> response(socket) end)

    assert first == {2, 62, 0}
    assert elapsed < 500, "lane 2 waited #{elapsed}ms behind a lane-1 busy rejection"
    assert [{1, 60, 0}, {1, 61, busy}] = rest
    assert busy != 0
  end

  test "pipelined frames on a busy lane classify each command once", %{keys: [_, key, _]} do
    assert :ok = FerricStore.set(key, "value")
    existing = connection_pids()
    socket = connect()
    connection_pid = wait_for_new_connection(existing)

    :erlang.trace_pattern({Codec, :peek_command_name, 2}, true, [:global])
    :erlang.trace(connection_pid, true, [:call])
    on_exit(fn -> :erlang.trace_pattern({Codec, :peek_command_name, 2}, false, [:global]) end)

    send_frames(socket, for(id <- 1..20, do: command(1, id, "GET", [key])))
    Enum.each(1..20, fn _ -> response(socket) end)
    disable_call_trace(connection_pid)

    peeks = Enum.count(collect_calls(connection_pid), &match?({:peek_command_name, _}, &1))
    assert peeks <= 20, "classified 20 frames with #{peeks} command-name peeks"
  end

  test "a barrier stops reading once a complete frame is buffered", %{keys: [_, key, _]} do
    assert :ok = FerricStore.set(key, "value")
    existing = connection_pids()
    socket = connect()
    connection_pid = wait_for_new_connection(existing)

    send_frames(socket, [command(1, 90, "DEBUG", ["SLEEP", "1"]), set_name(0, 91)])
    Process.sleep(100)

    :erlang.trace_pattern({Connection, :continue_lane_barrier_data, 6}, true, [:local])
    :erlang.trace(connection_pid, true, [:call])

    on_exit(fn ->
      :erlang.trace_pattern({Connection, :continue_lane_barrier_data, 6}, false, [:local])
    end)

    for id <- 92..94 do
      send_frames(socket, [command(2, id, "GET", [key])])
      Process.sleep(50)
    end

    responses = Enum.map(1..5, fn _ -> response(socket) end)
    disable_call_trace(connection_pid)

    assert Enum.sort(lane_ids(responses, 2)) == [92, 93, 94]
    barrier_reads = Enum.count(collect_calls(connection_pid))
    assert barrier_reads == 1, "barrier read #{barrier_reads} packets after a frame was ready"
  end

  test "heartbeat and flow-control frames answer while a data lane is busy" do
    socket = connect()

    send_frames(socket, [
      command(1, 100, "DEBUG", ["SLEEP", "1"]),
      ping(0, 101),
      Codec.encode_frame(
        @window_update_opcode,
        0,
        102,
        Codec.encode_value(%{"max_inflight_per_lane" => 64})
      )
    ])

    started = System.monotonic_time(:millisecond)
    first_two = Enum.map(1..2, fn _ -> response(socket) end)
    elapsed = System.monotonic_time(:millisecond) - started

    assert Enum.sort(first_two) == [{0, 101, 0}, {0, 102, 0}]
    assert elapsed < 500, "control replies waited #{elapsed}ms behind a busy lane"
    assert response(socket) == {1, 100, 0}
  end

  test "deferred lanes are not rescanned for unrelated traffic", %{keys: [list, key, _]} do
    assert :ok = FerricStore.set(key, "value")
    existing = connection_pids()
    socket = connect()
    connection_pid = wait_for_new_connection(existing)

    # Lane 1 parks a GET behind a BLPOP that never completes during the test.
    send_frames(socket, [command(1, 1, "BLPOP", [list, "0"]), command(1, 2, "GET", [key])])
    Process.sleep(100)

    :erlang.trace_pattern({Connection, :drain_deferred_frames, 2}, true, [:local])
    :erlang.trace(connection_pid, true, [:call])

    on_exit(fn ->
      :erlang.trace_pattern({Connection, :drain_deferred_frames, 2}, false, [:local])
    end)

    for id <- 10..49 do
      send_frames(socket, [command(2, id, "GET", [key])])
      assert response(socket) == {2, id, 0}
    end

    disable_call_trace(connection_pid)
    drains = Enum.count(collect_calls(connection_pid))
    assert drains <= 2, "rescanned the parked lane #{drains} times for unrelated traffic"
  end

  test "a MULTI behind a busy lane still captures later frames from other lanes", %{
    keys: [_, key, _]
  } do
    socket = connect()

    send_frames(socket, [
      command(1, 110, "DEBUG", ["SLEEP", "1"]),
      command(1, 111, "MULTI", []),
      command(2, 112, "SET", [key, "in-transaction"]),
      command(1, 113, "EXEC", [])
    ])

    replies = Map.new(1..4, fn _ -> response_value(socket) end)
    assert {0, "QUEUED"} = replies[112]
    assert {0, [_]} = replies[113]
    assert {:ok, "in-transaction"} = FerricStore.get(key)
  end

  test "a WATCH behind a busy lane still sees later writes from other lanes", %{
    keys: [_, key, _]
  } do
    assert :ok = FerricStore.set(key, "original")
    socket = connect()

    send_frames(socket, [
      command(1, 120, "DEBUG", ["SLEEP", "1"]),
      command(1, 121, "WATCH", [key]),
      command(2, 122, "SET", [key, "concurrent"])
    ])

    replies = Map.new(1..3, fn _ -> response_value(socket) end)
    assert {0, _} = replies[122]

    send_frames(socket, [
      command(1, 123, "MULTI", []),
      command(1, 124, "SET", [key, "mine"]),
      command(1, 125, "EXEC", [])
    ])

    replies = Map.new(1..3, fn _ -> response_value(socket) end)
    assert {0, nil} = replies[125], "EXEC should abort: the watched key changed after WATCH"
    assert {:ok, "concurrent"} = FerricStore.get(key)
  end

  test "MULTI is refused while a blocking command is pending on the connection", %{
    keys: [list, key, _]
  } do
    socket = connect()
    pusher = connect()

    # Lane 1 parks a SET (sent before MULTI) behind a BLPOP.
    send_frames(socket, [
      command(1, 130, "BLPOP", [list, "5"]),
      command(1, 131, "SET", [key, "before-multi"]),
      command(2, 132, "MULTI", []),
      command(2, 133, "EXEC", [])
    ])

    early = Map.new(1..2, fn _ -> response_value(socket) end)
    assert {status, _reason} = early[132]
    assert status != 0, "MULTI must not open while lane work is parked"
    assert {exec_status, _} = early[133]
    assert exec_status != 0

    send_frames(pusher, [command(1, 134, "RPUSH", [list, "item"])])
    late = Map.new(1..2, fn _ -> response_value(socket) end)
    assert {0, _} = late[130]
    assert {0, "OK"} = late[131]
    assert {:ok, "before-multi"} = FerricStore.get(key)
  end

  test "a transaction command behind a blocked lane gets an ordered error", %{
    keys: [list, key, _]
  } do
    assert :ok = FerricStore.set(key, "original")
    socket = connect()
    pusher = connect()

    send_frames(socket, [
      command(1, 140, "BLPOP", [list, "5"]),
      command(1, 141, "WATCH", [key]),
      command(2, 142, "GET", [key])
    ])

    # Lane 2 is unaffected; WATCH's reply stays behind BLPOP on lane 1.
    assert {142, {0, "original"}} = response_value(socket)
    send_frames(pusher, [command(1, 143, "RPUSH", [list, "item"])])
    assert {140, {0, _}} = response_value(socket)
    assert {141, {status, _reason}} = response_value(socket)
    assert status != 0, "WATCH behind a blocked lane must not take effect late"
  end

  test "a queued command that would be parked aborts the transaction", %{keys: [_, key, _]} do
    socket = connect()

    bad_flags_frame =
      Codec.encode_frame(
        @command_exec_opcode,
        1,
        151,
        Codec.encode_value(%{"command" => "GET", "args" => [key]}),
        0x40
      )

    send_frames(socket, [
      command(1, 150, "DEBUG", ["SLEEP", "1"]),
      command(2, 152, "MULTI", []),
      bad_flags_frame,
      command(1, 153, "SET", [key, "parked-in-multi"]),
      command(2, 154, "EXEC", [])
    ])

    replies = Map.new(1..5, fn _ -> response_value(socket) end)
    assert {0, "OK"} = replies[152]
    assert {status, _} = replies[154]
    assert status != 0, "EXEC must abort when a queued command could not join it"
    assert {set_status, _} = replies[153]
    assert set_status != 0
    assert {:ok, nil} = FerricStore.get(key)
  end

  test "the barrier timeout is fixed when the connection starts" do
    Application.put_env(:ferricstore, :native_lane_barrier_timeout_ms, 300)
    existing = connection_pids()
    socket = connect()
    wait_for_new_connection(existing)
    # Later env changes apply to new connections, not this one.
    Application.put_env(:ferricstore, :native_lane_barrier_timeout_ms, 60_000)

    send_frames(socket, [command(1, 160, "DEBUG", ["SLEEP", "1"]), set_name(0, 161)])
    started = System.monotonic_time(:millisecond)
    assert :closed = drain_until_ping_or_close(socket, 161)
    assert System.monotonic_time(:millisecond) - started < 900
  end

  test "per-frame dispatch cost does not grow with blocked requests on other lanes", %{
    keys: [list, key, _]
  } do
    assert :ok = FerricStore.set(key, "value")
    baseline = dispatch_reductions_per_frame(key, fn _socket -> :ok end)

    loaded =
      dispatch_reductions_per_frame(key, fn socket ->
        send_frames(socket, for(lane <- 1..500, do: command(lane, lane, "BLPOP", [list, "0"])))
        assert eventually(fn -> Ferricstore.Waiters.count(list) >= 500 end)
      end)

    assert loaded - baseline < 100,
           "500 blocked requests added #{loaded - baseline} reductions per unrelated frame"
  end

  defp dispatch_reductions_per_frame(key, setup) do
    existing = connection_pids()
    socket = connect()
    connection_pid = wait_for_new_connection(existing)
    setup.(socket)
    frames = 200
    {:reductions, before} = Process.info(connection_pid, :reductions)
    send_frames(socket, for(id <- 1..frames, do: command(900, 1_000 + id, "GET", [key])))
    Enum.each(1..frames, fn _ -> response(socket) end)
    {:reductions, after_count} = Process.info(connection_pid, :reductions)
    :gen_tcp.close(socket)
    div(after_count - before, frames)
  end

  defp eventually(fun, attempts \\ 200) do
    cond do
      fun.() -> true
      attempts == 0 -> false
      true -> Process.sleep(10) && eventually(fun, attempts - 1)
    end
  end

  test "pubsub messages are delivered while a barrier waits", %{keys: [list, _, _]} do
    channel = list <> ":channel"
    socket = connect()
    publisher = connect()

    send_frames(socket, [command(2, 170, "SUBSCRIBE", [channel])])
    assert {170, {0, _}} = response_value(socket)

    # SUBSCRIBE on busy lane 1 waits on that lane's barrier for about 1s.
    send_frames(socket, [
      command(1, 171, "DEBUG", ["SLEEP", "1"]),
      command(1, 172, "SUBSCRIBE", [channel <> ":other"])
    ])

    Process.sleep(150)
    started = System.monotonic_time(:millisecond)
    send_frames(publisher, [command(1, 173, "PUBLISH", [channel, "during-barrier"])])

    assert {:ok, _frame} = read_until_body_contains(socket, "during-barrier")
    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed < 400, "pubsub waited #{elapsed}ms behind a lane barrier"
  end

  test "pubsub during a barrier stops at an ACL invalidation in arrival order" do
    existing = connection_pids()
    socket = connect()
    connection_pid = wait_for_new_connection(existing)

    send_frames(socket, [command(1, 180, "DEBUG", ["SLEEP", "1"]), set_name(0, 181)])
    Process.sleep(150)
    send(connection_pid, {:pubsub_message, "ordered", "before-acl"})
    send(connection_pid, {:acl_invalidate, :all, 1})
    send(connection_pid, {:pubsub_message, "ordered", "after-acl"})

    assert {:ok, _} = read_until_body_contains(socket, "before-acl")
    assert {:error, :closed} = read_until_body_contains(socket, "after-acl")
  end

  defp read_until_body_contains(socket, needle) do
    case read_frame(socket, 3_000) do
      {:ok, {_lane, _opcode, _id, body} = frame} ->
        if :binary.match(body, needle) != :nomatch,
          do: {:ok, frame},
          else: read_until_body_contains(socket, needle)

      error ->
        error
    end
  end

  defp connect do
    {:ok, socket} =
      :gen_tcp.connect({127, 0, 0, 1}, Listener.port(), [:binary, active: false], 2_000)

    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp send_frames(socket, frames), do: assert(:ok = :gen_tcp.send(socket, frames))

  defp command(lane, id, command, args) do
    Codec.encode_frame(
      @command_exec_opcode,
      lane,
      id,
      Codec.encode_value(%{"command" => command, "args" => args})
    )
  end

  defp ping(lane, id), do: Codec.encode_frame(@ping_opcode, lane, id, "")

  # CLIENT.SETNAME changes connection state, so it stays an ordered control frame.
  defp set_name(lane, id),
    do:
      Codec.encode_frame(
        @client_set_name_opcode,
        lane,
        id,
        Codec.encode_value(%{"name" => "barrier-#{id}"})
      )

  defp response(socket), do: socket |> read_frame(3_000) |> summarize()

  defp response_value(socket) do
    assert {:ok, {_lane, _opcode, id, <<status::unsigned-16, body::binary>>}} =
             read_frame(socket, 3_000)

    value =
      case Codec.decode_body(body) do
        {:ok, value} -> value
        {:error, _reason} -> body
      end

    {id, {status, value}}
  end

  defp read_frame(socket, timeout) do
    with {:ok,
          <<"FSNP", 0x81, flags, lane::unsigned-32, opcode::unsigned-16, id::unsigned-64,
            size::unsigned-32>>} <- :gen_tcp.recv(socket, 24, timeout),
         {:ok, body} <- recv_body(socket, size, timeout) do
      body = if Bitwise.band(flags, 0x08) != 0, do: :zlib.uncompress(body), else: body
      {:ok, {lane, opcode, id, body}}
    end
  end

  defp recv_body(_socket, 0, _timeout), do: {:ok, ""}
  defp recv_body(socket, size, timeout), do: :gen_tcp.recv(socket, size, timeout)

  defp summarize({:ok, {lane, _opcode, id, <<status::unsigned-16, _::binary>>}}),
    do: {lane, id, status}

  defp summarize(other), do: flunk("expected a native response, got #{inspect(other)}")

  defp lane_ids(responses, lane),
    do: for({^lane, id, _status} <- responses, do: id)

  defp drain_until_ping_or_close(socket, ping_id) do
    case read_frame(socket, 5_000) do
      {:ok, {0, _opcode, ^ping_id, _body}} -> :ping
      {:ok, _other} -> drain_until_ping_or_close(socket, ping_id)
      {:error, _closed} -> :closed
    end
  end

  defp receive_until_closed(socket, acc \\ []) do
    case read_frame(socket, 5_000) do
      {:ok, {_lane, @event_opcode, _id, <<_status::unsigned-16, body::binary>>}} ->
        {:ok, %{"event" => event}} = Codec.decode_body(body)
        receive_until_closed(socket, [{:event, event} | acc])

      {:ok, {lane, _opcode, id, _body}} ->
        receive_until_closed(socket, [{lane, id} | acc])

      {:error, _closed} ->
        Enum.reverse(acc)
    end
  end

  defp collect_calls(pid, acc \\ []) do
    receive do
      {:trace, ^pid, :call, {_module, function, [arg | _]}} ->
        collect_calls(pid, [{function, arg} | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end

  defp connection_pids, do: ConnRegistry.snapshot(10_000).clients |> MapSet.new(& &1.pid)

  defp wait_for_new_connection(existing, attempts \\ 100)
  defp wait_for_new_connection(_existing, 0), do: flunk("native connection did not register")

  defp wait_for_new_connection(existing, attempts) do
    case Enum.find(connection_pids(), &(not MapSet.member?(existing, &1))) do
      nil ->
        Process.sleep(10)
        wait_for_new_connection(existing, attempts - 1)

      pid ->
        pid
    end
  end

  defp disable_call_trace(pid) do
    :erlang.trace(pid, false, [:call])
  rescue
    ArgumentError -> false
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
