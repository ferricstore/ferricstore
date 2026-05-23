Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

defmodule PubSubBench do
  @moduledoc false

  alias Ferricstore.PubSub
  alias FerricstoreServer.Resp.Encoder

  def run do
    stop_started_apps()

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-pubsub-bench-#{System.unique_integer([:positive])}"
      )

    configure_app(data_dir)
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)

    port = FerricstoreServer.Listener.port()

    IO.puts("FerricStore Pub/Sub benchmark")
    IO.puts("port=#{port}")
    IO.puts("data_dir=#{data_dir}")
    IO.puts("core_total=#{core_total()} tcp_total=#{tcp_total()} tcp_pipeline=#{tcp_pipeline()}")
    IO.puts("")

    IO.puts(
      "kind,scenario,total,subscribers,patterns,seconds,publishes_per_sec,deliveries_per_sec"
    )

    run_core()
    run_tcp(port)
  after
    _ = Application.stop(:ferricstore_server)
    _ = Application.stop(:ferricstore)
  end

  defp stop_started_apps do
    for app <- [:ferricstore_server, :ferricstore_ecto, :ferricstore_session, :ferricstore] do
      _ = Application.stop(app)
    end
  end

  defp configure_app(data_dir) do
    File.rm_rf!(data_dir)
    File.mkdir_p!(data_dir)

    Application.put_env(:libcluster, :topologies, [])
    Application.put_env(:ferricstore, :data_dir, data_dir)
    Application.put_env(:ferricstore, :port, 0)
    Application.put_env(:ferricstore, :health_port, 0)
    Application.put_env(:ferricstore, :shard_count, 1)
    Application.put_env(:ferricstore, :protected_mode, false)
    Application.put_env(:ferricstore, :max_memory_bytes, 100_000_000_000)
    Application.put_env(:ferricstore, :memory_guard_interval_ms, 60 * 60 * 1000)
  end

  defp run_core do
    for subscribers <- core_subscribers() do
      clear_pubsub!()
      channel = "bench:core:#{subscribers}"
      pids = spawn_drainers(subscribers)
      Enum.each(pids, &PubSub.subscribe(channel, &1))

      timed(:core, "exact", core_total(), subscribers, 0, fn ->
        repeat(core_total(), fn -> _ = PubSub.publish(channel, "payload") end)
      end)

      stop_drainers(pids)
    end

    for patterns <- core_patterns() do
      clear_pubsub!()
      channel = "bench:pattern:match"
      pids = spawn_drainers(patterns)

      Enum.each(pids, fn pid ->
        PubSub.psubscribe("bench:pattern:*", pid)
      end)

      timed(:core, "pattern_match", core_total(), 0, patterns, fn ->
        repeat(core_total(), fn -> _ = PubSub.publish(channel, "payload") end)
      end)

      stop_drainers(pids)
    end
  end

  defp run_tcp(port) do
    for subscribers <- tcp_subscribers() do
      clear_pubsub!()
      channel = "bench:tcp:#{subscribers}:#{System.unique_integer([:positive])}"
      subscriber_pids = start_tcp_subscribers(port, channel, subscribers)

      wait_until(fn -> PubSub.numsub([channel]) == [channel, subscribers] end, 2_000)

      timed(:tcp, "exact", tcp_total(), subscribers, 0, fn ->
        publish_tcp(port, channel, "payload", tcp_total(), tcp_pipeline())
      end)

      Enum.each(subscriber_pids, &Process.exit(&1, :kill))
    end
  end

  defp timed(kind, scenario, total, subscribers, patterns, fun) do
    {us, _result} = :timer.tc(fun)
    seconds = us / 1_000_000
    publish_rate = total / seconds
    deliveries = total * max(subscribers + patterns, 1)
    delivery_rate = deliveries / seconds

    IO.puts(
      [
        kind,
        scenario,
        total,
        subscribers,
        patterns,
        Float.round(seconds, 4),
        Float.round(publish_rate, 2),
        Float.round(delivery_rate, 2)
      ]
      |> Enum.join(",")
    )
  end

  defp publish_tcp(port, channel, payload, total, pipeline) do
    {:ok, socket} = connect(port)
    command = IO.iodata_to_binary(Encoder.encode(["PUBLISH", channel, payload]))
    loop_publish(socket, command, total, pipeline, <<>>)
    :gen_tcp.close(socket)
  end

  defp loop_publish(_socket, _command, 0, _pipeline, _buffer), do: :ok

  defp loop_publish(socket, command, remaining, pipeline, buffer) do
    batch = min(remaining, pipeline)
    :ok = :gen_tcp.send(socket, List.duplicate(command, batch))
    buffer = recv_integer_responses(socket, batch, buffer)
    loop_publish(socket, command, remaining - batch, pipeline, buffer)
  end

  defp recv_integer_responses(socket, needed, buffer) do
    case parse_integer_responses(buffer, needed, 0) do
      {:done, rest} ->
        rest

      {:need_more, count, rest} ->
        {:ok, chunk} = :gen_tcp.recv(socket, 0, 30_000)
        recv_integer_responses(socket, needed - count, rest <> chunk)
    end
  end

  defp parse_integer_responses(buffer, needed, count) when count >= needed,
    do: {:done, buffer}

  defp parse_integer_responses(<<":", rest::binary>>, needed, count) do
    case :binary.match(rest, "\r\n") do
      {idx, 2} ->
        <<_line::binary-size(idx), "\r\n", tail::binary>> = rest
        parse_integer_responses(tail, needed, count + 1)

      :nomatch ->
        {:need_more, count, <<":", rest::binary>>}
    end
  end

  defp parse_integer_responses(buffer, _needed, count), do: {:need_more, count, buffer}

  defp start_tcp_subscribers(port, channel, count) do
    parent = self()

    for _ <- one_to(count) do
      spawn(fn ->
        {:ok, socket} = connect(port)
        :ok = :gen_tcp.send(socket, Encoder.encode(["SUBSCRIBE", channel]))
        {:ok, _ack} = :gen_tcp.recv(socket, 0, 5_000)
        send(parent, {:subscriber_ready, self()})
        drain_socket(socket)
      end)
    end
    |> tap(fn pids ->
      Enum.each(pids, fn pid ->
        receive do
          {:subscriber_ready, ^pid} -> :ok
        after
          5_000 -> raise "subscriber did not become ready"
        end
      end)
    end)
  end

  defp drain_socket(socket) do
    case :gen_tcp.recv(socket, 0, 30_000) do
      {:ok, _data} -> drain_socket(socket)
      {:error, _reason} -> :ok
    end
  end

  defp connect(port) do
    :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw, nodelay: true])
  end

  defp spawn_drainers(count) do
    for _ <- one_to(count) do
      spawn(fn -> drain_mailbox() end)
    end
  end

  defp drain_mailbox do
    receive do
      :stop -> :ok
      _message -> drain_mailbox()
    end
  end

  defp stop_drainers(pids) do
    Enum.each(pids, fn pid ->
      PubSub.cleanup(pid)
      send(pid, :stop)
    end)
  end

  defp repeat(0, _fun), do: :ok

  defp repeat(count, fun) do
    fun.()
    repeat(count - 1, fun)
  end

  defp wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_until(fun, deadline, nil)
  end

  defp wait_until(fun, deadline, last_value) do
    case fun.() do
      true ->
        :ok

      other ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise "condition did not become true; last=#{inspect(other || last_value)}"
        end

        Process.sleep(10)
        wait_until(fun, deadline, other)
    end
  end

  defp clear_pubsub! do
    delete_all(:ferricstore_pubsub)
    delete_all(:ferricstore_pubsub_patterns)
  end

  defp delete_all(table) do
    case :ets.whereis(table) do
      :undefined -> :ok
      _tid -> :ets.delete_all_objects(table)
    end
  end

  defp core_total, do: int_env("PUBSUB_CORE_TOTAL", 100_000)
  defp tcp_total, do: int_env("PUBSUB_TCP_TOTAL", 50_000)
  defp tcp_pipeline, do: int_env("PUBSUB_TCP_PIPELINE", 100)

  defp core_subscribers, do: int_list_env("PUBSUB_CORE_SUBSCRIBERS", "0,1,10,100")
  defp core_patterns, do: int_list_env("PUBSUB_CORE_PATTERNS", "1,10,100")
  defp tcp_subscribers, do: int_list_env("PUBSUB_TCP_SUBSCRIBERS", "0,1,10")

  defp int_env(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp int_list_env(name, default) do
    name
    |> System.get_env(default)
    |> String.split(",", trim: true)
    |> Enum.map(&String.to_integer/1)
  end

  defp one_to(count) when count <= 0, do: []
  defp one_to(count), do: 1..count
end

PubSubBench.run()
