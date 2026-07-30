alias Ferricstore.Commands.Stream
alias FerricstoreServer.Native.{Codec, Commands}

Logger.configure(level: :warning)

run_id = "#{System.os_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"
data_dir = Path.join(System.tmp_dir!(), "ferricstore_native_range_bench_#{run_id}")

File.mkdir_p!(data_dir)
Application.put_env(:ferricstore, :data_dir, data_dir)
Application.put_env(:ferricstore, :native_port, 0)

{:ok, _started} = Application.ensure_all_started(:ferricstore_server)
store = FerricStore.Instance.get(:default)
key = "bench:native-range:#{run_id}"

System.at_exit(fn _status ->
  _ = Application.stop(:ferricstore_server)
  _ = Application.stop(:ferricstore)
  File.rm_rf!(data_dir)
end)

for id <- 1..3_072 do
  id = "#{id}-0"
  ^id = Stream.handle("XADD", [key, id, "field", "value"], store)
end

state = %{
  client_id: 1,
  client_name: nil,
  username: "default",
  authenticated: true,
  require_auth: false,
  acl_cache: :full_access,
  peer: {{127, 0, 0, 1}, 12_345},
  created_at: System.monotonic_time(:millisecond),
  instance_ctx: store,
  stats_counter: store.stats_counter,
  compression: :none,
  compact_flow_responses: false,
  compact_response_codecs: MapSet.new(),
  subscribed_events: MapSet.new(),
  flow_wake_subscriptions: MapSet.new()
}

direct_state = Map.put(state, :native_direct_stream_range_response, true)

payload = fn command, count ->
  bounds = if command == "XRANGE", do: ["-", "+"], else: ["+", "-"]

  %{
    "command" => command,
    "args" => [key | bounds] ++ ["COUNT", Integer.to_string(count)]
  }
end

execute = fn execution_state, command, count ->
  {status, value, _state} = Commands.execute(0x0100, payload.(command, count), execution_state)

  unless status == :ok do
    raise "native Stream range benchmark failed: #{inspect({status, value})}"
  end

  Codec.encode_command_response_frames(0x0100, 1, 1, status, value, max_response_bytes: 1_048_576)
end

true = execute.(state, "XRANGE", 10) == execute.(direct_state, "XRANGE", 10)
true = execute.(state, "XRANGE", 128) == execute.(direct_state, "XRANGE", 128)
true = execute.(state, "XRANGE", 1_000) == execute.(direct_state, "XRANGE", 1_000)
true = execute.(state, "XREVRANGE", 1_000) == execute.(direct_state, "XREVRANGE", 1_000)

Benchee.run(
  %{
    "materialized Rust frame XRANGE COUNT 128" => fn -> execute.(state, "XRANGE", 128) end,
    "raw Rust handoff XRANGE COUNT 128" => fn ->
      execute.(direct_state, "XRANGE", 128)
    end,
    "materialized Rust frame XRANGE COUNT 1000" => fn -> execute.(state, "XRANGE", 1_000) end,
    "raw Rust handoff XRANGE COUNT 1000" => fn ->
      execute.(direct_state, "XRANGE", 1_000)
    end,
    "materialized Rust frame XREVRANGE COUNT 1000" => fn ->
      execute.(state, "XREVRANGE", 1_000)
    end,
    "raw Rust handoff XREVRANGE COUNT 1000" => fn ->
      execute.(direct_state, "XREVRANGE", 1_000)
    end
  },
  warmup: 2,
  time: System.get_env("BENCH_TIME", "8") |> String.to_integer(),
  memory_time: 2,
  reduction_time: 2,
  print: [benchmarking: false, configuration: false]
)
