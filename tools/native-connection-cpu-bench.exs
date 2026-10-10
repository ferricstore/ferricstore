# Run with:
#   mise exec -- env ERL_FLAGS='+S 2:2' MIX_ENV=test mix run --no-start tools/native-connection-cpu-bench.exs
#
# This is a data-path benchmark only. It exercises connection frame
# preparation, flow accounting, lane batching, and the enqueue handoff for
# ordinary GET frames. It deliberately does not start storage or execute the
# GETs in a lane, so network, Router, and disk costs are excluded.

path = "apps/ferricstore_server/lib/ferricstore_server/native/connection.ex"
baseline_ref = System.get_env("CONNECTION_BENCH_BASELINE_REF", "HEAD")
sample_count = String.to_integer(System.get_env("CONNECTION_BENCH_SAMPLES", "31"))
calls_per_sample = String.to_integer(System.get_env("CONNECTION_BENCH_CALLS", "2000"))
warmup_calls = String.to_integer(System.get_env("CONNECTION_BENCH_WARMUP", "250"))

if sample_count < 3 or rem(sample_count, 2) == 0 do
  raise "CONNECTION_BENCH_SAMPLES must be an odd integer >= 3"
end

if calls_per_sample < 1 or warmup_calls < 0 do
  raise "CONNECTION_BENCH_CALLS must be positive and CONNECTION_BENCH_WARMUP non-negative"
end

defmodule NativeConnectionCpuBench.Loader do
  @moduledoc false

  def compile!(source, module) do
    {:defmodule, metadata, [_original_name, body]} = Code.string_to_quoted!(source)

    benchmark_entrypoint =
      quote do
        @doc false
        def benchmark_dispatch(frames, state), do: dispatch_frames(frames, state, [], 0)
      end

    body =
      case body do
        [do: {:__block__, body_metadata, forms}] ->
          [do: {:__block__, body_metadata, forms ++ [benchmark_entrypoint]}]

        [do: form] ->
          [do: {:__block__, [], [form, benchmark_entrypoint]}]
      end

    module_ast = {:__aliases__, [], Module.split(module) |> Enum.map(&String.to_atom/1)}

    [{^module, _bytecode}] = Code.compile_quoted({:defmodule, metadata, [module_ast, body]})
    module
  end
end

{baseline_source, 0} = System.cmd("git", ["show", baseline_ref <> ":" <> path])
candidate_source = File.read!(path)

baseline_module =
  NativeConnectionCpuBench.Loader.compile!(
    baseline_source,
    FerricstoreServer.Native.ConnectionBenchmarkBaseline
  )

candidate_module =
  NativeConnectionCpuBench.Loader.compile!(
    candidate_source,
    FerricstoreServer.Native.ConnectionBenchmarkCandidate
  )

alias FerricstoreServer.Native.Connection.FrameBuffer
alias FerricstoreServer.Native.ResourceBudget

{:ok, budget} =
  ResourceBudget.start_link(
    name: :native_connection_cpu_bench_budget,
    limits: %{inbound_bytes: 512 * 1024 * 1024}
  )

sink_loop = fn sink_loop ->
  receive do
    :stop -> :ok
    _message -> sink_loop.(sink_loop)
  end
end

make_sink = fn -> spawn(fn -> sink_loop.(sink_loop) end) end

op_get = 0x0101

frames_for = fn count ->
  for request_id <- 1..count do
    {1, op_get, request_id, 0, "bench-key-#{rem(request_id, 32)}"}
  end
end

state_for = fn module, sink, frames ->
  frame_bytes =
    Enum.reduce(frames, 0, fn {_lane, _opcode, _id, _flags, body}, total ->
      total + FrameBuffer.frame_bytes(byte_size(body))
    end)

  state =
    Map.merge(struct(module), %{
      socket: :native_connection_cpu_bench_socket,
      transport: nil,
      client_id: "native-connection-cpu-bench",
      client_name: nil,
      created_at: 0,
      peer: nil,
      instance_ctx: %{},
      stats_counter: nil,
      max_frame_bytes: 1_048_576,
      max_lanes: 16,
      lane_max_queue: 1_000_000,
      max_inflight_per_connection: 1_000_000,
      max_inflight_per_lane: 1_000_000,
      max_queued_request_bytes_per_connection: 512 * 1024 * 1024,
      max_queued_request_bytes_per_lane: 512 * 1024 * 1024,
      response_chunk_bytes: 0,
      max_pending_chunks: 1024,
      max_pending_chunk_bytes: 512 * 1024 * 1024,
      max_response_bytes: 512 * 1024 * 1024,
      max_outbound_bytes: 512 * 1024 * 1024,
      outbound_counter: nil,
      response_coalesce_max: 1,
      response_coalesce_bytes: 0,
      trusted_request_context_users: MapSet.new(),
      command_state: %{},
      idle_timeout_ms: 90_000,
      resource_budget: budget,
      preauth_max_frame_bytes: 1_048_576,
      frame_assembly_timeout_ms: 15_000,
      frame_assembly_deadline_ms: nil,
      chunk_assembly_deadline_ms: nil,
      inbound_buffer_token: nil,
      compression: :none,
      buffer: FrameBuffer.new(),
      lanes: %{1 => sink},
      chunk_buffers: %{},
      pending_chunk_bytes: 0,
      pending_chunk_metadata_bytes: 0,
      deferred_frame_metadata_bytes: 0,
      decoded_retained_bytes: frame_bytes,
      queued_request_bytes: 0,
      lane_queued_request_bytes: %{},
      inflight_total: 0,
      lane_inflight: %{},
      multi_state: :none,
      multi_queue: [],
      multi_queue_count: 0,
      multi_queue_bytes: 0,
      multi_error: false,
      blocked_requests: %{},
      deferred_frames: %{},
      authenticated: true,
      require_auth: false,
      compact_flow_responses: false,
      compact_response_codecs: MapSet.new(),
      decode_paused: false,
      decode_pending: false,
      input_active: false,
      username: "default",
      acl_cache: nil,
      close_after_reply: false
    })

  {state, frame_bytes}
end

summarize_state = fn state ->
  Map.take(state, [
    :decoded_retained_bytes,
    :queued_request_bytes,
    :lane_queued_request_bytes,
    :inflight_total,
    :lane_inflight,
    :pending_chunk_bytes,
    :pending_chunk_metadata_bytes,
    :deferred_frame_metadata_bytes,
    :deferred_frames,
    :close_after_reply
  ])
end

invoke = fn module, seed, frame_bytes, frames, sink ->
  state = %{
    seed
    | decoded_retained_bytes: frame_bytes,
      queued_request_bytes: 0,
      lane_queued_request_bytes: %{},
      inflight_total: 0,
      lane_inflight: %{},
      inbound_buffer_token: nil,
      close_after_reply: false
  }

  {responses, state} = module.benchmark_dispatch(frames, state)

  message =
    if sink == self() do
      receive do
        {:native_lane_frames, queued_frames} -> {:native_lane_frames, queued_frames}
        {:native_lane_frame, queued_frame} -> {:native_lane_frame, queued_frame}
      after
        1_000 -> raise "connection benchmark did not flush the lane batch"
      end
    else
      :async
    end

  if is_reference(state.inbound_buffer_token),
    do: ResourceBudget.release(budget, state.inbound_buffer_token)

  {responses, message, summarize_state.(state)}
end

preflight_frames = frames_for.(16)

{preflight_baseline_seed, preflight_frame_bytes} =
  state_for.(baseline_module, self(), preflight_frames)

{preflight_candidate_seed, ^preflight_frame_bytes} =
  state_for.(candidate_module, self(), preflight_frames)

preflight_baseline =
  invoke.(
    baseline_module,
    preflight_baseline_seed,
    preflight_frame_bytes,
    preflight_frames,
    self()
  )

preflight_candidate =
  invoke.(
    candidate_module,
    preflight_candidate_seed,
    preflight_frame_bytes,
    preflight_frames,
    self()
  )

unless preflight_baseline == preflight_candidate do
  IO.inspect(preflight_baseline, label: "baseline preflight", limit: :infinity)
  IO.inspect(preflight_candidate, label: "candidate preflight", limit: :infinity)
  raise "baseline and candidate ordinary GET dispatch outputs differ"
end

IO.inspect(
  %{baseline: baseline_module, candidate: candidate_module, frames: length(preflight_frames)},
  label: "connection_benchmark_preflight"
)

median = fn values ->
  values = Enum.sort(values)
  Enum.at(values, div(length(values), 2))
end

measure = fn module, scenario, sample, seed, frame_bytes, frames, sink ->
  invoke_warmup = fn ->
    Enum.each(1..warmup_calls, fn _ ->
      {_, _, _} = invoke.(module, seed, frame_bytes, frames, sink)
    end)
  end

  # Warmup is intentionally outside the measured sample. The sink consumes
  # asynchronously, while the benchmarked caller still pays the same send.
  invoke_warmup.()
  :erlang.garbage_collect()
  {:reductions, before_reductions} = Process.info(self(), :reductions)
  {before_cpu_ms, _} = :erlang.statistics(:runtime)

  {wall_us, _} =
    :timer.tc(fn ->
      Enum.each(1..calls_per_sample, fn _ ->
        {_, _, _} = invoke.(module, seed, frame_bytes, frames, sink)
      end)
    end)

  {after_cpu_ms, _} = :erlang.statistics(:runtime)
  {:reductions, after_reductions} = Process.info(self(), :reductions)

  %{
    implementation: module,
    scenario: scenario,
    sample: sample,
    calls: calls_per_sample,
    wall_us: wall_us,
    ns_per_call: wall_us * 1_000 / calls_per_sample,
    cpu_ms: after_cpu_ms - before_cpu_ms,
    reductions: after_reductions - before_reductions
  }
end

try do
  results =
    for count <- [1, 8, 32, 128],
        sample <- 1..sample_count do
      frames = frames_for.(count)
      scenario = {:ordinary_get_batch, count}

      order =
        if rem(sample, 2) == 1,
          do: [baseline_module, candidate_module],
          else: [candidate_module, baseline_module]

      for module <- order do
        sink = make_sink.()
        {seed, frame_bytes} = state_for.(module, sink, frames)

        try do
          result = measure.(module, scenario, sample, seed, frame_bytes, frames, sink)
          IO.inspect(result, label: "connection_benchmark")
          result
        after
          send(sink, :stop)
        end
      end
    end
    |> List.flatten()

  results
  |> Enum.group_by(& &1.scenario)
  |> Enum.each(fn {scenario, rows} ->
    by_implementation = Enum.group_by(rows, & &1.implementation)
    baseline_rows = Map.fetch!(by_implementation, baseline_module)
    candidate_rows = Map.fetch!(by_implementation, candidate_module)
    baseline_by_sample = Map.new(baseline_rows, &{&1.sample, &1})
    candidate_by_sample = Map.new(candidate_rows, &{&1.sample, &1})

    paired =
      for sample <- Map.keys(baseline_by_sample) do
        baseline = Map.fetch!(baseline_by_sample, sample)
        candidate = Map.fetch!(candidate_by_sample, sample)

        %{
          wall_delta_pct:
            100 * (candidate.ns_per_call - baseline.ns_per_call) / baseline.ns_per_call,
          reduction_delta_pct:
            100 * (candidate.reductions - baseline.reductions) / baseline.reductions
        }
      end

    IO.inspect(
      %{
        baseline_median_ns_per_call: median.(Enum.map(baseline_rows, & &1.ns_per_call)),
        candidate_median_ns_per_call: median.(Enum.map(candidate_rows, & &1.ns_per_call)),
        paired_median_wall_delta_pct: median.(Enum.map(paired, & &1.wall_delta_pct)),
        paired_median_reduction_delta_pct: median.(Enum.map(paired, & &1.reduction_delta_pct)),
        samples: length(paired)
      },
      label: "connection_benchmark_summary #{inspect(scenario)}"
    )
  end)
after
  GenServer.stop(budget)
end
