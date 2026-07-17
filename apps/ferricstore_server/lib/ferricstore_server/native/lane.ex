defmodule FerricstoreServer.Native.Lane do
  @moduledoc """
  Bounded ordered execution lane for native protocol multiplexing.

  A lane is one lightweight process per active logical stream. It preserves
  command order within the lane while different lanes can run concurrently.
  This gives Cassandra-style multiplexing without spawning an unbounded Task per
  frame.
  """

  alias Ferricstore.{LatencyTrace, Stats}
  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.Store.{ReadResult, Router}
  alias FerricstoreServer.Native.{Codec, Commands, ResourceBudget}

  @flag_trace 0x01
  @flag_custom_payload 0x02
  @flag_no_reply 0x10
  @op_pipeline 0x000E
  @op_get 0x0101
  @op_set 0x0102
  @op_mget 0x0104
  @op_fetch_or_compute 0x010B
  @compact_data_write_pipeline_modes [22, 23, 24, 25, 26, 31, 32]

  @spec start_link(pid(), non_neg_integer(), map()) :: {:ok, pid()} | {:error, term()}
  def start_link(owner, lane_id, command_state)
      when is_pid(owner) and is_integer(lane_id) and is_map(command_state) do
    starter = self()
    start_ref = make_ref()
    pid = spawn_link(fn -> init_lane(starter, start_ref, owner, lane_id, command_state) end)

    receive do
      {:native_lane_started, ^start_ref, ^pid} -> {:ok, pid}
      {:native_lane_rejected, ^start_ref, ^pid, reason} -> {:error, reason}
    after
      5_000 ->
        Process.exit(pid, :kill)
        {:error, :start_timeout}
    end
  end

  @spec enqueue(pid(), term()) :: :ok
  def enqueue(pid, frame) do
    send(pid, {:native_lane_frame, frame})
    :ok
  end

  @spec enqueue_many(pid(), [term()]) :: :ok
  def enqueue_many(_pid, []), do: :ok

  def enqueue_many(pid, [frame]) do
    enqueue(pid, frame)
  end

  def enqueue_many(pid, frames) when is_pid(pid) and is_list(frames) do
    send(pid, {:native_lane_frames, frames})
    :ok
  end

  @doc false
  @spec account_frame(term(), pos_integer()) :: term()
  def account_frame(frame, bytes) when is_integer(bytes) and bytes > 0,
    do: {:native_accounted_frame, frame, bytes}

  @doc false
  @spec execute_prepared_batch((-> term()), (term() -> term())) :: {:ok, term()}
  def execute_prepared_batch(execute, encode)
      when is_function(execute, 0) and is_function(encode, 1) do
    results = execute.()
    {:ok, encode.(results)}
  end

  @spec stop(pid()) :: :ok
  def stop(pid) when is_pid(pid) do
    Process.unlink(pid)
    Process.exit(pid, :shutdown)
    :ok
  end

  @spec update_command_state(pid(), map()) :: :ok
  def update_command_state(pid, command_state) when is_pid(pid) and is_map(command_state) do
    send(pid, {:native_lane_command_state, command_state})
    :ok
  end

  @doc false
  def loop(owner, lane_id, command_state) do
    receive do
      {:native_lane_frame, frame} ->
        execute_and_send_frame(owner, lane_id, frame, command_state)

        loop(owner, lane_id, command_state)

      {:native_lane_frames, frames} ->
        execute_and_send_frames(owner, lane_id, frames, command_state)
        loop(owner, lane_id, command_state)

      {:native_lane_command_state, command_state} ->
        loop(owner, lane_id, command_state)

      :shutdown ->
        :ok
    end
  end

  defp init_lane(starter, start_ref, owner, lane_id, command_state) do
    budget = Map.get(command_state, :resource_budget, ResourceBudget)

    case ResourceBudget.acquire(budget, :lanes, self(), 1) do
      {:ok, token} ->
        send(starter, {:native_lane_started, start_ref, self()})

        try do
          loop(owner, lane_id, command_state)
        after
          ResourceBudget.release_async(budget, token)
        end

      {:error, reason} ->
        send(starter, {:native_lane_rejected, start_ref, self(), reason})
    end
  end

  defp execute_and_send_frame(owner, lane_id, accounted_frame, command_state) do
    {frame, request_bytes} = take_frame_accounting(accounted_frame)

    case with_frame_slot(command_state, frame, fn -> execute_frame(frame, command_state) end) do
      {:ok, :noreply} ->
        send_lane_done(owner, lane_id, request_bytes)

      {:ok, iodata} ->
        send_lane_response(owner, lane_id, iodata, request_bytes)

      {:error, reason} ->
        if frame_no_reply?(frame) do
          send_lane_done(owner, lane_id, request_bytes)
        else
          send_lane_response(
            owner,
            lane_id,
            execution_budget_error_response(frame, command_state, reason),
            request_bytes
          )
        end
    end
  end

  defp execute_and_send_frames(owner, lane_id, frames, command_state) do
    if Enum.any?(frames, &streamed_response_frame?/1) do
      Enum.each(frames, &execute_and_send_frame(owner, lane_id, &1, command_state))
    else
      {frames, request_bytes} = take_frames_accounting(frames)
      execute_and_send_frame_batch(owner, lane_id, frames, request_bytes, command_state)
    end
  end

  defp execute_and_send_frame_batch(owner, lane_id, frames, request_bytes, command_state) do
    result =
      if Enum.any?(frames, &blocking_execution_frame?/1) do
        {:ok, execute_frames_with_individual_slots(frames, command_state)}
      else
        with_execution_slot(command_state, fn -> execute_frames(frames, command_state) end)
      end

    {responses, done_count} =
      case result do
        {:ok, result} ->
          result

        {:error, reason} ->
          responses =
            frames
            |> Enum.reject(&frame_no_reply?/1)
            |> Enum.map(&execution_budget_error_response(&1, command_state, reason))

          {responses, length(frames)}
      end

    case responses do
      [] -> send_lane_done_many(owner, lane_id, done_count, request_bytes)
      _ -> send_lane_responses(owner, lane_id, responses, done_count, request_bytes)
    end
  end

  defp take_frames_accounting(frames) do
    Enum.map_reduce(frames, 0, fn frame, total_bytes ->
      {frame, bytes} = take_frame_accounting(frame)
      {frame, total_bytes + bytes}
    end)
  end

  defp take_frame_accounting({:native_accounted_frame, frame, bytes}), do: {frame, bytes}
  defp take_frame_accounting(frame), do: {frame, 0}

  defp send_lane_response(owner, lane_id, iodata, 0),
    do: send(owner, {:native_lane_response, lane_id, iodata})

  defp send_lane_response(owner, lane_id, iodata, request_bytes),
    do: send(owner, {:native_lane_response, lane_id, iodata, request_bytes})

  defp send_lane_responses(owner, lane_id, responses, done_count, 0),
    do: send(owner, {:native_lane_responses, lane_id, responses, done_count})

  defp send_lane_responses(owner, lane_id, responses, done_count, request_bytes),
    do: send(owner, {:native_lane_responses, lane_id, responses, done_count, request_bytes})

  defp send_lane_done(owner, lane_id, 0), do: send(owner, {:native_lane_done, lane_id})

  defp send_lane_done(owner, lane_id, request_bytes),
    do: send(owner, {:native_lane_done, lane_id, request_bytes})

  defp send_lane_done_many(owner, lane_id, done_count, 0),
    do: send(owner, {:native_lane_done_many, lane_id, done_count})

  defp send_lane_done_many(owner, lane_id, done_count, request_bytes),
    do: send(owner, {:native_lane_done_many, lane_id, done_count, request_bytes})

  defp execute_frames_with_individual_slots(frames, command_state) do
    {responses, done_count} =
      Enum.reduce(frames, {[], 0}, fn frame, {responses, done_count} ->
        case with_frame_slot(command_state, frame, fn -> execute_frame(frame, command_state) end) do
          {:ok, :noreply} ->
            {responses, done_count + 1}

          {:ok, iodata} ->
            {[iodata | responses], done_count + 1}

          {:error, reason} ->
            if frame_no_reply?(frame) do
              {responses, done_count + 1}
            else
              response = execution_budget_error_response(frame, command_state, reason)
              {[response | responses], done_count + 1}
            end
        end
      end)

    {Enum.reverse(responses), done_count}
  end

  defp with_frame_slot(command_state, frame, fun) do
    if blocking_execution_frame?(frame) do
      with_resource_slot(command_state, :blocking_requests, :fail_fast, fun)
    else
      with_execution_slot(command_state, fun)
    end
  end

  defp with_execution_slot(command_state, fun) do
    with_resource_slot(command_state, :executions, :wait, fun)
  end

  defp with_resource_slot(command_state, resource, acquisition, fun) do
    budget = Map.get(command_state, :resource_budget, ResourceBudget)

    result =
      case acquisition do
        :wait -> ResourceBudget.acquire_wait(budget, resource, self(), 1)
        :fail_fast -> ResourceBudget.acquire(budget, resource, self(), 1)
      end

    case result do
      {:ok, token} ->
        try do
          {:ok, fun.()}
        after
          ResourceBudget.release_async(budget, token)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp blocking_execution_frame?(frame) do
    {_lane_id, opcode, _request_id, _flags, _body} = base_frame(frame)
    opcode == @op_fetch_or_compute
  end

  defp streamed_response_frame?(frame) do
    {_lane_id, opcode, _request_id, _flags, _body} = base_frame(frame)
    opcode == @op_mget
  end

  defp execution_budget_error_response(frame, command_state, reason) do
    {lane_id, opcode, request_id, _flags, _body} = base_frame(frame)

    message =
      case reason do
        {:limit, :executions} -> "ERR native global execution limit exceeded"
        {:limit, :blocking_requests} -> "ERR native global blocking request limit exceeded"
        _other -> "ERR native resource budget unavailable"
      end

    Codec.encode_command_response_frames(opcode, lane_id, request_id, :busy, message,
      compression: Map.get(command_state, :compression, :none),
      compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
      chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0),
      max_response_bytes: Map.get(command_state, :max_response_bytes)
    )
  end

  defp frame_no_reply?(frame), do: frame |> base_frame() |> no_reply?()
  defp base_frame({:native_accounted_frame, frame, _bytes}), do: base_frame(frame)
  defp base_frame({:native_trace, frame, _trace}), do: base_frame(frame)
  defp base_frame(frame), do: frame

  defp execute_frames(frames, command_state) do
    case try_compact_set_pipeline_batch(frames, command_state) do
      {:ok, responses} ->
        {responses, length(frames)}

      :fallback ->
        case try_compact_data_write_pipeline_batch(frames, command_state) do
          {:ok, responses} ->
            {responses, length(frames)}

          :fallback ->
            case try_compact_mget_batch(frames, command_state) do
              {:ok, responses} ->
                {responses, length(frames)}

              :fallback ->
                case try_plain_get_batch(frames, command_state) do
                  {:ok, responses} ->
                    {responses, length(frames)}

                  :fallback ->
                    case try_plain_set_batch(frames, command_state) do
                      {:ok, responses} ->
                        {responses, length(frames)}

                      :fallback ->
                        {responses, done_count} =
                          Enum.reduce(frames, {[], 0}, fn frame, {responses, done_count} ->
                            case execute_frame(frame, command_state) do
                              :noreply -> {responses, done_count + 1}
                              iodata -> {[iodata | responses], done_count + 1}
                            end
                          end)

                        {Enum.reverse(responses), done_count}
                    end
                end
            end
        end
    end
  end

  defp try_compact_set_pipeline_batch(frames, command_state) do
    case prepare_compact_set_pipeline_batch(frames, command_state) do
      {:ok, requests, counts, kv_pairs} ->
        Stats.incr_commands_by(command_state.stats_counter, length(kv_pairs))

        execute_prepared_batch(
          fn -> Router.batch_quorum_put(command_state.instance_ctx, kv_pairs) end,
          &encode_compact_set_pipeline_batch_responses(&1, requests, counts, command_state)
        )

      :fallback ->
        :fallback
    end
  end

  defp prepare_compact_set_pipeline_batch(frames, command_state) do
    with true <- plain_set_batch_allowed?(command_state),
         true <- pressure_ok?(command_state),
         {:ok, requests, counts, kv_pairs} <-
           extract_compact_set_pipeline_frames(frames, [], [], []),
         :ok <- authorize_batch_pairs(kv_pairs) do
      {:ok, requests, counts, kv_pairs}
    else
      _ -> :fallback
    end
  rescue
    _ -> :fallback
  end

  defp extract_compact_set_pipeline_frames([], requests, counts, kv_pairs) do
    {:ok, Enum.reverse(requests), Enum.reverse(counts),
     kv_pairs |> Enum.reverse() |> List.flatten()}
  end

  defp extract_compact_set_pipeline_frames(
         [
           {lane_id, @op_pipeline, request_id, @flag_custom_payload,
            <<0x94, 0x81, _rest::binary>> = body}
           | rest
         ],
         requests,
         counts,
         kv_pairs
       ) do
    case Codec.decode_body(@op_pipeline, @flag_custom_payload, body) do
      {:ok, %{"compact_pipeline" => {1, pairs}, "compact_values" => true}} when is_list(pairs) ->
        extract_compact_set_pipeline_frames(
          rest,
          [{lane_id, request_id} | requests],
          [length(pairs) | counts],
          [pairs | kv_pairs]
        )

      _ ->
        :fallback
    end
  end

  defp extract_compact_set_pipeline_frames(_frames, _requests, _counts, _kv_pairs), do: :fallback

  defp encode_compact_set_pipeline_batch_responses(results, requests, counts, command_state) do
    {responses, []} =
      requests
      |> Enum.zip(counts)
      |> Enum.map_reduce(results, fn {{lane_id, request_id}, count}, remaining ->
        {frame_results, rest} = Enum.split(remaining, count)

        response =
          Codec.encode_command_response_frames(
            @op_pipeline,
            lane_id,
            request_id,
            :ok,
            compact_set_pipeline_result_body(frame_results),
            compression: Map.get(command_state, :compression, :none),
            compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
            chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0),
            max_response_bytes: Map.get(command_state, :max_response_bytes)
          )

        {response, rest}
      end)

    responses
  end

  defp try_compact_data_write_pipeline_batch(frames, command_state) do
    case prepare_compact_data_write_pipeline_batch(frames, command_state) do
      {:ok, requests, counts, ops} ->
        Stats.incr_commands_by(command_state.stats_counter, length(ops))

        execute_prepared_batch(
          fn -> Router.pipeline_write_batch(command_state.instance_ctx, ops) end,
          &encode_compact_data_write_pipeline_batch_responses(&1, requests, counts, command_state)
        )

      :fallback ->
        :fallback
    end
  end

  defp prepare_compact_data_write_pipeline_batch(frames, command_state) do
    with true <- plain_set_batch_allowed?(command_state),
         true <- pressure_ok?(command_state),
         {:ok, requests, counts, ops} <-
           extract_compact_data_write_pipeline_frames(frames, [], [], []),
         :ok <- authorize_keyed_commands(ops) do
      {:ok, requests, counts, ops}
    else
      _ -> :fallback
    end
  rescue
    _ -> :fallback
  end

  defp extract_compact_data_write_pipeline_frames([], requests, counts, ops) do
    {:ok, Enum.reverse(requests), Enum.reverse(counts), ops |> Enum.reverse() |> List.flatten()}
  end

  defp extract_compact_data_write_pipeline_frames(
         [{lane_id, @op_pipeline, request_id, @flag_custom_payload, body} | rest],
         requests,
         counts,
         ops
       ) do
    case Codec.decode_body(@op_pipeline, @flag_custom_payload, body) do
      {:ok, %{"compact_pipeline" => {mode, items}}}
      when mode in @compact_data_write_pipeline_modes and is_list(items) ->
        frame_ops = compact_data_write_pipeline_ops(mode, items)

        extract_compact_data_write_pipeline_frames(
          rest,
          [{lane_id, request_id} | requests],
          [length(frame_ops) | counts],
          [frame_ops | ops]
        )

      _ ->
        :fallback
    end
  end

  defp extract_compact_data_write_pipeline_frames(_frames, _requests, _counts, _ops),
    do: :fallback

  defp compact_data_write_pipeline_ops(22, items) do
    Enum.map(items, fn {key, field, value} -> {key, {:hset_single, key, field, value}} end)
  end

  defp compact_data_write_pipeline_ops(23, items) do
    Enum.map(items, fn {key, value} -> {key, {:lpush_single, key, value}} end)
  end

  defp compact_data_write_pipeline_ops(24, items) do
    Enum.map(items, fn {key, value} -> {key, {:rpush_single, key, value}} end)
  end

  defp compact_data_write_pipeline_ops(25, items) do
    Enum.map(items, fn {key, member} -> {key, {:sadd_single, key, member}} end)
  end

  defp compact_data_write_pipeline_ops(26, items) do
    Enum.map(items, fn {key, score, member} -> {key, {:zadd_single, key, score, member}} end)
  end

  defp compact_data_write_pipeline_ops(31, items) do
    Enum.map(items, fn {key, member} -> {key, {:srem_single, key, member}} end)
  end

  defp compact_data_write_pipeline_ops(32, items) do
    Enum.map(items, fn {key, member} -> {key, {:zrem_single, key, member}} end)
  end

  defp encode_compact_data_write_pipeline_batch_responses(
         results,
         requests,
         counts,
         command_state
       ) do
    {responses, []} =
      requests
      |> Enum.zip(counts)
      |> Enum.map_reduce(results, fn {{lane_id, request_id}, count}, remaining ->
        {frame_results, rest} = Enum.split(remaining, count)

        response =
          Codec.encode_command_response_frames(
            @op_pipeline,
            lane_id,
            request_id,
            :ok,
            compact_pipeline_result_body(frame_results),
            compression: Map.get(command_state, :compression, :none),
            compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
            chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0),
            max_response_bytes: Map.get(command_state, :max_response_bytes)
          )

        {response, rest}
      end)

    responses
  end

  defp compact_pipeline_result_body(results) do
    pairs = Enum.map(results, &compact_set_result_pair/1)

    case Codec.encode_compact_pipeline_response(pairs) do
      payload when is_binary(payload) -> payload
      nil -> pairs
    end
  end

  defp compact_set_pipeline_result_body(results) do
    case compact_ok_result_count(results, 0) do
      {:ok, count} ->
        Codec.encode_compact_ok_count(count)

      :error ->
        results
        |> Enum.map(&compact_set_result_pair/1)
        |> Codec.encode_compact_pipeline_response()
    end
  end

  defp compact_ok_result_count([], count), do: {:ok, count}
  defp compact_ok_result_count([:ok | rest], count), do: compact_ok_result_count(rest, count + 1)

  defp compact_ok_result_count([{:ok, :ok} | rest], count),
    do: compact_ok_result_count(rest, count + 1)

  defp compact_ok_result_count([_other | _rest], _count), do: :error

  defp compact_set_result_pair(result) do
    {status, value} = plain_set_response(result)
    [Atom.to_string(status), value]
  end

  defp try_compact_mget_batch(frames, command_state) do
    with true <- plain_set_batch_allowed?(command_state),
         {:ok, requests, keys} <- extract_compact_mget_frames(frames, [], []),
         :ok <- InternalKey.authorize_public(keys) do
      Stats.incr_commands_by(command_state.stats_counter, length(requests))

      responses =
        command_state.instance_ctx
        |> Router.batch_get(keys)
        |> encode_compact_mget_batch_responses(requests, command_state)

      {:ok, responses}
    else
      _ -> :fallback
    end
  rescue
    _ -> :fallback
  end

  defp extract_compact_mget_frames([], requests, key_chunks) do
    {:ok, Enum.reverse(requests), key_chunks |> Enum.reverse() |> List.flatten()}
  end

  defp extract_compact_mget_frames(
         [{lane_id, @op_mget, request_id, @flag_custom_payload, body} | rest],
         requests,
         key_chunks
       ) do
    case Codec.decode_body(@op_mget, @flag_custom_payload, body) do
      {:ok, %{"keys" => keys}} when is_list(keys) ->
        extract_compact_mget_frames(
          rest,
          [{lane_id, request_id, length(keys)} | requests],
          [keys | key_chunks]
        )

      _ ->
        :fallback
    end
  end

  defp extract_compact_mget_frames(_frames, _requests, _key_chunks), do: :fallback

  defp encode_compact_mget_batch_responses(values, requests, command_state) do
    {responses, []} =
      Enum.map_reduce(requests, values, fn {lane_id, request_id, count}, remaining ->
        {frame_values, rest} = Enum.split(remaining, count)

        {status, payload} =
          case ReadResult.first_failure(frame_values) do
            nil -> {:ok, frame_values}
            failure -> ReadResult.command_error(failure)
          end

        response =
          Codec.encode_command_response_frames(@op_mget, lane_id, request_id, status, payload,
            compression: Map.get(command_state, :compression, :none),
            compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
            chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0),
            max_response_bytes: Map.get(command_state, :max_response_bytes)
          )

        {response, rest}
      end)

    responses
  end

  defp try_plain_get_batch(frames, command_state) do
    with true <- plain_set_batch_allowed?(command_state),
         {:ok, requests, keys} <- extract_plain_get_frames(frames, [], []),
         :ok <- InternalKey.authorize_public(keys),
         {:ok, values} <- batch_get_values(keys, command_state) do
      Stats.incr_commands_by(command_state.stats_counter, length(keys))

      responses =
        encode_plain_get_batch_responses(values, requests, command_state)

      {:ok, responses}
    else
      _ -> :fallback
    end
  rescue
    _ -> :fallback
  end

  defp batch_get_values(keys, %{instance_ctx: ctx} = command_state) do
    case Map.get(command_state, :max_response_bytes) do
      limit when is_integer(limit) and limit > 0 ->
        Router.batch_get_each_bounded(ctx, keys, max(limit - 7, 0))

      _unbounded ->
        {:ok, Router.batch_get(ctx, keys)}
    end
  end

  defp extract_plain_get_frames([], requests, keys),
    do: {:ok, Enum.reverse(requests), Enum.reverse(keys)}

  defp extract_plain_get_frames(
         [{lane_id, opcode, request_id, 0, body} | rest],
         requests,
         keys
       )
       when opcode == @op_get do
    case Codec.decode_body(opcode, 0, body) do
      {:ok, %{"key" => key} = payload} when is_binary(key) ->
        if map_size(payload) == 1 do
          extract_plain_get_frames(rest, [{opcode, lane_id, request_id} | requests], [key | keys])
        else
          :fallback
        end

      _ ->
        :fallback
    end
  end

  defp extract_plain_get_frames(_frames, _requests, _keys), do: :fallback

  defp encode_plain_get_batch_responses(values, requests, command_state) do
    requests
    |> Enum.zip(values)
    |> Enum.map(fn {{opcode, lane_id, request_id}, value} ->
      {status, payload} =
        case value do
          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)

          value ->
            {:ok, value}
        end

      Codec.encode_command_response_frames(opcode, lane_id, request_id, status, payload,
        compression: Map.get(command_state, :compression, :none),
        compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
        chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0),
        max_response_bytes: Map.get(command_state, :max_response_bytes)
      )
    end)
  end

  defp try_plain_set_batch(frames, command_state) do
    case prepare_plain_set_batch(frames, command_state) do
      {:ok, requests, kv_pairs} ->
        Stats.incr_commands_by(command_state.stats_counter, length(kv_pairs))

        execute_prepared_batch(
          fn -> Router.batch_quorum_put(command_state.instance_ctx, kv_pairs) end,
          &encode_plain_set_batch_responses(&1, requests, command_state)
        )

      :fallback ->
        :fallback
    end
  end

  defp prepare_plain_set_batch(frames, command_state) do
    with true <- plain_set_batch_allowed?(command_state),
         true <- pressure_ok?(command_state),
         {:ok, requests, kv_pairs} <- extract_plain_set_frames(frames, [], []),
         :ok <- authorize_batch_pairs(kv_pairs) do
      {:ok, requests, kv_pairs}
    else
      _ -> :fallback
    end
  rescue
    _ -> :fallback
  end

  defp plain_set_batch_allowed?(%{
         acl_cache: :full_access,
         require_auth: false,
         instance_ctx: ctx
       })
       when not is_nil(ctx),
       do: FerricStore.ResourceLimits.default_implementation?()

  defp plain_set_batch_allowed?(_command_state), do: false

  defp pressure_ok?(%{instance_ctx: %{pressure_flags: flags}}) when not is_nil(flags) do
    :atomics.get(flags, 1) == 0 and :atomics.get(flags, 2) == 0
  end

  defp pressure_ok?(_command_state), do: true

  defp authorize_batch_pairs(pairs) do
    if Enum.any?(pairs, fn {key, _value} -> InternalKey.reserved?(key) end),
      do: {:error, InternalKey.error_message()},
      else: :ok
  end

  defp authorize_keyed_commands(commands) do
    if Enum.any?(commands, fn {key, _command} -> InternalKey.reserved?(key) end),
      do: {:error, InternalKey.error_message()},
      else: :ok
  end

  defp extract_plain_set_frames([], requests, kv_pairs),
    do: {:ok, Enum.reverse(requests), Enum.reverse(kv_pairs)}

  defp extract_plain_set_frames(
         [{lane_id, @op_set, request_id, 0, body} | rest],
         requests,
         kv_pairs
       ) do
    case Codec.decode_body(@op_set, 0, body) do
      {:ok, %{"key" => key, "value" => value} = payload}
      when is_binary(key) and is_binary(value) ->
        if plain_set_payload?(payload) do
          extract_plain_set_frames(rest, [{lane_id, request_id} | requests], [
            {key, value} | kv_pairs
          ])
        else
          :fallback
        end

      _ ->
        :fallback
    end
  end

  defp extract_plain_set_frames(_frames, _requests, _kv_pairs), do: :fallback

  defp plain_set_payload?(payload) when is_map(payload) do
    not (Map.has_key?(payload, "ttl") or Map.has_key?(payload, "nx") or
           Map.has_key?(payload, "xx") or Map.has_key?(payload, "get") or
           Map.has_key?(payload, "keepttl") or Map.has_key?(payload, "exat") or
           Map.has_key?(payload, "pxat") or Map.has_key?(payload, "deadline_ms"))
  end

  defp encode_plain_set_batch_responses(results, requests, command_state) do
    requests
    |> Enum.zip(results)
    |> Enum.map(fn {{lane_id, request_id}, result} ->
      {status, value} = plain_set_response(result)

      Codec.encode_command_response_frames(@op_set, lane_id, request_id, status, value,
        compression: Map.get(command_state, :compression, :none),
        compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
        chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0),
        max_response_bytes: Map.get(command_state, :max_response_bytes)
      )
    end)
  end

  defp plain_set_response(:ok), do: {:ok, "OK"}
  defp plain_set_response({:ok, :ok}), do: {:ok, "OK"}
  defp plain_set_response({:ok, value}), do: {:ok, value}

  defp plain_set_response({:error, reason}) when is_binary(reason) do
    status =
      cond do
        String.starts_with?(reason, "BUSY") -> :busy
        String.starts_with?(reason, "OOM") -> :busy
        true -> :error
      end

    {status, reason}
  end

  defp plain_set_response({:error, reason}), do: {:error, inspect(reason)}
  defp plain_set_response(value), do: {:ok, value}

  defp execute_frame({:native_trace, frame, trace}, command_state) do
    if no_reply?(frame) do
      execute_frame_without_response({:native_trace, frame, trace}, command_state)
      :noreply
    else
      execute_frame_with_response({:native_trace, frame, trace}, command_state)
    end
  end

  defp execute_frame(frame, command_state) do
    if no_reply?(frame) do
      execute_frame_without_response(frame, command_state)
      :noreply
    else
      execute_frame_with_response(frame, command_state)
    end
  end

  defp execute_frame_without_response(
         {:native_trace, {_lane_id, opcode, _request_id, flags, body}, trace},
         command_state
       ) do
    case Codec.decode_body(opcode, flags, body) do
      {:ok, payload} ->
        Commands.mark_command_seen(command_state)
        _result = execute_traced_command(opcode, payload, command_state, trace)
        :ok

      {:error, _reason} ->
        :ok
    end
  end

  defp execute_frame_without_response({_lane_id, opcode, _request_id, flags, body}, command_state) do
    case Codec.decode_body(opcode, flags, body) do
      {:ok, payload} ->
        Commands.mark_command_seen(command_state)
        _result = Commands.execute(opcode, payload, command_state)
        :ok

      {:error, _reason} ->
        :ok
    end
  end

  defp execute_frame_with_response(
         {:native_trace, {lane_id, opcode, request_id, flags, body}, trace},
         command_state
       ) do
    queue_done_us = monotonic_us()

    trace =
      put_trace_duration(
        trace,
        "server_lane_queue_wait_us",
        queue_done_us,
        "server_lane_enqueue_us"
      )

    decode_started_us = monotonic_us()

    case Codec.decode_body(opcode, flags, body) do
      {:ok, payload} ->
        decode_done_us = monotonic_us()
        Commands.mark_command_seen(command_state)

        {status, value, execute_started_us, execute_done_us, trace} =
          execute_traced_command(opcode, payload, command_state, trace)

        trace =
          trace
          |> put_duration("server_body_decode_us", decode_started_us, decode_done_us)
          |> put_duration("server_command_execute_us", execute_started_us, execute_done_us)

        encode_traced_response(
          opcode,
          lane_id,
          request_id,
          status,
          value,
          trace,
          command_state
        )

      {:error, reason} ->
        decode_done_us = monotonic_us()

        trace =
          put_duration(trace, "server_body_decode_us", decode_started_us, decode_done_us)

        encode_traced_response(
          opcode,
          lane_id,
          request_id,
          :bad_request,
          reason,
          trace,
          command_state
        )
    end
  end

  defp execute_frame_with_response({lane_id, opcode, request_id, flags, body}, command_state) do
    case Codec.decode_body(opcode, flags, body) do
      {:ok, payload} ->
        Commands.mark_command_seen(command_state)
        {status, value, _state} = Commands.execute(opcode, payload, command_state)

        Codec.encode_command_response_frames(opcode, lane_id, request_id, status, value,
          compression: Map.get(command_state, :compression, :none),
          compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
          chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0),
          max_response_bytes: Map.get(command_state, :max_response_bytes)
        )

      {:error, reason} ->
        Codec.encode_command_response_frames(opcode, lane_id, request_id, :bad_request, reason,
          compression: Map.get(command_state, :compression, :none),
          compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
          chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0),
          max_response_bytes: Map.get(command_state, :max_response_bytes)
        )
    end
  end

  defp encode_traced_response(opcode, lane_id, request_id, status, value, trace, command_state) do
    encode_started_us = monotonic_us()

    _measurement_frames =
      encode_trace_frames(opcode, lane_id, request_id, status, value, trace, command_state)

    encode_done_us = monotonic_us()
    trace = put_duration(trace, "server_response_encode_us", encode_started_us, encode_done_us)

    encode_trace_frames(opcode, lane_id, request_id, status, value, trace, command_state)
  end

  defp encode_trace_frames(opcode, lane_id, request_id, status, value, trace, command_state) do
    Codec.encode_command_response_frames(
      opcode,
      lane_id,
      request_id,
      status,
      %{"value" => value, "trace" => public_trace(trace)},
      compression: Map.get(command_state, :compression, :none),
      compact_flow_responses: false,
      chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0),
      max_response_bytes: Map.get(command_state, :max_response_bytes),
      flags: @flag_trace
    )
  end

  defp no_reply?({_lane_id, _opcode, _request_id, flags, _body}),
    do: Bitwise.band(flags, @flag_no_reply) != 0

  defp execute_traced_command(opcode, payload, command_state, trace) do
    previous_trace = LatencyTrace.start(trace)

    try do
      execute_started_us = monotonic_us()
      {status, value, _state} = Commands.execute(opcode, payload, command_state)
      execute_done_us = monotonic_us()
      trace = LatencyTrace.finish(previous_trace)
      {status, value, execute_started_us, execute_done_us, trace}
    rescue
      error ->
        _ = LatencyTrace.finish(previous_trace)
        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        _ = LatencyTrace.finish(previous_trace)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp put_trace_duration(trace, key, now_us, source_key) do
    case Map.get(trace, source_key) do
      started_us when is_integer(started_us) -> Map.put(trace, key, max(now_us - started_us, 0))
      _ -> trace
    end
  end

  defp put_duration(trace, key, started_us, done_us),
    do: Map.put(trace, key, max(done_us - started_us, 0))

  defp public_trace(trace), do: Map.delete(trace, "server_lane_enqueue_us")

  defp monotonic_us, do: System.monotonic_time(:microsecond)
end
