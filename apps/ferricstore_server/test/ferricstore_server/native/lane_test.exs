defmodule FerricstoreServer.Native.LaneTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Native.{Codec, Lane, ResourceBudget}

  @op_pipeline 0x000E
  @op_get 0x0101
  @op_set 0x0102
  @op_fetch_or_compute 0x010B
  @op_fetch_or_compute_result 0x010C
  @lane_id 7
  @op_hget 0x0111
  @receive_timeout 5_000

  defmodule AuditedResourceLimits do
    @behaviour FerricStore.ResourceLimits

    @impl true
    def set_limit(_scope, _limit_spec, _opts), do: {:error, :unsupported}

    @impl true
    def get_limit(_scope, _opts), do: {:error, :unsupported}

    @impl true
    def usage(_scope, _opts), do: {:ok, %{}}

    @impl true
    def check(_scope, _resource, _amount, _opts), do: :ok

    @impl true
    def reserve(_scope, _resource, _amount, _opts), do: {:ok, nil}

    @impl true
    def release(_reservation, _opts), do: :ok

    @impl true
    def check_command(command, args, keys, _opts) do
      send(test_pid(), {:resource_check, command, args, keys})

      if Enum.any?(keys, &String.contains?(&1, ":blocked")) do
        {:error, "ERR quota blocked by test"}
      else
        :ok
      end
    end

    @impl true
    def record_activity(keys, _opts) do
      send(test_pid(), {:resource_activity, keys})
      :ok
    end

    defp test_pid, do: :persistent_term.get({__MODULE__, :test_pid})
  end

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    previous_resource_limits = Application.get_env(:ferricstore, FerricStore.ResourceLimits)

    on_exit(fn ->
      case previous_resource_limits do
        nil -> Application.delete_env(:ferricstore, FerricStore.ResourceLimits)
        module -> Application.put_env(:ferricstore, FerricStore.ResourceLimits, module)
      end

      :persistent_term.erase({AuditedResourceLimits, :test_pid})
    end)

    :ok
  end

  test "prepared fast batches never retry an applied mutation after encoding fails" do
    test_pid = self()

    execute = fn ->
      send(test_pid, :mutation_applied)
      :result
    end

    encode = fn :result -> raise "response encoding failed" end

    assert_raise RuntimeError, "response encoding failed", fn ->
      Lane.execute_prepared_batch(execute, encode)
    end

    assert_receive :mutation_applied
    refute_receive :mutation_applied
  end

  test "lane workers share server-wide lane capacity" do
    budget = :"native_lane_budget_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: budget,
       limits: %{executions: 1, lanes: 1, blocking_requests: 1, chunk_streams: 1, chunk_bytes: 1}}
    )

    command_state = Map.put(command_state(), :resource_budget, budget)
    assert {:ok, lane} = Lane.start_link(self(), 1001, command_state)
    assert {:error, {:limit, :lanes}} = Lane.start_link(self(), 1002, command_state)

    Process.unlink(lane)
    Process.exit(lane, :kill)
    assert eventually(fn -> ResourceBudget.usage(budget).lanes == 0 end)

    assert {:ok, replacement} = Lane.start_link(self(), 1002, command_state)
    Lane.stop(replacement)
  end

  test "lane reports resource governor failure instead of polling forever" do
    budget = :"native_lane_unavailable_#{System.unique_integer([:positive])}"

    {:ok, budget_pid} =
      ResourceBudget.start_link(
        name: budget,
        limits: %{executions: 1, lanes: 1, blocking_requests: 1, chunk_streams: 1, chunk_bytes: 1}
      )

    command_state = Map.put(command_state(), :resource_budget, budget)
    assert {:ok, lane} = Lane.start_link(self(), @lane_id, command_state)
    on_exit(fn -> if Process.alive?(lane), do: Lane.stop(lane) end)

    :ok = GenServer.stop(budget_pid)
    Lane.enqueue(lane, get_frame(10, "native:lane:unavailable"))

    assert_receive {:native_lane_response, @lane_id, response}, 500
    assert_error_response(response, @op_get, 10)
  end

  @tag :fetch_or_compute_execution_budget
  test "fetch waiters do not starve their completion at the global execution limit" do
    budget = :"native_fetch_completion_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: budget,
       limits: %{executions: 1, lanes: 2, blocking_requests: 1, chunk_streams: 1, chunk_bytes: 1}}
    )

    key = "native:lane:fetch-completion:#{System.unique_integer([:positive])}"
    {:ok, _deleted} = FerricStore.del([key])
    assert {:ok, {:compute, _hint, token}} = FerricStore.fetch_or_compute(key, ttl: 5_000)

    on_exit(fn ->
      _ = FerricStore.fetch_or_compute_error(key, "test cleanup", token: token)
      _ = FerricStore.del([key])
    end)

    command_state = Map.put(command_state(), :resource_budget, budget)
    assert {:ok, waiter_lane} = Lane.start_link(self(), @lane_id, command_state)
    assert {:ok, completion_lane} = Lane.start_link(self(), @lane_id, command_state)

    on_exit(fn ->
      if Process.alive?(waiter_lane), do: Lane.stop(waiter_lane)
      if Process.alive?(completion_lane), do: Lane.stop(completion_lane)
    end)

    Lane.enqueue(waiter_lane, fetch_or_compute_frame(91, key))

    assert eventually(fn ->
             ResourceBudget.usage(budget) == %{
               executions: 0,
               lanes: 2,
               blocking_requests: 1,
               chunk_streams: 0,
               chunk_bytes: 0,
               inbound_bytes: 0,
               subscription_bytes: 0
             }
           end)

    Lane.enqueue(completion_lane, fetch_or_compute_result_frame(92, key, token, "computed"))

    assert_receive {:native_lane_response, @lane_id, first_response}, @receive_timeout
    assert_receive {:native_lane_response, @lane_id, second_response}, @receive_timeout

    responses = Map.new([first_response, second_response], &{response_request_id(&1), &1})
    completion_response = Map.fetch!(responses, 92)
    waiter_response = Map.fetch!(responses, 91)

    assert_response(completion_response, @op_fetch_or_compute_result, 92, "OK")
    assert_response(waiter_response, @op_fetch_or_compute, 91, ["hit", "computed"])
  end

  test "already batched SET frames execute immediately without coalescing delay" do
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue_many(pid, [
      set_frame(21, "native:lane:coalesce:batched:a", "a"),
      set_frame(22, "native:lane:coalesce:batched:b", "b")
    ])

    assert_receive {:native_lane_responses, @lane_id, responses, 2}, 50
    assert length(responses) == 2
    assert_ok_response(Enum.at(responses, 0), @op_set, 21)
    assert_ok_response(Enum.at(responses, 1), @op_set, 22)
  end

  test "batched GET and SET frames cannot bypass reserved-key authorization" do
    digest = Base.url_encode64(:crypto.hash(:sha256, inspect(make_ref())), padding: false)
    reserved = "f:{f:#{digest}}:s:lane-probe"
    ordinary = "native:lane:reserved:ordinary"
    ctx = FerricStore.Instance.get(:default)

    :ok = Ferricstore.Store.Router.put(ctx, reserved, "protected", 0)
    :ok = Ferricstore.Store.Router.put(ctx, ordinary, "ordinary", 0)

    on_exit(fn ->
      Ferricstore.Store.Router.delete(ctx, reserved)
      Ferricstore.Store.Router.delete(ctx, ordinary)
    end)

    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue_many(pid, [get_frame(201, ordinary), get_frame(202, reserved)])

    assert_receive {:native_lane_responses, @lane_id, get_responses, 2}, @receive_timeout
    assert_response(Enum.at(get_responses, 0), @op_get, 201, "ordinary")
    assert_error_response(Enum.at(get_responses, 1), @op_get, 202)

    Lane.enqueue_many(pid, [
      set_frame(203, ordinary, "stored"),
      set_frame(204, reserved, "forged")
    ])

    assert_receive {:native_lane_responses, @lane_id, set_responses, 2}, @receive_timeout
    assert_ok_response(Enum.at(set_responses, 0), @op_set, 203)
    assert_error_response(Enum.at(set_responses, 1), @op_set, 204)
    assert "protected" == Ferricstore.Store.Router.get(ctx, reserved)
  end

  @tag :native_response_byte_budget
  test "batched GET and MGET reject oversized cold values before materializing them" do
    ctx = FerricStore.Instance.get(:default)
    key = "native:lane:cold-byte-budget:#{System.unique_integer([:positive])}"
    value = String.duplicate("x", 128)
    shard_index = Ferricstore.Store.Router.shard_for(ctx, key)
    keydir = elem(ctx.keydir_refs, shard_index)

    assert :ok = Ferricstore.Store.Router.put(ctx, key, value, 0)
    assert :ok = GenServer.call(elem(ctx.shard_names, shard_index), :flush, 5_000)
    on_exit(fn -> Ferricstore.Store.Router.delete(ctx, key) end)

    assert [{^key, ^value, expire_at_ms, lfu, file_id, offset, 128}] = :ets.lookup(keydir, key)
    assert true = :ets.insert(keydir, {key, nil, expire_at_ms, lfu, file_id, offset, 128})
    cold_reads_before = Ferricstore.Stats.total_cold_reads(ctx)

    {:ok, pid} =
      Lane.start_link(self(), @lane_id, Map.put(command_state(), :max_response_bytes, 32))

    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue_many(pid, [get_frame(301, key), get_frame(302, key)])
    assert_receive {:native_lane_responses, @lane_id, get_responses, 2}, @receive_timeout
    Enum.each(get_responses, &assert_error_response(&1, @op_get, response_request_id(&1)))

    Lane.enqueue_many(pid, [mget_frame(303, [key]), mget_frame(304, [key])])
    assert_receive {:native_lane_response, @lane_id, first_mget_response}, @receive_timeout
    assert_receive {:native_lane_response, @lane_id, second_mget_response}, @receive_timeout
    mget_responses = [first_mget_response, second_mget_response]
    Enum.each(mget_responses, &assert_error_response(&1, 0x0104, response_request_id(&1)))

    assert Ferricstore.Stats.total_cold_reads(ctx) == cold_reads_before
    assert [{^key, nil, ^expire_at_ms, ^lfu, ^file_id, ^offset, 128}] = :ets.lookup(keydir, key)
  end

  test "batched SET frames honor resource checks and activity accounting" do
    install_audited_resource_limits()
    allowed = "native:lane:governance:allowed"
    blocked = "native:lane:governance:blocked"
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue_many(pid, [
      set_frame(23, allowed, "stored"),
      set_frame(24, blocked, "rejected")
    ])

    assert_receive {:native_lane_responses, @lane_id, responses, 2}, @receive_timeout
    assert_ok_response(Enum.at(responses, 0), @op_set, 23)
    assert_error_response(Enum.at(responses, 1), @op_set, 24)
    assert_receive {:resource_check, "SET", [^allowed, "stored"], [^allowed]}
    assert_receive {:resource_check, "SET", [^blocked, "rejected"], [^blocked]}
    assert_receive {:resource_activity, [^allowed]}
    refute_receive {:resource_activity, [^blocked]}, 50
    assert {:ok, "stored"} == FerricStore.get(allowed)
    assert {:ok, nil} == FerricStore.get(blocked)
  end

  test "one-frame SET messages retain mailbox order" do
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    :erlang.suspend_process(pid)
    Lane.enqueue(pid, set_frame(25, "native:lane:set:drain:a", "a"))
    Lane.enqueue(pid, set_frame(26, "native:lane:set:drain:b", "b"))
    :erlang.resume_process(pid)

    assert_receive {:native_lane_response, @lane_id, response}, @receive_timeout
    assert_ok_response(response, @op_set, 25)
    assert_receive {:native_lane_response, @lane_id, response}, @receive_timeout
    assert_ok_response(response, @op_set, 26)
  end

  test "coalesces compact pipeline SET frames into one lane batch" do
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue_many(pid, [
      compact_set_pipeline_frame(31, [{"native:lane:pipeline:set:a", "a"}]),
      compact_set_pipeline_frame(32, [{"native:lane:pipeline:set:b", "b"}])
    ])

    assert_receive {:native_lane_responses, @lane_id, responses, 2}, @receive_timeout
    assert length(responses) == 2
    assert_compact_ok_count_response(Enum.at(responses, 0), @op_pipeline, 31, 1)
    assert_compact_ok_count_response(Enum.at(responses, 1), @op_pipeline, 32, 1)
  end

  test "compact pipeline fast paths cannot bypass resource governance" do
    install_audited_resource_limits()
    allowed = "native:lane:pipeline:governance:allowed"
    blocked = "native:lane:pipeline:governance:blocked"
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue_many(pid, [
      compact_set_pipeline_frame(33, [{allowed, "stored"}]),
      compact_set_pipeline_frame(34, [{blocked, "rejected"}])
    ])

    assert_receive {:native_lane_responses, @lane_id, responses, 2}, @receive_timeout
    assert_compact_ok_count_response(Enum.at(responses, 0), @op_pipeline, 33, 1)

    assert_compact_pipeline_error_response(
      Enum.at(responses, 1),
      @op_pipeline,
      34,
      "ERR quota blocked by test"
    )

    assert_receive {:resource_check, "SET", [^allowed, "stored"], [^allowed]}
    assert_receive {:resource_check, "SET", [^blocked, "rejected"], [^blocked]}
    assert_receive {:resource_activity, [^allowed]}
    refute_receive {:resource_activity, [^blocked]}, 50
    assert {:ok, "stored"} == FerricStore.get(allowed)
    assert {:ok, nil} == FerricStore.get(blocked)
  end

  test "compact pipeline fast paths cannot write reserved keys" do
    digest = Base.url_encode64(:crypto.hash(:sha256, inspect(make_ref())), padding: false)
    reserved = "f:{f:#{digest}}:s:lane-pipeline-probe"
    ordinary = "native:lane:pipeline:reserved:ordinary"
    ctx = FerricStore.Instance.get(:default)

    :ok = Ferricstore.Store.Router.put(ctx, reserved, "protected", 0)

    on_exit(fn ->
      Ferricstore.Store.Router.delete(ctx, reserved)
      Ferricstore.Store.Router.delete(ctx, ordinary)
    end)

    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue_many(pid, [
      compact_set_pipeline_frame(205, [{ordinary, "stored"}]),
      compact_set_pipeline_frame(206, [{reserved, "forged"}])
    ])

    assert_receive {:native_lane_responses, @lane_id, responses, 2}, @receive_timeout
    assert_compact_ok_count_response(Enum.at(responses, 0), @op_pipeline, 205, 1)

    assert_error_response(Enum.at(responses, 1), @op_pipeline, 206)

    assert "protected" == Ferricstore.Store.Router.get(ctx, reserved)
  end

  test "one-frame compact pipeline SET messages retain mailbox order" do
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    :erlang.suspend_process(pid)
    Lane.enqueue(pid, compact_set_pipeline_frame(41, [{"native:lane:pipeline:drain:a", "a"}]))
    Lane.enqueue(pid, compact_set_pipeline_frame(42, [{"native:lane:pipeline:drain:b", "b"}]))
    :erlang.resume_process(pid)

    assert_receive {:native_lane_response, @lane_id, response}, @receive_timeout
    assert_compact_ok_count_response(response, @op_pipeline, 41, 1)
    assert_receive {:native_lane_response, @lane_id, response}, @receive_timeout
    assert_compact_ok_count_response(response, @op_pipeline, 42, 1)
  end

  test "coalesces compact pipeline data write frames into one lane batch" do
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue_many(pid, [
      compact_data_write_pipeline_frame(51, 22, [
        {"native:lane:data:hset", "field", "value"}
      ]),
      compact_data_write_pipeline_frame(52, 23, [{"native:lane:data:lpush", "a"}]),
      compact_data_write_pipeline_frame(53, 24, [{"native:lane:data:rpush", "a"}]),
      compact_data_write_pipeline_frame(54, 25, [{"native:lane:data:set", "member"}]),
      compact_data_write_pipeline_frame(55, 31, [{"native:lane:data:set", "member"}]),
      compact_data_write_pipeline_frame(56, 26, [{"native:lane:data:zset", 1.5, "member"}]),
      compact_data_write_pipeline_frame(57, 32, [{"native:lane:data:zset", "member"}])
    ])

    assert_receive {:native_lane_responses, @lane_id, responses, 7}, @receive_timeout
    assert length(responses) == 7

    Enum.zip(responses, 51..57)
    |> Enum.each(fn {response, request_id} ->
      assert_pipeline_pairs_response(response, @op_pipeline, request_id, [["ok", 1]])
    end)
  end

  test "one-frame compact data writes retain mailbox order" do
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    :erlang.suspend_process(pid)

    Lane.enqueue(
      pid,
      compact_data_write_pipeline_frame(61, 22, [{"native:lane:data:drain:a", "field", "a"}])
    )

    Lane.enqueue(
      pid,
      compact_data_write_pipeline_frame(62, 22, [{"native:lane:data:drain:b", "field", "b"}])
    )

    :erlang.resume_process(pid)

    assert_receive {:native_lane_response, @lane_id, response}, @receive_timeout
    assert_pipeline_pairs_response(response, @op_pipeline, 61, [["ok", 1]])
    assert_receive {:native_lane_response, @lane_id, response}, @receive_timeout
    assert_pipeline_pairs_response(response, @op_pipeline, 62, [["ok", 1]])
  end

  test "command state updates are ordering barriers for batchable frames" do
    initial_state = command_state()
    {:ok, pid} = Lane.start_link(self(), @lane_id, initial_state)
    on_exit(fn -> Lane.stop(pid) end)

    :erlang.suspend_process(pid)
    Lane.enqueue(pid, set_frame(81, "native:lane:state:before", "ok"))

    Lane.update_command_state(pid, %{
      initial_state
      | authenticated: false,
        require_auth: true
    })

    Lane.enqueue(pid, set_frame(82, "native:lane:state:after", "denied"))
    :erlang.resume_process(pid)

    assert_receive {:native_lane_response, @lane_id, response}, @receive_timeout
    assert_ok_response(response, @op_set, 81)

    assert_receive {:native_lane_response, @lane_id, response}, @receive_timeout
    assert_error_response(response, @op_set, 82)
  end

  test "preserves order between compact data write and following read frames" do
    key = "native:lane:data:barrier"
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue(pid, compact_data_write_pipeline_frame(71, 22, [{key, "field", "visible"}]))
    Lane.enqueue(pid, hget_frame(72, key, "field"))

    assert_receive {:native_lane_response, @lane_id, hset_response}, @receive_timeout
    assert_pipeline_pairs_response(hset_response, @op_pipeline, 71, [["ok", 1]])

    assert_receive {:native_lane_response, @lane_id, hget_response}, @receive_timeout
    assert_response(hget_response, @op_hget, 72, "visible")
  end

  test "preserves order between SET and following GET frames" do
    key = "native:lane:coalesce:barrier"
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue(pid, set_frame(11, key, "visible"))
    Lane.enqueue(pid, get_frame(12, key))

    assert_receive {:native_lane_response, @lane_id, set_response}, @receive_timeout
    assert_ok_response(set_response, @op_set, 11)

    assert_receive {:native_lane_response, @lane_id, get_response}, @receive_timeout
    assert_response(get_response, @op_get, 12, "visible")
  end

  defp command_state do
    ctx = FerricStore.Instance.get(:default)

    %{
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      acl_cache: :full_access,
      authenticated: false,
      require_auth: false,
      compression: :none,
      compact_flow_responses: false,
      response_chunk_bytes: 0
    }
  end

  defp install_audited_resource_limits do
    Application.put_env(:ferricstore, FerricStore.ResourceLimits, AuditedResourceLimits)
    :persistent_term.put({AuditedResourceLimits, :test_pid}, self())
  end

  defp set_frame(request_id, key, value) do
    {@lane_id, @op_set, request_id, 0, Codec.encode_value(%{"key" => key, "value" => value})}
  end

  defp get_frame(request_id, key) do
    {@lane_id, @op_get, request_id, 0, Codec.encode_value(%{"key" => key})}
  end

  defp mget_frame(request_id, keys) do
    {@lane_id, 0x0104, request_id, 0, Codec.encode_value(%{"keys" => keys})}
  end

  defp hget_frame(request_id, key, field) do
    {@lane_id, @op_hget, request_id, 0, Codec.encode_value(%{"key" => key, "field" => field})}
  end

  defp fetch_or_compute_frame(request_id, key) do
    {@lane_id, @op_fetch_or_compute, request_id, 0,
     Codec.encode_value(%{"key" => key, "ttl_ms" => 5_000, "hint" => "wait"})}
  end

  defp fetch_or_compute_result_frame(request_id, key, token, value) do
    {@lane_id, @op_fetch_or_compute_result, request_id, 0,
     Codec.encode_value(%{"key" => key, "token" => token, "value" => value, "ttl_ms" => 5_000})}
  end

  defp compact_set_pipeline_frame(request_id, pairs) do
    body = [
      <<0x94, 0x81, length(pairs)::unsigned-32>>,
      Enum.map(pairs, fn {key, value} ->
        key = IO.iodata_to_binary(key)
        value = IO.iodata_to_binary(value)
        [<<byte_size(key)::unsigned-32>>, key, <<byte_size(value)::unsigned-32>>, value]
      end)
    ]

    {@lane_id, @op_pipeline, request_id, 0x02, IO.iodata_to_binary(body)}
  end

  defp compact_data_write_pipeline_frame(request_id, mode, items) do
    body = [
      <<0x94, mode, length(items)::unsigned-32>>,
      Enum.map(items, &compact_data_write_pipeline_item(mode, &1))
    ]

    {@lane_id, @op_pipeline, request_id, 0x02, IO.iodata_to_binary(body)}
  end

  defp compact_data_write_pipeline_item(22, {key, field, value}) do
    [compact_bin(key), compact_bin(field), compact_bin(value)]
  end

  defp compact_data_write_pipeline_item(mode, {key, value}) when mode in [23, 24, 25, 31, 32] do
    [compact_bin(key), compact_bin(value)]
  end

  defp compact_data_write_pipeline_item(26, {key, score, member}) do
    [compact_bin(key), <<score::float-64>>, compact_bin(member)]
  end

  defp compact_bin(value) do
    value = IO.iodata_to_binary(value)
    [<<byte_size(value)::unsigned-32>>, value]
  end

  defp assert_ok_response(iodata, opcode, request_id) do
    assert_response(iodata, opcode, request_id, "OK")
  end

  defp assert_compact_ok_count_response(iodata, opcode, request_id, expected_count) do
    response = IO.iodata_to_binary(iodata)

    assert <<"FSNP", 0x81, _flags, @lane_id::unsigned-32, ^opcode::unsigned-16,
             ^request_id::unsigned-64, body_len::unsigned-32, body::binary>> = response

    assert body_len == byte_size(body)
    assert <<0::unsigned-16, 0x81, ^expected_count::unsigned-32>> = body
  end

  defp assert_compact_pipeline_error_response(iodata, opcode, request_id, expected_reason) do
    response = IO.iodata_to_binary(iodata)

    assert <<"FSNP", 0x81, _flags, @lane_id::unsigned-32, ^opcode::unsigned-16,
             ^request_id::unsigned-64, body_len::unsigned-32, body::binary>> = response

    assert body_len == byte_size(body)

    assert <<0::unsigned-16, 0x95, 1::unsigned-32, 2, reason_len::unsigned-32,
             reason::binary-size(reason_len)>> = body

    assert reason == expected_reason
  end

  defp assert_pipeline_pairs_response(iodata, opcode, request_id, expected_value) do
    response = IO.iodata_to_binary(iodata)

    assert <<"FSNP", 0x81, _flags, @lane_id::unsigned-32, ^opcode::unsigned-16,
             ^request_id::unsigned-64, body_len::unsigned-32, body::binary>> = response

    assert body_len == byte_size(body)
    assert <<0::unsigned-16, value_body::binary>> = body
    assert {:ok, ^expected_value} = Codec.decode_body(value_body)
  end

  defp assert_response(iodata, opcode, request_id, expected_value) do
    response = IO.iodata_to_binary(iodata)

    assert <<"FSNP", 0x81, _flags, @lane_id::unsigned-32, ^opcode::unsigned-16,
             ^request_id::unsigned-64, body_len::unsigned-32, body::binary>> = response

    assert body_len == byte_size(body)
    assert <<0::unsigned-16, value_body::binary>> = body
    assert response_value(value_body) == expected_value
  end

  defp response_request_id(iodata) do
    <<"FSNP", 0x81, _flags, @lane_id::unsigned-32, _opcode::unsigned-16, request_id::unsigned-64,
      _rest::binary>> = IO.iodata_to_binary(iodata)

    request_id
  end

  defp assert_error_response(iodata, opcode, request_id) do
    response = IO.iodata_to_binary(iodata)

    assert <<"FSNP", 0x81, _flags, @lane_id::unsigned-32, ^opcode::unsigned-16,
             ^request_id::unsigned-64, body_len::unsigned-32, body::binary>> = response

    assert body_len == byte_size(body)
    assert <<status::unsigned-16, _value_body::binary>> = body
    assert status != 0
  end

  defp response_value(value_body) do
    case Codec.decode_body(value_body) do
      {:ok, value} -> value
      {:error, _reason} when value_body == <<0x81, 1::unsigned-32>> -> "OK"
      {:error, _reason} -> compact_get_value(value_body) || value_body
    end
  end

  defp compact_get_value(<<0x82, 1, size::unsigned-32, value::binary-size(size)>>),
    do: value

  defp compact_get_value(_value_body), do: nil

  defp eventually(fun, attempts \\ 50)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end
