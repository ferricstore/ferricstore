defmodule FerricstoreServer.Native.LaneTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Native.{Codec, Lane}

  @op_pipeline 0x000E
  @op_get 0x0101
  @op_set 0x0102
  @lane_id 7
  @op_hget 0x0111

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    :ok
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

  test "drains ready one-frame SET messages into one lane batch" do
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    :erlang.suspend_process(pid)
    Lane.enqueue(pid, set_frame(25, "native:lane:set:drain:a", "a"))
    Lane.enqueue(pid, set_frame(26, "native:lane:set:drain:b", "b"))
    :erlang.resume_process(pid)

    assert_receive {:native_lane_responses, @lane_id, responses, 2}, 250
    assert length(responses) == 2
    assert_ok_response(Enum.at(responses, 0), @op_set, 25)
    assert_ok_response(Enum.at(responses, 1), @op_set, 26)
  end

  test "coalesces compact pipeline SET frames into one lane batch" do
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue_many(pid, [
      compact_set_pipeline_frame(31, [{"native:lane:pipeline:set:a", "a"}]),
      compact_set_pipeline_frame(32, [{"native:lane:pipeline:set:b", "b"}])
    ])

    assert_receive {:native_lane_responses, @lane_id, responses, 2}, 250
    assert length(responses) == 2
    assert_compact_ok_count_response(Enum.at(responses, 0), @op_pipeline, 31, 1)
    assert_compact_ok_count_response(Enum.at(responses, 1), @op_pipeline, 32, 1)
  end

  test "drains ready one-frame compact pipeline SET messages into one lane batch" do
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    :erlang.suspend_process(pid)
    Lane.enqueue(pid, compact_set_pipeline_frame(41, [{"native:lane:pipeline:drain:a", "a"}]))
    Lane.enqueue(pid, compact_set_pipeline_frame(42, [{"native:lane:pipeline:drain:b", "b"}]))
    :erlang.resume_process(pid)

    assert_receive {:native_lane_responses, @lane_id, responses, 2}, 250
    assert length(responses) == 2
    assert_compact_ok_count_response(Enum.at(responses, 0), @op_pipeline, 41, 1)
    assert_compact_ok_count_response(Enum.at(responses, 1), @op_pipeline, 42, 1)
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

    assert_receive {:native_lane_responses, @lane_id, responses, 7}, 500
    assert length(responses) == 7

    Enum.zip(responses, 51..57)
    |> Enum.each(fn {response, request_id} ->
      assert_pipeline_pairs_response(response, @op_pipeline, request_id, [["ok", 1]])
    end)
  end

  test "drains ready compact pipeline data write messages into one lane batch" do
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

    assert_receive {:native_lane_responses, @lane_id, responses, 2}, 500
    assert length(responses) == 2
    assert_pipeline_pairs_response(Enum.at(responses, 0), @op_pipeline, 61, [["ok", 1]])
    assert_pipeline_pairs_response(Enum.at(responses, 1), @op_pipeline, 62, [["ok", 1]])
  end

  test "preserves order between compact data write and following read frames" do
    key = "native:lane:data:barrier"
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue(pid, compact_data_write_pipeline_frame(71, 22, [{key, "field", "visible"}]))
    Lane.enqueue(pid, hget_frame(72, key, "field"))

    assert_receive {:native_lane_responses, @lane_id, hset_responses, 1}, 500
    assert_pipeline_pairs_response(Enum.at(hset_responses, 0), @op_pipeline, 71, [["ok", 1]])

    assert_receive {:native_lane_response, @lane_id, hget_response}, 500
    assert_response(hget_response, @op_hget, 72, "visible")
  end

  test "preserves order between SET and following GET frames" do
    key = "native:lane:coalesce:barrier"
    {:ok, pid} = Lane.start_link(self(), @lane_id, command_state())
    on_exit(fn -> Lane.stop(pid) end)

    Lane.enqueue(pid, set_frame(11, key, "visible"))
    Lane.enqueue(pid, get_frame(12, key))

    assert_receive {:native_lane_responses, @lane_id, set_responses, 1}, 250
    assert_ok_response(Enum.at(set_responses, 0), @op_set, 11)

    assert_receive {:native_lane_response, @lane_id, get_response}, 250
    assert_response(get_response, @op_get, 12, "visible")
  end

  defp command_state do
    ctx = FerricStore.Instance.get(:default)

    %{
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      acl_cache: :full_access,
      require_auth: false,
      compression: :none,
      compact_flow_responses: false,
      response_chunk_bytes: 0
    }
  end

  defp set_frame(request_id, key, value) do
    {@lane_id, @op_set, request_id, 0, Codec.encode_value(%{"key" => key, "value" => value})}
  end

  defp get_frame(request_id, key) do
    {@lane_id, @op_get, request_id, 0, Codec.encode_value(%{"key" => key})}
  end

  defp hget_frame(request_id, key, field) do
    {@lane_id, @op_hget, request_id, 0, Codec.encode_value(%{"key" => key, "field" => field})}
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
end
