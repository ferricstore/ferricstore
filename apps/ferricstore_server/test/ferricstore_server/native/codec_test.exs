defmodule FerricstoreServer.Native.CodecTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Native.Codec
  alias FerricstoreServer.Native.NIF

  setup do
    old_max_value_items = Application.get_env(:ferricstore, :native_max_value_items)
    old_max_value_depth = Application.get_env(:ferricstore, :native_max_value_depth)

    on_exit(fn ->
      restore_env(:native_max_value_items, old_max_value_items)
      restore_env(:native_max_value_depth, old_max_value_depth)
    end)

    :ok
  end

  test "request frames round-trip with lane id and request id" do
    body = Codec.encode_value(%{"key" => "a", "value" => "b"})
    frame = Codec.encode_frame(0x0102, 7, 42, body)

    assert {:ok, [{7, 0x0102, 42, 0, ^body}], "", :done} =
             Codec.decode_frames(frame, 1024)
  end

  test "native NIF decodes the same frame tuple shape as the Elixir codec" do
    body = Codec.encode_value(%{"key" => "a", "value" => "b"})
    frame = Codec.encode_frame(0x0102, 7, 42, body)

    assert {:ok, [{7, 0x0102, 42, 0, ^body}], "", :done} =
             NIF.decode_frames(frame, 1024)
  end

  test "frame decoding bounds one pass and preserves buffered continuation frames" do
    encoded =
      1..129
      |> Enum.map(&Codec.encode_frame(0x0003, 0, &1, ""))
      |> IO.iodata_to_binary()

    assert {:ok, first_batch, rest, :more} =
             Codec.decode_frames(encoded, 16 * 1024 * 1024)

    assert Enum.map(first_batch, &elem(&1, 2)) == Enum.to_list(1..128)

    assert {:ok, [{0, 0x0003, 129, 0, ""}], "", :done} =
             Codec.decode_frames(rest, 16 * 1024 * 1024)
  end

  test "native NIF encodes the same frame header as the Elixir codec" do
    body = Codec.encode_value(%{"key" => "a"})

    assert NIF.encode_frame(0x0101, 3, 9, body, 0, false) ==
             Codec.encode_frame(0x0101, 3, 9, body)
  end

  test "partial request frame leaves bytes in remainder" do
    body = Codec.encode_value(%{"key" => "a"})
    frame = Codec.encode_frame(0x0101, 3, 9, body)
    partial = binary_part(frame, 0, byte_size(frame) - 2)

    assert {:ok, [], ^partial, :done} = Codec.decode_frames(partial, 1024)
  end

  @tag :native_command_peek
  test "command probe skips nested values and uses the last duplicate command" do
    nested_args =
      Codec.encode_value([
        %{"payload" => String.duplicate("x", 64 * 1024)},
        List.duplicate(nil, 32)
      ])

    body =
      IO.iodata_to_binary([
        <<6, 4::unsigned-32>>,
        encoded_map_entry("command", Codec.encode_value("MULTI")),
        encoded_map_entry("args", nested_args),
        encoded_map_entry("command", Codec.encode_value("GET")),
        encoded_map_entry(
          "metadata",
          Codec.encode_value(%{"trace" => true, "epoch" => 0xFFFF_FFFF_FFFF_FFFF})
        )
      ])

    assert {:ok, "GET"} = Codec.peek_command_name(0, body)
  end

  @tag :native_command_peek
  test "command probe rejects custom, malformed, and over-budget typed payloads" do
    flags = Codec.flags()
    body = Codec.encode_value(%{"command" => "GET"})

    assert :not_found = Codec.peek_command_name(flags.custom_payload, body)
    assert {:error, _reason} = Codec.peek_command_name(0, body <> <<0>>)

    Application.put_env(:ferricstore, :native_max_value_items, 1)

    over_budget =
      IO.iodata_to_binary([
        <<6, 2::unsigned-32>>,
        encoded_map_entry("command", Codec.encode_value("GET")),
        encoded_map_entry("args", Codec.encode_value([]))
      ])

    assert {:error, reason} = Codec.peek_command_name(0, over_budget)
    assert reason =~ "max items"
  end

  test "response frames use response direction bit and are rejected as client input" do
    response = Codec.encode_response(0x0101, 2, 99, :ok, "value")

    assert {:error, "ERR native client frame cannot use response direction"} =
             Codec.decode_frames(response, 1024)
  end

  test "response value sizing matches the wire encoding at exact boundaries" do
    values = [nil, true, 42, 1.5, "value", ["a", nil], {"tuple", 3}, %{"key" => [1, 2]}]

    Enum.each(values, fn value ->
      encoded_bytes = value |> Codec.encode_value() |> byte_size()

      assert Codec.encoded_value_fits?(value, encoded_bytes)
      refute Codec.encoded_value_fits?(value, encoded_bytes - 1)
    end)
  end

  test "successful responses over their byte budget become bounded errors" do
    [frame] =
      Codec.encode_command_response_frames(0x0101, 2, 99, :ok, String.duplicate("x", 128),
        max_response_bytes: 64
      )

    <<"FSNP", 0x81, _flags, 2::unsigned-32, 0x0101::unsigned-16, 99::unsigned-64,
      _body_len::unsigned-32, 6::unsigned-16, value_body::binary>> = IO.iodata_to_binary(frame)

    assert {:ok, %{"message" => message}} = Codec.decode_body(value_body)
    assert message == "ERR native response byte limit exceeded"
  end

  test "busy response is structured and retryable" do
    response = Codec.encode_response(0x0101, 2, 99, :busy, "ERR overloaded")

    <<"FSNP", 0x81, _flags, 2::unsigned-32, 0x0101::unsigned-16, 99::unsigned-64,
      body_len::unsigned-32, body::binary>> = response

    assert body_len == byte_size(body)
    <<4::unsigned-16, value_body::binary>> = body
    assert {:ok, value} = Codec.decode_body(value_body)
    assert value["code"] == "busy"
    assert value["message"] == "ERR overloaded"
    assert value["retryable"] == true
    assert value["safe_to_retry"] == true
    assert value["retry_after_ms"] == 100
  end

  test "chunked compressed responses mark compression only on the final logical chunk" do
    frames =
      Codec.encode_response_frames(0x0101, 2, 99, :ok, String.duplicate("x", 256),
        compression: :zlib,
        chunk_bytes: 12
      )

    assert length(frames) > 1
    compressed = Codec.flags().compressed
    more_chunks = Codec.flags().more_chunks

    Enum.each(Enum.drop(frames, -1), fn frame ->
      <<"FSNP", 0x81, flags, _rest::binary>> = frame
      assert Bitwise.band(flags, more_chunks) != 0
      assert Bitwise.band(flags, compressed) == 0
    end)

    <<"FSNP", 0x81, final_flags, _rest::binary>> = List.last(frames)
    assert Bitwise.band(final_flags, more_chunks) == 0
    assert Bitwise.band(final_flags, compressed) != 0
  end

  test "typed values round-trip nested protocol payloads" do
    value = %{
      "nil" => nil,
      "ok" => true,
      "count" => 123,
      "items" => ["a", %{"b" => false}],
      "ratio" => 1.5
    }

    encoded = Codec.encode_value(value)
    assert {:ok, ^value} = Codec.decode_body(encoded)
  end

  test "typed values round-trip the full unsigned 64-bit domain" do
    value = 0xFFFF_FFFF_FFFF_FFFF

    assert <<8, ^value::unsigned-64>> = Codec.encode_value(value)
    assert {:ok, ^value} = Codec.decode_body(Codec.encode_value(value))
  end

  test "typed value decoder rejects containers over configured item limit" do
    Application.put_env(:ferricstore, :native_max_value_items, 1)

    assert {:error, reason} = Codec.decode_body(Codec.encode_value(["a", "b"]))
    assert reason =~ "max items"
  end

  test "typed value decoder applies the item limit across nested containers" do
    Application.put_env(:ferricstore, :native_max_value_items, 3)

    assert {:ok, [[nil], nil]} = Codec.decode_body(Codec.encode_value([[nil], nil]))

    assert {:error, reason} =
             Codec.decode_body(Codec.encode_value([[nil, nil], [nil, nil]]))

    assert reason =~ "total items"
  end

  test "typed value decoder rejects nesting over configured depth limit" do
    Application.put_env(:ferricstore, :native_max_value_depth, 1)

    assert {:error, reason} = Codec.decode_body(Codec.encode_value([["nested"]]))
    assert reason =~ "max depth"
  end

  @tag :native_invalid_value_limits
  test "invalid typed value limits fail closed instead of disabling validation" do
    Application.put_env(:ferricstore, :native_max_value_items, -1)

    assert {:error, item_reason} = Codec.decode_body(Codec.encode_value(["item"]))
    assert item_reason =~ "max items"

    Application.put_env(:ferricstore, :native_max_value_items, 100_000)
    Application.put_env(:ferricstore, :native_max_value_depth, -1)

    assert {:error, depth_reason} = Codec.decode_body(Codec.encode_value([]))
    assert depth_reason =~ "max depth"
  end

  test "compact Flow claim jobs response uses custom payload tag" do
    payload =
      Codec.encode_compact_flow_claim_jobs([
        ["flow-1", "bucket-1", "lease-1", 42],
        ["flow-2", nil, "lease-2", 43]
      ])

    tag = Codec.compact_tags().flow_claim_jobs
    assert <<^tag, 2::unsigned-32, _rest::binary>> = payload
  end

  test "compact Flow claim jobs response encodes state-only tuples" do
    payload =
      Codec.encode_compact_flow_claim_jobs([
        ["flow-1", "bucket-1", "lease-1", 42, "running:step"]
      ])

    tag = Codec.compact_tags().flow_claim_jobs
    assert <<^tag, 1::unsigned-32, _rest::binary>> = payload
    assert payload =~ "running:step"
  end

  test "native NIF directly frames state-only compact Flow claims" do
    frame =
      NIF.encode_compact_claim_jobs_response_frame(
        0x0203,
        2,
        99,
        [["flow-1", "bucket-1", "lease-1", 42, "running:step"]]
      )

    custom_payload = Codec.flags().custom_payload
    tag = Codec.compact_tags().flow_claim_jobs

    assert <<"FSNP", 0x81, ^custom_payload, 2::unsigned-32, 0x0203::unsigned-16, 99::unsigned-64,
             _body_len::unsigned-32, 0::unsigned-16, ^tag, 1::unsigned-32, rest::binary>> = frame

    assert rest =~ "running:step"
  end

  test "compact Flow claim jobs response can include attributes" do
    attrs = %{"tenant" => "acme"}

    payload =
      Codec.encode_compact_flow_claim_jobs([
        ["flow-1", "bucket-1", "lease-1", 42, attrs],
        ["flow-2", nil, "lease-2", 43, %{}]
      ])

    tag = Codec.compact_tags().flow_claim_jobs
    encoded_attrs = Codec.encode_value(attrs)

    assert <<^tag, 2::unsigned-32, _rest::binary>> = payload
    assert :binary.match(payload, encoded_attrs) != :nomatch
  end

  test "compact Flow claim jobs response rejects mixed tuple modes" do
    jobs = [
      ["flow-1", "bucket-1", "lease-1", 42],
      ["flow-2", nil, "lease-2", 43, "running:step"]
    ]

    assert Codec.encode_compact_flow_claim_jobs(jobs) == nil
    assert NIF.encode_compact_claim_jobs_response_frame(0x0203, 2, 99, jobs) == nil
  end

  test "compact Flow record response encodes known atom fields" do
    tag = Codec.compact_tags().flow_record

    assert <<^tag, 4::unsigned-32, _entries::binary>> =
             Codec.encode_compact_flow_record(%{
               id: "flow-1",
               type: "order",
               state: "queued",
               version: 1
             })
  end

  test "compact Flow record response preserves unknown extension fields" do
    tag = Codec.compact_tags().flow_record

    payload =
      Codec.encode_compact_flow_record(%{
        id: "flow-1",
        type: "order",
        state: "queued",
        custom_extension: "kept"
      })

    assert <<^tag, 4::unsigned-32, _entries::binary>> = payload
    assert payload =~ "custom_extension"
    assert payload =~ "kept"
  end

  test "compact OK-list response stores only count" do
    tag = Codec.compact_tags().ok_list
    assert <<^tag, 3::unsigned-32>> = Codec.encode_compact_ok_list(["OK", "OK", "OK"])
    assert <<^tag, 3::unsigned-32>> = Codec.encode_compact_ok_list(["ok", "Ok", "oK"])
    assert <<^tag, 3::unsigned-32>> = Codec.encode_compact_ok_count(3)
    assert Codec.encode_compact_ok_list(["OK", "ERR failed"]) == nil
  end

  test "compact integer-list response stores pipeline count results directly" do
    tag = Codec.compact_tags().integer_list

    assert <<^tag, 3::unsigned-32, 1::signed-64, 0::signed-64, -2::signed-64>> =
             Codec.encode_compact_integer_list([1, 0, -2])

    assert Codec.encode_compact_integer_list([1, "0"]) == nil
  end

  test "native NIF encodes compact MGET payload directly" do
    assert <<0x89, 2::unsigned-32, 2::unsigned-32, "v1", "v2">> =
             Codec.encode_compact_kv_mget(["v1", "v2"])

    assert <<0x83, 3::unsigned-32, 1, 2::unsigned-32, "v1", 0, 1, 2::unsigned-32, "v2">> =
             Codec.encode_compact_kv_mget(["v1", nil, "v2"])

    assert Codec.encode_compact_kv_mget(["v1", 123]) == nil
  end

  test "decodes compact MSET and MGET request bodies" do
    mset_body =
      <<0x94, 1, 2::unsigned-32, compact_bin("k1")::binary, compact_bin("v1")::binary,
        compact_bin("k2")::binary, compact_bin("v2")::binary>>

    assert {:ok, %{"pairs" => [{"k1", "v1"}, {"k2", "v2"}]}} =
             Codec.decode_body(0x0105, Codec.flags().custom_payload, mset_body)

    mget_body = <<0x94, 2, 2::unsigned-32, compact_bin("k1")::binary, compact_bin("k2")::binary>>

    assert {:ok, %{"keys" => ["k1", "k2"]}} =
             Codec.decode_body(0x0104, Codec.flags().custom_payload, mget_body)
  end

  test "compact request decoder rejects collections over the native item budget before allocating" do
    Application.put_env(:ferricstore, :native_max_value_items, 2)

    mget_body =
      <<0x94, 2, 3::unsigned-32, compact_bin("")::binary, compact_bin("")::binary,
        compact_bin("")::binary>>

    assert {:error, reason} =
             Codec.decode_body(0x0104, Codec.flags().custom_payload, mget_body)

    assert reason =~ "exceeds max items"
  end

  test "compact HMGET decoder applies one budget across nested field lists" do
    Application.put_env(:ferricstore, :native_max_value_items, 5)

    body =
      <<0x94, 0x9C, 2::unsigned-32, compact_bin("h1")::binary, 2::unsigned-32,
        compact_bin("f1")::binary, compact_bin("f2")::binary, compact_bin("h2")::binary,
        2::unsigned-32, compact_bin("f3")::binary, compact_bin("f4")::binary>>

    assert {:error, reason} = Codec.decode_body(0x000E, Codec.flags().custom_payload, body)
    assert reason =~ "exceeds max items"
  end

  test "decodes compact SMEMBERS pipeline request body" do
    body = <<0x94, 0x9B, 2::unsigned-32, compact_bin("s1")::binary, compact_bin("s2")::binary>>

    assert {:ok,
            %{
              "atomicity" => "none",
              "compact_count" => 2,
              "compact_values" => true,
              "return" => "compact",
              "compact_pipeline" => {27, ["s1", "s2"]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, body)
  end

  test "decodes compact HMGET and ZSCORE pipeline request bodies" do
    hmget_body =
      <<0x94, 0x9C, 1::unsigned-32, compact_bin("h1")::binary, 2::unsigned-32,
        compact_bin("f1")::binary, compact_bin("f2")::binary>>

    assert {:ok,
            %{
              "atomicity" => "none",
              "compact_count" => 1,
              "compact_values" => true,
              "return" => "compact",
              "compact_pipeline" => {28, [{"h1", ["f1", "f2"]}]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, hmget_body)

    zscore_body =
      <<0x94, 0x9D, 1::unsigned-32, compact_bin("z1")::binary, compact_bin("m1")::binary>>

    assert {:ok,
            %{
              "atomicity" => "none",
              "compact_count" => 1,
              "compact_values" => true,
              "return" => "compact",
              "compact_pipeline" => {29, [{"z1", "m1"}]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, zscore_body)
  end

  test "decodes compact HGETALL pipeline request body and encodes binary map lists" do
    body = <<0x94, 0x9E, 2::unsigned-32, compact_bin("h1")::binary, compact_bin("h2")::binary>>

    assert {:ok,
            %{
              "atomicity" => "none",
              "compact_count" => 2,
              "compact_values" => true,
              "return" => "compact",
              "compact_pipeline" => {30, ["h1", "h2"]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, body)

    assert <<0x87, 1::unsigned-32, 1::unsigned-32, 1::unsigned-32, "f", 1::unsigned-32, "v">> =
             Codec.encode_compact_binary_map_list([%{"f" => "v"}])

    assert Codec.encode_compact_binary_map_list([%{"f" => 1}]) == nil
  end

  test "decodes compact pipeline FLOW.STEP_CONTINUE request bodies" do
    body =
      <<0x94, 0x86, 1::unsigned-32, compact_bin("queued")::binary, compact_bin("next")::binary,
        30_000::signed-64, compact_bin("flow-1")::binary, compact_bin("__flow_auto__:1")::binary,
        compact_bin("lease-1")::binary, 7::signed-64, 123::signed-64>>

    assert {:ok,
            %{
              "atomicity" => "none",
              "compact_count" => 1,
              "compact_values" => true,
              "return" => "compact",
              "compact_pipeline" =>
                {6,
                 [
                   {:flow_step_continue, "flow-1", "lease-1", "queued", "next",
                    [
                      partition_key: "__flow_auto__:1",
                      fencing_token: 7,
                      lease_ms: 30_000,
                      now_ms: 123
                    ]}
                 ]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, body)
  end

  test "decodes compact pipeline FLOW.VALUE.PUT request bodies" do
    shared_body =
      <<0x94, 0x87, 1::unsigned-32, compact_bin("value-1")::binary, 123::signed-64>>

    assert {:ok,
            %{
              "compact_values" => true,
              "compact_pipeline" => {7, [{"value-1", [now_ms: 123]}]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, shared_body)

    named_body =
      <<0x94, 0x88, 1::unsigned-32, compact_bin("value-1")::binary, compact_bin("flow-1")::binary,
        compact_bin("reservation")::binary, compact_bin("__flow_auto__:1")::binary,
        124::signed-64>>

    assert {:ok,
            %{
              "compact_values" => true,
              "compact_pipeline" =>
                {8,
                 [
                   {:flow_named_value_put, "value-1",
                    [
                      partition_key: "__flow_auto__:1",
                      owner_flow_id: "flow-1",
                      name: "reservation",
                      now_ms: 124
                    ]}
                 ]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, named_body)

    named_ok_body =
      <<0x94, 0x8E, 1::unsigned-32, compact_bin("value-1")::binary, compact_bin("flow-1")::binary,
        compact_bin("reservation")::binary, compact_bin("__flow_auto__:1")::binary,
        125::signed-64>>

    assert {:ok,
            %{
              "compact_values" => true,
              "compact_pipeline" =>
                {14,
                 [
                   {:flow_named_value_put, "value-1",
                    [
                      partition_key: "__flow_auto__:1",
                      owner_flow_id: "flow-1",
                      name: "reservation",
                      now_ms: 125,
                      return: :ok_on_success
                    ]}
                 ]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, named_ok_body)

    shared_ok_body =
      <<0x94, 0x8F, 1::unsigned-32, compact_bin("value-2")::binary, 126::signed-64>>

    assert {:ok,
            %{
              "compact_values" => true,
              "compact_pipeline" => {15, [{"value-2", [now_ms: 126, return: :ok_on_success]}]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, shared_ok_body)
  end

  test "decodes compact pipeline FLOW.GET request bodies" do
    body =
      <<0x94, 0x89, 2::unsigned-32, compact_bin("flow-1")::binary, compact_bin("flow-2")::binary>>

    assert {:ok,
            %{
              "compact_values" => true,
              "compact_pipeline" => {9, [{:flow_get, "flow-1", []}, {:flow_get, "flow-2", []}]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, body)
  end

  test "decodes compact pipeline partitioned FLOW.GET request bodies" do
    body =
      <<0x94, 0x90, 2::unsigned-32, compact_bin("flow-1")::binary,
        compact_bin("tenant-a")::binary, compact_bin("flow-2")::binary,
        compact_bin("tenant-b")::binary>>

    assert {:ok,
            %{
              "compact_values" => true,
              "compact_pipeline" =>
                {16,
                 [
                   {:flow_get, "flow-1", [partition_key: "tenant-a"]},
                   {:flow_get, "flow-2", [partition_key: "tenant-b"]}
                 ]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, body)
  end

  test "decodes compact pipeline FLOW.GET meta request bodies" do
    body =
      <<0x94, 0x91, 2::unsigned-32, compact_bin("flow-1")::binary,
        compact_bin("tenant-a")::binary, compact_bin("flow-2")::binary,
        compact_bin("tenant-b")::binary>>

    assert {:ok,
            %{
              "compact_values" => true,
              "compact_pipeline" =>
                {17,
                 [
                   {:flow_get, "flow-1", [partition_key: "tenant-a", return: :meta]},
                   {:flow_get, "flow-2", [partition_key: "tenant-b", return: :meta]}
                 ]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, body)
  end

  test "decodes compact pipeline FLOW.HISTORY request bodies" do
    body =
      <<0x94, 0x8A, 1::unsigned-32, 10::signed-64, 1, 2, compact_bin("flow-1")::binary,
        compact_bin("__flow_auto__:1")::binary>>

    assert {:ok,
            %{
              "compact_values" => true,
              "compact_pipeline" =>
                {10,
                 [
                   {:flow_history, "flow-1",
                    [
                      partition_key: "__flow_auto__:1",
                      count: 10,
                      include_cold: false,
                      consistent_projection: true
                    ]}
                 ]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, body)
  end

  test "decodes compact pipeline FLOW.SIGNAL request bodies" do
    body =
      <<0x94, 0x8B, 1::unsigned-32, compact_bin("bench_signal")::binary,
        compact_bin("queued")::binary, compact_bin("next")::binary, compact_bin("flow-1")::binary,
        compact_bin("__flow_auto__:1")::binary, 123::signed-64>>

    assert {:ok,
            %{
              "compact_values" => true,
              "compact_pipeline" =>
                {11,
                 [
                   {:flow_signal, "flow-1",
                    [
                      partition_key: "__flow_auto__:1",
                      signal: "bench_signal",
                      if_state: "queued",
                      transition_to: "next",
                      now_ms: 123
                    ]}
                 ]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, body)
  end

  test "decodes compact pipeline FLOW.START_AND_CLAIM request bodies" do
    body =
      <<0x94, 0x8C, 1::unsigned-32, compact_bin("email")::binary, compact_bin("queued")::binary,
        compact_bin("worker-1")::binary, 30_000::signed-64, compact_bin("flow-1")::binary,
        compact_bin("__flow_auto__:1")::binary, compact_bin("payload")::binary, 123::signed-64>>

    assert {:ok,
            %{
              "compact_values" => true,
              "compact_pipeline" =>
                {12,
                 [
                   {:flow_start_and_claim, "flow-1", "email", "queued",
                    [
                      payload: "payload",
                      partition_key: "__flow_auto__:1",
                      worker: "worker-1",
                      lease_ms: 30_000,
                      now_ms: 123
                    ]}
                 ]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, body)
  end

  test "decodes compact pipeline FLOW.START_AND_CLAIM job-only request bodies" do
    body =
      <<0x94, 0x8D, 1::unsigned-32, compact_bin("email")::binary, compact_bin("queued")::binary,
        compact_bin("worker-1")::binary, 30_000::signed-64, compact_bin("flow-1")::binary,
        compact_bin("__flow_auto__:1")::binary, compact_bin("payload")::binary, 123::signed-64>>

    assert {:ok,
            %{
              "compact_values" => true,
              "compact_pipeline" =>
                {13, [{:flow_start_and_claim, "flow-1", "email", "queued", opts}]}
            }} = Codec.decode_body(0x000E, Codec.flags().custom_payload, body)

    assert Keyword.get(opts, :payload) == "payload"
    assert Keyword.get(opts, :partition_key) == "__flow_auto__:1"
    assert Keyword.get(opts, :return) == :jobs_compact
    assert Keyword.get(opts, :worker) == "worker-1"
    assert Keyword.get(opts, :lease_ms) == 30_000
    assert Keyword.get(opts, :now_ms) == 123
  end

  test "native NIF encodes compact OK-list response frame directly" do
    frame = NIF.encode_compact_ok_list_response_frame(0x0210, 3, 42, ["OK", "ok", "Ok"])
    tag = Codec.compact_tags().ok_list
    custom_payload = Codec.flags().custom_payload

    <<"FSNP", 0x81, flags, 3::unsigned-32, 0x0210::unsigned-16, 42::unsigned-64,
      body_len::unsigned-32, body::binary>> = frame

    assert body_len == byte_size(body)
    assert Bitwise.band(flags, custom_payload) != 0
    assert <<0::unsigned-16, ^tag, 3::unsigned-32>> = body
    assert NIF.encode_compact_ok_list_response_frame(0x0210, 3, 42, ["OK", "ERR"]) == nil
  end

  test "native NIF encodes compact KV response frames directly" do
    custom_payload = Codec.flags().custom_payload

    get_frame = NIF.encode_compact_kv_get_response_frame(0x0101, 3, 42, "value")

    <<"FSNP", 0x81, get_flags, 3::unsigned-32, 0x0101::unsigned-16, 42::unsigned-64,
      get_body_len::unsigned-32, get_body::binary>> = get_frame

    assert get_body_len == byte_size(get_body)
    assert Bitwise.band(get_flags, custom_payload) != 0
    assert <<0::unsigned-16, 0x82, 1, 5::unsigned-32, "value">> = get_body

    fixed_mget_frame = NIF.encode_compact_kv_mget_response_frame(0x0104, 4, 44, ["v1", "v2"])

    <<"FSNP", 0x81, fixed_mget_flags, 4::unsigned-32, 0x0104::unsigned-16, 44::unsigned-64,
      fixed_mget_body_len::unsigned-32, fixed_mget_body::binary>> = fixed_mget_frame

    assert fixed_mget_body_len == byte_size(fixed_mget_body)
    assert Bitwise.band(fixed_mget_flags, custom_payload) != 0
    assert <<0::unsigned-16, 0x89, 2::unsigned-32, 2::unsigned-32, "v1", "v2">> = fixed_mget_body

    mget_frame = NIF.encode_compact_kv_mget_response_frame(0x0104, 4, 43, ["v1", nil, "v2"])

    <<"FSNP", 0x81, mget_flags, 4::unsigned-32, 0x0104::unsigned-16, 43::unsigned-64,
      mget_body_len::unsigned-32, mget_body::binary>> = mget_frame

    assert mget_body_len == byte_size(mget_body)
    assert Bitwise.band(mget_flags, custom_payload) != 0

    assert <<0::unsigned-16, 0x83, 3::unsigned-32, 1, 2::unsigned-32, "v1", 0, 1, 2::unsigned-32,
             "v2">> = mget_body

    assert NIF.encode_compact_kv_get_response_frame(0x0101, 3, 42, 123) == nil
    assert NIF.encode_compact_kv_mget_response_frame(0x0104, 4, 43, ["v1", 123]) == nil
  end

  test "command response frames use native compact KV fast path" do
    custom_payload = Codec.flags().custom_payload

    [set_frame] = Codec.encode_command_response_frames(0x0102, 4, 41, :ok, "OK")

    <<"FSNP", 0x81, set_flags, 4::unsigned-32, 0x0102::unsigned-16, 41::unsigned-64,
      set_body_len::unsigned-32, set_body::binary>> = set_frame

    assert set_body_len == byte_size(set_body)
    assert Bitwise.band(set_flags, custom_payload) != 0
    assert <<0::unsigned-16, 0x81, 1::unsigned-32>> = set_body

    [mset_frame] = Codec.encode_command_response_frames(0x0105, 4, 42, :ok, "OK")

    <<"FSNP", 0x81, mset_flags, 4::unsigned-32, 0x0105::unsigned-16, 42::unsigned-64,
      mset_body_len::unsigned-32, mset_body::binary>> = mset_frame

    assert mset_body_len == byte_size(mset_body)
    assert Bitwise.band(mset_flags, custom_payload) != 0
    assert <<0::unsigned-16, 0x81, 1::unsigned-32>> = mset_body

    [frame] = Codec.encode_command_response_frames(0x0104, 4, 43, :ok, ["v1", nil, "v2"])

    <<"FSNP", 0x81, flags, 4::unsigned-32, 0x0104::unsigned-16, 43::unsigned-64,
      body_len::unsigned-32, body::binary>> = frame

    assert body_len == byte_size(body)
    assert Bitwise.band(flags, custom_payload) != 0
    assert <<0::unsigned-16, 0x83, 3::unsigned-32, _items::binary>> = body

    [fixed_frame] = Codec.encode_command_response_frames(0x0104, 4, 44, :ok, ["v1", "v2"])

    <<"FSNP", 0x81, fixed_flags, 4::unsigned-32, 0x0104::unsigned-16, 44::unsigned-64,
      fixed_body_len::unsigned-32, fixed_body::binary>> = fixed_frame

    assert fixed_body_len == byte_size(fixed_body)
    assert Bitwise.band(fixed_flags, custom_payload) != 0
    assert <<0::unsigned-16, 0x89, 2::unsigned-32, 2::unsigned-32, "v1", "v2">> = fixed_body
  end

  @tag :streamed_mget_encoding
  test "chunked MGET encoding keeps values as bounded iodata frames" do
    first = String.duplicate("a", 80)
    second = String.duplicate("b", 80)

    frames =
      Codec.encode_command_response_frames(0x0104, 4, 45, :ok, [first, nil, second],
        chunk_bytes: 64
      )

    assert length(frames) > 1
    assert Enum.any?(frames, &(not is_binary(&1)))

    body =
      Enum.map(frames, fn frame ->
        frame = IO.iodata_to_binary(frame)

        assert <<"FSNP", 0x81, _flags, 4::unsigned-32, 0x0104::unsigned-16, 45::unsigned-64,
                 body_len::unsigned-32, frame_body::binary>> = frame

        assert body_len == byte_size(frame_body)
        assert body_len <= 64
        frame_body
      end)
      |> IO.iodata_to_binary()

    assert <<0::unsigned-16, 0x83, 3::unsigned-32, 1, 80::unsigned-32, ^first::binary-size(80), 0,
             1, 80::unsigned-32, ^second::binary-size(80)>> = body
  end

  test "command response frames compact hot Flow many responses when negotiated" do
    [frame] =
      Codec.encode_command_response_frames(0x0210, 3, 42, :ok, ["OK", "OK"],
        compact_flow_responses: true
      )

    tag = Codec.compact_tags().ok_list
    custom_payload = Codec.flags().custom_payload

    <<"FSNP", 0x81, flags, 3::unsigned-32, 0x0210::unsigned-16, 42::unsigned-64,
      body_len::unsigned-32, body::binary>> = frame

    assert body_len == byte_size(body)
    assert Bitwise.band(flags, custom_payload) != 0
    assert <<0::unsigned-16, ^tag, 2::unsigned-32>> = body
  end

  test "command response frames use native compact claim jobs fast path for list jobs" do
    [frame] =
      Codec.encode_command_response_frames(
        0x0203,
        2,
        99,
        :ok,
        [
          ["flow-1", "bucket-1", "lease-1", 42],
          ["flow-2", nil, "lease-2", 43]
        ],
        compact_flow_responses: true
      )

    tag = Codec.compact_tags().flow_claim_jobs
    custom_payload = Codec.flags().custom_payload

    <<"FSNP", 0x81, flags, 2::unsigned-32, 0x0203::unsigned-16, 99::unsigned-64,
      body_len::unsigned-32, body::binary>> = frame

    assert body_len == byte_size(body)
    assert Bitwise.band(flags, custom_payload) != 0
    assert <<0::unsigned-16, ^tag, 2::unsigned-32, _items::binary>> = body
  end

  test "command response frames fall back for compact claim job maps" do
    [frame] =
      Codec.encode_command_response_frames(
        0x0203,
        2,
        99,
        :ok,
        [
          %{
            "id" => "flow-1",
            "partition_key" => "bucket-1",
            "lease_token" => "lease-1",
            "fencing_token" => 42
          }
        ],
        compact_flow_responses: true
      )

    tag = Codec.compact_tags().flow_claim_jobs

    <<"FSNP", 0x81, _flags, _lane::unsigned-32, 0x0203::unsigned-16, _request::unsigned-64,
      _body_len::unsigned-32, body::binary>> = frame

    assert <<0::unsigned-16, ^tag, 1::unsigned-32, _items::binary>> = body
  end

  test "command response frames compact direct Flow records when negotiated" do
    [frame] =
      Codec.encode_command_response_frames(
        0x0202,
        2,
        99,
        :ok,
        %{id: "flow-1", type: "email", state: "queued", version: 1, priority: 0},
        compact_flow_responses: true
      )

    tag = Codec.compact_tags().flow_record
    custom_payload = Codec.flags().custom_payload

    <<"FSNP", 0x81, flags, 2::unsigned-32, 0x0202::unsigned-16, 99::unsigned-64,
      body_len::unsigned-32, body::binary>> = frame

    assert body_len == byte_size(body)
    assert Bitwise.band(flags, custom_payload) != 0
    assert <<0::unsigned-16, ^tag, 5::unsigned-32, _entries::binary>> = body
  end

  test "compact pipeline response supports Flow records and Flow record lists" do
    record = %{id: "flow-1", type: "email", state: "queued", version: 1}
    payload = Codec.encode_compact_pipeline_response([["ok", record], ["ok", [record]]])

    flow_record = Codec.compact_tags().flow_record
    flow_record_list = Codec.compact_tags().flow_record_list

    assert <<0x95, 2::unsigned-32, 0, 2, ^flow_record, 4::unsigned-32, _rest::binary>> =
             payload

    assert payload =~ <<0, 3, flow_record_list, 1::unsigned-32, flow_record, 4::unsigned-32>>
  end

  test "compact pipeline response supports Flow value refs" do
    payload =
      Codec.encode_compact_pipeline_response([
        ["ok", %{ref: "flow/value/ref-1", partition_key: "__flow_auto__:1"}]
      ])

    assert <<0x95, 1::unsigned-32, 0, 5, ref_len::unsigned-32, rest::binary>> = payload
    assert ref_len == byte_size("flow/value/ref-1")
    assert <<"flow/value/ref-1", partition_len::unsigned-32, rest::binary>> = rest
    assert partition_len == byte_size("__flow_auto__:1")
    assert <<"__flow_auto__:1", 0xFFFF_FFFF::unsigned-32>> = rest
  end

  test "compact pipeline response supports binary lists" do
    payload = Codec.encode_compact_pipeline_response([["ok", ["a", "bb"]], ["ok", []]])

    assert <<0x95, 2::unsigned-32, 0, 6, 2::unsigned-32, 1::unsigned-32, "a", 2::unsigned-32,
             "bb", 0, 6, 0::unsigned-32>> = payload
  end

  test "compact binary list list encodes values-only range responses" do
    payload = Codec.encode_compact_binary_list_list([["a", "bb"], []])

    assert <<0x86, 2::unsigned-32, 2::unsigned-32, 1::unsigned-32, "a", 2::unsigned-32, "bb",
             0::unsigned-32>> = payload
  end

  test "compact binary map entry list encodes values-only hash responses without maps" do
    payload = Codec.encode_compact_binary_map_entry_list([[{"field", "value"}], []])

    assert <<0x87, 2::unsigned-32, 1::unsigned-32, 5::unsigned-32, "field", 5::unsigned-32,
             "value", 0::unsigned-32>> = payload
  end

  test "command response frames keep normal encoding when compact is disabled" do
    [frame] =
      Codec.encode_command_response_frames(0x0210, 3, 42, :ok, ["OK", "OK"],
        compact_flow_responses: false
      )

    custom_payload = Codec.flags().custom_payload

    <<"FSNP", 0x81, flags, _rest::binary>> = frame
    assert Bitwise.band(flags, custom_payload) == 0
  end

  test "decodes compact FLOW.CREATE_MANY request body" do
    body =
      <<
        0x90,
        compact_bin("email")::binary,
        compact_bin("queued")::binary,
        123::signed-64,
        124::signed-64,
        2,
        1,
        2::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("payload-1")::binary,
        compact_bin("flow-2")::binary,
        compact_bin("payload-2")::binary
      >>

    assert {:ok, payload} = Codec.decode_body(0x020F, 0x02, body)

    assert %{
             "items" => [
               {:id, "flow-1", :payload, "payload-1"},
               {:id, "flow-2", :payload, "payload-2"}
             ],
             __wire_flow_items_normalized__: true,
             __wire_flow_opts__: [
               return: :ok_on_success,
               independent: true,
               type: "email",
               state: "queued",
               now_ms: 123,
               run_at_ms: 124
             ]
           } = payload

    refute Map.has_key?(payload, "type")
    refute Map.has_key?(payload, "state")
    refute Map.has_key?(payload, "now_ms")
    refute Map.has_key?(payload, "run_at_ms")
    refute Map.has_key?(payload, "independent")
    refute Map.has_key?(payload, "return")
  end

  test "decodes compact FLOW.CREATE_MANY request body with partition key" do
    body =
      <<
        0x96,
        compact_bin("email")::binary,
        compact_bin("queued")::binary,
        compact_bin("__flow_auto__:7")::binary,
        123::signed-64,
        124::signed-64,
        2,
        1,
        1::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("payload-1")::binary
      >>

    assert {:ok, payload} = Codec.decode_body(0x020F, 0x02, body)

    assert %{
             "partition_key" => "__flow_auto__:7",
             "items" => [{:id, "flow-1", :payload, "payload-1"}],
             __wire_flow_items_normalized__: true,
             __wire_flow_opts__: [
               return: :ok_on_success,
               independent: true,
               type: "email",
               state: "queued",
               now_ms: 123,
               run_at_ms: 124
             ]
           } = payload

    refute Map.has_key?(payload, "type")
    refute Map.has_key?(payload, "state")
    refute Map.has_key?(payload, "now_ms")
    refute Map.has_key?(payload, "run_at_ms")
    refute Map.has_key?(payload, "independent")
    refute Map.has_key?(payload, "return")
  end

  test "decodes compact FLOW.CREATE_MANY request body with mixed item partitions" do
    body =
      <<
        0x9E,
        compact_bin("email")::binary,
        compact_bin("queued")::binary,
        123::signed-64,
        124::signed-64,
        2,
        1,
        2::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("tenant-a")::binary,
        compact_bin("payload-1")::binary,
        compact_bin("flow-2")::binary,
        compact_bin("tenant-b")::binary,
        compact_bin("payload-2")::binary
      >>

    assert {:ok, payload} = Codec.decode_body(0x020F, 0x02, body)

    assert %{
             "items" => [
               {:id, "flow-1", :partition_key, "tenant-a", :payload, "payload-1"},
               {:id, "flow-2", :partition_key, "tenant-b", :payload, "payload-2"}
             ],
             __wire_flow_items_normalized__: true,
             __wire_flow_opts__: [
               return: :ok_on_success,
               independent: true,
               type: "email",
               state: "queued",
               now_ms: 123,
               run_at_ms: 124
             ]
           } = payload

    refute Map.has_key?(payload, "type")
    refute Map.has_key?(payload, "state")
    refute Map.has_key?(payload, "now_ms")
    refute Map.has_key?(payload, "run_at_ms")
    refute Map.has_key?(payload, "independent")
    refute Map.has_key?(payload, "return")
  end

  test "decodes compact FLOW.CLAIM_DUE request body" do
    body =
      <<
        0x91,
        compact_bin("email")::binary,
        compact_bin("queued")::binary,
        compact_bin("worker-1")::binary,
        30_000::signed-64,
        500::signed-64,
        -1::signed-64,
        0,
        25::signed-64,
        -9_223_372_036_854_775_808::signed-64,
        1,
        2,
        2::unsigned-32,
        compact_bin("p1")::binary,
        compact_bin("p2")::binary
      >>

    assert {:ok,
            %{
              "type" => "email",
              "state" => "queued",
              "worker" => "worker-1",
              "lease_ms" => 30_000,
              "limit" => 500,
              "reclaim_expired" => false,
              "reclaim_ratio" => 25,
              "return" => "jobs_compact",
              "partition_keys" => ["p1", "p2"]
            }} = Codec.decode_body(0x0203, 0x02, body)
  end

  test "decodes compact FLOW.VALUE.MGET request body" do
    body =
      <<
        0x9D,
        64::signed-64,
        2::unsigned-32,
        compact_bin("ref-1")::binary,
        compact_bin("ref-2")::binary
      >>

    assert {:ok,
            %{
              "refs" => ["ref-1", "ref-2"],
              "max_bytes" => 64
            }} = Codec.decode_body(0x020C, 0x02, body)
  end

  test "rejects the retired compact FLOW.LIST request marker" do
    body =
      <<
        0x9F,
        compact_bin("email")::binary,
        compact_bin("queued")::binary,
        500::signed-64,
        1
      >>

    assert {:error, "ERR native custom request payload is unsupported"} =
             Codec.decode_body(0x020E, 0x02, body)
  end

  test "decodes compact FLOW.COMPLETE_MANY request body" do
    body =
      <<
        0x92,
        0xFFFF_FFFF::unsigned-32,
        123::signed-64,
        2,
        1::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("p1")::binary,
        compact_bin("lease-1")::binary,
        7::signed-64
      >>

    assert {:ok, payload} = Codec.decode_body(0x0210, 0x02, body)

    assert %{
             "items" => [
               {:id, "flow-1", :partition_key, "p1", :lease_token, "lease-1", :fencing_token, 7}
             ],
             __wire_flow_items_normalized__: true,
             __wire_flow_opts__: [independent: true, now_ms: 123]
           } = payload

    refute Map.has_key?(payload, "now_ms")
    refute Map.has_key?(payload, "independent")
  end

  test "decodes compact FLOW.COMPLETE_MANY ok-on-success request body" do
    body =
      <<
        0x93,
        0xFFFF_FFFF::unsigned-32,
        123::signed-64,
        2,
        1::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("p1")::binary,
        compact_bin("lease-1")::binary,
        7::signed-64
      >>

    assert {:ok, payload} = Codec.decode_body(0x0210, 0x02, body)

    assert %{
             "items" => [
               {:id, "flow-1", :partition_key, "p1", :lease_token, "lease-1", :fencing_token, 7}
             ],
             __wire_flow_items_normalized__: true,
             __wire_flow_opts__: [return: :ok_on_success, independent: true, now_ms: 123]
           } = payload

    refute Map.has_key?(payload, "now_ms")
    refute Map.has_key?(payload, "independent")
    refute Map.has_key?(payload, "return")
  end

  test "decodes compact FLOW.COMPLETE_MANY local-only request marker" do
    body =
      <<
        0x92,
        0xFFFF_FFFF::unsigned-32,
        123::signed-64,
        3,
        1::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("p1")::binary,
        compact_bin("lease-1")::binary,
        7::signed-64
      >>

    assert {:ok, payload} = Codec.decode_body(0x0210, 0x02, body)

    assert %{
             __wire_flow_items_normalized__: true,
             __wire_flow_opts__: [
               independent: true,
               terminal_local_only: true,
               now_ms: 123
             ]
           } = payload
  end

  test "decodes compact FLOW.RETRY_MANY ok-on-success request body" do
    body =
      <<
        0x98,
        0xFFFF_FFFF::unsigned-32,
        123::signed-64,
        456::signed-64,
        2,
        1::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("p1")::binary,
        compact_bin("lease-1")::binary,
        7::signed-64
      >>

    assert {:ok, payload} = Codec.decode_body(0x0212, 0x02, body)

    assert %{
             "items" => [
               {:id, "flow-1", :partition_key, "p1", :lease_token, "lease-1", :fencing_token, 7}
             ],
             __wire_flow_items_normalized__: true,
             __wire_flow_opts__: [
               return: :ok_on_success,
               independent: true,
               now_ms: 123,
               run_at_ms: 456
             ]
           } = payload

    refute Map.has_key?(payload, "now_ms")
    refute Map.has_key?(payload, "run_at_ms")
    refute Map.has_key?(payload, "independent")
    refute Map.has_key?(payload, "return")
  end

  test "decodes compact FLOW.FAIL_MANY ok-on-success request body" do
    body =
      <<
        0x93,
        0xFFFF_FFFF::unsigned-32,
        123::signed-64,
        2,
        1::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("p1")::binary,
        compact_bin("lease-1")::binary,
        7::signed-64
      >>

    assert {:ok, payload} = Codec.decode_body(0x0213, 0x02, body)

    assert %{
             "items" => [
               {:id, "flow-1", :partition_key, "p1", :lease_token, "lease-1", :fencing_token, 7}
             ],
             __wire_flow_items_normalized__: true,
             __wire_flow_opts__: [return: :ok_on_success, independent: true, now_ms: 123]
           } = payload

    refute Map.has_key?(payload, "now_ms")
    refute Map.has_key?(payload, "independent")
    refute Map.has_key?(payload, "return")
  end

  test "decodes compact FLOW.CANCEL_MANY ok-on-success request body" do
    body =
      <<
        0x9A,
        0xFFFF_FFFF::unsigned-32,
        123::signed-64,
        2,
        1::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("p1")::binary,
        7::signed-64
      >>

    assert {:ok, payload} = Codec.decode_body(0x0214, 0x02, body)

    assert %{
             "items" => [
               {:id, "flow-1", :partition_key, "p1", :fencing_token, 7}
             ],
             __wire_flow_items_normalized__: true,
             __wire_flow_opts__: [return: :ok_on_success, independent: true, now_ms: 123]
           } = payload

    refute Map.has_key?(payload, "now_ms")
    refute Map.has_key?(payload, "independent")
    refute Map.has_key?(payload, "return")
  end

  test "decodes compact FLOW.TRANSITION_MANY ok-on-success request body" do
    body =
      <<
        0x9C,
        compact_bin("queued")::binary,
        compact_bin("next")::binary,
        0xFFFF_FFFF::unsigned-32,
        123::signed-64,
        456::signed-64,
        2,
        1::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("p1")::binary,
        7::signed-64,
        0xFFFF_FFFF::unsigned-32
      >>

    assert {:ok, payload} = Codec.decode_body(0x0211, 0x02, body)

    assert %{
             "from_state" => "queued",
             "to_state" => "next",
             "items" => [
               {:id, "flow-1", :partition_key, "p1", :fencing_token, 7, :lease_token, nil}
             ],
             __wire_flow_items_normalized__: true,
             __wire_flow_opts__: [
               return: :ok_on_success,
               independent: true,
               now_ms: 123,
               run_at_ms: 456
             ]
           } = payload

    refute Map.has_key?(payload, "now_ms")
    refute Map.has_key?(payload, "run_at_ms")
    refute Map.has_key?(payload, "independent")
    refute Map.has_key?(payload, "return")
  end

  defp compact_bin(value) do
    value = IO.iodata_to_binary(value)
    <<byte_size(value)::unsigned-32, value::binary>>
  end

  defp encoded_map_entry(key, encoded_value) do
    [<<byte_size(key)::unsigned-32>>, key, encoded_value]
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
