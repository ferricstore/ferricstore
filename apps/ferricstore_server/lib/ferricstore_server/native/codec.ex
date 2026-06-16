defmodule FerricstoreServer.Native.Codec do
  @moduledoc """
  Binary framing and value codec for the FerricStore native TCP protocol.

  The protocol is request-id based and pipeline friendly:

      magic(4) version(1) flags(1) lane_id(4) opcode(2) request_id(8) body_len(4) body(N)

  Request bodies and response payloads use a compact typed value codec. This
  keeps client SDKs independent from RESP while still preserving binary-safe
  payloads and structured Flow metadata.
  """

  alias FerricstoreServer.Native.NIF

  @version 1

  @flag_trace 0x01
  @flag_custom_payload 0x02
  @flag_warning 0x04
  @flag_compressed 0x08
  @flag_no_reply 0x10
  @flag_more_chunks 0x20

  @status_codes %{
    ok: 0,
    error: 1,
    auth: 2,
    noperm: 3,
    busy: 4,
    reroute: 5,
    bad_request: 6
  }

  @default_max_value_items 100_000
  @default_max_value_depth 64

  @compact_flow_claim_jobs 0x80
  @compact_ok_list 0x81
  @compact_kv_get 0x82
  @compact_flow_record 0x84
  @compact_flow_record_list 0x85
  @compact_binary_list_list 0x86
  @compact_binary_map_list 0x87
  @compact_integer_list 0x88
  @compact_flow_create_many_request 0x90
  @compact_flow_create_many_partition_request 0x96
  @compact_flow_create_many_mixed_request 0x9E
  @compact_flow_claim_due_request 0x91
  @compact_flow_complete_many_request 0x92
  @compact_flow_complete_many_ok_request 0x93
  @compact_pipeline_request 0x94
  @compact_pipeline_response 0x95
  @compact_flow_retry_many_request 0x97
  @compact_flow_retry_many_ok_request 0x98
  @compact_flow_cancel_many_request 0x99
  @compact_flow_cancel_many_ok_request 0x9A
  @compact_flow_transition_many_request 0x9B
  @compact_flow_transition_many_ok_request 0x9C
  @compact_flow_value_mget_request 0x9D
  @compact_flow_list_request 0x9F

  @compact_flow_record_field_ids %{
    "id" => 1,
    "type" => 2,
    "state" => 3,
    "version" => 4,
    "priority" => 5,
    "partition_key" => 6,
    "payload_ref" => 7,
    "result_ref" => 8,
    "error_ref" => 9,
    "payload" => 10,
    "result" => 11,
    "error" => 12,
    "created_at_ms" => 13,
    "updated_at_ms" => 14,
    "next_run_at_ms" => 15,
    "lease_deadline_ms" => 16,
    "lease_owner" => 17,
    "lease_token" => 18,
    "fencing_token" => 19,
    "attempts" => 20,
    "history_max_events" => 21,
    "history_hot_max_events" => 22,
    "child_groups" => 23,
    "parent_flow_id" => 24,
    "parent_partition_key" => 25,
    "root_flow_id" => 26,
    "correlation_id" => 27,
    "terminal_retention_until_ms" => 28,
    "ttl_ms" => 29,
    "retention_ttl_ms" => 30,
    "run_state" => 31,
    "value_refs" => 32,
    "values" => 33,
    "payload_omitted" => 34,
    "payload_size" => 35,
    "result_omitted" => 36,
    "result_size" => 37,
    "error_omitted" => 38,
    "error_size" => 39,
    "max_attempts" => 40,
    "attributes" => 41
  }

  @compact_flow_record_atom_field_ids %{
    id: 1,
    type: 2,
    state: 3,
    version: 4,
    priority: 5,
    partition_key: 6,
    payload_ref: 7,
    result_ref: 8,
    error_ref: 9,
    payload: 10,
    result: 11,
    error: 12,
    created_at_ms: 13,
    updated_at_ms: 14,
    next_run_at_ms: 15,
    lease_deadline_ms: 16,
    lease_owner: 17,
    lease_token: 18,
    fencing_token: 19,
    attempts: 20,
    history_max_events: 21,
    history_hot_max_events: 22,
    child_groups: 23,
    parent_flow_id: 24,
    parent_partition_key: 25,
    root_flow_id: 26,
    correlation_id: 27,
    terminal_retention_until_ms: 28,
    ttl_ms: 29,
    retention_ttl_ms: 30,
    run_state: 31,
    value_refs: 32,
    values: 33,
    payload_omitted: 34,
    payload_size: 35,
    result_omitted: 36,
    result_size: 37,
    error_omitted: 38,
    error_size: 39,
    max_attempts: 40,
    attributes: 41
  }

  @type frame ::
          {
            lane_id :: non_neg_integer(),
            opcode :: non_neg_integer(),
            request_id :: non_neg_integer(),
            flags :: non_neg_integer(),
            body :: binary()
          }

  @spec version() :: pos_integer()
  def version, do: @version

  @spec flags() :: map()
  def flags do
    %{
      trace: @flag_trace,
      custom_payload: @flag_custom_payload,
      warning: @flag_warning,
      compressed: @flag_compressed,
      no_reply: @flag_no_reply,
      more_chunks: @flag_more_chunks
    }
  end

  @spec status_codes() :: map()
  def status_codes, do: @status_codes

  @spec compact_tags() :: map()
  def compact_tags do
    %{
      flow_claim_jobs: @compact_flow_claim_jobs,
      ok_list: @compact_ok_list,
      integer_list: @compact_integer_list,
      flow_record: @compact_flow_record,
      flow_record_list: @compact_flow_record_list
    }
  end

  @spec decode_frames(binary(), pos_integer()) ::
          {:ok, [frame()], binary()} | {:error, binary()}
  def decode_frames(buffer, max_frame_bytes) when is_binary(buffer) do
    NIF.decode_frames(buffer, max_frame_bytes)
  end

  @spec decode_body(binary()) :: {:ok, term()} | {:error, binary()}
  def decode_body(""), do: {:ok, %{}}

  def decode_body(body) when is_binary(body) do
    case decode_value(body) do
      {:ok, value, ""} -> {:ok, value}
      {:ok, _value, _rest} -> {:error, "ERR native frame body has trailing bytes"}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec decode_body(non_neg_integer(), non_neg_integer(), binary()) ::
          {:ok, term()} | {:error, binary()}
  def decode_body(opcode, flags, body) when is_integer(opcode) and is_integer(flags) do
    if Bitwise.band(flags, @flag_custom_payload) != 0 do
      decode_custom_request_body(opcode, body)
    else
      decode_body(body)
    end
  end

  @spec encode_response(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          atom(),
          term(),
          non_neg_integer()
        ) ::
          binary()
  def encode_response(opcode, lane_id, request_id, status, value, extra_flags \\ 0) do
    [frame] =
      encode_response_frames(opcode, lane_id, request_id, status, value,
        flags: extra_flags,
        compression: :none,
        chunk_bytes: 0
      )

    frame
  end

  @spec encode_response_frames(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          atom(),
          term(),
          keyword()
        ) :: [binary()]
  def encode_response_frames(opcode, lane_id, request_id, status, value, opts \\ []) do
    extra_flags = Keyword.get(opts, :flags, 0)
    status_code = Map.fetch!(@status_codes, status)
    payload = encode_value(response_value(status, value))
    body = <<status_code::unsigned-16, payload::binary>>
    {body, flags} = maybe_compress_body(body, extra_flags, Keyword.get(opts, :compression, :none))

    encode_frame_chunks(
      opcode,
      lane_id,
      request_id,
      body,
      flags,
      Keyword.get(opts, :chunk_bytes, 0)
    )
  end

  @spec encode_command_response_frames(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          atom(),
          term(),
          keyword()
        ) :: [binary()]
  def encode_command_response_frames(opcode, lane_id, request_id, status, value, opts \\ []) do
    if Keyword.get(opts, :compact_flow_responses, false) or compact_kv_opcode?(opcode) do
      case compact_response_frame(opcode, lane_id, request_id, status, value, opts) do
        frame when is_binary(frame) ->
          [frame]

        nil ->
          case compact_response_payload(opcode, status, value) do
            nil ->
              encode_response_frames(opcode, lane_id, request_id, status, value, opts)

            payload ->
              encode_compact_response_frames(opcode, lane_id, request_id, status, payload, opts)
          end
      end
    else
      encode_response_frames(opcode, lane_id, request_id, status, value, opts)
    end
  end

  @spec encode_compact_flow_claim_jobs(term()) :: binary() | nil
  def encode_compact_flow_claim_jobs(jobs) when is_list(jobs) do
    case compact_claim_job_items(jobs, []) do
      {:ok, items} ->
        [<<@compact_flow_claim_jobs, length(items)::unsigned-32>>, items]
        |> IO.iodata_to_binary()

      :error ->
        nil
    end
  end

  def encode_compact_flow_claim_jobs(_jobs), do: nil

  @spec encode_compact_ok_list(term()) :: binary() | nil
  def encode_compact_ok_list(values) when is_list(values) do
    if Enum.all?(values, &ok_value?/1) do
      encode_compact_ok_count(length(values))
    end
  end

  def encode_compact_ok_list(_values), do: nil

  def encode_compact_ok_count(count)
      when is_integer(count) and count >= 0 and count <= 0xFFFF_FFFF,
      do: <<@compact_ok_list, count::unsigned-32>>

  def encode_compact_ok_count(_count), do: nil

  def encode_compact_integer_list(values) when is_list(values) do
    encoded =
      Enum.reduce_while(values, [], fn
        value, acc
        when is_integer(value) and value >= -0x8000_0000_0000_0000 and
               value <= 0x7FFF_FFFF_FFFF_FFFF ->
          {:cont, [<<value::signed-64>> | acc]}

        _value, _acc ->
          {:halt, :error}
      end)

    case encoded do
      :error ->
        nil

      integers ->
        [<<@compact_integer_list, length(integers)::unsigned-32>>, Enum.reverse(integers)]
        |> IO.iodata_to_binary()
    end
  end

  def encode_compact_integer_list(_values), do: nil

  def encode_compact_kv_get(nil), do: <<@compact_kv_get, 0>>

  def encode_compact_kv_get(value) when is_binary(value),
    do: <<@compact_kv_get, 1, byte_size(value)::unsigned-32, value::binary>>

  def encode_compact_kv_get(_value), do: nil

  def encode_compact_kv_mget(values) when is_list(values), do: NIF.encode_compact_kv_mget(values)

  def encode_compact_kv_mget(_values), do: nil

  def encode_compact_binary_list_list(values) when is_list(values) do
    encoded =
      Enum.reduce_while(values, [], fn
        list, acc when is_list(list) ->
          case compact_binary_list_payload(list) do
            payload when is_binary(payload) -> {:cont, [payload | acc]}
            nil -> {:halt, :error}
          end

        _value, _acc ->
          {:halt, :error}
      end)

    case encoded do
      :error ->
        nil

      lists ->
        [<<@compact_binary_list_list, length(lists)::unsigned-32>>, Enum.reverse(lists)]
        |> IO.iodata_to_binary()
    end
  end

  def encode_compact_binary_list_list(_values), do: nil

  def encode_compact_binary_map_list(values) when is_list(values) do
    encoded =
      Enum.reduce_while(values, [], fn
        value, acc when is_map(value) ->
          case compact_binary_map_payload(value) do
            payload when is_binary(payload) -> {:cont, [payload | acc]}
            nil -> {:halt, :error}
          end

        _value, _acc ->
          {:halt, :error}
      end)

    case encoded do
      :error ->
        nil

      maps ->
        [<<@compact_binary_map_list, length(maps)::unsigned-32>>, Enum.reverse(maps)]
        |> IO.iodata_to_binary()
    end
  end

  def encode_compact_binary_map_list(_values), do: nil

  def encode_compact_binary_map_entry_list(values) when is_list(values) do
    encoded =
      Enum.reduce_while(values, [], fn
        entries, acc when is_list(entries) ->
          case compact_binary_map_entries_payload(entries) do
            payload when is_binary(payload) -> {:cont, [payload | acc]}
            nil -> {:halt, :error}
          end

        _value, _acc ->
          {:halt, :error}
      end)

    case encoded do
      :error ->
        nil

      maps ->
        [<<@compact_binary_map_list, length(maps)::unsigned-32>>, Enum.reverse(maps)]
        |> IO.iodata_to_binary()
    end
  end

  def encode_compact_binary_map_entry_list(_values), do: nil

  def encode_compact_pipeline_response(values) when is_list(values) do
    encoded =
      Enum.reduce_while(values, [], fn
        ["ok", nil], acc ->
          {:cont, [<<0, 0>> | acc]}

        ["ok", value], acc when is_binary(value) ->
          {:cont, [<<0, 1, byte_size(value)::unsigned-32, value::binary>> | acc]}

        ["ok", value], acc ->
          case compact_pipeline_ok_payload(value) do
            payload when is_binary(payload) -> {:cont, [[<<0>>, payload] | acc]}
            nil -> {:halt, :error}
          end

        ["busy", reason], acc when is_binary(reason) ->
          {:cont, [<<1, byte_size(reason)::unsigned-32, reason::binary>> | acc]}

        ["error", reason], acc when is_binary(reason) ->
          {:cont, [<<2, byte_size(reason)::unsigned-32, reason::binary>> | acc]}

        [status, reason], acc when is_binary(status) and is_binary(reason) ->
          code = if status == "busy", do: 1, else: 2
          {:cont, [<<code, byte_size(reason)::unsigned-32, reason::binary>> | acc]}

        _value, _acc ->
          {:halt, :error}
      end)

    case encoded do
      :error ->
        nil

      values ->
        [<<@compact_pipeline_response, length(values)::unsigned-32>>, Enum.reverse(values)]
        |> IO.iodata_to_binary()
    end
  end

  def encode_compact_pipeline_response(_values), do: nil

  @spec encode_compact_flow_record(term()) :: binary() | nil
  def encode_compact_flow_record(record) when is_map(record) do
    if flow_record_map?(record) do
      [
        <<@compact_flow_record, map_size(record)::unsigned-32>>,
        compact_flow_record_entries(record)
      ]
      |> IO.iodata_to_binary()
    end
  end

  def encode_compact_flow_record(_record), do: nil

  @spec encode_compact_flow_record_list(term()) :: binary() | nil
  def encode_compact_flow_record_list(records) when is_list(records) do
    encoded =
      Enum.reduce_while(records, [], fn record, acc ->
        case encode_compact_flow_record(record) do
          payload when is_binary(payload) -> {:cont, [payload | acc]}
          nil -> {:halt, :error}
        end
      end)

    case encoded do
      :error ->
        nil

      values ->
        [<<@compact_flow_record_list, length(values)::unsigned-32>>, Enum.reverse(values)]
        |> IO.iodata_to_binary()
    end
  end

  def encode_compact_flow_record_list(_records), do: nil

  @spec encode_compact_response_frames(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          atom(),
          binary(),
          keyword()
        ) :: [binary()]
  def encode_compact_response_frames(opcode, lane_id, request_id, status, payload, opts \\ [])
      when is_binary(payload) do
    extra_flags = Keyword.get(opts, :flags, 0)
    status_code = Map.fetch!(@status_codes, status)
    body = <<status_code::unsigned-16, payload::binary>>

    {body, flags} =
      maybe_compress_body(
        body,
        Bitwise.bor(extra_flags, @flag_custom_payload),
        Keyword.get(opts, :compression, :none)
      )

    encode_frame_chunks(
      opcode,
      lane_id,
      request_id,
      body,
      flags,
      Keyword.get(opts, :chunk_bytes, 0)
    )
  end

  @spec encode_event(non_neg_integer(), term()) :: binary()
  def encode_event(opcode, value) do
    encode_response(opcode, 0, 0, :ok, value)
  end

  @spec encode_frame(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          :request | :response
        ) :: binary()
  def encode_frame(opcode, lane_id, request_id, body, flags \\ 0, direction \\ :request)
      when is_integer(opcode) and is_integer(lane_id) and is_integer(request_id) and
             is_binary(body) do
    NIF.encode_frame(opcode, lane_id, request_id, body, flags, direction == :response)
  end

  defp maybe_compress_body(body, flags, :zlib) when byte_size(body) > 0,
    do: {:zlib.compress(body), Bitwise.bor(flags, @flag_compressed)}

  defp maybe_compress_body(body, flags, _compression), do: {body, flags}

  defp encode_frame_chunks(opcode, lane_id, request_id, body, flags, chunk_bytes)
       when not is_integer(chunk_bytes) or chunk_bytes <= 0 or byte_size(body) <= chunk_bytes do
    [encode_frame(opcode, lane_id, request_id, body, flags, :response)]
  end

  defp encode_frame_chunks(opcode, lane_id, request_id, body, flags, chunk_bytes) do
    chunks = chunks(body, chunk_bytes, [])
    last_index = length(chunks) - 1

    chunks
    |> Enum.with_index()
    |> Enum.map(fn {chunk, index} ->
      frame_flags =
        if index == last_index do
          flags
        else
          flags
          |> Bitwise.band(Bitwise.bnot(@flag_compressed))
          |> Bitwise.bor(@flag_more_chunks)
        end

      encode_frame(opcode, lane_id, request_id, chunk, frame_flags, :response)
    end)
  end

  defp chunks("", _chunk_bytes, acc), do: Enum.reverse(acc)

  defp chunks(body, chunk_bytes, acc) when byte_size(body) <= chunk_bytes,
    do: Enum.reverse([body | acc])

  defp chunks(body, chunk_bytes, acc) do
    <<chunk::binary-size(chunk_bytes), rest::binary>> = body
    chunks(rest, chunk_bytes, [chunk | acc])
  end

  defp compact_claim_job_items([], acc), do: {:ok, Enum.reverse(acc)}

  defp compact_claim_job_items([job | rest], acc) do
    case compact_claim_job_item(job) do
      {:ok, item} -> compact_claim_job_items(rest, [item | acc])
      :error -> :error
    end
  end

  defp compact_response_payload(0x0203, :ok, value),
    do: encode_compact_flow_claim_jobs(value)

  defp compact_response_payload(opcode, :ok, value)
       when opcode in [0x020F, 0x0210, 0x0212, 0x0213, 0x0214],
       do: encode_compact_ok_list(value)

  defp compact_response_payload(0x0202, :ok, value), do: encode_compact_flow_record(value)

  defp compact_response_payload(opcode, :ok, value)
       when opcode in [0x020E, 0x0217, 0x0218, 0x0219, 0x021A, 0x021B, 0x021D],
       do: encode_compact_flow_record_list(value)

  defp compact_response_payload(0x0101, :ok, value), do: encode_compact_kv_get(value)

  defp compact_response_payload(opcode, :ok, value)
       when opcode in [0x0102, 0x0105],
       do: encode_compact_ok_list([value])

  defp compact_response_payload(0x0104, :ok, value), do: encode_compact_kv_mget(value)

  defp compact_response_payload(0x020C, :ok, value), do: encode_compact_kv_mget(value)

  defp compact_response_payload(0x000E, :ok, value) when is_binary(value), do: value

  defp compact_response_payload(_opcode, _status, _value), do: nil

  defp compact_response_frame(0x0203, lane_id, request_id, :ok, value, opts) do
    if direct_compact_frame?(opts) do
      NIF.encode_compact_claim_jobs_response_frame(0x0203, lane_id, request_id, value)
    end
  end

  defp compact_response_frame(opcode, lane_id, request_id, :ok, value, opts)
       when opcode in [0x020F, 0x0210, 0x0212, 0x0213, 0x0214] do
    if direct_compact_frame?(opts) do
      NIF.encode_compact_ok_list_response_frame(opcode, lane_id, request_id, value)
    end
  end

  defp compact_response_frame(0x0101, lane_id, request_id, :ok, value, opts) do
    if direct_compact_frame?(opts) do
      NIF.encode_compact_kv_get_response_frame(0x0101, lane_id, request_id, value)
    end
  end

  defp compact_response_frame(opcode, lane_id, request_id, :ok, value, opts)
       when opcode in [0x0102, 0x0105] do
    if direct_compact_frame?(opts) do
      NIF.encode_compact_ok_list_response_frame(opcode, lane_id, request_id, [value])
    end
  end

  defp compact_response_frame(0x0104, lane_id, request_id, :ok, value, opts) do
    if direct_compact_frame?(opts) do
      NIF.encode_compact_kv_mget_response_frame(0x0104, lane_id, request_id, value)
    end
  end

  defp compact_response_frame(0x020C, lane_id, request_id, :ok, value, opts) do
    if direct_compact_frame?(opts) do
      NIF.encode_compact_kv_mget_response_frame(0x020C, lane_id, request_id, value)
    end
  end

  defp compact_response_frame(_opcode, _lane_id, _request_id, _status, _value, _opts), do: nil

  defp compact_kv_opcode?(opcode), do: opcode in [0x000E, 0x0101, 0x0102, 0x0104, 0x0105]

  defp direct_compact_frame?(opts) do
    chunk_bytes = Keyword.get(opts, :chunk_bytes, 0)

    Keyword.get(opts, :flags, 0) == 0 and Keyword.get(opts, :compression, :none) == :none and
      (not is_integer(chunk_bytes) or chunk_bytes <= 0)
  end

  defp compact_claim_job_item([id, partition_key, lease_token, fencing_token])
       when is_binary(id) and is_binary(lease_token) and is_integer(fencing_token) do
    case compact_optional_binary(partition_key) do
      :error ->
        :error

      encoded_partition ->
        {:ok,
         [
           compact_binary(id),
           encoded_partition,
           compact_binary(lease_token),
           <<fencing_token::signed-64>>
         ]}
    end
  end

  defp compact_claim_job_item([id, partition_key, lease_token, fencing_token, attrs])
       when is_binary(id) and is_binary(lease_token) and is_integer(fencing_token) and
              is_map(attrs) do
    case compact_claim_job_item([id, partition_key, lease_token, fencing_token]) do
      {:ok, item} -> {:ok, [item, encode_value(attrs)]}
      :error -> :error
    end
  end

  defp compact_claim_job_item([id, partition_key, lease_token, fencing_token, run_state, attrs])
       when is_binary(id) and is_binary(lease_token) and is_integer(fencing_token) and
              is_map(attrs) do
    case compact_claim_job_item([id, partition_key, lease_token, fencing_token]) do
      {:ok, item} when is_binary(run_state) ->
        {:ok, [item, compact_binary(run_state), encode_value(attrs)]}

      {:ok, item} when is_nil(run_state) ->
        {:ok, [item, compact_optional_binary(nil), encode_value(attrs)]}

      _ ->
        :error
    end
  end

  defp compact_claim_job_item({id, partition_key, lease_token, fencing_token}),
    do: compact_claim_job_item([id, partition_key, lease_token, fencing_token])

  defp compact_claim_job_item(%{"id" => id} = job) do
    compact_claim_job_item([
      id,
      Map.get(job, "partition_key"),
      Map.get(job, "lease_token"),
      Map.get(job, "fencing_token"),
      Map.get(job, "run_state") || Map.get(job, "state"),
      Map.get(job, "attributes", %{})
    ])
  end

  defp compact_claim_job_item(%{id: id} = job) do
    compact_claim_job_item([
      id,
      Map.get(job, :partition_key),
      Map.get(job, :lease_token),
      Map.get(job, :fencing_token),
      Map.get(job, :run_state) || Map.get(job, :state),
      Map.get(job, :attributes, %{})
    ])
  end

  defp compact_claim_job_item(_job), do: :error

  defp compact_binary(value) when is_binary(value),
    do: [<<byte_size(value)::unsigned-32>>, value]

  defp compact_optional_binary(nil), do: <<0xFFFF_FFFF::unsigned-32>>
  defp compact_optional_binary(value) when is_binary(value), do: compact_binary(value)
  defp compact_optional_binary(_value), do: :error

  defp compact_pipeline_ok_payload(value) when is_map(value) do
    case encode_compact_flow_record(value) do
      payload when is_binary(payload) ->
        <<2, payload::binary>>

      nil ->
        case compact_flow_value_ref_payload(value) do
          payload when is_binary(payload) ->
            <<5, payload::binary>>

          nil ->
            case compact_binary_map_payload(value) do
              payload when is_binary(payload) -> [<<7>>, payload] |> IO.iodata_to_binary()
              nil -> nil
            end
        end
    end
  end

  defp compact_pipeline_ok_payload(values) when is_list(values) do
    case compact_binary_list_payload(values) do
      payload when is_binary(payload) ->
        [<<6>>, payload] |> IO.iodata_to_binary()

      nil ->
        case compact_claim_job_item(values) do
          {:ok, payload} ->
            [<<4>>, payload] |> IO.iodata_to_binary()

          :error ->
            case encode_compact_flow_record_list(values) do
              payload when is_binary(payload) -> <<3, payload::binary>>
              nil -> nil
            end
        end
    end
  end

  defp compact_pipeline_ok_payload(_value), do: nil

  defp compact_binary_list_payload(values) when is_list(values) do
    encoded =
      Enum.reduce_while(values, [], fn
        value, acc when is_binary(value) ->
          {:cont, [compact_binary(value) | acc]}

        _value, _acc ->
          {:halt, :error}
      end)

    case encoded do
      :error ->
        nil

      values ->
        [<<length(values)::unsigned-32>>, Enum.reverse(values)]
        |> IO.iodata_to_binary()
    end
  end

  defp compact_binary_list_payload(_values), do: nil

  defp compact_binary_map_payload(value) when is_map(value) do
    encoded =
      Enum.reduce_while(value, [], fn
        {key, item}, acc when is_binary(key) and is_binary(item) ->
          {:cont, [[compact_binary(key), compact_binary(item)] | acc]}

        _entry, _acc ->
          {:halt, :error}
      end)

    case encoded do
      :error ->
        nil

      entries ->
        [<<length(entries)::unsigned-32>>, Enum.reverse(entries)]
        |> IO.iodata_to_binary()
    end
  end

  defp compact_binary_map_payload(_value), do: nil

  defp compact_binary_map_entries_payload(entries) when is_list(entries) do
    encoded =
      Enum.reduce_while(entries, [], fn
        {key, item}, acc when is_binary(key) and is_binary(item) ->
          {:cont, [[compact_binary(key), compact_binary(item)] | acc]}

        _entry, _acc ->
          {:halt, :error}
      end)

    case encoded do
      :error ->
        nil

      entries ->
        [<<length(entries)::unsigned-32>>, Enum.reverse(entries)]
        |> IO.iodata_to_binary()
    end
  end

  defp compact_binary_map_entries_payload(_entries), do: nil

  defp compact_flow_value_ref_payload(value) do
    ref = map_get_either(value, :ref, "ref")
    partition_key = map_get_either(value, :partition_key, "partition_key")
    owner_flow_id = map_get_either(value, :owner_flow_id, "owner_flow_id")

    cond do
      not is_binary(ref) ->
        nil

      not compact_optional_binary_value?(partition_key) ->
        nil

      not compact_optional_binary_value?(owner_flow_id) ->
        nil

      true ->
        [
          compact_binary(ref),
          compact_optional_binary(partition_key),
          compact_optional_binary(owner_flow_id)
        ]
        |> IO.iodata_to_binary()
    end
  end

  defp compact_optional_binary_value?(nil), do: true
  defp compact_optional_binary_value?(value), do: is_binary(value)

  defp map_get_either(map, atom_key, string_key) do
    case Map.fetch(map, atom_key) do
      {:ok, value} -> value
      :error -> Map.get(map, string_key)
    end
  end

  defp compact_flow_record_entries(record) do
    record
    |> Enum.map(fn {key, value} -> compact_flow_record_entry(key, value) end)
  end

  defp compact_flow_record_entry(key, value) when is_atom(key) do
    case Map.get(@compact_flow_record_atom_field_ids, key) do
      id when is_integer(id) -> [<<id>>, encode_value(value)]
      nil -> compact_flow_record_string_entry(Atom.to_string(key), value)
    end
  end

  defp compact_flow_record_entry(key, value) when is_binary(key),
    do: compact_flow_record_string_entry(key, value)

  defp compact_flow_record_entry(key, value),
    do: compact_flow_record_string_entry(to_string(key), value)

  defp compact_flow_record_string_entry(key, value) do
    case Map.get(@compact_flow_record_field_ids, key) do
      id when is_integer(id) -> [<<id>>, encode_value(value)]
      nil -> [<<0>>, compact_binary(key), encode_value(value)]
    end
  end

  defp flow_record_map?(record) do
    flow_record_key?(record, "id") and flow_record_key?(record, "type") and
      flow_record_key?(record, "state")
  end

  defp flow_record_key?(record, "id"), do: Map.has_key?(record, "id") or Map.has_key?(record, :id)

  defp flow_record_key?(record, "type"),
    do: Map.has_key?(record, "type") or Map.has_key?(record, :type)

  defp flow_record_key?(record, "state"),
    do: Map.has_key?(record, "state") or Map.has_key?(record, :state)

  defp flow_record_key?(_record, _key), do: false

  defp ok_value?(:ok), do: true
  defp ok_value?("OK"), do: true
  defp ok_value?("ok"), do: true
  defp ok_value?("Ok"), do: true
  defp ok_value?("oK"), do: true
  defp ok_value?(_value), do: false

  defp decode_custom_request_body(
         0x000E,
         <<@compact_pipeline_request, mode, count::unsigned-32, rest::binary>>
       ) do
    values_only? = Bitwise.band(mode, 0x80) != 0
    mode = Bitwise.band(mode, 0x7F)

    with {:ok, items, ""} <- take_compact_pipeline_items(mode, count, rest, []) do
      payload =
        %{
          "atomicity" => "none",
          "return" => "compact",
          "compact_count" => count,
          "compact_pipeline" => {mode, items}
        }

      payload = if values_only?, do: Map.put(payload, "compact_values", true), else: payload
      {:ok, payload}
    else
      _ -> {:error, "ERR native compact PIPELINE payload is invalid"}
    end
  end

  defp decode_custom_request_body(
         0x0105,
         <<@compact_pipeline_request, 1, count::unsigned-32, rest::binary>>
       ) do
    case take_compact_pipeline_items(1, count, rest, []) do
      {:ok, pairs, ""} -> {:ok, %{"pairs" => pairs, __wire_compact_validated__: true}}
      _ -> {:error, "ERR native compact MSET payload is invalid"}
    end
  end

  defp decode_custom_request_body(
         opcode,
         <<@compact_pipeline_request, mode, count::unsigned-32, rest::binary>>
       )
       when opcode == 0x0104 and mode == 2 do
    case take_compact_pipeline_items(mode, count, rest, []) do
      {:ok, keys, ""} -> {:ok, %{"keys" => keys, __wire_compact_validated__: true}}
      _ -> {:error, "ERR native compact MGET payload is invalid"}
    end
  end

  defp decode_custom_request_body(0x020F, <<@compact_flow_create_many_request, rest::binary>>) do
    with {:ok, type, rest} <- take_compact_binary(rest),
         {:ok, state, rest} <- take_compact_binary(rest),
         <<now_ms::signed-64, run_at_ms::signed-64, independent::unsigned-8,
           return_mode::unsigned-8, count::unsigned-32, rest::binary>> <- rest,
         {:ok, items, ""} <- take_compact_create_items(count, rest, []) do
      payload =
        %{
          "items" => items,
          __wire_flow_items_normalized__: true,
          __wire_flow_opts__:
            flow_create_many_wire_opts(type, state, now_ms, run_at_ms, independent, return_mode)
        }

      {:ok, payload}
    else
      _ -> {:error, "ERR native compact FLOW.CREATE_MANY payload is invalid"}
    end
  end

  defp decode_custom_request_body(
         0x020F,
         <<@compact_flow_create_many_partition_request, rest::binary>>
       ) do
    with {:ok, type, rest} <- take_compact_binary(rest),
         {:ok, state, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         <<now_ms::signed-64, run_at_ms::signed-64, independent::unsigned-8,
           return_mode::unsigned-8, count::unsigned-32, rest::binary>> <- rest,
         {:ok, items, ""} <- take_compact_create_items(count, rest, []) do
      payload =
        %{
          "items" => items,
          __wire_flow_items_normalized__: true,
          __wire_flow_opts__:
            flow_create_many_wire_opts(type, state, now_ms, run_at_ms, independent, return_mode)
        }
        |> put_optional_binary("partition_key", partition_key)

      {:ok, payload}
    else
      _ -> {:error, "ERR native compact FLOW.CREATE_MANY payload is invalid"}
    end
  end

  defp decode_custom_request_body(
         0x020F,
         <<@compact_flow_create_many_mixed_request, rest::binary>>
       ) do
    with {:ok, type, rest} <- take_compact_binary(rest),
         {:ok, state, rest} <- take_compact_binary(rest),
         <<now_ms::signed-64, run_at_ms::signed-64, independent::unsigned-8,
           return_mode::unsigned-8, count::unsigned-32, rest::binary>> <- rest,
         {:ok, items, ""} <- take_compact_create_mixed_items(count, rest, []) do
      payload =
        %{
          "items" => items,
          __wire_flow_items_normalized__: true,
          __wire_flow_opts__:
            flow_create_many_wire_opts(type, state, now_ms, run_at_ms, independent, return_mode)
        }

      {:ok, payload}
    else
      _ -> {:error, "ERR native compact FLOW.CREATE_MANY payload is invalid"}
    end
  end

  defp decode_custom_request_body(0x0203, <<@compact_flow_claim_due_request, rest::binary>>) do
    with {:ok, type, rest} <- take_compact_binary(rest),
         {:ok, state, rest} <- take_compact_optional_binary(rest),
         {:ok, worker, rest} <- take_compact_binary(rest),
         <<lease_ms::signed-64, limit::signed-64, block_ms::signed-64,
           reclaim_expired::unsigned-8, reclaim_ratio::signed-64, priority_marker::signed-64,
           return_mode::unsigned-8, partition_mode::unsigned-8, rest::binary>> <- rest,
         {:ok, partitions, ""} <- take_compact_partitions(partition_mode, rest) do
      payload =
        %{
          "type" => type,
          "worker" => worker,
          "lease_ms" => lease_ms,
          "limit" => limit,
          "reclaim_expired" => reclaim_expired != 0,
          "reclaim_ratio" => reclaim_ratio
        }
        |> put_optional_binary("state", state)
        |> put_block_ms(block_ms)
        |> put_priority(priority_marker)
        |> put_return_mode(return_mode)
        |> put_partition_values(partition_mode, partitions)

      {:ok, payload}
    else
      _ -> {:error, "ERR native compact FLOW.CLAIM_DUE payload is invalid"}
    end
  end

  defp decode_custom_request_body(
         0x020C,
         <<@compact_flow_value_mget_request, max_bytes::signed-64, count::unsigned-32,
           rest::binary>>
       ) do
    with {:ok, refs, ""} <- take_compact_binary_items(count, rest, []) do
      payload =
        %{"refs" => refs}
        |> put_compact_i64_optional("max_bytes", max_bytes)

      {:ok, payload}
    else
      _ -> {:error, "ERR native compact FLOW.VALUE.MGET payload is invalid"}
    end
  end

  defp decode_custom_request_body(
         0x020E,
         <<@compact_flow_list_request, rest::binary>>
       ) do
    with {:ok, type, rest} <- take_compact_binary(rest),
         {:ok, state, rest} <- take_compact_optional_binary(rest),
         <<count::signed-64, return_mode::unsigned-8, "">> <- rest do
      payload =
        %{"type" => type, "count" => count}
        |> put_optional_binary("state", state)
        |> put_flow_list_return_mode(return_mode)

      {:ok, payload}
    else
      _ -> {:error, "ERR native compact FLOW.LIST payload is invalid"}
    end
  end

  defp decode_custom_request_body(0x0210, <<@compact_flow_complete_many_request, rest::binary>>) do
    decode_compact_flow_complete_many_request(rest, nil)
  end

  defp decode_custom_request_body(
         0x0210,
         <<@compact_flow_complete_many_ok_request, rest::binary>>
       ) do
    decode_compact_flow_complete_many_request(rest, "OK_ON_SUCCESS")
  end

  defp decode_custom_request_body(0x0213, <<@compact_flow_complete_many_request, rest::binary>>) do
    decode_compact_flow_complete_many_request(rest, nil)
  end

  defp decode_custom_request_body(
         0x0213,
         <<@compact_flow_complete_many_ok_request, rest::binary>>
       ) do
    decode_compact_flow_complete_many_request(rest, "OK_ON_SUCCESS")
  end

  defp decode_custom_request_body(0x0212, <<@compact_flow_retry_many_request, rest::binary>>) do
    decode_compact_flow_retry_many_request(rest, nil)
  end

  defp decode_custom_request_body(
         0x0212,
         <<@compact_flow_retry_many_ok_request, rest::binary>>
       ) do
    decode_compact_flow_retry_many_request(rest, "OK_ON_SUCCESS")
  end

  defp decode_custom_request_body(0x0214, <<@compact_flow_cancel_many_request, rest::binary>>) do
    decode_compact_flow_cancel_many_request(rest, nil)
  end

  defp decode_custom_request_body(
         0x0214,
         <<@compact_flow_cancel_many_ok_request, rest::binary>>
       ) do
    decode_compact_flow_cancel_many_request(rest, "OK_ON_SUCCESS")
  end

  defp decode_custom_request_body(
         0x0211,
         <<@compact_flow_transition_many_request, rest::binary>>
       ) do
    decode_compact_flow_transition_many_request(rest, nil)
  end

  defp decode_custom_request_body(
         0x0211,
         <<@compact_flow_transition_many_ok_request, rest::binary>>
       ) do
    decode_compact_flow_transition_many_request(rest, "OK_ON_SUCCESS")
  end

  defp decode_custom_request_body(_opcode, _body),
    do: {:error, "ERR native custom request payload is unsupported"}

  defp take_compact_pipeline_items(_mode, 0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_pipeline_items(1, count, rest, acc) when count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest),
         {:ok, value, rest} <- take_compact_binary(rest) do
      take_compact_pipeline_items(1, count - 1, rest, [{key, value} | acc])
    end
  end

  defp take_compact_pipeline_items(mode, count, rest, acc)
       when mode in [2, 27, 30] and count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest) do
      take_compact_pipeline_items(mode, count - 1, rest, [key | acc])
    end
  end

  defp take_compact_pipeline_items(mode, count, rest, acc)
       when mode in [18, 19] and count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest),
         {:ok, item, rest} <- take_compact_binary(rest) do
      take_compact_pipeline_items(mode, count - 1, rest, [{key, item} | acc])
    end
  end

  defp take_compact_pipeline_items(28, count, rest, acc) when count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest),
         <<field_count::unsigned-32, rest::binary>> <- rest,
         true <- field_count > 0,
         {:ok, fields, rest} <- take_compact_binary_list(field_count, rest, []) do
      take_compact_pipeline_items(28, count - 1, rest, [{key, fields} | acc])
    else
      _ -> :error
    end
  end

  defp take_compact_pipeline_items(29, count, rest, acc) when count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest),
         {:ok, member, rest} <- take_compact_binary(rest) do
      take_compact_pipeline_items(29, count - 1, rest, [{key, member} | acc])
    end
  end

  defp take_compact_pipeline_items(20, count, rest, acc) when count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest),
         <<start::signed-64, stop::signed-64, rest::binary>> <- rest do
      take_compact_pipeline_items(20, count - 1, rest, [{key, start, stop} | acc])
    end
  end

  defp take_compact_pipeline_items(21, count, rest, acc) when count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest),
         <<start::signed-64, stop::signed-64, with_scores::unsigned-8, rest::binary>> <- rest do
      take_compact_pipeline_items(21, count - 1, rest, [
        {key, start, stop, with_scores != 0} | acc
      ])
    end
  end

  defp take_compact_pipeline_items(22, count, rest, acc) when count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest),
         {:ok, field, rest} <- take_compact_binary(rest),
         {:ok, value, rest} <- take_compact_binary(rest) do
      take_compact_pipeline_items(22, count - 1, rest, [{key, field, value} | acc])
    end
  end

  defp take_compact_pipeline_items(mode, count, rest, acc)
       when mode in [23, 24, 25, 31, 32] and count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest),
         {:ok, item, rest} <- take_compact_binary(rest) do
      take_compact_pipeline_items(mode, count - 1, rest, [{key, item} | acc])
    end
  end

  defp take_compact_pipeline_items(26, count, rest, acc) when count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest),
         <<score::float-64, rest::binary>> <- rest,
         {:ok, member, rest} <- take_compact_binary(rest) do
      take_compact_pipeline_items(26, count - 1, rest, [{key, score, member} | acc])
    end
  end

  defp take_compact_pipeline_items(5, count, <<1, rest::binary>>, acc) when count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest),
         {:ok, value, rest} <- take_compact_binary(rest) do
      take_compact_pipeline_items(5, count - 1, rest, [{:set, key, value} | acc])
    end
  end

  defp take_compact_pipeline_items(5, count, <<2, rest::binary>>, acc) when count > 0 do
    with {:ok, key, rest} <- take_compact_binary(rest) do
      take_compact_pipeline_items(5, count - 1, rest, [{:get, key} | acc])
    end
  end

  defp take_compact_pipeline_items(6, count, rest, _acc) when count >= 0 do
    with {:ok, from_state, rest} <- take_compact_binary(rest),
         {:ok, to_state, rest} <- take_compact_binary(rest),
         <<lease_ms::signed-64, rest::binary>> <- rest do
      take_compact_step_continue_items(count, rest, from_state, to_state, lease_ms, [], [])
    end
  end

  defp take_compact_pipeline_items(33, count, rest, _acc) when count >= 0 do
    with {:ok, from_state, rest} <- take_compact_binary(rest),
         {:ok, to_state, rest} <- take_compact_binary(rest),
         <<lease_ms::signed-64, rest::binary>> <- rest do
      take_compact_step_continue_items(
        count,
        rest,
        from_state,
        to_state,
        lease_ms,
        [
          return: :jobs_compact
        ],
        []
      )
    end
  end

  defp take_compact_pipeline_items(7, count, rest, _acc) when count >= 0 do
    take_compact_shared_value_put_items(count, rest, [])
  end

  defp take_compact_pipeline_items(8, count, rest, _acc) when count >= 0 do
    take_compact_named_value_put_items(count, rest, [])
  end

  defp take_compact_pipeline_items(14, count, rest, _acc) when count >= 0 do
    take_compact_named_value_put_items(count, rest, [return: :ok_on_success], [])
  end

  defp take_compact_pipeline_items(15, count, rest, _acc) when count >= 0 do
    take_compact_shared_value_put_items(count, rest, [return: :ok_on_success], [])
  end

  defp take_compact_pipeline_items(9, count, rest, _acc) when count >= 0 do
    take_compact_flow_get_items(count, rest, [])
  end

  defp take_compact_pipeline_items(16, count, rest, _acc) when count >= 0 do
    take_compact_flow_get_partition_items(count, rest, [])
  end

  defp take_compact_pipeline_items(17, count, rest, _acc) when count >= 0 do
    take_compact_flow_get_meta_items(count, rest, [])
  end

  defp take_compact_pipeline_items(10, count, rest, _acc) when count >= 0 do
    with <<history_count::signed-64, include_cold::unsigned-8, consistent_projection::unsigned-8,
           rest::binary>> <- rest do
      opts =
        [
          count: history_count,
          include_cold: compact_bool_value(include_cold),
          consistent_projection: compact_bool_value(consistent_projection)
        ]

      take_compact_flow_history_items(count, rest, opts, [])
    end
  end

  defp take_compact_pipeline_items(11, count, rest, _acc) when count >= 0 do
    with {:ok, signal, rest} <- take_compact_binary(rest),
         {:ok, if_state, rest} <- take_compact_binary(rest),
         {:ok, transition_to, rest} <- take_compact_binary(rest) do
      take_compact_flow_signal_items(count, rest, signal, if_state, transition_to, [])
    end
  end

  defp take_compact_pipeline_items(12, count, rest, _acc) when count >= 0 do
    with {:ok, type, rest} <- take_compact_binary(rest),
         {:ok, initial_state, rest} <- take_compact_binary(rest),
         {:ok, worker, rest} <- take_compact_binary(rest),
         <<lease_ms::signed-64, rest::binary>> <- rest do
      take_compact_flow_start_and_claim_items(
        count,
        rest,
        type,
        initial_state,
        worker,
        lease_ms,
        [],
        []
      )
    end
  end

  defp take_compact_pipeline_items(13, count, rest, _acc) when count >= 0 do
    with {:ok, type, rest} <- take_compact_binary(rest),
         {:ok, initial_state, rest} <- take_compact_binary(rest),
         {:ok, worker, rest} <- take_compact_binary(rest),
         <<lease_ms::signed-64, rest::binary>> <- rest do
      take_compact_flow_start_and_claim_items(
        count,
        rest,
        type,
        initial_state,
        worker,
        lease_ms,
        [return: :jobs_compact],
        []
      )
    end
  end

  defp take_compact_pipeline_items(_mode, _count, _rest, _acc), do: :error

  defp take_compact_binary_list(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_binary_list(count, rest, acc) when count > 0 do
    with {:ok, value, rest} <- take_compact_binary(rest) do
      take_compact_binary_list(count - 1, rest, [value | acc])
    end
  end

  defp take_compact_step_continue_items(
         0,
         rest,
         _from_state,
         _to_state,
         _lease_ms,
         _base_opts,
         acc
       ),
       do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_step_continue_items(
         count,
         rest,
         from_state,
         to_state,
         lease_ms,
         base_opts,
         acc
       )
       when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         {:ok, lease_token, rest} <- take_compact_binary(rest),
         <<fencing_token::signed-64, now_ms::signed-64, rest::binary>> <- rest do
      opts =
        [fencing_token: fencing_token, lease_ms: lease_ms, now_ms: now_ms]
        |> Keyword.merge(base_opts)
        |> maybe_put_compact_partition_key(partition_key)

      item = {:flow_step_continue, id, lease_token, from_state, to_state, opts}

      take_compact_step_continue_items(
        count - 1,
        rest,
        from_state,
        to_state,
        lease_ms,
        base_opts,
        [item | acc]
      )
    end
  end

  defp maybe_put_compact_partition_key(opts, nil), do: opts

  defp maybe_put_compact_partition_key(opts, partition_key),
    do: [{:partition_key, partition_key} | opts]

  defp take_compact_shared_value_put_items(count, rest, acc),
    do: take_compact_shared_value_put_items(count, rest, [], acc)

  defp take_compact_shared_value_put_items(0, rest, _base_opts, acc),
    do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_shared_value_put_items(count, rest, base_opts, acc) when count > 0 do
    with {:ok, value, rest} <- take_compact_binary(rest),
         <<now_ms::signed-64, rest::binary>> <- rest do
      opts = [now_ms: now_ms] |> Keyword.merge(base_opts)
      take_compact_shared_value_put_items(count - 1, rest, base_opts, [{value, opts} | acc])
    end
  end

  defp take_compact_named_value_put_items(count, rest, acc),
    do: take_compact_named_value_put_items(count, rest, [], acc)

  defp take_compact_named_value_put_items(0, rest, _base_opts, acc),
    do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_named_value_put_items(count, rest, base_opts, acc) when count > 0 do
    with {:ok, value, rest} <- take_compact_binary(rest),
         {:ok, owner_flow_id, rest} <- take_compact_binary(rest),
         {:ok, name, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         <<now_ms::signed-64, rest::binary>> <- rest do
      opts =
        [owner_flow_id: owner_flow_id, name: name, now_ms: now_ms]
        |> Keyword.merge(base_opts)
        |> maybe_put_compact_partition_key(partition_key)

      item = {:flow_named_value_put, value, opts}
      take_compact_named_value_put_items(count - 1, rest, base_opts, [item | acc])
    end
  end

  defp take_compact_flow_get_items(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_flow_get_items(count, rest, acc) when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest) do
      take_compact_flow_get_items(count - 1, rest, [{:flow_get, id, []} | acc])
    end
  end

  defp take_compact_flow_get_partition_items(0, rest, acc),
    do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_flow_get_partition_items(count, rest, acc) when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest) do
      opts = maybe_put_compact_partition_key([], partition_key)
      take_compact_flow_get_partition_items(count - 1, rest, [{:flow_get, id, opts} | acc])
    end
  end

  defp take_compact_flow_get_meta_items(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_flow_get_meta_items(count, rest, acc) when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest) do
      opts =
        [return: :meta]
        |> maybe_put_compact_partition_key(partition_key)

      take_compact_flow_get_meta_items(count - 1, rest, [{:flow_get, id, opts} | acc])
    end
  end

  defp take_compact_flow_history_items(0, rest, _opts, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_flow_history_items(count, rest, opts, acc) when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest) do
      item = {:flow_history, id, maybe_put_compact_partition_key(opts, partition_key)}
      take_compact_flow_history_items(count - 1, rest, opts, [item | acc])
    end
  end

  defp compact_bool_value(2), do: true
  defp compact_bool_value(_value), do: false

  defp take_compact_flow_signal_items(0, rest, _signal, _if_state, _transition_to, acc),
    do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_flow_signal_items(count, rest, signal, if_state, transition_to, acc)
       when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         <<now_ms::signed-64, rest::binary>> <- rest do
      opts =
        [
          signal: signal,
          if_state: if_state,
          transition_to: transition_to,
          now_ms: now_ms
        ]
        |> maybe_put_compact_partition_key(partition_key)

      item = {:flow_signal, id, opts}

      take_compact_flow_signal_items(count - 1, rest, signal, if_state, transition_to, [
        item | acc
      ])
    end
  end

  defp take_compact_flow_start_and_claim_items(
         0,
         rest,
         _type,
         _initial_state,
         _worker,
         _lease_ms,
         _base_opts,
         acc
       ),
       do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_flow_start_and_claim_items(
         count,
         rest,
         type,
         initial_state,
         worker,
         lease_ms,
         base_opts,
         acc
       )
       when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         {:ok, payload, rest} <- take_compact_optional_binary(rest),
         <<now_ms::signed-64, rest::binary>> <- rest do
      opts =
        [worker: worker, lease_ms: lease_ms, now_ms: now_ms]
        |> Keyword.merge(base_opts)
        |> maybe_put_compact_partition_key(partition_key)
        |> maybe_put_compact_payload(payload)

      item = {:flow_start_and_claim, id, type, initial_state, opts}

      take_compact_flow_start_and_claim_items(
        count - 1,
        rest,
        type,
        initial_state,
        worker,
        lease_ms,
        base_opts,
        [item | acc]
      )
    end
  end

  defp maybe_put_compact_payload(opts, nil), do: opts
  defp maybe_put_compact_payload(opts, payload), do: [{:payload, payload} | opts]

  defp decode_compact_flow_complete_many_request(rest, return_mode) do
    with {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         <<now_ms::signed-64, independent::unsigned-8, count::unsigned-32, rest::binary>> <- rest,
         {:ok, items, ""} <- take_compact_claimed_items(count, rest, []) do
      payload =
        %{
          "items" => items,
          __wire_flow_items_normalized__: true,
          __wire_flow_opts__: flow_complete_many_wire_opts(now_ms, independent, return_mode)
        }
        |> put_optional_binary("partition_key", partition_key)

      {:ok, payload}
    else
      _ -> {:error, "ERR native compact FLOW.COMPLETE_MANY payload is invalid"}
    end
  end

  defp decode_compact_flow_retry_many_request(rest, return_mode) do
    with {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         <<now_ms::signed-64, run_at_ms::signed-64, independent::unsigned-8, count::unsigned-32,
           rest::binary>> <- rest,
         {:ok, items, ""} <- take_compact_claimed_items(count, rest, []) do
      payload =
        %{
          "items" => items,
          __wire_flow_items_normalized__: true,
          __wire_flow_opts__:
            flow_retry_many_wire_opts(now_ms, run_at_ms, independent, return_mode)
        }
        |> put_optional_binary("partition_key", partition_key)

      {:ok, payload}
    else
      _ -> {:error, "ERR native compact FLOW.RETRY_MANY payload is invalid"}
    end
  end

  defp decode_compact_flow_cancel_many_request(rest, return_mode) do
    with {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         <<now_ms::signed-64, independent::unsigned-8, count::unsigned-32, rest::binary>> <- rest,
         {:ok, items, ""} <- take_compact_fenced_items(count, rest, []) do
      payload =
        %{
          "items" => items,
          __wire_flow_items_normalized__: true,
          __wire_flow_opts__: flow_terminal_many_wire_opts(now_ms, independent, return_mode)
        }
        |> put_optional_binary("partition_key", partition_key)

      {:ok, payload}
    else
      _ -> {:error, "ERR native compact FLOW.CANCEL_MANY payload is invalid"}
    end
  end

  defp decode_compact_flow_transition_many_request(rest, return_mode) do
    with {:ok, from_state, rest} <- take_compact_binary(rest),
         {:ok, to_state, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         <<now_ms::signed-64, run_at_ms::signed-64, independent::unsigned-8, count::unsigned-32,
           rest::binary>> <- rest,
         {:ok, items, ""} <- take_compact_transition_items(count, rest, []) do
      payload =
        %{
          "from_state" => from_state,
          "to_state" => to_state,
          "items" => items,
          __wire_flow_items_normalized__: true,
          __wire_flow_opts__:
            flow_retry_many_wire_opts(now_ms, run_at_ms, independent, return_mode)
        }
        |> put_optional_binary("partition_key", partition_key)

      {:ok, payload}
    else
      _ -> {:error, "ERR native compact FLOW.TRANSITION_MANY payload is invalid"}
    end
  end

  defp take_compact_create_items(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_create_items(count, rest, acc) when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, payload, rest} <- take_compact_binary(rest) do
      take_compact_create_items(count - 1, rest, [{:id, id, :payload, payload} | acc])
    end
  end

  defp take_compact_create_mixed_items(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_create_mixed_items(count, rest, acc) when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_binary(rest),
         {:ok, payload, rest} <- take_compact_binary(rest) do
      take_compact_create_mixed_items(count - 1, rest, [
        {:id, id, :partition_key, partition_key, :payload, payload} | acc
      ])
    end
  end

  defp take_compact_binary_items(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_binary_items(count, rest, acc) when count > 0 do
    with {:ok, value, rest} <- take_compact_binary(rest) do
      take_compact_binary_items(count - 1, rest, [value | acc])
    end
  end

  defp take_compact_claimed_items(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_claimed_items(count, rest, acc) when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         {:ok, lease_token, rest} <- take_compact_binary(rest),
         <<fencing_token::signed-64, rest::binary>> <- rest do
      item =
        case partition_key do
          nil ->
            {:id, id, :lease_token, lease_token, :fencing_token, fencing_token}

          value ->
            {:id, id, :partition_key, value, :lease_token, lease_token, :fencing_token,
             fencing_token}
        end

      take_compact_claimed_items(count - 1, rest, [item | acc])
    end
  end

  defp take_compact_fenced_items(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_fenced_items(count, rest, acc) when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         <<fencing_token::signed-64, rest::binary>> <- rest do
      item =
        case partition_key do
          nil -> {:id, id, :fencing_token, fencing_token}
          value -> {:id, id, :partition_key, value, :fencing_token, fencing_token}
        end

      take_compact_fenced_items(count - 1, rest, [item | acc])
    end
  end

  defp take_compact_transition_items(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_transition_items(count, rest, acc) when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         <<fencing_token::signed-64, rest::binary>> <- rest,
         {:ok, lease_token, rest} <- take_compact_optional_binary(rest) do
      item =
        case partition_key do
          nil ->
            {:id, id, :fencing_token, fencing_token, :lease_token, lease_token}

          value ->
            {:id, id, :partition_key, value, :fencing_token, fencing_token, :lease_token,
             lease_token}
        end

      take_compact_transition_items(count - 1, rest, [item | acc])
    end
  end

  defp take_compact_partitions(0, rest), do: {:ok, nil, rest}

  defp take_compact_partitions(1, rest) do
    with {:ok, partition_key, rest} <- take_compact_binary(rest),
         do: {:ok, partition_key, rest}
  end

  defp take_compact_partitions(2, <<count::unsigned-32, rest::binary>>),
    do: take_compact_partition_list(count, rest, [])

  defp take_compact_partitions(_mode, _rest), do: :error

  defp take_compact_partition_list(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_partition_list(count, rest, acc) when count > 0 do
    with {:ok, partition_key, rest} <- take_compact_binary(rest) do
      take_compact_partition_list(count - 1, rest, [partition_key | acc])
    end
  end

  defp take_compact_binary(<<len::unsigned-32, rest::binary>>) when byte_size(rest) >= len do
    <<value::binary-size(len), next::binary>> = rest
    {:ok, value, next}
  end

  defp take_compact_binary(_rest), do: :error

  defp take_compact_optional_binary(<<0xFFFF_FFFF::unsigned-32, rest::binary>>),
    do: {:ok, nil, rest}

  defp take_compact_optional_binary(rest), do: take_compact_binary(rest)

  defp put_optional_binary(payload, _key, nil), do: payload
  defp put_optional_binary(payload, key, value), do: Map.put(payload, key, value)

  defp put_block_ms(payload, value) when is_integer(value) and value >= 0,
    do: Map.put(payload, "block_ms", value)

  defp put_block_ms(payload, _value), do: payload

  defp put_priority(payload, -9_223_372_036_854_775_808), do: payload
  defp put_priority(payload, value), do: Map.put(payload, "priority", value)

  defp put_compact_i64_optional(payload, _key, -9_223_372_036_854_775_808), do: payload
  defp put_compact_i64_optional(payload, key, value), do: Map.put(payload, key, value)

  defp put_return_mode(payload, 0), do: payload
  defp put_return_mode(payload, 1), do: Map.put(payload, "return", "jobs_compact")
  defp put_return_mode(payload, 2), do: Map.put(payload, "return", "jobs_compact_state")
  defp put_return_mode(payload, 3), do: Map.put(payload, "return", "jobs_compact_attrs")
  defp put_return_mode(payload, 4), do: Map.put(payload, "return", "jobs_compact_state_attrs")
  defp put_return_mode(payload, _mode), do: payload

  defp put_flow_list_return_mode(payload, 0), do: payload
  defp put_flow_list_return_mode(payload, 1), do: Map.put(payload, "return", "meta")
  defp put_flow_list_return_mode(payload, _mode), do: payload

  defp flow_create_many_wire_opts(type, state, now_ms, run_at_ms, independent, return_mode) do
    [type: type, state: state, now_ms: now_ms, run_at_ms: run_at_ms]
    |> maybe_wire_independent(independent)
    |> maybe_wire_ok_on_success(return_mode)
  end

  defp flow_terminal_many_wire_opts(now_ms, independent, return_mode) do
    [now_ms: now_ms]
    |> maybe_wire_independent(independent)
    |> maybe_wire_ok_on_success(return_mode)
  end

  defp flow_complete_many_wire_opts(now_ms, independent, return_mode) do
    [now_ms: now_ms]
    |> maybe_wire_terminal_local_only(independent)
    |> maybe_wire_independent(independent)
    |> maybe_wire_ok_on_success(return_mode)
  end

  defp flow_retry_many_wire_opts(now_ms, run_at_ms, independent, return_mode) do
    [now_ms: now_ms, run_at_ms: run_at_ms]
    |> maybe_wire_independent(independent)
    |> maybe_wire_ok_on_success(return_mode)
  end

  defp maybe_wire_independent(opts, 1), do: [{:independent, false} | opts]
  defp maybe_wire_independent(opts, 2), do: [{:independent, true} | opts]
  defp maybe_wire_independent(opts, 3), do: [{:independent, true} | opts]
  defp maybe_wire_independent(opts, 4), do: [{:independent, false} | opts]
  defp maybe_wire_independent(opts, _value), do: opts

  defp maybe_wire_terminal_local_only(opts, 3), do: [{:terminal_local_only, true} | opts]
  defp maybe_wire_terminal_local_only(opts, 4), do: [{:terminal_local_only, true} | opts]
  defp maybe_wire_terminal_local_only(opts, _value), do: opts

  defp maybe_wire_ok_on_success(opts, 1), do: [{:return, :ok_on_success} | opts]
  defp maybe_wire_ok_on_success(opts, "OK_ON_SUCCESS"), do: [{:return, :ok_on_success} | opts]
  defp maybe_wire_ok_on_success(opts, _value), do: opts

  defp put_partition_values(payload, 0, _partitions), do: payload

  defp put_partition_values(payload, 1, partition_key),
    do: Map.put(payload, "partition_key", partition_key)

  defp put_partition_values(payload, 2, partition_keys),
    do: Map.put(payload, "partition_keys", partition_keys)

  defp put_partition_values(payload, _mode, _partitions), do: payload

  @spec encode_value(term()) :: binary()
  def encode_value(nil), do: <<0>>
  def encode_value(true), do: <<1>>
  def encode_value(false), do: <<2>>

  def encode_value(value) when is_integer(value),
    do: <<3, value::signed-64>>

  def encode_value(value) when is_binary(value) do
    len = byte_size(value)
    <<4, len::unsigned-32, value::binary>>
  end

  def encode_value(value) when is_atom(value),
    do: value |> Atom.to_string() |> encode_value()

  def encode_value(values) when is_list(values) do
    body = values |> Enum.map(&encode_value/1) |> IO.iodata_to_binary()
    <<5, length(values)::unsigned-32, body::binary>>
  end

  def encode_value(%_{} = struct),
    do: struct |> Map.from_struct() |> encode_value()

  def encode_value(values) when is_map(values) do
    entries =
      values
      |> Enum.map(fn {key, value} ->
        key = encode_key(key)
        [<<byte_size(key)::unsigned-32>>, key, encode_value(value)]
      end)
      |> IO.iodata_to_binary()

    <<6, map_size(values)::unsigned-32, entries::binary>>
  end

  def encode_value(value) when is_float(value),
    do: <<7, value::float-64>>

  def encode_value(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> encode_value()

  def encode_value(value),
    do: value |> inspect(limit: 50) |> encode_value()

  @spec decode_value(binary()) :: {:ok, term(), binary()} | {:error, binary()}
  def decode_value(binary), do: decode_value(binary, native_value_limits(), 0)

  defp decode_value(<<0, rest::binary>>, _limits, _depth), do: {:ok, nil, rest}
  defp decode_value(<<1, rest::binary>>, _limits, _depth), do: {:ok, true, rest}
  defp decode_value(<<2, rest::binary>>, _limits, _depth), do: {:ok, false, rest}

  defp decode_value(<<3, value::signed-64, rest::binary>>, _limits, _depth),
    do: {:ok, value, rest}

  defp decode_value(<<4, len::unsigned-32, rest::binary>>, _limits, _depth) do
    decode_binary(len, rest)
  end

  defp decode_value(<<5, count::unsigned-32, rest::binary>>, limits, depth) do
    with :ok <- validate_value_container(count, limits, depth) do
      decode_array(count, rest, [], limits, depth + 1)
    end
  end

  defp decode_value(<<6, count::unsigned-32, rest::binary>>, limits, depth) do
    with :ok <- validate_value_container(count, limits, depth) do
      decode_map(count, rest, %{}, limits, depth + 1)
    end
  end

  defp decode_value(<<7, value::float-64, rest::binary>>, _limits, _depth), do: {:ok, value, rest}
  defp decode_value(<<>>, _limits, _depth), do: {:error, "ERR native value is empty"}

  defp decode_value(_, _limits, _depth),
    do: {:error, "ERR native value has unknown or truncated tag"}

  defp decode_binary(len, rest) when byte_size(rest) >= len do
    <<value::binary-size(len), next::binary>> = rest
    {:ok, value, next}
  end

  defp decode_binary(_len, _rest), do: {:error, "ERR native binary value is truncated"}

  defp decode_array(0, rest, acc, _limits, _depth), do: {:ok, Enum.reverse(acc), rest}

  defp decode_array(count, rest, acc, limits, depth) do
    case decode_value(rest, limits, depth) do
      {:ok, value, next} -> decode_array(count - 1, next, [value | acc], limits, depth)
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_map(0, rest, acc, _limits, _depth), do: {:ok, acc, rest}

  defp decode_map(count, <<key_len::unsigned-32, rest::binary>>, acc, limits, depth) do
    with {:ok, key, after_key} <- decode_binary(key_len, rest),
         {:ok, value, after_value} <- decode_value(after_key, limits, depth) do
      decode_map(count - 1, after_value, Map.put(acc, key, value), limits, depth)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_map(_count, _rest, _acc, _limits, _depth),
    do: {:error, "ERR native map value is truncated"}

  defp validate_value_container(_count, %{max_depth: max_depth}, depth)
       when is_integer(max_depth) and max_depth >= 0 and depth >= max_depth,
       do: {:error, "ERR native value nesting exceeds max depth"}

  defp validate_value_container(count, %{max_items: max_items}, _depth)
       when is_integer(max_items) and max_items >= 0 and count > max_items,
       do: {:error, "ERR native value container exceeds max items"}

  defp validate_value_container(_count, _limits, _depth), do: :ok

  defp native_value_limits do
    %{
      max_items:
        Application.get_env(:ferricstore, :native_max_value_items, @default_max_value_items),
      max_depth:
        Application.get_env(:ferricstore, :native_max_value_depth, @default_max_value_depth)
    }
  end

  defp encode_key(key) when is_binary(key), do: key
  defp encode_key(key) when is_atom(key), do: Atom.to_string(key)
  defp encode_key(key), do: to_string(key)

  defp response_value(:ok, value), do: value

  defp response_value(status, value) when is_map(value) do
    value
    |> Map.put_new("code", Atom.to_string(status))
    |> Map.put_new("retryable", retryable?(status))
    |> Map.put_new("safe_to_retry", retryable?(status))
  end

  defp response_value(status, value) do
    %{
      "code" => Atom.to_string(status),
      "message" => message(value),
      "retryable" => retryable?(status),
      "safe_to_retry" => retryable?(status),
      "retry_after_ms" => retry_after_ms(status)
    }
  end

  defp retryable?(status), do: status in [:busy, :reroute]
  defp retry_after_ms(:busy), do: 100
  defp retry_after_ms(_status), do: 0

  defp message(value) when is_binary(value), do: value
  defp message(value), do: inspect(value, limit: 50)
end
