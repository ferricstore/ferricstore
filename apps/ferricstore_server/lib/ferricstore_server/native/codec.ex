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

  @compact_flow_claim_jobs 0x80
  @compact_ok_list 0x81
  @compact_flow_create_many_request 0x90
  @compact_flow_claim_due_request 0x91
  @compact_flow_complete_many_request 0x92

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
      ok_list: @compact_ok_list
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
    if Keyword.get(opts, :compact_flow_responses, false) do
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
      <<@compact_ok_list, length(values)::unsigned-32>>
    end
  end

  def encode_compact_ok_list(_values), do: nil

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

  defp compact_response_frame(_opcode, _lane_id, _request_id, _status, _value, _opts), do: nil

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

  defp compact_claim_job_item({id, partition_key, lease_token, fencing_token}),
    do: compact_claim_job_item([id, partition_key, lease_token, fencing_token])

  defp compact_claim_job_item(%{"id" => id} = job) do
    compact_claim_job_item([
      id,
      Map.get(job, "partition_key"),
      Map.get(job, "lease_token"),
      Map.get(job, "fencing_token")
    ])
  end

  defp compact_claim_job_item(%{id: id} = job) do
    compact_claim_job_item([
      id,
      Map.get(job, :partition_key),
      Map.get(job, :lease_token),
      Map.get(job, :fencing_token)
    ])
  end

  defp compact_claim_job_item(_job), do: :error

  defp compact_binary(value) when is_binary(value),
    do: [<<byte_size(value)::unsigned-32>>, value]

  defp compact_optional_binary(nil), do: <<0xFFFF_FFFF::unsigned-32>>
  defp compact_optional_binary(value) when is_binary(value), do: compact_binary(value)
  defp compact_optional_binary(_value), do: :error

  defp ok_value?(:ok), do: true
  defp ok_value?("OK"), do: true
  defp ok_value?("ok"), do: true
  defp ok_value?("Ok"), do: true
  defp ok_value?("oK"), do: true
  defp ok_value?(_value), do: false

  defp decode_custom_request_body(0x020F, <<@compact_flow_create_many_request, rest::binary>>) do
    with {:ok, type, rest} <- take_compact_binary(rest),
         {:ok, state, rest} <- take_compact_binary(rest),
         <<now_ms::signed-64, run_at_ms::signed-64, independent::unsigned-8,
           return_mode::unsigned-8, count::unsigned-32, rest::binary>> <- rest,
         {:ok, items, ""} <- take_compact_create_items(count, rest, []) do
      payload =
        %{
          "type" => type,
          "state" => state,
          "now_ms" => now_ms,
          "run_at_ms" => run_at_ms,
          "items" => items
        }
        |> put_compact_bool("independent", independent)
        |> put_create_many_return_mode(return_mode)

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

  defp decode_custom_request_body(0x0210, <<@compact_flow_complete_many_request, rest::binary>>) do
    with {:ok, partition_key, rest} <- take_compact_optional_binary(rest),
         <<now_ms::signed-64, independent::unsigned-8, count::unsigned-32, rest::binary>> <- rest,
         {:ok, items, ""} <- take_compact_claimed_items(count, rest, []) do
      payload =
        %{"now_ms" => now_ms, "items" => items}
        |> put_optional_binary("partition_key", partition_key)
        |> put_compact_bool("independent", independent)

      {:ok, payload}
    else
      _ -> {:error, "ERR native compact FLOW.COMPLETE_MANY payload is invalid"}
    end
  end

  defp decode_custom_request_body(_opcode, _body),
    do: {:error, "ERR native custom request payload is unsupported"}

  defp take_compact_create_items(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_compact_create_items(count, rest, acc) when count > 0 do
    with {:ok, id, rest} <- take_compact_binary(rest),
         {:ok, payload, rest} <- take_compact_binary(rest) do
      take_compact_create_items(count - 1, rest, [[id, payload] | acc])
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
          nil -> [id, lease_token, fencing_token]
          value -> [id, value, lease_token, fencing_token]
        end

      take_compact_claimed_items(count - 1, rest, [item | acc])
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

  defp put_compact_bool(payload, _key, 0), do: payload
  defp put_compact_bool(payload, key, 1), do: Map.put(payload, key, false)
  defp put_compact_bool(payload, key, 2), do: Map.put(payload, key, true)
  defp put_compact_bool(payload, _key, _value), do: payload

  defp put_optional_binary(payload, _key, nil), do: payload
  defp put_optional_binary(payload, key, value), do: Map.put(payload, key, value)

  defp put_block_ms(payload, value) when is_integer(value) and value >= 0,
    do: Map.put(payload, "block_ms", value)

  defp put_block_ms(payload, _value), do: payload

  defp put_priority(payload, -9_223_372_036_854_775_808), do: payload
  defp put_priority(payload, value), do: Map.put(payload, "priority", value)

  defp put_return_mode(payload, 0), do: payload
  defp put_return_mode(payload, 1), do: Map.put(payload, "return", "jobs_compact")
  defp put_return_mode(payload, 2), do: Map.put(payload, "return", "jobs_compact_state")
  defp put_return_mode(payload, _mode), do: payload

  defp put_create_many_return_mode(payload, 0), do: payload
  defp put_create_many_return_mode(payload, 1), do: Map.put(payload, "return", "OK_ON_SUCCESS")
  defp put_create_many_return_mode(payload, _mode), do: payload

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
  def decode_value(<<0, rest::binary>>), do: {:ok, nil, rest}
  def decode_value(<<1, rest::binary>>), do: {:ok, true, rest}
  def decode_value(<<2, rest::binary>>), do: {:ok, false, rest}
  def decode_value(<<3, value::signed-64, rest::binary>>), do: {:ok, value, rest}

  def decode_value(<<4, len::unsigned-32, rest::binary>>) do
    decode_binary(len, rest)
  end

  def decode_value(<<5, count::unsigned-32, rest::binary>>) do
    decode_array(count, rest, [])
  end

  def decode_value(<<6, count::unsigned-32, rest::binary>>) do
    decode_map(count, rest, %{})
  end

  def decode_value(<<7, value::float-64, rest::binary>>), do: {:ok, value, rest}
  def decode_value(<<>>), do: {:error, "ERR native value is empty"}
  def decode_value(_), do: {:error, "ERR native value has unknown or truncated tag"}

  defp decode_binary(len, rest) when byte_size(rest) >= len do
    <<value::binary-size(len), next::binary>> = rest
    {:ok, value, next}
  end

  defp decode_binary(_len, _rest), do: {:error, "ERR native binary value is truncated"}

  defp decode_array(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_array(count, rest, acc) do
    case decode_value(rest) do
      {:ok, value, next} -> decode_array(count - 1, next, [value | acc])
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_map(0, rest, acc), do: {:ok, acc, rest}

  defp decode_map(count, <<key_len::unsigned-32, rest::binary>>, acc) do
    with {:ok, key, after_key} <- decode_binary(key_len, rest),
         {:ok, value, after_value} <- decode_value(after_key) do
      decode_map(count - 1, after_value, Map.put(acc, key, value))
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_map(_count, _rest, _acc), do: {:error, "ERR native map value is truncated"}

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
