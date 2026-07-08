defmodule Ferricstore.Flow.Codec do
  @moduledoc false

  import Bitwise

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.Codec.Primitives
  alias Ferricstore.Flow.Codec.Support

  @history_tag :flow_history_v1

  # Flow records and history are durable bytes. Before Flow is public, keep one
  # current compact schema and change it directly. Once users can have persisted
  # Flow data, incompatible field-order/type changes need explicit migration.
  @record_bin_magic "FSF5"
  @history_bin_magic "FSH2"

  # FSF5 stores only the required mutable state fields inline. Optional fields
  # are controlled by the flag word below so nil/default values do not repeat on
  # every state record. Keep this layout in lockstep with the Rust NIF codec.
  @record_flag_attempts 1 <<< 0
  @record_flag_fencing_token 1 <<< 1
  @record_flag_next_run_at_ms 1 <<< 2
  @record_flag_priority 1 <<< 3
  @record_flag_ttl_ms 1 <<< 4
  @record_flag_history_hot_max_events 1 <<< 5
  @record_flag_history_max_events 1 <<< 6
  @record_flag_retention_ttl_ms 1 <<< 7
  @record_flag_terminal_retention_until_ms 1 <<< 8
  @record_flag_partition_key 1 <<< 9
  @record_flag_payload_ref 1 <<< 10
  @record_flag_parent_flow_id 1 <<< 11
  @record_flag_parent_partition_key 1 <<< 12
  @record_flag_root_flow_id 1 <<< 13
  @record_flag_root_flow_id_self 1 <<< 14
  @record_flag_correlation_id 1 <<< 15
  @record_flag_result_ref 1 <<< 16
  @record_flag_error_ref 1 <<< 17
  @record_flag_lease_owner 1 <<< 18
  @record_flag_lease_token 1 <<< 19
  @record_flag_lease_deadline_ms 1 <<< 20
  @record_flag_run_state 1 <<< 21
  @record_flag_rewound_to_event_id 1 <<< 22
  @record_flag_sidecar 1 <<< 23
  @record_flag_max_active_ms 1 <<< 24

  # FSH2 stores per-event history only. Immutable workflow metadata such as id,
  # type, parent/root, and correlation id is restored from the current/snapshot
  # record when user-facing history is decoded.
  @history_flag_priority 1 <<< 0
  @history_flag_attempts 1 <<< 1
  @history_flag_fencing_token 1 <<< 2
  @history_flag_created_at_ms 1 <<< 3
  @history_flag_updated_at_ms 1 <<< 4
  @history_flag_next_run_at_ms 1 <<< 5
  @history_flag_lease_deadline_ms 1 <<< 6
  @history_flag_lease_owner 1 <<< 7
  @history_flag_payload_ref 1 <<< 8
  @history_flag_result_ref 1 <<< 9
  @history_flag_error_ref 1 <<< 10
  @history_flag_rewound_to_event_id 1 <<< 11
  @history_flag_meta 1 <<< 12

  @doc false
  # Encodes the current Flow metadata schema. User payload bytes are not encoded
  # here; only payload_ref/result_ref/error_ref metadata is stored. Flow records
  # are not public-persisted yet, so this intentionally supports one current
  # format.
  def encode_record(record) when is_map(record) do
    NIF.flow_record_encode(
      Map.get(record, :id),
      Map.get(record, :type),
      Map.get(record, :state),
      Map.get(record, :version),
      Map.get(record, :attempts),
      Map.get(record, :fencing_token),
      Map.get(record, :created_at_ms),
      Map.get(record, :updated_at_ms),
      Map.get(record, :next_run_at_ms),
      Map.get(record, :priority),
      Map.get(record, :ttl_ms),
      Map.get(record, :history_hot_max_events),
      Map.get(record, :history_max_events),
      Map.get(record, :retention_ttl_ms),
      Map.get(record, :terminal_retention_until_ms),
      Map.get(record, :max_active_ms),
      Map.get(record, :partition_key),
      Map.get(record, :payload_ref),
      Map.get(record, :parent_flow_id),
      Map.get(record, :parent_partition_key),
      Map.get(record, :root_flow_id),
      Map.get(record, :correlation_id),
      Map.get(record, :result_ref),
      Map.get(record, :error_ref),
      Map.get(record, :lease_owner),
      Map.get(record, :lease_token),
      Map.get(record, :lease_deadline_ms),
      Map.get(record, :run_state),
      Map.get(record, :rewound_to_event_id),
      record |> Support.encode_record_sidecar() |> IO.iodata_to_binary()
    )
  end

  @doc false
  def encode_record_elixir(record) when is_map(record) do
    sidecar =
      record
      |> Support.encode_record_sidecar()
      |> IO.iodata_to_binary()

    flags = Support.encode_record_flags(record, sidecar)

    # Wire order is part of the durable schema. Add new fields as flagged
    # trailing data or bump @record_bin_magic and update the Rust NIF/test
    # parity checks.
    [
      @record_bin_magic,
      encode_int(flags),
      encode_bin(Map.get(record, :id)),
      encode_bin(Map.get(record, :type)),
      encode_bin(Map.get(record, :state)),
      encode_int(Map.get(record, :version)),
      encode_int(Map.get(record, :created_at_ms)),
      encode_int(Map.get(record, :updated_at_ms)),
      Support.encode_flagged_int(flags, @record_flag_attempts, Map.get(record, :attempts)),
      Support.encode_flagged_int(
        flags,
        @record_flag_fencing_token,
        Map.get(record, :fencing_token)
      ),
      Support.encode_flagged_int(
        flags,
        @record_flag_next_run_at_ms,
        Map.get(record, :next_run_at_ms)
      ),
      Support.encode_flagged_int(flags, @record_flag_priority, Map.get(record, :priority)),
      Support.encode_flagged_int(flags, @record_flag_ttl_ms, Map.get(record, :ttl_ms)),
      Support.encode_flagged_int(
        flags,
        @record_flag_history_hot_max_events,
        Map.get(record, :history_hot_max_events)
      ),
      Support.encode_flagged_int(
        flags,
        @record_flag_history_max_events,
        Map.get(record, :history_max_events)
      ),
      Support.encode_flagged_int(
        flags,
        @record_flag_retention_ttl_ms,
        Map.get(record, :retention_ttl_ms)
      ),
      Support.encode_flagged_int(
        flags,
        @record_flag_terminal_retention_until_ms,
        Map.get(record, :terminal_retention_until_ms)
      ),
      Support.encode_flagged_int(
        flags,
        @record_flag_max_active_ms,
        Map.get(record, :max_active_ms)
      ),
      Support.encode_flagged_bin(
        flags,
        @record_flag_partition_key,
        Map.get(record, :partition_key)
      ),
      Support.encode_flagged_bin(flags, @record_flag_payload_ref, Map.get(record, :payload_ref)),
      Support.encode_flagged_bin(
        flags,
        @record_flag_parent_flow_id,
        Map.get(record, :parent_flow_id)
      ),
      Support.encode_flagged_bin(
        flags,
        @record_flag_parent_partition_key,
        Map.get(record, :parent_partition_key)
      ),
      Support.encode_flagged_bin(
        flags,
        @record_flag_root_flow_id,
        Map.get(record, :root_flow_id)
      ),
      Support.encode_flagged_bin(
        flags,
        @record_flag_correlation_id,
        Map.get(record, :correlation_id)
      ),
      Support.encode_flagged_bin(flags, @record_flag_result_ref, Map.get(record, :result_ref)),
      Support.encode_flagged_bin(flags, @record_flag_error_ref, Map.get(record, :error_ref)),
      Support.encode_flagged_bin(flags, @record_flag_lease_owner, Map.get(record, :lease_owner)),
      Support.encode_flagged_bin(flags, @record_flag_lease_token, Map.get(record, :lease_token)),
      Support.encode_flagged_int(
        flags,
        @record_flag_lease_deadline_ms,
        Map.get(record, :lease_deadline_ms)
      ),
      Support.encode_flagged_bin(flags, @record_flag_run_state, Map.get(record, :run_state)),
      Support.encode_flagged_bin(
        flags,
        @record_flag_rewound_to_event_id,
        Map.get(record, :rewound_to_event_id)
      ),
      if((flags &&& @record_flag_sidecar) != 0, do: sidecar, else: [])
    ]
    |> IO.iodata_to_binary()
  end

  @doc false
  def encode_value(value), do: Support.encode_value(value)

  @doc false
  def decode_value(value), do: Support.decode_value(value)

  @doc false
  def decode_value_with_user_size(value), do: Support.decode_value_with_user_size(value)

  @doc false
  # Flow has not shipped as a public durable format yet, so recovery accepts
  # only the current compact record encoding.
  def record_blob?(@record_bin_magic <> _rest), do: true
  def record_blob?(_value), do: false

  def decode_record(@record_bin_magic <> _rest = value) do
    case NIF.flow_record_decode(value) do
      {:ok, fields} -> decode_record_fields(fields)
      _ -> raise(ArgumentError, "invalid flow record")
    end
  end

  def decode_record(_value), do: raise(ArgumentError, "invalid flow record")

  def decode_record_meta(@record_bin_magic <> _rest = value) do
    case NIF.flow_record_decode_meta(value) do
      {:ok, fields} -> decode_record_meta_fields(fields)
      _ -> raise(ArgumentError, "invalid flow record")
    end
  end

  def decode_record_meta(_value), do: raise(ArgumentError, "invalid flow record")

  @doc false
  def decode_record_elixir(@record_bin_magic <> rest), do: decode_record_bin(rest)

  def decode_record_elixir(_value), do: raise(ArgumentError, "invalid flow record")

  @doc false
  # History entries have their own durable schema because they are retained for
  # audit/debug and rewind.
  def encode_history_fields(record, event, now_ms, meta \\ %{})
      when is_map(record) and is_binary(event) and is_integer(now_ms) do
    encode_history_parts(
      event,
      Map.get(record, :version),
      now_ms,
      Map.get(record, :id),
      Map.get(record, :type),
      Map.get(record, :state),
      Map.get(record, :priority, 0),
      Map.get(record, :attempts, 0),
      Map.get(record, :fencing_token, 0),
      Map.get(record, :created_at_ms, now_ms),
      Map.get(record, :updated_at_ms, now_ms),
      Map.get(record, :next_run_at_ms),
      Map.get(record, :lease_deadline_ms),
      Map.get(record, :lease_owner),
      Map.get(record, :payload_ref),
      Map.get(record, :parent_flow_id),
      Map.get(record, :root_flow_id),
      Map.get(record, :correlation_id),
      Map.get(record, :result_ref),
      Map.get(record, :error_ref),
      Map.get(record, :rewound_to_event_id),
      Support.normalize_history_meta(Support.record_history_meta(record, meta))
    )
  end

  @doc false
  def encode_history_fields_elixir(record, event, now_ms, meta \\ %{})
      when is_map(record) and is_binary(event) and is_integer(now_ms) do
    encode_history_parts_elixir(
      event,
      Map.get(record, :version),
      now_ms,
      Map.get(record, :id),
      Map.get(record, :type),
      Map.get(record, :state),
      Map.get(record, :priority, 0),
      Map.get(record, :attempts, 0),
      Map.get(record, :fencing_token, 0),
      Map.get(record, :created_at_ms, now_ms),
      Map.get(record, :updated_at_ms, now_ms),
      Map.get(record, :next_run_at_ms),
      Map.get(record, :lease_deadline_ms),
      Map.get(record, :lease_owner),
      Map.get(record, :payload_ref),
      Map.get(record, :parent_flow_id),
      Map.get(record, :root_flow_id),
      Map.get(record, :correlation_id),
      Map.get(record, :result_ref),
      Map.get(record, :error_ref),
      Map.get(record, :rewound_to_event_id),
      Support.normalize_history_meta(Support.record_history_meta(record, meta))
    )
  end

  @doc false
  def history_snapshot(record, event, now_ms, meta \\ %{})
      when is_map(record) and is_binary(event) and is_integer(now_ms) do
    {
      event,
      Map.get(record, :version),
      now_ms,
      Map.get(record, :id),
      Map.get(record, :type),
      Map.get(record, :state),
      Map.get(record, :priority, 0),
      Map.get(record, :attempts, 0),
      Map.get(record, :fencing_token, 0),
      Map.get(record, :created_at_ms, now_ms),
      Map.get(record, :updated_at_ms, now_ms),
      Map.get(record, :next_run_at_ms),
      Map.get(record, :lease_deadline_ms),
      Map.get(record, :lease_owner),
      Map.get(record, :payload_ref),
      Map.get(record, :parent_flow_id),
      Map.get(record, :root_flow_id),
      Map.get(record, :correlation_id),
      Map.get(record, :result_ref),
      Map.get(record, :error_ref),
      Map.get(record, :rewound_to_event_id),
      Support.normalize_history_meta(Support.record_history_meta(record, meta))
    }
  end

  @doc false
  def encode_history_snapshot({
        event,
        version,
        now_ms,
        id,
        type,
        state,
        priority,
        attempts,
        fencing_token,
        created_at_ms,
        updated_at_ms,
        next_run_at_ms,
        lease_deadline_ms,
        lease_owner,
        payload_ref,
        parent_flow_id,
        root_flow_id,
        correlation_id,
        result_ref,
        error_ref,
        rewound_to_event_id,
        meta_fields
      }) do
    encode_history_parts(
      event,
      version,
      now_ms,
      id,
      type,
      state,
      priority,
      attempts,
      fencing_token,
      created_at_ms,
      updated_at_ms,
      next_run_at_ms,
      lease_deadline_ms,
      lease_owner,
      payload_ref,
      parent_flow_id,
      root_flow_id,
      correlation_id,
      result_ref,
      error_ref,
      rewound_to_event_id,
      meta_fields
    )
  end

  defp encode_history_parts(
         event,
         version,
         now_ms,
         id,
         type,
         state,
         priority,
         attempts,
         fencing_token,
         created_at_ms,
         updated_at_ms,
         next_run_at_ms,
         lease_deadline_ms,
         lease_owner,
         payload_ref,
         parent_flow_id,
         root_flow_id,
         correlation_id,
         result_ref,
         error_ref,
         rewound_to_event_id,
         meta_fields
       ) do
    NIF.flow_history_encode(
      event,
      version,
      now_ms,
      id,
      type,
      state,
      priority,
      attempts,
      fencing_token,
      created_at_ms,
      updated_at_ms,
      next_run_at_ms,
      lease_deadline_ms,
      lease_owner,
      payload_ref,
      parent_flow_id,
      root_flow_id,
      correlation_id,
      result_ref,
      error_ref,
      rewound_to_event_id,
      meta_fields |> Support.encode_history_meta() |> IO.iodata_to_binary()
    )
  end

  defp encode_history_parts_elixir(
         event,
         version,
         now_ms,
         _id,
         _type,
         state,
         priority,
         attempts,
         fencing_token,
         created_at_ms,
         updated_at_ms,
         next_run_at_ms,
         lease_deadline_ms,
         lease_owner,
         payload_ref,
         _parent_flow_id,
         _root_flow_id,
         _correlation_id,
         result_ref,
         error_ref,
         rewound_to_event_id,
         meta_fields
       ) do
    flags =
      encode_history_flags(
        priority,
        attempts,
        fencing_token,
        created_at_ms,
        updated_at_ms,
        now_ms,
        next_run_at_ms,
        lease_deadline_ms,
        lease_owner,
        payload_ref,
        result_ref,
        error_ref,
        rewound_to_event_id,
        meta_fields
      )

    # History entries intentionally omit immutable workflow identity fields.
    # decode_history_fields/2 must get record context when callers need the full
    # protocol-facing history shape.
    [
      @history_bin_magic,
      encode_int(flags),
      encode_bin(event),
      encode_int(version),
      encode_int(now_ms),
      encode_bin(state),
      Support.encode_flagged_int(flags, @history_flag_priority, priority),
      Support.encode_flagged_int(flags, @history_flag_attempts, attempts),
      Support.encode_flagged_int(flags, @history_flag_fencing_token, fencing_token),
      Support.encode_flagged_int(flags, @history_flag_created_at_ms, created_at_ms),
      Support.encode_flagged_int(flags, @history_flag_updated_at_ms, updated_at_ms),
      Support.encode_flagged_int(flags, @history_flag_next_run_at_ms, next_run_at_ms),
      Support.encode_flagged_int(flags, @history_flag_lease_deadline_ms, lease_deadline_ms),
      Support.encode_flagged_bin(flags, @history_flag_lease_owner, lease_owner),
      Support.encode_flagged_bin(flags, @history_flag_payload_ref, payload_ref),
      Support.encode_flagged_bin(flags, @history_flag_result_ref, result_ref),
      Support.encode_flagged_bin(flags, @history_flag_error_ref, error_ref),
      Support.encode_flagged_bin(flags, @history_flag_rewound_to_event_id, rewound_to_event_id),
      if((flags &&& @history_flag_meta) != 0,
        do: Support.encode_history_meta(meta_fields),
        else: []
      )
    ]
    |> IO.iodata_to_binary()
  end

  defp encode_history_flags(
         priority,
         attempts,
         fencing_token,
         created_at_ms,
         updated_at_ms,
         now_ms,
         next_run_at_ms,
         lease_deadline_ms,
         lease_owner,
         payload_ref,
         result_ref,
         error_ref,
         rewound_to_event_id,
         meta_fields
       ) do
    0
    |> Support.maybe_put_flag(@history_flag_priority, is_integer(priority) and priority != 0)
    |> Support.maybe_put_flag(@history_flag_attempts, is_integer(attempts) and attempts != 0)
    |> Support.maybe_put_flag(
      @history_flag_fencing_token,
      is_integer(fencing_token) and fencing_token != 0
    )
    |> Support.maybe_put_flag(
      @history_flag_created_at_ms,
      is_integer(created_at_ms) and created_at_ms != now_ms
    )
    |> Support.maybe_put_flag(
      @history_flag_updated_at_ms,
      is_integer(updated_at_ms) and updated_at_ms != now_ms
    )
    |> Support.maybe_put_flag(@history_flag_next_run_at_ms, is_integer(next_run_at_ms))
    |> Support.maybe_put_flag(
      @history_flag_lease_deadline_ms,
      is_integer(lease_deadline_ms) and lease_deadline_ms != 0
    )
    |> Support.maybe_put_flag(@history_flag_lease_owner, Support.nonempty_binary?(lease_owner))
    |> Support.maybe_put_flag(@history_flag_payload_ref, Support.nonempty_binary?(payload_ref))
    |> Support.maybe_put_flag(@history_flag_result_ref, Support.nonempty_binary?(result_ref))
    |> Support.maybe_put_flag(@history_flag_error_ref, Support.nonempty_binary?(error_ref))
    |> Support.maybe_put_flag(
      @history_flag_rewound_to_event_id,
      Support.nonempty_binary?(rewound_to_event_id)
    )
    |> Support.maybe_put_flag(@history_flag_meta, is_list(meta_fields) and meta_fields != [])
  end

  @doc false
  # Decode history into the current protocol-facing field list. FSH2 callers should
  # pass the state record/context so omitted immutable fields can be restored.
  def decode_history_fields(value, context \\ %{})

  def decode_history_fields(@history_bin_magic <> rest, context),
    do: decode_history_fields_bin(rest, context)

  def decode_history_fields(_value, _context), do: []

  @doc false
  def decode_history_fields_elixir(value, context \\ %{})

  def decode_history_fields_elixir(@history_bin_magic <> rest, context),
    do: decode_history_fields_bin(rest, context)

  def decode_history_fields_elixir(_value, _context), do: []

  defp decode_history_fields_term(
         {
           @history_tag,
           event,
           version,
           at,
           id,
           type,
           state,
           priority,
           attempts,
           fencing_token,
           created_at_ms,
           updated_at_ms,
           next_run_at_ms,
           lease_deadline_ms,
           lease_owner,
           payload_ref,
           parent_flow_id,
           root_flow_id,
           correlation_id,
           result_ref,
           error_ref,
           rewound_to_event_id,
           meta_fields
         },
         context
       ) do
    id = Support.history_context_string(context, :id, id)
    type = Support.history_context_string(context, :type, type)
    parent_flow_id = Support.history_context_string(context, :parent_flow_id, parent_flow_id)
    root_flow_id = Support.history_context_string(context, :root_flow_id, root_flow_id)
    correlation_id = Support.history_context_string(context, :correlation_id, correlation_id)

    base_fields = [
      "event",
      event,
      "version",
      Support.history_integer(version),
      "at",
      Support.history_integer(at),
      "id",
      Support.history_string(id),
      "type",
      Support.history_string(type),
      "state",
      Support.history_string(state),
      "priority",
      Support.history_integer(priority),
      "attempts",
      Support.history_integer(attempts),
      "fencing_token",
      Support.history_integer(fencing_token),
      "created_at_ms",
      Support.history_integer(created_at_ms),
      "updated_at_ms",
      Support.history_integer(updated_at_ms),
      "next_run_at_ms",
      Support.history_optional_integer(next_run_at_ms),
      "lease_deadline_ms",
      Support.history_optional_integer(lease_deadline_ms),
      "lease_owner",
      Support.history_string(lease_owner),
      "payload_ref",
      Support.history_string(payload_ref),
      "parent_flow_id",
      Support.history_string(parent_flow_id),
      "root_flow_id",
      Support.history_string(root_flow_id),
      "correlation_id",
      Support.history_string(correlation_id),
      "result_ref",
      Support.history_string(result_ref),
      "error_ref",
      Support.history_string(error_ref),
      "rewound_to_event_id",
      Support.history_string(rewound_to_event_id)
    ]

    base_fields ++ Support.normalize_history_decoded_meta(meta_fields)
  end

  defp decode_history_fields_term(_value, _context), do: []

  defp decode_record_bin(rest) do
    with {:ok, flags, rest} <- decode_int(rest),
         flags when is_integer(flags) <- flags,
         {:ok, id, rest} <- decode_bin(rest),
         {:ok, type, rest} <- decode_bin(rest),
         {:ok, state, rest} <- decode_bin(rest),
         {:ok, version, rest} <- decode_int(rest),
         {:ok, created_at_ms, rest} <- decode_int(rest),
         {:ok, updated_at_ms, rest} <- decode_int(rest),
         {:ok, attempts, rest} <- decode_flagged_int(flags, @record_flag_attempts, rest, 0),
         {:ok, fencing_token, rest} <-
           decode_flagged_int(flags, @record_flag_fencing_token, rest, 0),
         {:ok, next_run_at_ms, rest} <-
           decode_flagged_int(flags, @record_flag_next_run_at_ms, rest, nil),
         {:ok, priority, rest} <- decode_flagged_int(flags, @record_flag_priority, rest, 0),
         {:ok, ttl_ms, rest} <- decode_flagged_int(flags, @record_flag_ttl_ms, rest, nil),
         {:ok, history_hot_max_events, rest} <-
           decode_flagged_int(flags, @record_flag_history_hot_max_events, rest, nil),
         {:ok, history_max_events, rest} <-
           decode_flagged_int(flags, @record_flag_history_max_events, rest, nil),
         {:ok, retention_ttl_ms, rest} <-
           decode_flagged_int(flags, @record_flag_retention_ttl_ms, rest, nil),
         {:ok, terminal_retention_until_ms, rest} <-
           decode_flagged_int(flags, @record_flag_terminal_retention_until_ms, rest, nil),
         {:ok, max_active_ms, rest} <-
           decode_flagged_int(flags, @record_flag_max_active_ms, rest, nil),
         {:ok, partition_key, rest} <-
           decode_flagged_bin(flags, @record_flag_partition_key, rest, nil),
         {:ok, payload_ref, rest} <-
           decode_flagged_bin(flags, @record_flag_payload_ref, rest, nil),
         {:ok, parent_flow_id, rest} <-
           decode_flagged_bin(flags, @record_flag_parent_flow_id, rest, nil),
         {:ok, parent_partition_key, rest} <-
           decode_flagged_bin(flags, @record_flag_parent_partition_key, rest, nil),
         {:ok, root_flow_id, rest} <- decode_record_root(flags, id, rest),
         {:ok, correlation_id, rest} <-
           decode_flagged_bin(flags, @record_flag_correlation_id, rest, nil),
         {:ok, result_ref, rest} <- decode_flagged_bin(flags, @record_flag_result_ref, rest, nil),
         {:ok, error_ref, rest} <- decode_flagged_bin(flags, @record_flag_error_ref, rest, nil),
         {:ok, lease_owner, rest} <-
           decode_flagged_bin(flags, @record_flag_lease_owner, rest, nil),
         {:ok, lease_token, rest} <-
           decode_flagged_bin(flags, @record_flag_lease_token, rest, nil),
         {:ok, lease_deadline_ms, rest} <-
           decode_flagged_int(flags, @record_flag_lease_deadline_ms, rest, 0),
         {:ok, run_state, rest} <- decode_flagged_bin(flags, @record_flag_run_state, rest, nil),
         {:ok, rewound_to_event_id, rest} <-
           decode_flagged_bin(flags, @record_flag_rewound_to_event_id, rest, nil),
         {:ok, child_groups, ""} <- decode_record_sidecar(flags, rest) do
      {child_groups, value_refs, attributes, indexed_attributes, state_meta, indexed_state_meta,
       state_enter_seq} = Support.split_record_sidecar(child_groups)

      record =
        %{
          id: id,
          type: type,
          state: state,
          version: version,
          attempts: attempts,
          fencing_token: fencing_token,
          created_at_ms: created_at_ms,
          updated_at_ms: updated_at_ms,
          next_run_at_ms: next_run_at_ms,
          priority: priority,
          ttl_ms: ttl_ms,
          history_hot_max_events: history_hot_max_events,
          history_max_events: history_max_events,
          retention_ttl_ms: retention_ttl_ms,
          terminal_retention_until_ms: terminal_retention_until_ms,
          partition_key: partition_key,
          payload_ref: payload_ref,
          parent_flow_id: parent_flow_id,
          parent_partition_key: parent_partition_key,
          root_flow_id: root_flow_id,
          correlation_id: correlation_id,
          result_ref: result_ref,
          error_ref: error_ref,
          lease_owner: lease_owner,
          lease_token: lease_token,
          lease_deadline_ms: lease_deadline_ms,
          run_state: run_state,
          child_groups: Support.normalize_child_groups(child_groups)
        }
        |> maybe_put_decoded_value_refs(value_refs)
        |> maybe_put_decoded_attributes(attributes)
        |> maybe_put_decoded_indexed_attributes(indexed_attributes)
        |> maybe_put_decoded_state_meta(state_meta)
        |> maybe_put_decoded_indexed_state_meta(indexed_state_meta)
        |> maybe_put_decoded_state_enter_seq(state_enter_seq)
        |> maybe_put_decoded_max_active_ms(max_active_ms)

      if is_nil(rewound_to_event_id) do
        record
      else
        Map.put(record, :rewound_to_event_id, rewound_to_event_id)
      end
    else
      _ -> raise ArgumentError, "invalid flow record"
    end
  end

  defp decode_flagged_int(flags, flag, rest, default) do
    if (flags &&& flag) != 0 do
      decode_int(rest)
    else
      {:ok, default, rest}
    end
  end

  defp decode_flagged_bin(flags, flag, rest, default) do
    if (flags &&& flag) != 0 do
      decode_bin(rest)
    else
      {:ok, default, rest}
    end
  end

  defp decode_record_root(flags, id, rest) do
    cond do
      (flags &&& @record_flag_root_flow_id_self) != 0 ->
        {:ok, id, rest}

      (flags &&& @record_flag_root_flow_id) != 0 ->
        decode_bin(rest)

      true ->
        {:ok, nil, rest}
    end
  end

  defp decode_record_sidecar(flags, rest) do
    if (flags &&& @record_flag_sidecar) != 0 do
      Support.decode_child_groups(rest)
    else
      empty = Support.record_empty_sidecar()
      {:ok, child_groups, ""} = Support.decode_child_groups(empty)
      {:ok, child_groups, rest}
    end
  end

  defp decode_record_fields([
         id,
         type,
         state,
         version,
         attempts,
         fencing_token,
         created_at_ms,
         updated_at_ms,
         next_run_at_ms,
         priority,
         ttl_ms,
         history_hot_max_events,
         history_max_events,
         retention_ttl_ms,
         terminal_retention_until_ms,
         max_active_ms,
         partition_key,
         payload_ref,
         parent_flow_id,
         parent_partition_key,
         root_flow_id,
         correlation_id,
         result_ref,
         error_ref,
         lease_owner,
         lease_token,
         lease_deadline_ms,
         run_state,
         rewound_to_event_id,
         child_groups_encoded
       ])
       when is_binary(child_groups_encoded) do
    with {:ok, child_groups, ""} <- Support.decode_child_groups(child_groups_encoded) do
      {child_groups, value_refs, attributes, indexed_attributes, state_meta, indexed_state_meta,
       state_enter_seq} = Support.split_record_sidecar(child_groups)

      record =
        %{
          id: id,
          type: type,
          state: state,
          version: version,
          attempts: attempts,
          fencing_token: fencing_token,
          created_at_ms: created_at_ms,
          updated_at_ms: updated_at_ms,
          next_run_at_ms: next_run_at_ms,
          priority: priority,
          ttl_ms: ttl_ms,
          history_hot_max_events: history_hot_max_events,
          history_max_events: history_max_events,
          retention_ttl_ms: retention_ttl_ms,
          terminal_retention_until_ms: terminal_retention_until_ms,
          partition_key: partition_key,
          payload_ref: payload_ref,
          parent_flow_id: parent_flow_id,
          parent_partition_key: parent_partition_key,
          root_flow_id: root_flow_id,
          correlation_id: correlation_id,
          result_ref: result_ref,
          error_ref: error_ref,
          lease_owner: lease_owner,
          lease_token: lease_token,
          lease_deadline_ms: lease_deadline_ms,
          run_state: run_state,
          child_groups: Support.normalize_child_groups(child_groups)
        }
        |> maybe_put_decoded_value_refs(value_refs)
        |> maybe_put_decoded_attributes(attributes)
        |> maybe_put_decoded_indexed_attributes(indexed_attributes)
        |> maybe_put_decoded_state_meta(state_meta)
        |> maybe_put_decoded_indexed_state_meta(indexed_state_meta)
        |> maybe_put_decoded_state_enter_seq(state_enter_seq)
        |> maybe_put_decoded_max_active_ms(max_active_ms)

      if is_nil(rewound_to_event_id) do
        record
      else
        Map.put(record, :rewound_to_event_id, rewound_to_event_id)
      end
    else
      _ -> raise ArgumentError, "invalid flow record"
    end
  end

  defp decode_record_fields(_fields), do: raise(ArgumentError, "invalid flow record")

  defp decode_record_meta_fields([
         id,
         type,
         state,
         version,
         priority,
         partition_key,
         payload_ref,
         result_ref,
         error_ref,
         created_at_ms,
         updated_at_ms,
         next_run_at_ms,
         lease_deadline_ms,
         lease_owner,
         lease_token,
         fencing_token,
         attempts,
         run_state,
         max_active_ms,
         child_groups_encoded
       ])
       when is_binary(child_groups_encoded) do
    with {:ok, child_groups, ""} <- Support.decode_child_groups(child_groups_encoded) do
      {_child_groups, value_refs, attributes, indexed_attributes, state_meta, indexed_state_meta,
       _state_enter_seq} = Support.split_record_sidecar(child_groups)

      %{
        id: id,
        type: type,
        state: state,
        version: version,
        priority: priority,
        partition_key: partition_key,
        payload_ref: payload_ref,
        result_ref: result_ref,
        error_ref: error_ref,
        created_at_ms: created_at_ms,
        updated_at_ms: updated_at_ms,
        next_run_at_ms: next_run_at_ms,
        lease_deadline_ms: lease_deadline_ms,
        lease_owner: lease_owner,
        lease_token: lease_token,
        fencing_token: fencing_token,
        attempts: attempts,
        run_state: run_state
      }
      |> maybe_put_decoded_value_refs(value_refs)
      |> maybe_put_decoded_attributes(attributes)
      |> maybe_put_decoded_indexed_attributes(indexed_attributes)
      |> maybe_put_decoded_state_meta(state_meta)
      |> maybe_put_decoded_indexed_state_meta(indexed_state_meta)
      |> maybe_put_decoded_max_active_ms(max_active_ms)
    else
      _ -> raise ArgumentError, "invalid flow record"
    end
  end

  defp decode_record_meta_fields(_fields), do: raise(ArgumentError, "invalid flow record")

  defp maybe_put_decoded_value_refs(record, refs) when is_map(refs) and map_size(refs) > 0,
    do: Map.put(record, :value_refs, refs)

  defp maybe_put_decoded_value_refs(record, _refs), do: record

  defp maybe_put_decoded_attributes(record, attrs) when is_map(attrs) and map_size(attrs) > 0,
    do: Map.put(record, :attributes, attrs)

  defp maybe_put_decoded_attributes(record, _attrs), do: record

  defp maybe_put_decoded_indexed_attributes(record, names) when is_list(names) and names != [],
    do: Map.put(record, :indexed_attributes, names)

  defp maybe_put_decoded_indexed_attributes(record, _names), do: record

  defp maybe_put_decoded_state_meta(record, state_meta)
       when is_map(state_meta) and map_size(state_meta) > 0,
       do: Map.put(record, :state_meta, state_meta)

  defp maybe_put_decoded_state_meta(record, _state_meta), do: record

  defp maybe_put_decoded_indexed_state_meta(record, key) when is_binary(key) and key != "",
    do: Map.put(record, :indexed_state_meta, key)

  defp maybe_put_decoded_indexed_state_meta(record, _key), do: record

  defp maybe_put_decoded_state_enter_seq(record, seq) when is_integer(seq) and seq >= 0,
    do: Map.put(record, :state_enter_seq, seq)

  defp maybe_put_decoded_state_enter_seq(record, _seq), do: record

  defp maybe_put_decoded_max_active_ms(record, max_active_ms)
       when is_integer(max_active_ms) and max_active_ms > 0,
       do: Map.put(record, :max_active_ms, max_active_ms)

  defp maybe_put_decoded_max_active_ms(record, _max_active_ms), do: record

  defp decode_history_fields_bin(rest, context) do
    with {:ok, flags, rest} <- decode_int(rest),
         flags when is_integer(flags) <- flags,
         {:ok, event, rest} <- decode_bin(rest),
         {:ok, version, rest} <- decode_int(rest),
         {:ok, at, rest} <- decode_int(rest),
         {:ok, state, rest} <- decode_bin(rest),
         {:ok, priority, rest} <- decode_flagged_int(flags, @history_flag_priority, rest, 0),
         {:ok, attempts, rest} <- decode_flagged_int(flags, @history_flag_attempts, rest, 0),
         {:ok, fencing_token, rest} <-
           decode_flagged_int(flags, @history_flag_fencing_token, rest, 0),
         {:ok, created_at_ms, rest} <-
           decode_flagged_int(flags, @history_flag_created_at_ms, rest, at),
         {:ok, updated_at_ms, rest} <-
           decode_flagged_int(flags, @history_flag_updated_at_ms, rest, at),
         {:ok, next_run_at_ms, rest} <-
           decode_flagged_int(flags, @history_flag_next_run_at_ms, rest, nil),
         {:ok, lease_deadline_ms, rest} <-
           decode_flagged_int(flags, @history_flag_lease_deadline_ms, rest, nil),
         {:ok, lease_owner, rest} <-
           decode_flagged_bin(flags, @history_flag_lease_owner, rest, nil),
         {:ok, payload_ref, rest} <-
           decode_flagged_bin(flags, @history_flag_payload_ref, rest, nil),
         {:ok, result_ref, rest} <- decode_flagged_bin(flags, @history_flag_result_ref, rest, nil),
         {:ok, error_ref, rest} <- decode_flagged_bin(flags, @history_flag_error_ref, rest, nil),
         {:ok, rewound_to_event_id, rest} <-
           decode_flagged_bin(flags, @history_flag_rewound_to_event_id, rest, nil),
         {:ok, meta_fields, ""} <- Support.decode_history_meta_for_flags(flags, rest) do
      decode_history_fields_term(
        {
          @history_tag,
          event,
          version,
          at,
          nil,
          nil,
          state,
          priority,
          attempts,
          fencing_token,
          created_at_ms,
          updated_at_ms,
          next_run_at_ms,
          lease_deadline_ms,
          lease_owner,
          payload_ref,
          nil,
          nil,
          nil,
          result_ref,
          error_ref,
          rewound_to_event_id,
          meta_fields
        },
        context
      )
    else
      _ -> []
    end
  end

  def encode_int(value), do: Primitives.encode_int(value)
  def decode_int(value), do: Primitives.decode_int(value)
  def encode_bin(value), do: Primitives.encode_bin(value)
  def decode_bin(value), do: Primitives.decode_bin(value)

  def flow_record_value_refs(record), do: Support.flow_record_value_refs(record)
end
