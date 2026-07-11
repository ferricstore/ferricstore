defmodule Ferricstore.Flow.Codec.Support do
  @moduledoc false

  import Bitwise

  alias Ferricstore.Flow.Codec.Primitives

  @value_bin_magic "FSV2"
  @record_value_refs_key "__value_refs__"
  @record_attributes_key "__attributes__"
  @record_indexed_attributes_key "__indexed_attributes__"
  @record_state_meta_key "__state_meta__"
  @record_indexed_state_meta_key "__indexed_state_meta__"
  @record_state_enter_seq_key "__state_enter_seq__"
  @record_governance_limit_key "__governance_limit__"
  @history_value_refs_key "value_refs"
  @history_attributes_key "attributes"
  @history_state_meta_key "state_meta"
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
  @history_flag_meta 1 <<< 12

  def encode_record_flags(record, sidecar) do
    0
    |> record_flag_int(record, :attempts, @record_flag_attempts, 0)
    |> record_flag_int(record, :fencing_token, @record_flag_fencing_token, 0)
    |> record_flag_int(record, :next_run_at_ms, @record_flag_next_run_at_ms, nil)
    |> record_flag_int(record, :priority, @record_flag_priority, 0)
    |> record_flag_int(record, :ttl_ms, @record_flag_ttl_ms, nil)
    |> record_flag_int(record, :history_hot_max_events, @record_flag_history_hot_max_events, nil)
    |> record_flag_int(record, :history_max_events, @record_flag_history_max_events, nil)
    |> record_flag_int(record, :retention_ttl_ms, @record_flag_retention_ttl_ms, nil)
    |> record_flag_int(
      record,
      :terminal_retention_until_ms,
      @record_flag_terminal_retention_until_ms,
      nil
    )
    |> record_flag_int(record, :max_active_ms, @record_flag_max_active_ms, nil)
    |> record_flag_bin(record, :partition_key, @record_flag_partition_key)
    |> record_flag_bin(record, :payload_ref, @record_flag_payload_ref)
    |> record_flag_bin(record, :parent_flow_id, @record_flag_parent_flow_id)
    |> record_flag_bin(record, :parent_partition_key, @record_flag_parent_partition_key)
    |> record_flag_root(record)
    |> record_flag_bin(record, :correlation_id, @record_flag_correlation_id)
    |> record_flag_bin(record, :result_ref, @record_flag_result_ref)
    |> record_flag_bin(record, :error_ref, @record_flag_error_ref)
    |> record_flag_bin(record, :lease_owner, @record_flag_lease_owner)
    |> record_flag_bin(record, :lease_token, @record_flag_lease_token)
    |> record_flag_int(record, :lease_deadline_ms, @record_flag_lease_deadline_ms, 0)
    |> record_flag_bin(record, :run_state, @record_flag_run_state)
    |> record_flag_bin(record, :rewound_to_event_id, @record_flag_rewound_to_event_id)
    |> maybe_put_flag(@record_flag_sidecar, not record_empty_sidecar?(sidecar))
  end

  def record_flag_int(flags, record, key, flag, omitted_default) do
    value = Map.get(record, key)
    maybe_put_flag(flags, flag, not is_nil(value) and value != omitted_default)
  end

  def record_flag_bin(flags, record, key, flag) do
    maybe_put_flag(flags, flag, is_binary(Map.get(record, key)))
  end

  def record_flag_root(flags, record) do
    root_flow_id = Map.get(record, :root_flow_id)
    id = Map.get(record, :id)

    # Most root flows point to themselves. Store that common case as a flag
    # instead of repeating the id bytes in every state record.
    cond do
      is_binary(root_flow_id) and root_flow_id == id ->
        flags ||| @record_flag_root_flow_id_self

      is_binary(root_flow_id) ->
        flags ||| @record_flag_root_flow_id

      true ->
        flags
    end
  end

  def maybe_put_flag(flags, flag, true), do: flags ||| flag
  def maybe_put_flag(flags, _flag, _false), do: flags

  def nonempty_binary?(value), do: is_binary(value) and value != ""

  def encode_flagged_int(flags, flag, value) do
    if (flags &&& flag) != 0, do: Primitives.encode_int(value), else: []
  end

  def encode_flagged_bin(flags, flag, value) do
    if (flags &&& flag) != 0, do: Primitives.encode_bin(value), else: []
  end

  @doc false
  def encode_value(@value_bin_magic <> _rest = value), do: @value_bin_magic <> <<1>> <> value

  def encode_value(value) when is_binary(value), do: value
  def encode_value(value), do: @value_bin_magic <> <<2>> <> :erlang.term_to_binary(value)

  @doc false
  def decode_value(@value_bin_magic <> <<1, encoded::binary>>), do: encoded

  def decode_value(@value_bin_magic <> <<2, encoded::binary>>) do
    :erlang.binary_to_term(encoded, [:safe])
  rescue
    _ -> encoded
  end

  def decode_value(value), do: value

  def decode_value_with_user_size(@value_bin_magic <> <<1, encoded::binary>>) do
    {encoded, byte_size(encoded)}
  end

  def decode_value_with_user_size(@value_bin_magic <> <<2, encoded::binary>>) do
    {:erlang.binary_to_term(encoded, [:safe]), byte_size(encoded)}
  rescue
    _ -> {encoded, byte_size(encoded)}
  end

  def decode_value_with_user_size(value) when is_binary(value), do: {value, byte_size(value)}
  def decode_value_with_user_size(value), do: {value, 0}

  def decode_history_meta_for_flags(flags, rest) do
    if (flags &&& @history_flag_meta) != 0 do
      decode_history_meta(rest)
    else
      {:ok, [], rest}
    end
  end

  def normalize_history_meta(meta) when is_map(meta) do
    meta
    |> Enum.flat_map(fn
      {key, value} when is_atom(key) -> history_meta_pair(Atom.to_string(key), value)
      {key, value} when is_binary(key) -> history_meta_pair(key, value)
      _other -> []
    end)
  end

  def normalize_history_meta(_meta), do: []

  def record_history_meta(record, meta) when is_map(record) and is_map(meta) do
    refs = flow_record_value_refs(record)
    attributes = Ferricstore.Flow.Attributes.record(record)
    state_meta = Ferricstore.Flow.StateMeta.record(record)

    meta
    |> maybe_put_history_value_refs(refs)
    |> maybe_put_history_attributes(attributes)
    |> maybe_put_history_state_meta(state_meta)
  end

  def record_history_meta(_record, meta), do: meta

  defp maybe_put_history_value_refs(meta, refs) when is_map(refs) and map_size(refs) > 0,
    do: Map.put_new(meta, @history_value_refs_key, Jason.encode!(encode_value_refs(refs)))

  defp maybe_put_history_value_refs(meta, _refs), do: meta

  defp maybe_put_history_attributes(meta, attrs) when is_map(attrs) and map_size(attrs) > 0,
    do: Map.put_new(meta, @history_attributes_key, Jason.encode!(attrs))

  defp maybe_put_history_attributes(meta, _attrs), do: meta

  defp maybe_put_history_state_meta(meta, state_meta)
       when is_map(state_meta) and map_size(state_meta) > 0,
       do: Map.put_new(meta, @history_state_meta_key, Jason.encode!(state_meta))

  defp maybe_put_history_state_meta(meta, _state_meta), do: meta

  def history_meta_pair(key, nil), do: [{key, ""}]
  def history_meta_pair(key, value) when is_binary(value), do: [{key, value}]
  def history_meta_pair(key, value) when is_atom(value), do: [{key, Atom.to_string(value)}]
  def history_meta_pair(key, value) when is_integer(value), do: [{key, Integer.to_string(value)}]
  def history_meta_pair(key, value), do: [{key, to_string(value)}]

  def encode_history_meta([]), do: <<1>>

  def encode_history_meta(fields) do
    [Primitives.encode_int(length(fields)), Enum.map(fields, &encode_history_meta_pair/1)]
  end

  def encode_history_meta_pair({key, value}),
    do: [Primitives.encode_bin(key), Primitives.encode_bin(value)]

  def decode_history_meta(rest) do
    with {:ok, count, rest} <- Primitives.decode_int(rest) do
      decode_history_meta_pairs(count, rest, [])
    end
  end

  def decode_history_meta_pairs(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  def decode_history_meta_pairs(count, rest, acc) when is_integer(count) and count > 0 do
    with {:ok, key, rest} <- Primitives.decode_bin(rest),
         {:ok, value, rest} <- Primitives.decode_bin(rest) do
      decode_history_meta_pairs(count - 1, rest, [{key, value} | acc])
    end
  end

  def decode_history_meta_pairs(_count, _rest, _acc), do: :error

  def normalize_history_decoded_meta(fields) when is_list(fields) do
    Enum.flat_map(fields, fn
      {key, value} when is_binary(key) and is_binary(value) -> [key, value]
      _other -> []
    end)
  end

  def normalize_history_decoded_meta(_fields), do: []

  def encode_child_groups(groups) when is_map(groups) and map_size(groups) == 0,
    do: Primitives.encode_bin("J{}")

  def encode_child_groups(groups) when is_map(groups) do
    ["J", Jason.encode!(groups)]
    |> IO.iodata_to_binary()
    |> Primitives.encode_bin()
  end

  def encode_child_groups(_groups), do: Primitives.encode_bin("J{}")

  def encode_record_sidecar(record) when is_map(record) do
    child_groups =
      record
      |> Map.get(:child_groups, %{})
      |> normalize_child_groups()

    refs = flow_record_value_refs(record)
    attributes = Ferricstore.Flow.Attributes.record(record)
    indexed_attributes = Ferricstore.Flow.Attributes.indexed_names(record)
    state_meta = Ferricstore.Flow.StateMeta.record(record)
    indexed_state_meta = Ferricstore.Flow.StateMeta.indexed_key(record)
    state_enter_seq = Map.get(record, :state_enter_seq)
    governance_limit = encode_governance_limit(Map.get(record, :governance_limit))

    if map_size(refs) == 0 and map_size(attributes) == 0 and indexed_attributes == [] and
         map_size(state_meta) == 0 and is_nil(indexed_state_meta) and
         not is_integer(state_enter_seq) and is_nil(governance_limit) do
      encode_child_groups(child_groups)
    else
      child_groups
      |> maybe_put_record_refs(refs)
      |> maybe_put_record_attributes(attributes)
      |> maybe_put_record_indexed_attributes(indexed_attributes)
      |> maybe_put_record_state_meta(state_meta)
      |> maybe_put_record_indexed_state_meta(indexed_state_meta)
      |> maybe_put_record_state_enter_seq(state_enter_seq)
      |> maybe_put_record_governance_limit(governance_limit)
      |> encode_child_groups()
    end
  end

  def record_empty_sidecar do
    %{}
    |> encode_child_groups()
    |> IO.iodata_to_binary()
  end

  def record_empty_sidecar?(sidecar), do: sidecar == record_empty_sidecar()

  def split_record_sidecar(groups) when is_map(groups) do
    {encoded_refs, child_groups} = Map.pop(groups, @record_value_refs_key, %{})
    {encoded_attributes, child_groups} = Map.pop(child_groups, @record_attributes_key, %{})
    {indexed_attributes, child_groups} = Map.pop(child_groups, @record_indexed_attributes_key, [])
    {encoded_state_meta, child_groups} = Map.pop(child_groups, @record_state_meta_key, %{})

    {indexed_state_meta, child_groups} =
      Map.pop(child_groups, @record_indexed_state_meta_key, nil)

    {state_enter_seq, child_groups} =
      Map.pop(child_groups, @record_state_enter_seq_key, nil)

    {governance_limit, child_groups} =
      Map.pop(child_groups, @record_governance_limit_key, nil)

    {child_groups, decode_value_refs(encoded_refs),
     Ferricstore.Flow.Attributes.decode_sidecar(encoded_attributes),
     decode_record_indexed_attributes(indexed_attributes),
     Ferricstore.Flow.StateMeta.decode_sidecar(encoded_state_meta),
     decode_record_indexed_state_meta(indexed_state_meta),
     decode_record_state_enter_seq(state_enter_seq), decode_governance_limit(governance_limit)}
  end

  def split_record_sidecar(_groups), do: {%{}, %{}, %{}, [], %{}, nil, nil, nil}

  def maybe_put_record_refs(groups, refs) when is_map(refs) and map_size(refs) > 0,
    do: Map.put(groups, @record_value_refs_key, encode_value_refs(refs))

  def maybe_put_record_refs(groups, _refs), do: groups

  def maybe_put_record_attributes(groups, attrs) when is_map(attrs) and map_size(attrs) > 0,
    do: Map.put(groups, @record_attributes_key, Ferricstore.Flow.Attributes.encode_sidecar(attrs))

  def maybe_put_record_attributes(groups, _attrs), do: groups

  def maybe_put_record_indexed_attributes(groups, names) when is_list(names) and names != [],
    do: Map.put(groups, @record_indexed_attributes_key, names)

  def maybe_put_record_indexed_attributes(groups, _names), do: groups

  def maybe_put_record_state_meta(groups, state_meta)
      when is_map(state_meta) and map_size(state_meta) > 0,
      do:
        Map.put(
          groups,
          @record_state_meta_key,
          Ferricstore.Flow.StateMeta.encode_sidecar(state_meta)
        )

  def maybe_put_record_state_meta(groups, _state_meta), do: groups

  def maybe_put_record_indexed_state_meta(groups, key) when is_binary(key) and key != "",
    do: Map.put(groups, @record_indexed_state_meta_key, key)

  def maybe_put_record_indexed_state_meta(groups, _key), do: groups

  def maybe_put_record_state_enter_seq(groups, seq) when is_integer(seq) and seq >= 0,
    do: Map.put(groups, @record_state_enter_seq_key, seq)

  def maybe_put_record_state_enter_seq(groups, _seq), do: groups

  def maybe_put_record_governance_limit(groups, limit) when is_map(limit),
    do: Map.put(groups, @record_governance_limit_key, limit)

  def maybe_put_record_governance_limit(groups, _limit), do: groups

  def encode_governance_limit(nil), do: nil

  def encode_governance_limit(%{
        scope: scope,
        shard_id: shard_id,
        enforcement: enforcement,
        reservation_id: reservation_id
      })
      when is_binary(scope) and scope != "" and is_integer(shard_id) and shard_id >= 0 and
             enforcement in [:strict_global, :approximate_global] and
             is_binary(reservation_id) and reservation_id != "" do
    %{
      "scope" => scope,
      "shard_id" => shard_id,
      "enforcement" => Atom.to_string(enforcement),
      "reservation_id" => reservation_id
    }
  end

  def encode_governance_limit(_limit),
    do: raise(ArgumentError, "flow record requires the current governance limit shape")

  def decode_governance_limit(nil), do: nil

  def decode_governance_limit(%{
        "scope" => scope,
        "shard_id" => shard_id,
        "enforcement" => enforcement,
        "reservation_id" => reservation_id
      })
      when is_binary(scope) and scope != "" and is_integer(shard_id) and shard_id >= 0 and
             enforcement in ["strict_global", "approximate_global"] and
             is_binary(reservation_id) and reservation_id != "" do
    %{
      scope: scope,
      shard_id: shard_id,
      enforcement: String.to_existing_atom(enforcement),
      reservation_id: reservation_id
    }
  end

  def decode_governance_limit(_limit),
    do: raise(ArgumentError, "flow record requires the current governance limit shape")

  def decode_record_indexed_attributes(names) do
    case Ferricstore.Flow.Attributes.normalize_indexed_names(names) do
      {:ok, names} -> names
      {:error, _reason} -> []
    end
  end

  def decode_record_indexed_state_meta(key) do
    case Ferricstore.Flow.StateMeta.normalize_indexed_key(key) do
      {:ok, key} -> key
      {:error, _reason} -> nil
    end
  end

  def decode_record_state_enter_seq(seq) when is_integer(seq) and seq >= 0, do: seq
  def decode_record_state_enter_seq(_seq), do: nil

  def flow_record_value_refs(record) when is_map(record) do
    record
    |> Map.get(:value_refs, %{})
    |> decode_value_refs()
  end

  def flow_record_value_refs(_record), do: %{}

  def encode_value_refs(refs) when is_map(refs) do
    refs
    |> decode_value_refs()
    |> Map.new(fn {name, entry} ->
      {name,
       %{
         "ref" => Map.get(entry, :ref),
         "version" => Map.get(entry, :version),
         "digest" => Map.get(entry, :digest)
       }}
    end)
  end

  def encode_value_refs(_refs), do: %{}

  def decode_value_refs(refs) when is_map(refs) do
    refs
    |> Enum.reduce(%{}, fn
      {name, %{} = entry}, acc when is_binary(name) and name != "" ->
        ref = Map.get(entry, :ref) || Map.get(entry, "ref")

        if is_binary(ref) and ref != "" do
          Map.put(acc, name, %{
            ref: ref,
            version: value_ref_integer(Map.get(entry, :version) || Map.get(entry, "version")),
            digest: value_ref_binary(Map.get(entry, :digest) || Map.get(entry, "digest"))
          })
        else
          acc
        end

      {name, ref}, acc when is_binary(name) and name != "" and is_binary(ref) and ref != "" ->
        Map.put(acc, name, %{ref: ref, version: nil, digest: nil})

      _entry, acc ->
        acc
    end)
  end

  def decode_value_refs(value) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} -> decode_value_refs(decoded)
      _ -> %{}
    end
  end

  def decode_value_refs(_refs), do: %{}

  def value_ref_integer(value) when is_integer(value), do: value

  def value_ref_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _ -> nil
    end
  end

  def value_ref_integer(_value), do: nil

  def value_ref_binary(value) when is_binary(value) and value != "", do: value
  def value_ref_binary(_value), do: nil

  def decode_child_groups(binary) do
    with {:ok, encoded, rest} <- Primitives.decode_bin(binary),
         {:ok, decoded} <- decode_child_groups_payload(encoded) do
      {:ok, decoded, rest}
    end
  end

  def decode_child_groups_payload("J{}"), do: {:ok, %{}}
  def decode_child_groups_payload("J" <> json), do: Jason.decode(json)

  def decode_child_groups_payload(encoded) do
    {:ok, :erlang.binary_to_term(encoded, [:safe])}
  rescue
    _ -> :error
  end

  def normalize_child_groups(groups) when is_map(groups), do: groups
  def normalize_child_groups(_groups), do: %{}

  def history_integer(value) when is_integer(value), do: Integer.to_string(value)
  def history_integer(_value), do: "0"

  def history_optional_integer(value) when is_integer(value), do: Integer.to_string(value)
  def history_optional_integer(_value), do: ""

  def history_string(value) when is_binary(value), do: value
  def history_string(_value), do: ""

  def history_context_string(context, key, fallback) when is_map(context) do
    case Map.get(context, key) || Map.get(context, Atom.to_string(key)) do
      value when is_binary(value) -> value
      _ -> fallback
    end
  end

  def history_context_string(_context, _key, fallback), do: fallback
end
