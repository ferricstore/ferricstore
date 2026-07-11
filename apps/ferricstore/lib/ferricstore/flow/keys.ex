defmodule Ferricstore.Flow.Keys do
  @moduledoc false

  @global_tag "{f}"
  @partition_tag_prefix "{f:"
  @auto_partition_prefix "__flow_auto__:"
  @auto_partition_buckets 256
  @auto_partition_tags 0..(@auto_partition_buckets - 1)
                       |> Enum.map(&("{fa:" <> Integer.to_string(&1) <> "}"))
                       |> List.to_tuple()
  @auto_partition_keys 0..(@auto_partition_buckets - 1)
                       |> Enum.map(&(@auto_partition_prefix <> Integer.to_string(&1)))

  def state_key(id, partition_key \\ nil)

  def state_key(id, nil) when is_binary(id) do
    "f:" <> tag(auto_partition_key(id)) <> ":s:" <> id
  end

  def state_key(id, partition_key) do
    "f:" <> tag(partition_key) <> ":s:" <> id
  end

  def registry_key(id, partition_key \\ nil)

  def registry_key(id, nil) when is_binary(id) do
    "f:" <> tag(auto_partition_key(id)) <> ":r:" <> id
  end

  def registry_key(id, partition_key) do
    "f:" <> tag(partition_key) <> ":r:" <> id
  end

  def state_key_from_due_key(due_key, id) when is_binary(due_key) and is_binary(id) do
    case :binary.match(due_key, "}:d:") do
      {pos, _len} when pos >= 2 ->
        tag = binary_part(due_key, 2, pos + 1 - 2)
        {:ok, "f:" <> tag <> ":s:" <> id}

      :nomatch ->
        :error
    end
  end

  def state_key_from_registry_key(registry_key) when is_binary(registry_key) do
    case :binary.match(registry_key, "}:r:") do
      {pos, marker_size} when pos >= 2 ->
        id_offset = pos + marker_size
        id_size = byte_size(registry_key) - id_offset

        if id_size > 0 do
          tag = binary_part(registry_key, 2, pos + 1 - 2)
          id = binary_part(registry_key, id_offset, id_size)
          {:ok, "f:" <> tag <> ":s:" <> id}
        else
          :error
        end

      :nomatch ->
        :error
    end
  end

  def state_key_from_registry_key(_registry_key), do: :error

  def registry_key_from_state_key(state_key) when is_binary(state_key) do
    case :binary.match(state_key, "}:s:") do
      {pos, marker_size} when pos >= 2 ->
        id_offset = pos + marker_size
        id_size = byte_size(state_key) - id_offset

        if id_size > 0 do
          tag = binary_part(state_key, 2, pos + 1 - 2)
          id = binary_part(state_key, id_offset, id_size)
          {:ok, "f:" <> tag <> ":r:" <> id}
        else
          :error
        end

      :nomatch ->
        :error
    end
  end

  def registry_key_from_state_key(_state_key), do: :error

  def history_key(id, partition_key \\ nil)

  def history_key(id, nil) when is_binary(id) do
    "f:" <> tag(auto_partition_key(id)) <> ":h:" <> id
  end

  def history_key(id, partition_key) do
    "f:" <> tag(partition_key) <> ":h:" <> id
  end

  def value_key(id, kind, version, partition_key \\ nil)

  def value_key(id, kind, version, nil)
      when kind in [:payload, :result, :error, :shared] and is_integer(version) and
             is_binary(id) do
    value_key(id, kind, version, auto_partition_key(id))
  end

  def value_key(id, kind, version, partition_key)
      when kind in [:payload, :result, :error, :shared] and is_integer(version) do
    "f:" <>
      tag(partition_key) <>
      ":v:" <> flow_value_kind(kind) <> ":" <> id <> ":" <> Integer.to_string(version)
  end

  def shared_value_link_prefix(owner_flow_id, partition_key \\ nil)
      when is_binary(owner_flow_id) do
    "f:" <> tag(partition_key) <> ":svl:" <> owner_flow_id <> ":"
  end

  def shared_value_ref_registry_key(flow_id, partition_key \\ nil) when is_binary(flow_id) do
    "f:" <> tag(partition_key) <> ":svr:" <> flow_id
  end

  def shared_value_ref_count_key(ref, shard_index)
      when is_binary(ref) and is_integer(shard_index) and shard_index >= 0 do
    digest = :crypto.hash(:sha256, ref) |> Base.url_encode64(padding: false)
    "f:" <> @global_tag <> ":svc:" <> Integer.to_string(shard_index) <> ":" <> digest
  end

  def shared_value_orphan_key(ref) when is_binary(ref) do
    digest = :crypto.hash(:sha256, ref) |> Base.url_encode64(padding: false)
    "f:" <> @global_tag <> ":svo:" <> digest
  end

  def retention_guard_key(id, partition_key \\ nil) when is_binary(id) do
    "f:" <> tag(partition_key) <> ":rtg:" <> id
  end

  def retention_cleanup_index_key(id, partition_key \\ nil) when is_binary(id) do
    "f:" <> tag(partition_key) <> ":i:rtc:" <> id
  end

  def retention_cleanup_member_prefix(id, partition_key \\ nil) when is_binary(id) do
    "f:" <> tag(partition_key) <> ":rtm:" <> id <> ":"
  end

  def retention_cleanup_member_key(id, owned_key, partition_key \\ nil)
      when is_binary(id) and is_binary(owned_key) do
    digest = :crypto.hash(:sha256, owned_key) |> Base.url_encode64(padding: false)
    retention_cleanup_member_prefix(id, partition_key) <> digest
  end

  def shared_value_ref_backfill_key(shard_index)
      when is_integer(shard_index) and shard_index >= 0 do
    "f:" <> @global_tag <> ":svb:1:" <> Integer.to_string(shard_index)
  end

  def shared_value_ref_backfill_key?("f:" <> @global_tag <> ":svb:1:" <> shard_index),
    do: shard_index != ""

  def shared_value_ref_backfill_key?(_key), do: false

  def retention_cleanup_member?(<<"f:{", rest::binary>>),
    do: :binary.match(rest, "}:rtm:") != :nomatch

  def retention_cleanup_member?(_key), do: false

  def shared_value_ref?(<<"f:{", rest::binary>>),
    do: :binary.match(rest, "}:v:s:") != :nomatch

  def shared_value_ref?(_ref), do: false

  def signal_idempotency_key(id, idempotency_key, partition_key \\ nil)
      when is_binary(id) and is_binary(idempotency_key) do
    "f:" <> tag(partition_key) <> ":sig:" <> id <> ":" <> idempotency_key
  end

  def policy_key(type) do
    "f:" <> @global_tag <> ":policy:" <> type
  end

  def type_catalog_member_key(type, state_key)
      when is_binary(type) and type != "" and is_binary(state_key) do
    case flow_key_tag_prefix(state_key, "}:s:") do
      {:ok, tag_prefix} ->
        tag_prefix <>
          ":tc:1:" <> digest(type) <> ":" <> digest(state_key)

      :error ->
        raise ArgumentError, "invalid Flow state key"
    end
  end

  def type_catalog_descriptor_key(type) when is_binary(type) and type != "" do
    type_catalog_descriptor_key_from_digest(digest(type))
  end

  def type_catalog_descriptor_key_from_member(key) when is_binary(key) do
    with {:ok, type_digest} <- type_catalog_digest_from_member(key) do
      {:ok, type_catalog_descriptor_key_from_digest(type_digest)}
    end
  end

  def type_catalog_descriptor_key_from_member(_key), do: :error

  def type_catalog_member_key?(key),
    do: match?({:ok, _type_digest}, type_catalog_digest_from_member(key))

  def type_catalog_member_owns_state_key?(key, state_key)
      when is_binary(key) and is_binary(state_key) do
    with {:ok, _tag_prefix, remainder} <- split_internal_flow_key(key, "}:tc:1:"),
         <<_type_digest::binary-size(43), ?:, state_digest::binary-size(43)>> <- remainder,
         true <- valid_digest?(state_digest) do
      state_digest == digest(state_key)
    else
      _invalid -> false
    end
  end

  def type_catalog_member_owns_state_key?(_key, _state_key), do: false

  def policy_migration_job_key(type) when is_binary(type) and type != "" do
    "f:" <> @global_tag <> ":pm:1:" <> digest(type)
  end

  def policy_migration_job_prefix, do: "f:" <> @global_tag <> ":pm:1:"

  def policy_migration_marker_key(type) when is_binary(type) and type != "" do
    "f:" <> @global_tag <> ":pmg:1:" <> digest(type)
  end

  def policy_catalog_backfill_key(shard_index)
      when is_integer(shard_index) and shard_index >= 0 do
    "f:" <> @global_tag <> ":pcb:1:" <> Integer.to_string(shard_index)
  end

  def policy_migration_job_key?("f:" <> @global_tag <> ":pm:1:" <> type_digest),
    do: valid_digest?(type_digest)

  def policy_migration_job_key?(_key), do: false

  def policy_catalog_projection_prefix(type) when is_binary(type) and type != "" do
    <<0, "fpc:1:", digest(type)::binary, ?:>>
  end

  def policy_catalog_projection_key(type, catalog_key, generation)
      when is_binary(type) and type != "" and is_binary(catalog_key) and
             is_integer(generation) and generation >= 0 and generation <= 0xFFFFFFFFFFFFFFFF do
    policy_catalog_projection_prefix(type) <>
      <<generation::unsigned-big-64, catalog_key::binary>>
  end

  def decode_policy_catalog_projection_key(type, key)
      when is_binary(type) and type != "" and is_binary(key) do
    prefix = policy_catalog_projection_prefix(type)

    case key do
      <<^prefix::binary, generation::unsigned-big-64, catalog_key::binary>>
      when catalog_key != "" ->
        case type_catalog_descriptor_key_from_member(catalog_key) do
          {:ok, descriptor_key} ->
            if descriptor_key == type_catalog_descriptor_key(type) do
              {:ok, %{catalog_key: catalog_key, migration_generation: generation}}
            else
              :error
            end

          :error ->
            :error
        end

      _invalid ->
        :error
    end
  end

  def decode_policy_catalog_projection_key(_type, _key), do: :error

  def policy_type("f:" <> @global_tag <> ":policy:" <> type) when type != "", do: {:ok, type}
  def policy_type(_key), do: :error

  def governance_effect_key(id, effect_key, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:e:" <> id <> ":" <> effect_key
  end

  def governance_effect_key_prefix(id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:e:" <> id <> ":"
  end

  def governance_ledger_key(id, event_id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:l:" <> id <> ":" <> event_id
  end

  def governance_ledger_key_prefix(id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:l:" <> id <> ":"
  end

  def governance_ledger_index_key(id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:li:" <> id
  end

  def governance_scope_key(scope) when is_binary(scope) do
    "f:" <> tag(scope) <> ":gov:scope:" <> scope
  end

  def governance_approval_key(id) when is_binary(id) do
    "f:" <> tag(id) <> ":gov:a:" <> id
  end

  def governance_approval_key?(<<"f:{", rest::binary>>),
    do: :binary.match(rest, "}:gov:a:") != :nomatch

  def governance_approval_key?(_key), do: false

  def governance_circuit_key(scope) when is_binary(scope) do
    "f:" <> tag(scope) <> ":gov:c:" <> scope
  end

  def governance_circuit_key?(<<"f:{", rest::binary>>),
    do: :binary.match(rest, "}:gov:c:") != :nomatch

  def governance_circuit_key?(_key), do: false

  def governance_budget_key(scope) when is_binary(scope) do
    "f:" <> tag(scope) <> ":gov:b:" <> scope
  end

  def governance_budget_key?(<<"f:{", rest::binary>>),
    do: :binary.match(rest, "}:gov:b:") != :nomatch

  def governance_budget_key?(_key), do: false

  def governance_limit_key(scope) when is_binary(scope) do
    "f:" <> tag(scope) <> ":gov:limit:" <> scope
  end

  def governance_limit_key?(<<"f:{", rest::binary>>),
    do: :binary.match(rest, "}:gov:limit:") != :nomatch

  def governance_limit_key?(_key), do: false

  def governance_limit_reservation_prefix(scope, shard_id, epoch)
      when is_binary(scope) and is_integer(shard_id) and shard_id >= 0 and
             is_integer(epoch) and epoch > 0 do
    governance_limit_storage_prefix(scope) <>
      ":reservation:" <> Integer.to_string(shard_id) <> ":" <> Integer.to_string(epoch) <> ":"
  end

  def governance_limit_reservation_key(scope, shard_id, epoch, reservation_id)
      when is_binary(reservation_id) and reservation_id != "" do
    governance_limit_reservation_prefix(scope, shard_id, epoch) <> digest(reservation_id)
  end

  def governance_limit_reservation_page_key(scope, shard_id, epoch, page)
      when is_binary(scope) and is_integer(shard_id) and shard_id >= 0 and
             is_integer(epoch) and epoch > 0 and is_integer(page) and page > 0 do
    governance_limit_storage_prefix(scope) <>
      ":page:" <>
      Integer.to_string(shard_id) <>
      ":" <> Integer.to_string(epoch) <> ":" <> Integer.to_string(page)
  end

  def governance_limit_cleanup_key(scope, sequence)
      when is_binary(scope) and is_integer(sequence) and sequence > 0 do
    governance_limit_storage_prefix(scope) <> ":cleanup:" <> Integer.to_string(sequence)
  end

  def governance_limit_cleanup_progress_key do
    "f:{flow-governance}:gov:limit-storage-cleanup:progress"
  end

  def governance_limit_catalog_outbox_meta_key(shard_index)
      when is_integer(shard_index) and shard_index >= 0 do
    "f:{flow-governance}:gov:limit-catalog-outbox:" <> Integer.to_string(shard_index) <> ":meta"
  end

  def governance_limit_catalog_outbox_intent_key(shard_index, sequence)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(sequence) and sequence > 0 do
    "f:{flow-governance}:gov:limit-catalog-outbox:" <>
      Integer.to_string(shard_index) <> ":intent:" <> Integer.to_string(sequence)
  end

  def governance_limit_cache_session_head_key(node_id, instance_name)
      when is_binary(node_id) and node_id != "" and is_binary(instance_name) and
             instance_name != "" do
    governance_limit_cache_session_prefix(node_id, instance_name) <> ":head"
  end

  def governance_limit_cache_session_meta_key(node_id, instance_name, session_id)
      when is_binary(node_id) and node_id != "" and is_binary(instance_name) and
             instance_name != "" and is_binary(session_id) and session_id != "" do
    governance_limit_cache_session_prefix(node_id, instance_name) <>
      ":session:" <> governance_catalog_digest(session_id) <> ":meta"
  end

  def governance_limit_cache_session_page_key(node_id, instance_name, session_id, sequence)
      when is_binary(node_id) and node_id != "" and is_binary(instance_name) and
             instance_name != "" and is_binary(session_id) and session_id != "" and
             is_integer(sequence) and sequence > 0 do
    governance_limit_cache_session_prefix(node_id, instance_name) <>
      ":session:" <>
      governance_catalog_digest(session_id) <> ":page:" <> Integer.to_string(sequence)
  end

  def governance_release_outbox_meta_key(shard_index)
      when is_integer(shard_index) and shard_index >= 0 do
    "f:{flow-governance}:gov:release-outbox:" <> Integer.to_string(shard_index) <> ":meta"
  end

  def governance_release_outbox_intent_key(shard_index, sequence)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(sequence) and sequence > 0 do
    "f:{flow-governance}:gov:release-outbox:" <>
      Integer.to_string(shard_index) <> ":intent:" <> Integer.to_string(sequence)
  end

  def governance_release_outbox_completed_key(shard_index, sequence)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(sequence) and sequence > 0 do
    "f:{flow-governance}:gov:release-outbox:" <>
      Integer.to_string(shard_index) <> ":completed:" <> Integer.to_string(sequence)
  end

  def governance_catalog_key(kind)
      when kind in [:approval, :budget, :circuit, :limit] do
    "f:{flow-governance}:gov:catalog:" <> Atom.to_string(kind)
  end

  def governance_approval_scope_catalog_key(scope) when is_binary(scope) do
    "f:{flow-governance}:gov:catalog:approval:scope:" <> governance_catalog_digest(scope)
  end

  def governance_approval_flow_catalog_key(flow_id) when is_binary(flow_id) do
    "f:{flow-governance}:gov:catalog:approval:flow:" <> governance_catalog_digest(flow_id)
  end

  def policy_key?(key) when is_binary(key),
    do: String.starts_with?(key, "f:" <> @global_tag <> ":policy:")

  def policy_key?(_key), do: false

  defp governance_catalog_digest(value) do
    :sha256
    |> :crypto.hash(value)
    |> Base.url_encode64(padding: false)
  end

  defp governance_limit_cache_session_prefix(node_id, instance_name) do
    family = governance_catalog_digest(node_id <> <<0>> <> instance_name)
    "f:{fgc:" <> family <> "}:gov:limit-cache-session"
  end

  defp governance_limit_storage_prefix(scope) do
    "f:" <> tag(scope) <> ":gov:limit-storage:" <> digest(scope)
  end

  def value_key?(<<"f:{", rest::binary>>), do: :binary.match(rest, "}:v:") != :nomatch
  def value_key?(_key), do: false

  def history_key?(<<"f:{", rest::binary>>), do: :binary.match(rest, "}:h:") != :nomatch
  def history_key?(_key), do: false

  def due_key(type, state, priority, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":d:" <> type <> ":" <> state <> ":p" <> Integer.to_string(priority)
  end

  def due_any_key(type, priority, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":da:" <> type <> ":p" <> Integer.to_string(priority)
  end

  def state_index_key(type, state, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:s:" <> type <> ":" <> state
  end

  def inflight_index_key(type, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:r:" <> type
  end

  def worker_index_key(worker, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:w:" <> worker
  end

  def active_timeout_index_key, do: "f:" <> @global_tag <> ":i:active-timeout"

  def terminal_retention_index_key, do: "f:" <> @global_tag <> ":i:terminal-retention"

  def parent_index_key(parent_flow_id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:p:" <> parent_flow_id
  end

  def root_index_key(root_flow_id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:o:" <> root_flow_id
  end

  def correlation_index_key(correlation_id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:c:" <> correlation_id
  end

  def attribute_index_key(type, state, name, value, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:a:" <> type <> ":" <> state <> ":" <> name <> "=" <> value
  end

  def attribute_index_prefix(type, state, name, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:a:" <> type <> ":" <> state <> ":" <> name <> "="
  end

  def attribute_type_index_key(type, name, value, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:at:" <> type <> ":" <> name <> "=" <> value
  end

  def attribute_type_index_prefix(type, name, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:at:" <> type <> ":" <> name <> "="
  end

  def attribute_state_index_key(state, name, value, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:as:" <> state <> ":" <> name <> "=" <> value
  end

  def attribute_state_index_prefix(state, name, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:as:" <> state <> ":" <> name <> "="
  end

  def attribute_partition_index_key(name, value, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:ap:" <> name <> "=" <> value
  end

  def attribute_partition_index_prefix(name, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:ap:" <> name <> "="
  end

  def state_meta_index_key(type, state, name, value, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:sm:" <> type <> ":" <> state <> ":" <> name <> "=" <> value
  end

  def stream_entry_key(id, event_id, partition_key \\ nil) do
    stream_entry_key_from_history_key(history_key(id, partition_key), event_id)
  end

  def stream_entry_key_from_history_key(history_key, event_id)
      when is_binary(history_key) and is_binary(event_id) do
    "X:" <> history_key <> <<0>> <> event_id
  end

  def state_key?(<<"f:{", rest::binary>>), do: :binary.match(rest, "}:s:") != :nomatch

  def state_key?(_key), do: false

  def retention_guard_key_from_state_key(state_key) when is_binary(state_key) do
    with {:ok, tag_prefix, id} <- split_internal_flow_key(state_key, "}:s:"),
         true <- id != "" do
      {:ok, tag_prefix <> ":rtg:" <> id}
    else
      _invalid -> :error
    end
  end

  def retention_guard_key_from_state_key(_state_key), do: :error

  def registry_key?(<<"f:{", rest::binary>>), do: :binary.match(rest, "}:r:") != :nomatch

  def registry_key?(_key), do: false

  def tag(nil), do: @global_tag
  def tag(:global), do: @global_tag

  def tag(<<@auto_partition_prefix, bucket::binary>> = partition_key) do
    case auto_partition_bucket(bucket) do
      {:ok, bucket_index} ->
        elem(@auto_partition_tags, bucket_index)

      :error ->
        hashed_partition_tag(partition_key)
    end
  end

  def tag(partition_key) when is_binary(partition_key), do: hashed_partition_tag(partition_key)

  defp hashed_partition_tag(partition_key) do
    @partition_tag_prefix <>
      Base.url_encode64(:crypto.hash(:sha256, partition_key), padding: false) <> "}"
  end

  defp type_catalog_descriptor_key_from_digest(type_digest) do
    "f:" <> @global_tag <> ":td:1:" <> type_digest
  end

  defp type_catalog_digest_from_member(key) do
    with {:ok, _tag_prefix, remainder} <- split_internal_flow_key(key, "}:tc:1:"),
         <<type_digest::binary-size(43), ?:, state_digest::binary-size(43)>> <- remainder,
         true <- valid_digest?(type_digest) and valid_digest?(state_digest) do
      {:ok, type_digest}
    else
      _invalid -> :error
    end
  end

  defp digest(value),
    do: value |> then(&:crypto.hash(:sha256, &1)) |> Base.url_encode64(padding: false)

  defp valid_digest?(digest) when is_binary(digest) and byte_size(digest) == 43 do
    case Base.url_decode64(digest, padding: false) do
      {:ok, decoded} when byte_size(decoded) == 32 ->
        Base.url_encode64(decoded, padding: false) == digest

      _invalid ->
        false
    end
  end

  defp valid_digest?(_digest), do: false

  defp flow_key_tag_prefix(key, marker) do
    case split_internal_flow_key(key, marker) do
      {:ok, tag_prefix, _remainder} -> {:ok, tag_prefix}
      :error -> :error
    end
  end

  defp split_internal_flow_key(<<"f:{", rest::binary>> = key, marker) do
    case :binary.match(rest, marker) do
      {position, marker_size} when position > 0 ->
        tag = binary_part(rest, 0, position)
        offset = 3 + position + marker_size

        if valid_flow_tag?(tag) do
          {:ok, binary_part(key, 0, 3 + position + 1),
           binary_part(key, offset, byte_size(key) - offset)}
        else
          :error
        end

      :nomatch ->
        :error
    end
  end

  defp split_internal_flow_key(_key, _marker), do: :error

  defp valid_flow_tag?("f"), do: true

  defp valid_flow_tag?(<<"fa:", bucket::binary>>) do
    case Integer.parse(bucket) do
      {number, ""} when number in 0..255 -> bucket == Integer.to_string(number)
      _invalid -> false
    end
  end

  defp valid_flow_tag?(<<"f:", digest::binary>>), do: valid_digest?(digest)
  defp valid_flow_tag?(_tag), do: false

  def auto_partition_key(id) when is_binary(id) do
    bucket =
      id
      |> :erlang.crc32()
      |> rem(@auto_partition_buckets)

    @auto_partition_prefix <> Integer.to_string(bucket)
  end

  def auto_partition_keys do
    @auto_partition_keys
  end

  def auto_partition_key?(<<@auto_partition_prefix, bucket::binary>>) do
    match?({:ok, _bucket_index}, auto_partition_bucket(bucket))
  end

  def auto_partition_key?(_partition_key), do: false

  defp auto_partition_bucket(bucket), do: auto_partition_bucket(bucket, 0, false)

  defp auto_partition_bucket(<<>>, value, true) when value < @auto_partition_buckets,
    do: {:ok, value}

  defp auto_partition_bucket(<<digit, rest::binary>>, value, _seen?)
       when digit >= ?0 and digit <= ?9 do
    next = value * 10 + (digit - ?0)

    if next < @auto_partition_buckets do
      auto_partition_bucket(rest, next, true)
    else
      :error
    end
  end

  defp auto_partition_bucket(_bucket, _value, _seen?), do: :error

  defp flow_value_kind(:payload), do: "p"
  defp flow_value_kind(:result), do: "r"
  defp flow_value_kind(:error), do: "e"
  defp flow_value_kind(:shared), do: "s"
end
