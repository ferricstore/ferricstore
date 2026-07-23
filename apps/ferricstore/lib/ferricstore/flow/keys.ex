defmodule Ferricstore.Flow.Keys do
  @moduledoc false

  @global_tag "{f}"
  @policy_prefix "f:" <> @global_tag <> ":policy:"
  @policy_attribute_count_prefix "f:" <> @global_tag <> ":policy-attribute:1:"
  @policy_attribute_member_prefix "f:" <> @global_tag <> ":policy-attribute-member:1:"
  @policy_attribute_revision_prefix "f:" <> @global_tag <> ":policy-attribute-revision:1:"
  @policy_attribute_repair_prefix "f:" <> @global_tag <> ":policy-attribute-repair:1:"
  @partition_tag_prefix "{f:"
  @auto_partition_prefix "__flow_auto__:"
  @auto_partition_buckets 256
  @max_exact_integer 9_007_199_254_740_991
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
    with {:ok, tag_prefix, remainder} <- split_internal_flow_key(due_key, "}:d:"),
         true <- id != "" and remainder != "" do
      {:ok, tag_prefix <> ":s:" <> id}
    else
      _invalid -> :error
    end
  end

  def state_key_from_registry_key(registry_key) when is_binary(registry_key) do
    with {:ok, tag_prefix, id} <- split_internal_flow_key(registry_key, "}:r:"),
         true <- id != "" do
      {:ok, tag_prefix <> ":s:" <> id}
    else
      _invalid -> :error
    end
  end

  def state_key_from_registry_key(_registry_key), do: :error

  def registry_key_from_state_key(state_key) when is_binary(state_key) do
    with {:ok, tag_prefix, id} <- split_internal_flow_key(state_key, "}:s:"),
         true <- id != "" do
      {:ok, tag_prefix <> ":r:" <> id}
    else
      _invalid -> :error
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

  def named_shared_value_key(owner_flow_id, name, version, partition_key \\ nil)

  def named_shared_value_key(owner_flow_id, name, version, nil)
      when is_binary(owner_flow_id) and is_binary(name) and is_integer(version) do
    named_shared_value_key(owner_flow_id, name, version, auto_partition_key(owner_flow_id))
  end

  def named_shared_value_key(owner_flow_id, name, version, partition_key)
      when is_binary(owner_flow_id) and is_binary(name) and is_integer(version) do
    "f:" <>
      tag(partition_key) <>
      ":v:n:" <>
      index_component(owner_flow_id) <>
      ":" <> index_component(name) <> ":" <> Integer.to_string(version)
  end

  def shared_value_link_prefix(owner_flow_id, partition_key \\ nil)

  def shared_value_link_prefix(owner_flow_id, nil) when is_binary(owner_flow_id) do
    shared_value_link_prefix(owner_flow_id, auto_partition_key(owner_flow_id))
  end

  def shared_value_link_prefix(owner_flow_id, partition_key) when is_binary(owner_flow_id) do
    "f:" <> tag(partition_key) <> ":svl:" <> index_component(owner_flow_id) <> ":"
  end

  def shared_value_link_key(owner_flow_id, name, version, partition_key \\ nil)
      when is_binary(owner_flow_id) and is_binary(name) and is_integer(version) do
    shared_value_link_prefix(owner_flow_id, partition_key) <>
      index_component(name) <> ":" <> Integer.to_string(version)
  end

  def named_shared_value_parts(key) when is_binary(key) do
    named_shared_key_parts(key, "}:v:n:")
  end

  def named_shared_value_parts(_key), do: :error

  def shared_value_link_parts(key) when is_binary(key) do
    named_shared_key_parts(key, "}:svl:")
  end

  def shared_value_link_parts(_key), do: :error

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

  def retention_cleanup_member?(key), do: internal_key_with_remainder?(key, "}:rtm:")

  def shared_value_ref?(ref), do: match?({:ok, kind} when kind in [?s, ?n], value_ref_kind(ref))

  def signal_idempotency_key(id, idempotency_key, partition_key \\ nil)
      when is_binary(id) and is_binary(idempotency_key) do
    "f:" <>
      tag(partition_key) <>
      ":sig:" <> index_component(id) <> ":" <> index_component(idempotency_key)
  end

  def policy_key(type) do
    @policy_prefix <> type
  end

  def policy_indexed_attribute_count_key(name) when is_binary(name) and name != "" do
    @policy_attribute_count_prefix <> digest(name)
  end

  def policy_indexed_attribute_member_prefix(name) when is_binary(name) and name != "" do
    @policy_attribute_member_prefix <> digest(name) <> ":"
  end

  def policy_indexed_attribute_member_key(name, type)
      when is_binary(name) and name != "" and is_binary(type) and type != "" do
    policy_indexed_attribute_member_prefix(name) <> digest(type)
  end

  def policy_indexed_attribute_revision_key(name) when is_binary(name) and name != "" do
    @policy_attribute_revision_prefix <> digest(name)
  end

  def policy_indexed_attribute_repair_key(name) when is_binary(name) and name != "" do
    policy_indexed_attribute_repair_prefix() <> digest(name)
  end

  def policy_indexed_attribute_repair_prefix do
    @policy_attribute_repair_prefix
  end

  def policy_indexed_attribute_catalog_key?(
        <<@policy_attribute_count_prefix::binary, _::binary>>
      ),
      do: true

  def policy_indexed_attribute_catalog_key?(
        <<@policy_attribute_member_prefix::binary, _::binary>>
      ),
      do: true

  def policy_indexed_attribute_catalog_key?(
        <<@policy_attribute_revision_prefix::binary, _::binary>>
      ),
      do: true

  def policy_indexed_attribute_catalog_key?(
        <<@policy_attribute_repair_prefix::binary, _::binary>>
      ),
      do: true

  def policy_indexed_attribute_catalog_key?(_key), do: false

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
    policy_catalog_projection_global_prefix() <> digest(type) <> ":"
  end

  def policy_catalog_projection_global_prefix, do: <<0, "fpc:1:">>

  def policy_catalog_projection_key(type, catalog_key, generation)
      when is_binary(type) and type != "" and is_binary(catalog_key) and
             is_integer(generation) and generation >= 0 and generation <= 0xFFFFFFFFFFFFFFFF do
    policy_catalog_projection_prefix(type) <>
      <<generation::unsigned-big-64, catalog_key::binary>>
  end

  def decode_policy_catalog_projection_key(type, key)
      when is_binary(type) and type != "" and is_binary(key) do
    with {:ok, decoded} <- decode_policy_catalog_projection_key(key),
         true <- decoded.type_digest == digest(type) do
      {:ok, Map.delete(decoded, :type_digest)}
    else
      _invalid -> :error
    end
  end

  def decode_policy_catalog_projection_key(_type, _key), do: :error

  def decode_policy_catalog_projection_key(key) when is_binary(key) do
    prefix = policy_catalog_projection_global_prefix()

    with <<^prefix::binary, type_digest::binary-size(43), ?:, generation::unsigned-big-64,
           catalog_key::binary>>
         when catalog_key != "" <- key,
         true <- valid_digest?(type_digest),
         {:ok, ^type_digest} <- type_catalog_digest_from_member(catalog_key) do
      {:ok,
       %{
         catalog_key: catalog_key,
         migration_generation: generation,
         type_digest: type_digest
       }}
    else
      _invalid -> :error
    end
  end

  def decode_policy_catalog_projection_key(_key), do: :error

  def policy_type("f:" <> @global_tag <> ":policy:" <> type) when type != "", do: {:ok, type}
  def policy_type(_key), do: :error

  def governance_effect_key(id, effect_key, partition_key \\ nil) do
    governance_effect_key_prefix(id, partition_key) <> index_component(effect_key)
  end

  def governance_effect_key_prefix(id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:e:" <> index_component(id) <> ":"
  end

  def governance_ledger_key(id, event_id, partition_key \\ nil) do
    governance_ledger_key_prefix(id, partition_key) <> index_component(event_id)
  end

  def governance_ledger_key_prefix(id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:l:" <> index_component(id) <> ":"
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

  def governance_approval_key?(key), do: internal_key_with_remainder?(key, "}:gov:a:")

  def governance_circuit_key(scope) when is_binary(scope) do
    "f:" <> tag(scope) <> ":gov:c:" <> scope
  end

  def governance_circuit_key?(key), do: internal_key_with_remainder?(key, "}:gov:c:")

  def governance_budget_key(scope) when is_binary(scope) do
    "f:" <> tag(scope) <> ":gov:b:" <> scope
  end

  def governance_budget_key?(key), do: internal_key_with_remainder?(key, "}:gov:b:")

  def governance_limit_key(scope) when is_binary(scope) do
    "f:" <> tag(scope) <> ":gov:limit:" <> scope
  end

  def governance_limit_key?(key), do: internal_key_with_remainder?(key, "}:gov:limit:")

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

  def policy_key?(<<@policy_prefix::binary, _::binary>>), do: true

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

  def value_key?(key), do: match?({:ok, _kind}, value_ref_kind(key))

  def history_key?(key), do: internal_key_with_remainder?(key, "}:h:")

  def due_key(type, state, priority, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":d:" <>
      index_component(type) <>
      ":" <> index_component(state) <> ":p" <> Integer.to_string(priority)
  end

  def due_any_key(type, priority, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":da:" <> index_component(type) <> ":p" <> Integer.to_string(priority)
  end

  @spec decode_due_key(binary()) ::
          {:ok,
           %{
             type: binary(),
             state: binary(),
             priority: integer(),
             tag_prefix: binary(),
             auto_partition?: boolean()
           }}
          | :error
  def decode_due_key(key) when is_binary(key) do
    with {:ok, tag_prefix, remainder} <- split_internal_flow_key(key, "}:d:"),
         [encoded_type, encoded_state_priority] <- :binary.split(remainder, ":"),
         [encoded_state, encoded_priority] <- :binary.split(encoded_state_priority, ":p"),
         true <- encoded_type != "" and encoded_state != "" and encoded_priority != "",
         {:ok, type} <- decode_index_component(encoded_type),
         {:ok, state} <- decode_index_component(encoded_state),
         {priority, ""} <- Integer.parse(encoded_priority),
         true <- encoded_priority == Integer.to_string(priority) do
      {:ok,
       %{
         type: type,
         state: state,
         priority: priority,
         tag_prefix: tag_prefix,
         auto_partition?: String.starts_with?(tag_prefix, "f:{fa:")
       }}
    else
      _invalid -> :error
    end
  end

  def decode_due_key(_key), do: :error

  def state_index_key(type, state, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":i:s:" <> index_component(type) <> ":" <> index_component(state)
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
    "f:" <>
      tag(partition_key) <>
      ":i:a:" <>
      index_component(type) <>
      ":" <>
      index_component(state) <>
      ":" <> index_component(name) <> "=" <> index_component(value)
  end

  def attribute_index_prefix(type, state, name, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":i:a:" <>
      index_component(type) <>
      ":" <> index_component(state) <> ":" <> index_component(name) <> "="
  end

  def attribute_type_index_key(type, name, value, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":i:at:" <>
      index_component(type) <> ":" <> index_component(name) <> "=" <> index_component(value)
  end

  def attribute_type_index_prefix(type, name, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":i:at:" <> index_component(type) <> ":" <> index_component(name) <> "="
  end

  def attribute_state_index_key(state, name, value, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":i:as:" <>
      index_component(state) <> ":" <> index_component(name) <> "=" <> index_component(value)
  end

  def attribute_state_index_prefix(state, name, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":i:as:" <> index_component(state) <> ":" <> index_component(name) <> "="
  end

  def attribute_partition_index_key(name, value, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":i:ap:" <> index_component(name) <> "=" <> index_component(value)
  end

  def attribute_partition_index_prefix(name, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:ap:" <> index_component(name) <> "="
  end

  def state_meta_index_key(type, state, name, value, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":i:sm:" <>
      index_component(type) <>
      ":" <>
      index_component(state) <>
      ":" <> index_component(name) <> "=" <> index_component(value)
  end

  @doc false
  def index_component(value) when is_binary(value),
    do: Base.url_encode64(value, padding: false)

  def stream_entry_key(id, event_id, partition_key \\ nil) do
    stream_entry_key_from_history_key(history_key(id, partition_key), event_id)
  end

  def stream_entry_key_from_history_key(history_key, event_id)
      when is_binary(history_key) and is_binary(event_id) do
    "X:" <> history_key <> <<0>> <> event_id
  end

  def state_key?(key), do: internal_key_with_remainder?(key, "}:s:")

  @doc false
  @spec run_id_from_state_key(binary()) :: {:ok, binary()} | :error
  def run_id_from_state_key(state_key) when is_binary(state_key) do
    case split_internal_flow_key(state_key, "}:s:") do
      {:ok, _tag_prefix, id} when id != "" -> {:ok, id}
      _invalid -> :error
    end
  end

  def run_id_from_state_key(_state_key), do: :error

  def retention_guard_key_from_state_key(state_key) when is_binary(state_key) do
    with {:ok, tag_prefix, id} <- split_internal_flow_key(state_key, "}:s:"),
         true <- id != "" do
      {:ok, tag_prefix <> ":rtg:" <> id}
    else
      _invalid -> :error
    end
  end

  def retention_guard_key_from_state_key(_state_key), do: :error

  def registry_key?(key), do: internal_key_with_remainder?(key, "}:r:")

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

  defp internal_key_with_remainder?(key, marker) do
    case split_internal_flow_key(key, marker) do
      {:ok, _tag_prefix, remainder} -> remainder != ""
      :error -> false
    end
  end

  defp value_ref_kind(key) do
    case split_internal_flow_key(key, "}:v:") do
      {:ok, _tag_prefix, <<kind, ?:, rest::binary>>}
      when kind in [?p, ?r, ?e, ?s] ->
        if valid_regular_value_ref_remainder?(rest), do: {:ok, kind}, else: :error

      {:ok, _tag_prefix, <<?n, ?:, _rest::binary>>} ->
        if match?({:ok, _owner, _name, _version}, named_shared_key_parts(key, "}:v:n:")),
          do: {:ok, ?n},
          else: :error

      _invalid ->
        :error
    end
  end

  defp valid_regular_value_ref_remainder?(rest) when is_binary(rest) do
    case :binary.matches(rest, ":") do
      [] ->
        false

      matches ->
        {version_separator, 1} = List.last(matches)
        id = binary_part(rest, 0, version_separator)
        version_offset = version_separator + 1
        encoded_version = binary_part(rest, version_offset, byte_size(rest) - version_offset)

        id != "" and canonical_non_neg_integer?(encoded_version)
    end
  end

  defp canonical_non_neg_integer?(encoded) when is_binary(encoded) do
    case Integer.parse(encoded) do
      {value, ""} when value >= 0 and value <= @max_exact_integer ->
        encoded == Integer.to_string(value)

      _invalid ->
        false
    end
  end

  defp named_shared_key_parts(key, marker) do
    with {:ok, _tag_prefix, remainder} <- split_internal_flow_key(key, marker),
         [encoded_owner, encoded_name, encoded_version] <-
           :binary.split(remainder, ":", [:global]),
         {:ok, owner_flow_id} <- decode_index_component(encoded_owner),
         {:ok, name} <- decode_index_component(encoded_name),
         true <- owner_flow_id != "" and name != "",
         {version, ""} when version >= 0 and version <= @max_exact_integer <-
           Integer.parse(encoded_version),
         true <- encoded_version == Integer.to_string(version) do
      {:ok, owner_flow_id, name, version}
    else
      _invalid -> :error
    end
  end

  @doc false
  @spec decode_index_component(binary()) :: {:ok, binary()} | :error
  def decode_index_component(encoded) when is_binary(encoded) do
    with {:ok, decoded} <- Base.url_decode64(encoded, padding: false),
         true <- index_component(decoded) == encoded do
      {:ok, decoded}
    else
      _invalid -> :error
    end
  end

  def decode_index_component(_encoded), do: :error

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

  defp auto_partition_bucket("0"), do: {:ok, 0}

  defp auto_partition_bucket(<<digit, rest::binary>>) when digit >= ?1 and digit <= ?9 do
    auto_partition_bucket_digits(rest, digit - ?0)
  end

  defp auto_partition_bucket(_bucket), do: :error

  defp auto_partition_bucket_digits(<<>>, value) when value < @auto_partition_buckets,
    do: {:ok, value}

  defp auto_partition_bucket_digits(<<digit, rest::binary>>, value)
       when digit >= ?0 and digit <= ?9 do
    next = value * 10 + (digit - ?0)

    if next < @auto_partition_buckets do
      auto_partition_bucket_digits(rest, next)
    else
      :error
    end
  end

  defp auto_partition_bucket_digits(_bucket, _value), do: :error

  defp flow_value_kind(:payload), do: "p"
  defp flow_value_kind(:result), do: "r"
  defp flow_value_kind(:error), do: "e"
  defp flow_value_kind(:shared), do: "s"
end
