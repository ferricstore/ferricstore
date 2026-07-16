defmodule Ferricstore.Flow.InternalKeyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.Flow.Keys
  alias Ferricstore.ServerCatalog

  @digest Base.url_encode64(:crypto.hash(:sha256, "tenant-a"), padding: false)
  @error "ERR access to internal keys is not allowed"

  test "recognizes every canonical Flow hash-tag family" do
    assert InternalKey.internal?("f:{f}:s:flow-1")
    assert InternalKey.internal?("f:{fa:0}:q:ready")
    assert InternalKey.internal?("f:{fa:255}:q:ready")
    assert InternalKey.internal?("f:{f:#{@digest}}:gov:c:tenant-a")
  end

  test "recognizes governance control and cache-session tag families" do
    control_keys = [
      Keys.governance_limit_cleanup_progress_key(),
      Keys.governance_limit_catalog_outbox_meta_key(0),
      Keys.governance_limit_catalog_outbox_intent_key(0, 1),
      Keys.governance_release_outbox_meta_key(0),
      Keys.governance_release_outbox_intent_key(0, 1),
      Keys.governance_release_outbox_completed_key(0, 1),
      Keys.governance_catalog_key(:limit),
      Keys.governance_approval_scope_catalog_key("scope"),
      Keys.governance_approval_flow_catalog_key("flow")
    ]

    cache_keys = [
      Keys.governance_limit_cache_session_head_key("node", "default"),
      Keys.governance_limit_cache_session_meta_key("node", "default", "session"),
      Keys.governance_limit_cache_session_page_key("node", "default", "session", 1)
    ]

    Enum.each(control_keys ++ cache_keys, fn key ->
      assert InternalKey.internal?(key)
      assert {:error, @error} = InternalKey.authorize_public([key])
    end)
  end

  test "recognizes only well-formed physical history entries" do
    assert InternalKey.internal?("X:f:{f}:h:flow:with:colon\0" <> "123-4")
    assert InternalKey.internal?("X:f:{f:#{@digest}}:h:flow-1\0" <> "0-0")

    refute InternalKey.internal?("X:f:{f}:h:\0" <> "123-4")
    refute InternalKey.internal?("X:f:{f}:h:flow-1\0" <> "01-4")
    refute InternalKey.internal?("X:f:{f}:h:flow-1\0" <> "123--4")
    refute InternalKey.internal?("X:f:{f}:s:flow-1\0" <> "123-4")
  end

  test "does not reserve malformed tags or ordinary lookalike user keys" do
    refute InternalKey.internal?("f:{fa:01}:q:ready")
    refute InternalKey.internal?("f:{fa:256}:q:ready")
    refute InternalKey.internal?("f:{f:short}:s:flow-1")
    refute InternalKey.internal?("f:{flow-governance-extra}:gov:catalog:limit")
    refute InternalKey.internal?("f:{fgc:short}:gov:limit-cache-session:head")
    refute InternalKey.internal?("f:{tenant}:s:flow-1")
    refute InternalKey.internal?("f:{f}ordinary-user-key")
    refute InternalKey.internal?("X:user-stream\0" <> "123-4")
  end

  test "Flow key classifiers require canonical hash tags" do
    state_key = Keys.state_key("flow-1", "tenant")
    registry_key = Keys.registry_key("flow-1", "tenant")

    assert Keys.state_key?(state_key)
    assert Keys.value_key?(Keys.value_key("flow-1", :payload, 1, "tenant"))
    assert Keys.shared_value_ref?(Keys.value_key("flow-1", :shared, 1, "tenant"))
    assert Keys.history_key?(Keys.history_key("flow-1", "tenant"))
    assert Keys.registry_key?(registry_key)
    assert {:ok, ^registry_key} = Keys.registry_key_from_state_key(state_key)
    assert {:ok, ^state_key} = Keys.state_key_from_registry_key(registry_key)

    refute Keys.state_key?("f:{tenant}:s:flow-1")
    refute Keys.value_key?("f:{tenant}:v:p:flow-1:1")
    refute Keys.shared_value_ref?("f:{tenant}:v:s:flow-1:1")
    refute Keys.history_key?("f:{tenant}:h:flow-1")
    refute Keys.registry_key?("f:{tenant}:r:type:state")
    refute Keys.value_key?(ServerCatalog.revision_key("acl"))
    assert :error = Keys.registry_key_from_state_key("f:{tenant}:s:flow-1")
    assert :error = Keys.state_key_from_registry_key("f:{tenant}:r:flow-1")
  end

  test "value key classifiers reject malformed value identities and versions" do
    tag = Keys.tag("tenant")

    assert Keys.value_key?("f:#{tag}:v:p:flow:with:colon:1")
    refute Keys.value_key?("f:#{tag}:v:p::1")
    refute Keys.value_key?("f:#{tag}:v:p:flow-1:-1")
    refute Keys.value_key?("f:#{tag}:v:p:flow-1:01")
    refute Keys.value_key?("f:#{tag}:v:p:flow-1:not-a-version")
    refute Keys.value_key?("f:#{tag}:v:p:flow-1:9007199254740992")

    refute Keys.shared_value_ref?("f:#{tag}:v:s::1")
    refute Keys.shared_value_ref?("f:#{tag}:v:n::bmFtZQ:1")
    refute Keys.shared_value_ref?("f:#{tag}:v:n:b3duZXI::1")
  end

  test "index key components cannot collide through delimiter injection" do
    partition_key = "tenant-a"

    refute Keys.due_key("a", "b:c", 0, partition_key) ==
             Keys.due_key("a:b", "c", 0, partition_key)

    refute Keys.state_index_key("a", "b:c", partition_key) ==
             Keys.state_index_key("a:b", "c", partition_key)

    refute Keys.attribute_index_key("a", "b:c", "name", "value", partition_key) ==
             Keys.attribute_index_key("a:b", "c", "name", "value", partition_key)

    refute Keys.attribute_index_key("type", "state", "name=value", "x", partition_key) ==
             Keys.attribute_index_key("type", "state", "name", "value=x", partition_key)

    refute Keys.state_meta_index_key("a", "b:c", "name", "value", partition_key) ==
             Keys.state_meta_index_key("a:b", "c", "name", "value", partition_key)

    refute Keys.signal_idempotency_key("a", "b:c", partition_key) ==
             Keys.signal_idempotency_key("a:b", "c", partition_key)

    refute Keys.governance_effect_key("a", "b:c", partition_key) ==
             Keys.governance_effect_key("a:b", "c", partition_key)

    refute Keys.governance_ledger_key("a", "b:c", partition_key) ==
             Keys.governance_ledger_key("a:b", "c", partition_key)

    refute Keys.named_shared_value_key("a", "b:c", 1, partition_key) ==
             Keys.named_shared_value_key("a:b", "c", 1, partition_key)

    refute Keys.shared_value_link_key("a", "b:c", 1, partition_key) ==
             Keys.shared_value_link_key("a:b", "c", 1, partition_key)

    named_ref = Keys.named_shared_value_key("a:b", "c=d", 7, partition_key)
    link_key = Keys.shared_value_link_key("a:b", "c=d", 7, partition_key)

    assert {:ok, "a:b", "c=d", 7} = Keys.named_shared_value_parts(named_ref)
    assert {:ok, "a:b", "c=d", 7} = Keys.shared_value_link_parts(link_key)
    assert Keys.shared_value_ref?(named_ref)
  end

  test "auto partition bucket names must be canonical" do
    assert Keys.auto_partition_key?("__flow_auto__:0")
    assert Keys.auto_partition_key?("__flow_auto__:255")

    refute Keys.auto_partition_key?("__flow_auto__:00")
    refute Keys.auto_partition_key?("__flow_auto__:01")
    refute Keys.auto_partition_key?("__flow_auto__:+1")

    refute Keys.tag("__flow_auto__:01") == Keys.tag("__flow_auto__:1")
  end

  test "denies generic public access but keeps dedicated Flow commands trusted" do
    key = "f:{f}:s:flow-1"
    catalog_key = ServerCatalog.revision_key("acl")

    assert {:error, @error} = InternalKey.authorize_public(["ordinary", key])
    assert :ok = InternalKey.authorize_public(["ordinary"])
    assert {:error, @error} = InternalKey.authorize_command("GET", [key])
    assert :ok = InternalKey.authorize_command("FLOW.GET", [key])
    assert {:error, @error} = InternalKey.authorize_command("FLOW.GET", [catalog_key])
    assert {:error, @error} = InternalKey.authorize_command("FLOW.VALUE.MGET", [catalog_key])
    assert {:error, @error} = InternalKey.authorize_command("FLOW.GET", ["T:ordinary"])
  end

  test "reserves physical compound namespaces from generic access" do
    assert InternalKey.reserved?("T:ordinary")
    assert InternalKey.reserved?("H:ordinary\0field")
    assert InternalKey.reserved?("X:ordinary\0" <> "1-0")
    refute InternalKey.reserved?("ordinary")
  end

  test "reserves durable server catalog keys from generic access" do
    keys = [
      ServerCatalog.entry_key("acl", "default"),
      ServerCatalog.revision_key("acl"),
      ServerCatalog.live_count_key("acl")
    ]

    Enum.each(keys, fn key ->
      assert ServerCatalog.internal_key?(key)
      assert InternalKey.internal?(key)
      assert InternalKey.reserved?(key)
      assert {:error, @error} = InternalKey.authorize_public([key])
    end)

    refute ServerCatalog.internal_key?("f:{__server__}:catalogue:acl:revision")
  end
end
