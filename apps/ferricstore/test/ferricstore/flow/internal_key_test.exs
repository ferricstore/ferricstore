defmodule Ferricstore.Flow.InternalKeyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.Flow.Keys

  @digest Base.url_encode64(:crypto.hash(:sha256, "tenant-a"), padding: false)
  @error "ERR access to internal Flow keys is not allowed"

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

  test "denies generic public access but keeps dedicated Flow commands trusted" do
    key = "f:{f}:s:flow-1"

    assert {:error, @error} = InternalKey.authorize_public(["ordinary", key])
    assert :ok = InternalKey.authorize_public(["ordinary"])
    assert {:error, @error} = InternalKey.authorize_command("GET", [key])
    assert :ok = InternalKey.authorize_command("FLOW.GET", [key])
  end

  test "reserves physical compound namespaces from generic access" do
    assert InternalKey.reserved?("T:ordinary")
    assert InternalKey.reserved?("H:ordinary\0field")
    assert InternalKey.reserved?("X:ordinary\0" <> "1-0")
    refute InternalKey.reserved?("ordinary")
  end
end
