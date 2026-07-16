defmodule Ferricstore.FlowGovernanceCacheSessionStoreTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.Governance.CacheSessionStore
  alias Ferricstore.Flow.Governance.LimitCache
  alias Ferricstore.Flow.Governance.LimitStore
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  setup do
    old_enabled = Application.get_env(:ferricstore, :flow_governance_limit_cache_enabled)
    old_multiplier = Application.get_env(:ferricstore, :flow_governance_limit_cache_multiplier)
    old_max_chunk = Application.get_env(:ferricstore, :flow_governance_limit_cache_max_chunk)
    old_page_size = Application.get_env(:ferricstore, :flow_governance_cache_session_page_size)

    on_exit(fn ->
      restore_env(:flow_governance_limit_cache_enabled, old_enabled)
      restore_env(:flow_governance_limit_cache_multiplier, old_multiplier)
      restore_env(:flow_governance_limit_cache_max_chunk, old_max_chunk)
      restore_env(:flow_governance_cache_session_page_size, old_page_size)
    end)

    :ok
  end

  test "session heads reject trailing external-term bytes" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-head-canonical-node")
    instance_name = unique_flow_id("cache-head-canonical-instance")

    assert {:ok, _session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    head_key = Keys.governance_limit_cache_session_head_key(node_id, instance_name)
    value = Router.get(ctx, head_key)
    assert :ok = Router.put(ctx, head_key, value <> <<0>>, 0)

    assert {:error, :cache_session_head_corrupt} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)
  end

  test "session manifests reject trailing external-term bytes" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-meta-canonical-node")
    instance_name = unique_flow_id("cache-meta-canonical-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, session.session_id)

    value = Router.get(ctx, meta_key)
    assert :ok = Router.put(ctx, meta_key, value <> <<0>>, 0)

    assert {:error, :cache_session_manifest_corrupt} =
             CacheSessionStore.manifest_bounds(ctx, session)
  end

  test "session pages reject trailing external-term bytes" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-page-canonical-node")
    instance_name = unique_flow_id("cache-page-canonical-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               session,
               "canonical-page-scope",
               0,
               ["canonical-page-reservation"],
               page_size: 1,
               expires_at_ms: 11_001
             )

    page_key =
      Keys.governance_limit_cache_session_page_key(
        node_id,
        instance_name,
        session.session_id,
        page.sequence
      )

    value = Router.get(ctx, page_key)
    assert :ok = Router.put(ctx, page_key, value <> <<0>>, 0)

    assert {:error, :cache_session_page_corrupt} =
             CacheSessionStore.activate_page(ctx, session, page)
  end

  test "power loss before activation releases every durably proven-unused page" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-unused")
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")
    lease_limit!(scope, 6)

    assert {:ok, %{reservation_ids: reservation_ids}} =
             LimitStore.spend(ctx, scope,
               shard_id: 0,
               amount: 6,
               ttl_ms: 10_000,
               now_ms: 1_001
             )

    [_claimed_id | prefetched_ids] = reservation_ids

    assert {:ok, first_session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, pages} =
             CacheSessionStore.persist_prefetch(
               ctx,
               first_session,
               scope,
               0,
               prefetched_ids,
               page_size: 2,
               expires_at_ms: 11_001
             )

    assert Enum.map(pages, &length(&1.reservation_ids)) == [2, 2, 1]

    assert {:ok, restarted_session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, counts} = recover_all(ctx, restarted_session, 1)
    assert counts == %{released: 5, retained: 0, errors: 0, processed: 3}

    assert Enum.all?(pages, &(not CacheSessionStore.page_present?(ctx, &1)))

    first_meta_key =
      Keys.governance_limit_cache_session_meta_key(
        node_id,
        instance_name,
        first_session.session_id
      )

    assert Router.get(ctx, first_meta_key) == nil

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_002)
    assert owner.leases[0].in_use == 1
  end

  test "restart retains the uncertain consumption window and releases later unused pages" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-uncertain")
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")
    lease_limit!(scope, 6)

    assert {:ok, %{reservation_ids: [_claimed_id | prefetched_ids]}} =
             LimitStore.spend(ctx, scope,
               shard_id: 0,
               amount: 6,
               ttl_ms: 10_000,
               now_ms: 1_001
             )

    assert {:ok, first_session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [first_page | _] = pages} =
             CacheSessionStore.persist_prefetch(
               ctx,
               first_session,
               scope,
               0,
               prefetched_ids,
               page_size: 2,
               expires_at_ms: 11_001
             )

    assert length(pages) == 3

    assert {:ok, activated_page} =
             CacheSessionStore.activate_page(ctx, first_session, first_page)

    assert activated_page.state == :uncertain

    assert {:ok, restarted_session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, counts} = recover_all(ctx, restarted_session, 256)
    assert counts == %{released: 3, retained: 2, errors: 0, processed: 3}

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_002)
    assert owner.leases[0].in_use == 3
  end

  test "a newer cache session fences stale persistence and activation" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, stale_session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               stale_session,
               "stale-scope",
               0,
               ["reservation-before-fence"],
               page_size: 2,
               expires_at_ms: 11_001
             )

    assert {:ok, current_session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert current_session.generation == stale_session.generation + 1

    assert {:error, :stale_cache_session} =
             CacheSessionStore.activate_page(ctx, stale_session, page)

    assert {:error, :stale_cache_session} =
             CacheSessionStore.persist_prefetch(
               ctx,
               stale_session,
               "stale-scope",
               0,
               ["reservation-after-fence"],
               page_size: 2,
               expires_at_ms: 11_001
             )
  end

  test "acknowledged activated pages advance a bounded recovery floor and are deleted" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    Enum.each(1..20, fn index ->
      reservation_id = "bounded-reservation-#{index}"

      assert {:ok, [page]} =
               CacheSessionStore.persist_prefetch(
                 ctx,
                 session,
                 "bounded-scope",
                 0,
                 [reservation_id],
                 page_size: 1,
                 expires_at_ms: 11_001
               )

      assert {:ok, activated} = CacheSessionStore.activate_page(ctx, session, page)
      assert :ok = CacheSessionStore.acknowledge_page(ctx, session, activated)
      refute CacheSessionStore.page_present?(ctx, activated)
    end)

    assert {:ok, bounds} = CacheSessionStore.manifest_bounds(ctx, session)
    assert bounds == %{page_count: 20, recovery_floor: 21}
  end

  test "acknowledge deletes its page before advancing the recovery floor" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               session,
               "ack-crash-scope",
               0,
               ["ack-crash-reservation"],
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert {:ok, activated} = CacheSessionStore.activate_page(ctx, session, page)

    assert catch_throw(
             CacheSessionStore.acknowledge_page(ctx, session, activated,
               after_page_delete_fun: fn -> throw(:simulated_power_loss) end
             )
           ) == :simulated_power_loss

    refute CacheSessionStore.page_present?(ctx, activated)

    assert {:ok, %{page_count: 1, recovery_floor: 1}} =
             CacheSessionStore.manifest_bounds(ctx, session)

    assert {:ok, restarted} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, %{processed: 1, errors: 0, next_cursor: nil}} =
             CacheSessionStore.recover(ctx, restarted,
               limit: 256,
               release_fun: fn _ctx, _scope, _opts ->
                 flunk("a deleted acknowledged page must not be released")
               end
             )

    old_meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, session.session_id)

    assert Router.get(ctx, old_meta_key) == nil
  end

  test "discarded denied plans advance the recovery floor without durable holes" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    Enum.each(1..20, fn index ->
      assert {:ok, [page]} =
               CacheSessionStore.persist_prefetch(
                 ctx,
                 session,
                 "denied-scope",
                 0,
                 ["denied-reservation-#{index}"],
                 page_size: 1,
                 expires_at_ms: 11_001
               )

      assert :ok = CacheSessionStore.discard_pages(ctx, session, [page])
      refute CacheSessionStore.page_present?(ctx, page)
    end)

    assert {:ok, %{page_count: 20, recovery_floor: 21}} =
             CacheSessionStore.manifest_bounds(ctx, session)
  end

  test "acknowledging an early gap compacts a bounded run of later deleted pages" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [first_page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               session,
               "floor-gap-scope",
               0,
               ["floor-gap-first"],
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert {:ok, activated_first} = CacheSessionStore.activate_page(ctx, session, first_page)
    later_ids = Enum.map(1..20, &"floor-gap-later-#{&1}")

    assert {:ok, later_pages} =
             CacheSessionStore.persist_prefetch(
               ctx,
               session,
               "floor-gap-scope",
               0,
               later_ids,
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert :ok = CacheSessionStore.discard_pages(ctx, session, later_pages)

    assert {:ok, %{page_count: 21, recovery_floor: 1}} =
             CacheSessionStore.manifest_bounds(ctx, session)

    assert :ok = CacheSessionStore.acknowledge_page(ctx, session, activated_first)

    assert {:ok, %{page_count: 21, recovery_floor: 22}} =
             CacheSessionStore.manifest_bounds(ctx, session)
  end

  test "floor compaction does not treat an unavailable page read as deletion" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [first_page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               session,
               "floor-unavailable-scope",
               0,
               ["floor-unavailable-first"],
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert {:ok, activated_first} = CacheSessionStore.activate_page(ctx, session, first_page)

    assert {:ok, [later_page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               session,
               "floor-unavailable-scope",
               0,
               ["floor-unavailable-later"],
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert :ok = CacheSessionStore.discard_pages(ctx, session, [later_page])

    assert :ok =
             CacheSessionStore.acknowledge_page(ctx, session, activated_first,
               floor_read_fun: fn _ctx, _keys -> :unavailable end
             )

    assert {:ok, %{page_count: 2, recovery_floor: 2}} =
             CacheSessionStore.manifest_bounds(ctx, session)
  end

  test "discard cannot delete a page that won an activation race" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [unused_page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               session,
               "discard-activation-race",
               0,
               ["discard-activation-reservation"],
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert {:ok, %{state: :uncertain}} =
             CacheSessionStore.activate_page(ctx, session, unused_page)

    assert {:error, :cache_session_page_not_discardable} =
             CacheSessionStore.discard_pages(ctx, session, [unused_page])

    assert CacheSessionStore.page_present?(ctx, unused_page)

    assert :ok =
             CacheSessionStore.discard_pages(ctx, session, [unused_page],
               allowed_states: [:unused, :uncertain]
             )

    refute CacheSessionStore.page_present?(ctx, unused_page)
  end

  test "partial manifest persistence failure deletes every unspent reserved page" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:error, :injected_page_persist_failure} =
             CacheSessionStore.persist_prefetch(
               ctx,
               session,
               "partial-persist-scope",
               0,
               ["partial-id-1", "partial-id-2", "partial-id-3"],
               page_size: 1,
               expires_at_ms: 11_001,
               after_page_persist_fun: fn _page -> {:error, :injected_page_persist_failure} end
             )

    Enum.each(1..3, fn sequence ->
      page_key =
        Keys.governance_limit_cache_session_page_key(
          node_id,
          instance_name,
          session.session_id,
          sequence
        )

      assert Router.get(ctx, page_key) == nil
    end)

    assert {:ok, %{page_count: 3, recovery_floor: 4}} =
             CacheSessionStore.manifest_bounds(ctx, session)
  end

  test "discard deletes its pages before advancing the recovery floor" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               session,
               "discard-crash-scope",
               0,
               ["discard-crash-reservation"],
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert catch_throw(
             CacheSessionStore.discard_pages(ctx, session, [page],
               after_page_delete_fun: fn -> throw(:simulated_power_loss) end
             )
           ) == :simulated_power_loss

    refute CacheSessionStore.page_present?(ctx, page)

    assert {:ok, %{page_count: 1, recovery_floor: 1}} =
             CacheSessionStore.manifest_bounds(ctx, session)

    assert {:ok, restarted} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, %{processed: 1, errors: 0, next_cursor: nil}} =
             CacheSessionStore.recover(ctx, restarted,
               limit: 256,
               release_fun: fn _ctx, _scope, _opts ->
                 flunk("a deleted discarded page must not be released")
               end
             )

    old_meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, session.session_id)

    assert Router.get(ctx, old_meta_key) == nil
  end

  test "discard rejects pages owned by another cache session" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, first} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [first_page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               first,
               "foreign-discard-scope",
               0,
               ["foreign-discard-reservation"],
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert {:ok, second} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:error, :cache_session_page_mismatch} =
             CacheSessionStore.discard_pages(ctx, second, [first_page])

    assert CacheSessionStore.page_present?(ctx, first_page)
  end

  test "completed recovery severs the historical session chain" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, first} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, second} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert second.previous_session_id == first.session_id

    assert {:ok, %{next_cursor: nil, errors: 0}} =
             CacheSessionStore.recover(ctx, second, limit: 1)

    assert {:ok, nil} = CacheSessionStore.manifest_previous(ctx, second)

    first_meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, first.session_id)

    assert Router.get(ctx, first_meta_key) == nil

    assert {:ok, third} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, %{processed: 0, next_cursor: nil, errors: 0}} =
             CacheSessionStore.recover(ctx, third, limit: 1)

    second_meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, second.session_id)

    assert Router.get(ctx, second_meta_key) == nil
  end

  test "recovery resumes a durable metadata cleanup checkpoint after process loss" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, first} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, second} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert catch_throw(
             CacheSessionStore.recover(ctx, second,
               limit: 1,
               after_cleanup_mark_fun: fn recovered_session_id ->
                 assert recovered_session_id == first.session_id
                 throw(:simulated_power_loss)
               end
             )
           ) == :simulated_power_loss

    first_meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, first.session_id)

    assert is_binary(Router.get(ctx, first_meta_key))

    assert {:ok, third} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, %{processed: 0, next_cursor: nil, errors: 0}} =
             CacheSessionStore.recover(ctx, third, limit: 1)

    second_meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, second.session_id)

    assert Router.get(ctx, first_meta_key) == nil
    assert Router.get(ctx, second_meta_key) == nil
    assert {:ok, nil} = CacheSessionStore.manifest_previous(ctx, third)
  end

  test "recovery repairs a stale cursor after metadata deletion but before checkpoint clearing" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, first} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, second} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    second_meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, second.session_id)

    pending_cleanup =
      :erlang.term_to_binary({
        :flow_governance_cache_session_meta_v1,
        second.session_id,
        second.generation,
        nil,
        first.session_id,
        0,
        1,
        :active
      })

    assert :ok = Router.put(ctx, second_meta_key, pending_cleanup, 0)

    first_meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, first.session_id)

    assert :ok = Router.delete(ctx, first_meta_key)

    stale_cursor = %{session_id: first.session_id, sequence: 1}

    assert {:ok, %{processed: 0, errors: 0, next_cursor: nil}} =
             CacheSessionStore.recover(ctx, second, cursor: stale_cursor, limit: 1)

    assert {:ok, nil} = CacheSessionStore.manifest_previous(ctx, second)
  end

  test "corrupt oversized manifest pages fail closed without invoking release" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, first} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               first,
               "oversized-scope",
               0,
               ["valid-id"],
               page_size: 1,
               expires_at_ms: 11_001
             )

    oversized_ids = Enum.map(1..257, &"oversized-id-#{&1}")

    corrupt =
      :erlang.term_to_binary(
        {:flow_governance_cache_session_page_v1, page.node_id, page.instance_name,
         page.session_id, page.generation, page.sequence, page.scope, page.shard_id,
         page.expires_at_ms, 0, nil, oversized_ids, :unused}
      )

    page_key =
      Keys.governance_limit_cache_session_page_key(
        page.node_id,
        page.instance_name,
        page.session_id,
        page.sequence
      )

    assert :ok = Router.put(ctx, page_key, corrupt, 0)

    assert {:ok, restarted} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    parent = self()

    assert {:ok, %{released: 0, errors: 1, processed: 1}} =
             CacheSessionStore.recover(ctx, restarted,
               limit: 1,
               release_fun: fn _ctx, _scope, _opts ->
                 send(parent, :unexpected_oversized_release)
                 {:ok, %{}}
               end
             )

    refute_receive :unexpected_oversized_release
  end

  test "recovery rejects cursors outside the durable exact-integer boundary" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:error, "ERR invalid flow governance cache session recovery options"} =
             CacheSessionStore.recover(ctx, session,
               cursor: %{session_id: session.session_id, sequence: 9_007_199_254_740_992},
               limit: 1
             )
  end

  test "opening a session fails closed when the durable generation is exhausted" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")
    head_key = Keys.governance_limit_cache_session_head_key(node_id, instance_name)

    exhausted_head =
      :erlang.term_to_binary({
        :flow_governance_cache_session_head_v1,
        9_007_199_254_740_991,
        "exhausted-session"
      })

    assert :ok = Router.put(ctx, head_key, exhausted_head, 0)

    assert {:error, :cache_session_generation_exhausted} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert Router.get(ctx, head_key) == exhausted_head
  end

  test "session open reuses staged metadata after head publication is interrupted" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")
    parent = self()

    assert {:error, :simulated_head_publish_interruption} =
             CacheSessionStore.open(ctx,
               node_id: node_id,
               instance_name: instance_name,
               before_head_replace_fun: fn session ->
                 send(parent, {:staged_session, session})
                 {:error, :simulated_head_publish_interruption}
               end
             )

    assert_receive {:staged_session, staged}

    refute CacheSessionStore.head_present?(ctx,
             node_id: node_id,
             instance_name: instance_name
           )

    staged_meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, staged.session_id)

    assert is_binary(Router.get(ctx, staged_meta_key))

    assert {:ok, opened} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert opened.session_id == staged.session_id
    assert opened.generation == staged.generation
    assert CacheSessionStore.current?(ctx, opened)
    assert is_binary(Router.get(ctx, staged_meta_key))
  end

  test "session pages use the same reservation id byte boundary as LimitStore" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [_page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               session,
               "boundary-scope",
               0,
               [String.duplicate("a", 256)],
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert {:error, "ERR invalid flow governance cache reservation ids"} =
             CacheSessionStore.persist_prefetch(
               ctx,
               session,
               "boundary-scope",
               0,
               [String.duplicate("b", 257)],
               page_size: 1,
               expires_at_ms: 11_001
             )
  end

  test "transient recovery release failure retains its cursor and retries before compaction" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-recovery-retry")
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")
    lease_limit!(scope, 2)

    assert {:ok, %{reservation_ids: [_claimed_id, cached_id]}} =
             LimitStore.spend(ctx, scope,
               shard_id: 0,
               amount: 2,
               ttl_ms: 10_000,
               now_ms: 1_001
             )

    assert {:ok, first} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [_page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               first,
               scope,
               0,
               [cached_id],
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert {:ok, restarted} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, failed} =
             CacheSessionStore.recover(ctx, restarted,
               limit: 1,
               release_fun: fn _ctx, _scope, _opts -> {:error, :injected_transient} end
             )

    assert failed.errors == 1
    assert failed.next_cursor == %{session_id: first.session_id, sequence: 1}
    first_session_id = first.session_id
    assert {:ok, ^first_session_id} = CacheSessionStore.manifest_previous(ctx, restarted)

    assert {:ok, succeeded} =
             CacheSessionStore.recover(ctx, restarted, cursor: failed.next_cursor, limit: 1)

    assert succeeded.released == 1
    assert succeeded.next_cursor == %{session_id: first.session_id, sequence: 2}

    assert {:ok, %{processed: 0, errors: 0, next_cursor: nil}} =
             CacheSessionStore.recover(ctx, restarted,
               cursor: succeeded.next_cursor,
               limit: 1
             )

    assert {:ok, nil} = CacheSessionStore.manifest_previous(ctx, restarted)
  end

  test "transient recovery page deletion failure retains its cursor before metadata compaction" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-delete-retry")
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")
    lease_limit!(scope, 2)

    assert {:ok, %{reservation_ids: [_claimed_id, prefetched_id]}} =
             LimitStore.spend(ctx, scope,
               shard_id: 0,
               amount: 2,
               ttl_ms: 10_000,
               now_ms: 1_001
             )

    assert {:ok, first} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               first,
               scope,
               0,
               [prefetched_id],
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert {:ok, restarted} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok,
            %{
              released: 0,
              errors: 1,
              processed: 1,
              next_cursor: %{session_id: failed_session_id, sequence: 1}
            }} =
             CacheSessionStore.recover(ctx, restarted,
               limit: 1,
               now_ms: 1_002,
               page_delete_fun: fn _ctx, _key -> {:error, :transient_delete_failure} end
             )

    assert failed_session_id == first.session_id
    assert CacheSessionStore.page_present?(ctx, page)

    first_meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, first.session_id)

    assert is_binary(Router.get(ctx, first_meta_key))

    assert {:ok, counts} = recover_all(ctx, restarted, 1)
    assert counts == %{released: 1, retained: 0, errors: 0, processed: 1}
    refute CacheSessionStore.page_present?(ctx, page)
    assert Router.get(ctx, first_meta_key) == nil

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_003)
    assert owner.leases[0].in_use == 1
  end

  test "unavailable recovery page read retains its cursor without releasing or compacting" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-session-node")
    instance_name = unique_flow_id("cache-session-instance")

    assert {:ok, first} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               first,
               "unavailable-recovery-scope",
               0,
               ["unavailable-recovery-id"],
               page_size: 1,
               expires_at_ms: 11_001
             )

    assert {:ok, restarted} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    parent = self()

    assert {:ok,
            %{
              released: 0,
              errors: 1,
              processed: 1,
              next_cursor: %{session_id: failed_session_id, sequence: 1}
            }} =
             CacheSessionStore.recover(ctx, restarted,
               limit: 1,
               page_read_fun: fn _ctx, _key -> :unavailable end,
               release_fun: fn _ctx, _scope, _opts ->
                 send(parent, :unexpected_unavailable_release)
                 {:ok, %{}}
               end
             )

    assert failed_session_id == first.session_id
    refute_receive :unexpected_unavailable_release
    assert CacheSessionStore.page_present?(ctx, page)

    first_meta_key =
      Keys.governance_limit_cache_session_meta_key(node_id, instance_name, first.session_id)

    assert is_binary(Router.get(ctx, first_meta_key))
    assert {:ok, first_session_id} = CacheSessionStore.manifest_previous(ctx, restarted)
    assert first_session_id == first.session_id
  end

  test "session key families co-locate internally and distribute by node and instance" do
    head = Keys.governance_limit_cache_session_head_key("node-a", "instance-a")
    meta = Keys.governance_limit_cache_session_meta_key("node-a", "instance-a", "session")
    page = Keys.governance_limit_cache_session_page_key("node-a", "instance-a", "session", 1)
    other_head = Keys.governance_limit_cache_session_head_key("node-b", "instance-a")

    assert Router.extract_hash_tag(head) == Router.extract_hash_tag(meta)
    assert Router.extract_hash_tag(meta) == Router.extract_hash_tag(page)
    refute Router.extract_hash_tag(head) == Router.extract_hash_tag(other_head)
  end

  test "graceful flush releases activated and unopened manifest pages" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_multiplier, 6)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_max_chunk, 6)
    Application.put_env(:ferricstore, :flow_governance_cache_session_page_size, 2)

    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-graceful-flush")
    lease_limit!(scope, 6)

    assert {:ok, %{reservation_ids: [_claimed_id]}} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 10_000,
               now_ms: 1_001
             )

    cache_state = :sys.get_state(LimitCache)
    session = Map.fetch!(cache_state.sessions, :default)

    pending_pages =
      cache_state.pending_pages
      |> Map.values()
      |> List.flatten()
      |> Enum.filter(&(&1.scope == scope))

    assert length(pending_pages) == 2

    assert {:ok, %{released: 5, errors: 0}} = LimitCache.flush(ctx, now_ms: 1_002)

    assert Enum.all?(pending_pages, &(not CacheSessionStore.page_present?(ctx, &1)))

    assert {:ok, %{page_count: 3, recovery_floor: 4}} =
             CacheSessionStore.manifest_bounds(ctx, session)

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_003)
    assert owner.leases[0].in_use == 1
  end

  test "clear without context refuses pending pages left by a partial flush failure" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_multiplier, 6)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_max_chunk, 6)
    Application.put_env(:ferricstore, :flow_governance_cache_session_page_size, 2)

    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-partial-flush")
    lease_limit!(scope, 6)

    assert {:ok, %{reservation_ids: [_claimed_id]}} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 10_000,
               now_ms: 1_001
             )

    counter = start_supervised!({Agent, fn -> 0 end})

    release_fun = fn release_ctx, release_scope, opts ->
      call = Agent.get_and_update(counter, fn count -> {count, count + 1} end)

      if call == 0 do
        LimitStore.release(release_ctx, release_scope, opts)
      else
        {:error, :injected_pending_release_failure}
      end
    end

    assert {:ok, %{released: 2, errors: 2}} =
             LimitCache.flush(ctx, now_ms: 1_002, release_fun: release_fun)

    assert {:error, :cached_reservations_present} = LimitCache.clear()

    assert {:ok, %{released: 3, errors: 0}} = LimitCache.clear(ctx, now_ms: 1_003)
    assert :ok = LimitCache.clear()
  end

  test "expired pending pages are exact-released after the durable lease is renewed" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_multiplier, 6)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_max_chunk, 6)
    Application.put_env(:ferricstore, :flow_governance_cache_session_page_size, 2)

    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-renew-pending")

    assert {:ok, _lease} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 0,
               amount: 6,
               limit: 6,
               ttl_ms: 100,
               now_ms: 1_000
             )

    assert {:ok, _first} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 100,
               now_ms: 1_001
             )

    assert {:ok, %{cache: :hit}} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 1_000,
               now_ms: 1_050
             )

    assert {:ok, %{cache: :hit}} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 1_000,
               now_ms: 1_051
             )

    assert {:ok, _after_pending_expiry} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 1_000,
               now_ms: 1_102
             )

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_103)
    assert owner.leases[0].in_use == 4
  end

  test "newer policy config fences and releases prefetched credits before a cache hit" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)

    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-policy-fence")

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 4,
               limit: 4,
               ttl_ms: 10_000,
               now_ms: 1_000,
               config_version: 1,
               policy_version: "policy-v1"
             )

    assert {:ok, %{reservation_ids: [_claimed_id]}} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 10_000,
               now_ms: 1_001,
               limit: 4,
               config_version: 1,
               policy_version: "policy-v1"
             )

    assert {:error, denial} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 10_000,
               now_ms: 1_002,
               limit: 1,
               config_version: 2,
               policy_version: "policy-v2"
             )

    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_003)
    assert owner.limit == 1
    assert owner.config_version == 2
    assert owner.leases[0].in_use == 1

    assert {:error, _denial} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 10_000,
               now_ms: 1_004,
               limit: 1,
               config_version: 2,
               policy_version: "policy-v2"
             )
  end

  test "failed policy-fence release persists retry pages instead of losing detached ids" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)

    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-policy-fence-retry")

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 4,
               limit: 4,
               ttl_ms: 10_000,
               now_ms: 1_000,
               config_version: 1,
               policy_version: "policy-v1"
             )

    assert {:ok, _first} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 10_000,
               now_ms: 1_001,
               limit: 4,
               config_version: 1,
               policy_version: "policy-v1"
             )

    assert {:error, _denial} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 10_000,
               now_ms: 1_002,
               limit: 1,
               config_version: 2,
               policy_version: "policy-v2",
               cache_release_fun: fn _ctx, _scope, _opts ->
                 {:error, :injected_release_failure}
               end
             )

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_003)
    assert owner.leases[0].in_use == 4

    assert {:error, _denial} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 10_000,
               now_ms: 1_004,
               limit: 1,
               config_version: 2,
               policy_version: "policy-v2"
             )

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_005)
    assert owner.leases[0].in_use == 1
  end

  test "large valid cache spend clamps prefetch to the store mutation maximum" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_multiplier, 4)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_max_chunk, 10_000)

    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-large-prefetch")
    lease_limit!(scope, 1_000)

    assert {:ok, %{reservation_ids: ids}} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 300,
               ttl_ms: 10_000,
               now_ms: 1_001
             )

    assert length(ids) == 300
    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_002)
    assert owner.leases[0].in_use == 1_000

    assert {:ok, %{cache: :hit}} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 10_000,
               now_ms: 1_003
             )
  end

  test "crash after exact spend but before cache activation leaves prefetched ids recoverable" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_multiplier, 6)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_max_chunk, 6)
    Application.put_env(:ferricstore, :flow_governance_cache_session_page_size, 2)

    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-post-spend-crash")
    now_ms = Ferricstore.CommandTime.now_ms()
    lease_limit!(scope, 6, now_ms)
    parent = self()
    cache_pid = Process.whereis(LimitCache)

    {_caller, monitor} =
      spawn_monitor(fn ->
        LimitCache.spend(ctx, scope,
          shard_id: 0,
          amount: 1,
          ttl_ms: 10_000,
          now_ms: now_ms + 1,
          after_reserved_spend_fun: fn _result ->
            send(parent, :reserved_spend_committed)
            Process.exit(cache_pid, :kill)
            exit(:simulated_power_loss)
          end
        )
      end)

    assert_receive :reserved_spend_committed, 2_000
    assert_receive {:DOWN, ^monitor, :process, _pid, :simulated_power_loss}, 2_000

    assert_eventually(fn ->
      restarted_pid = Process.whereis(LimitCache)
      assert is_pid(restarted_pid)
      assert restarted_pid != cache_pid
    end)

    assert_eventually(fn ->
      assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: now_ms + 2)
      assert owner.leases[0].in_use == 1
    end)
  end

  test "LimitCache process restart automatically recovers untouched manifest pages" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_multiplier, 6)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_max_chunk, 6)
    Application.put_env(:ferricstore, :flow_governance_cache_session_page_size, 2)

    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-session-process-restart")
    now_ms = Ferricstore.CommandTime.now_ms()
    lease_limit!(scope, 6, now_ms)

    assert {:ok, %{reservation_ids: [_claimed_id]}} =
             LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 10_000,
               now_ms: now_ms + 1
             )

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: now_ms + 2)
    assert owner.leases[0].in_use == 6

    old_pid = Process.whereis(LimitCache)
    Process.exit(old_pid, :kill)

    assert_eventually(fn ->
      restarted_pid = Process.whereis(LimitCache)
      assert is_pid(restarted_pid)
      assert restarted_pid != old_pid
    end)

    assert_eventually(fn ->
      assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: now_ms + 4)
      assert owner.leases[0].in_use == 3
    end)
  end

  defp recover_all(ctx, session, limit) do
    do_recover_all(
      ctx,
      session,
      nil,
      limit,
      1_002,
      %{released: 0, retained: 0, errors: 0, processed: 0}
    )
  end

  defp do_recover_all(ctx, session, cursor, limit, now_ms, totals) do
    assert {:ok, page} =
             CacheSessionStore.recover(ctx, session,
               cursor: cursor,
               limit: limit,
               now_ms: now_ms
             )

    totals =
      Enum.reduce([:released, :retained, :errors, :processed], totals, fn key, acc ->
        Map.update!(acc, key, &(&1 + Map.fetch!(page, key)))
      end)

    case page.next_cursor do
      nil -> {:ok, totals}
      next_cursor -> do_recover_all(ctx, session, next_cursor, limit, now_ms, totals)
    end
  end

  defp lease_limit!(scope, limit, now_ms \\ 1_000) do
    assert {:ok, _lease} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 0,
               amount: limit,
               limit: limit,
               ttl_ms: 10_000,
               now_ms: now_ms
             )
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
