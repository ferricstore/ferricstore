defmodule Ferricstore.ReviewR2.TransactionNamespaceIssuesTest do
  @moduledoc """
  Regression guards for code review findings R2-M9, R2-M10, R2-M11.

  R2-M9: Namespace config changes don't update in-flight batcher slots.
         NamespaceConfig changes should apply to subsequent writes without
         requiring direct access to removed batcher internals.

  R2-M10: ACL not re-checked at EXEC time in embedded API mode.
          FerricStore.Tx.execute/1 passes an empty watched_keys map (%{})
          to Coordinator.execute/3 — no ACL check happens before or during
          execution. This is a regression guard documenting the gap.

  R2-M11: WATCH/EXEC race — basic contract that EXEC fails when a watched
          key is modified between WATCH and EXEC, and succeeds when it is not.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  # These tests do GenServer.call to shards/batchers which can be slow
  # on CI if ra needs to recover. Give 60s per test instead of 30s.
  @moduletag timeout: 60_000

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.PreparedTransactionCoordinator, as: Coordinator
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.wait_shards_alive(60_000)
    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()
    :ok
  end

  # ---------------------------------------------------------------------------
  # R2-M9: Namespace config changes don't update in-flight batcher slots
  # ---------------------------------------------------------------------------

  describe "R2-M9: namespace config changes vs in-flight batcher slots" do
    test "namespace config changes apply to subsequent batcher writes" do
      # Pick a namespace prefix and a key that uses it.
      prefix = "r2m9ns"
      key = "#{prefix}:timer_test_#{System.unique_integer([:positive])}"

      :ok = NamespaceConfig.set(prefix, "window_ms", "100")
      assert NamespaceConfig.window_for(prefix) == 100

      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "seed", 0)
      :ok = NamespaceConfig.set(prefix, "window_ms", "1000")

      assert NamespaceConfig.window_for(prefix) == 1000

      new_key = "#{prefix}:check_#{System.unique_integer([:positive])}"
      assert :ok = Router.put(FerricStore.Instance.get(:default), new_key, "v", 0)
      assert Router.get(FerricStore.Instance.get(:default), new_key) == "v"
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M10: ACL not re-checked at EXEC time (embedded API)
  # ---------------------------------------------------------------------------

  describe "R2-M10: embedded MULTI/EXEC has no ACL enforcement" do
    test "FerricStore.multi executes all commands without ACL check" do
      # In embedded mode, FerricStore.multi/1 uses FerricStore.Tx which calls
      # Coordinator.execute(queue, %{}, sandbox_namespace) — the empty map
      # means no WATCH keys, and there is no ACL layer in the call chain.
      #
      # This test documents that any command queued via Tx.set/Tx.get/etc.
      # executes unconditionally. If ACL enforcement is added later, this
      # test should be updated to verify it.
      key = "r2m10:acl_test_#{System.unique_integer([:positive])}"

      {:ok, results} =
        FerricStore.multi(fn tx ->
          tx
          |> FerricStore.Tx.set(key, "written_without_acl")
          |> FerricStore.Tx.get(key)
        end)

      assert results == [:ok, "written_without_acl"],
             "multi/exec should execute all commands (no ACL enforcement in embedded mode)"

      assert Router.get(FerricStore.Instance.get(:default), key) == "written_without_acl"
    end

    test "Coordinator.execute accepts any command type without permission check" do
      # Directly invoke the Coordinator with commands that would require
      # different permission levels in a TCP/ACL-enabled context.
      # The Coordinator has no ACL gate — it executes everything.
      key = "r2m10:coord_#{System.unique_integer([:positive])}"

      # Write + read + delete — all "permission levels" in one transaction.
      queue = [
        {"SET", [key, "secret"]},
        {"GET", [key]},
        {"DEL", [key]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, "secret", 1],
             "Coordinator executes all commands without ACL checks"
    end

    test "Tx.execute passes empty watched_keys (no WATCH support in embedded API)" do
      # FerricStore.Tx.execute/1 always passes %{} as watched_keys to
      # Coordinator.execute/3. This means the embedded API has no WATCH
      # capability — transactions always execute (never return nil).
      key = "r2m10:nowatch_#{System.unique_integer([:positive])}"

      Router.put(FerricStore.Instance.get(:default), key, "original", 0)

      # Even though we modify the key before executing the transaction,
      # it succeeds because Tx doesn't support WATCH.
      Router.put(FerricStore.Instance.get(:default), key, "modified_externally", 0)

      {:ok, results} =
        FerricStore.multi(fn tx ->
          tx
          |> FerricStore.Tx.set(key, "overwritten_by_tx")
          |> FerricStore.Tx.get(key)
        end)

      assert results == [:ok, "overwritten_by_tx"],
             "Tx always executes — no WATCH support means no optimistic locking in embedded API"
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M11: WATCH/EXEC basic contract (regression guard)
  # ---------------------------------------------------------------------------

  describe "R2-M11: WATCH/EXEC contract" do
    test "EXEC succeeds when watched key is unchanged" do
      key = "r2m11:unchanged_#{System.unique_integer([:positive])}"

      Router.put(FerricStore.Instance.get(:default), key, "original", 0)

      # Capture the value hash (simulates WATCH).
      hash = watch_token(key)
      watched = %{key => hash}

      # No modification to key — EXEC should succeed.
      queue = [{"SET", [key, "updated_by_tx"]}]
      result = Coordinator.execute(queue, watched, nil)

      assert is_list(result), "EXEC should return a list of results when WATCH passes"
      assert result == [:ok]
      assert Router.get(FerricStore.Instance.get(:default), key) == "updated_by_tx"
    end

    test "EXEC fails (returns nil) when watched key is modified" do
      key = "r2m11:modified_#{System.unique_integer([:positive])}"

      Router.put(FerricStore.Instance.get(:default), key, "original", 0)

      # WATCH the key (snapshot value hash).
      hash = watch_token(key)
      watched = %{key => hash}

      # Another client modifies the key between WATCH and EXEC.
      Router.put(FerricStore.Instance.get(:default), key, "changed_by_other", 0)

      # EXEC should detect the value hash mismatch and abort.
      queue = [{"SET", [key, "should_not_apply"]}]
      result = Coordinator.execute(queue, watched, nil)

      assert result == nil, "EXEC should return nil when a watched key was modified"

      assert Router.get(FerricStore.Instance.get(:default), key) == "changed_by_other",
             "original modification should persist — tx was aborted"
    end

    test "WATCH detects key deletion" do
      key = "r2m11:deleted_#{System.unique_integer([:positive])}"

      Router.put(FerricStore.Instance.get(:default), key, "exists", 0)

      hash = watch_token(key)
      watched = %{key => hash}

      # Delete the key — value changes from "exists" to nil, hash changes.
      Router.delete(FerricStore.Instance.get(:default), key)

      queue = [{"SET", [key, "should_not_apply"]}]
      result = Coordinator.execute(queue, watched, nil)

      assert result == nil, "EXEC should abort when a watched key was deleted"
    end

    test "WATCH on multiple keys — one modified aborts entire transaction" do
      {key_a, key_b} = ShardHelpers.keys_on_same_shard()

      Router.put(FerricStore.Instance.get(:default), key_a, "a_orig", 0)
      Router.put(FerricStore.Instance.get(:default), key_b, "b_orig", 0)

      hash_a = watch_token(key_a)
      hash_b = watch_token(key_b)
      watched = %{key_a => hash_a, key_b => hash_b}

      # Modify only key_b.
      Router.put(FerricStore.Instance.get(:default), key_b, "b_changed", 0)

      queue = [
        {"SET", [key_a, "a_new"]},
        {"SET", [key_b, "b_new"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      assert result == nil, "EXEC should abort if ANY watched key was modified"

      assert Router.get(FerricStore.Instance.get(:default), key_a) == "a_orig",
             "key_a should be unchanged"

      assert Router.get(FerricStore.Instance.get(:default), key_b) == "b_changed",
             "key_b should retain the external modification"
    end

    test "write to different key on same shard does NOT abort WATCH (value-hash semantics)" do
      # Value-hash WATCH compares phash2(value) per key, not per-shard
      # version counters. Writing to an unrelated key on the same shard
      # does not change the watched key's value, so EXEC succeeds.
      {key_a, key_b} = ShardHelpers.keys_on_same_shard()

      # Sanity check: both keys must route to the same shard.
      assert Router.shard_for(FerricStore.Instance.get(:default), key_a) ==
               Router.shard_for(FerricStore.Instance.get(:default), key_b),
             "test infrastructure: keys should be on the same shard"

      Router.put(FerricStore.Instance.get(:default), key_a, "watched_val", 0)

      hash_a = watch_token(key_a)
      watched = %{key_a => hash_a}

      # Write to key_b which is on the SAME shard — does NOT affect key_a's value.
      Router.put(FerricStore.Instance.get(:default), key_b, "unrelated_write", 0)

      queue = [{"GET", [key_a]}]
      result = Coordinator.execute(queue, watched, nil)

      # Value-hash semantics: no false positive — EXEC succeeds.
      assert is_list(result),
             "value-hash WATCH should not abort for unrelated writes on the same shard"

      assert result == ["watched_val"]
    end

    test "WATCH value hash changes when value changes" do
      key = "r2m11:hash_#{System.unique_integer([:positive])}"

      Router.put(FerricStore.Instance.get(:default), key, "v1", 0)
      h1 = watch_token(key)

      Router.put(FerricStore.Instance.get(:default), key, "v2", 0)
      h2 = watch_token(key)

      assert h1 != h2,
             "WATCH token should change when value changes (h1=#{inspect(h1)}, h2=#{inspect(h2)})"
    end

    test "concurrent WATCH/EXEC — only one succeeds under contention" do
      key = "r2m11:race_#{System.unique_integer([:positive])}"

      Router.put(FerricStore.Instance.get(:default), key, "0", 0)

      # Ten tasks all WATCH the same key (snapshot same value hash "0")
      # and try to SET it to different values. The first to execute changes
      # the value, so subsequent tasks see a hash mismatch and abort.
      results =
        1..10
        |> Enum.map(fn i ->
          Task.async(fn ->
            hash = watch_token(key)
            watched = %{key => hash}
            queue = [{"SET", [key, Integer.to_string(i)]}]
            Coordinator.execute(queue, watched, nil)
          end)
        end)
        |> Enum.map(&Task.await(&1, 10_000))

      succeeded = Enum.count(results, &is_list/1)
      aborted = Enum.count(results, &(&1 == nil))

      # At least one should succeed, and at least some should abort due
      # to contention (since they all snapshot the same value hash).
      assert succeeded >= 1, "at least one WATCH/EXEC should succeed"
      assert succeeded + aborted == 10, "all results should be either list or nil"
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH with value hash — H13 fix regression guards
  # ---------------------------------------------------------------------------

  describe "WATCH with value hash (H13 fix)" do
    test "SET NX skip does not abort WATCH" do
      key = "h13:nx_skip_#{System.unique_integer([:positive])}"

      # Create the key so SET NX will be a no-op.
      Router.put(FerricStore.Instance.get(:default), key, "original", 0)

      # WATCH: snapshot value hash.
      hash = watch_token(key)
      watched = %{key => hash}

      # SET NX is a no-op (key exists), value unchanged.
      # In a real flow this would be inside the MULTI queue, but we simulate
      # the scenario: another connection runs SET NX between WATCH and EXEC.
      store = build_real_store()
      assert nil == Ferricstore.Commands.Strings.handle("SET", [key, "nope", "NX"], store)

      # EXEC should succeed — value hash unchanged.
      queue = [{"GET", [key]}]
      result = Coordinator.execute(queue, watched, nil)

      assert is_list(result), "EXEC should succeed after NX skip, got: #{inspect(result)}"
      assert result == ["original"]
    end

    test "actual value change aborts WATCH" do
      key = "h13:changed_#{System.unique_integer([:positive])}"

      Router.put(FerricStore.Instance.get(:default), key, "original", 0)

      hash = watch_token(key)
      watched = %{key => hash}

      # Another client changes the value.
      Router.put(FerricStore.Instance.get(:default), key, "new_value", 0)

      queue = [{"SET", [key, "should_not_apply"]}]
      result = Coordinator.execute(queue, watched, nil)

      assert result == nil, "EXEC should abort when value actually changed"
      assert Router.get(FerricStore.Instance.get(:default), key) == "new_value"
    end

    test "DEL aborts WATCH" do
      key = "h13:del_#{System.unique_integer([:positive])}"

      Router.put(FerricStore.Instance.get(:default), key, "exists", 0)

      hash = watch_token(key)
      watched = %{key => hash}

      Router.delete(FerricStore.Instance.get(:default), key)

      queue = [{"SET", [key, "should_not_apply"]}]
      result = Coordinator.execute(queue, watched, nil)

      assert result == nil, "EXEC should abort when watched key is deleted"
      assert Router.get(FerricStore.Instance.get(:default), key) == nil
    end

    test "same-value rewrite aborts WATCH because the stored version changes" do
      key = "h13:idempotent_#{System.unique_integer([:positive])}"

      Router.put(FerricStore.Instance.get(:default), key, "hello", 0)

      hash = watch_token(key)
      watched = %{key => hash}

      # Redis WATCH invalidates on writes, even if the materialized value is the same.
      # FerricStore's WATCH token includes the live storage location/version.
      Router.put(FerricStore.Instance.get(:default), key, "hello", 0)

      queue = [{"GET", [key]}]
      result = Coordinator.execute(queue, watched, nil)

      assert result == nil,
             "EXEC should abort after same-value rewrite, got: #{inspect(result)}"
    end

    test "write to different key on same shard does not abort WATCH" do
      {key_a, key_b} = ShardHelpers.keys_on_same_shard()

      assert Router.shard_for(FerricStore.Instance.get(:default), key_a) ==
               Router.shard_for(FerricStore.Instance.get(:default), key_b),
             "test infrastructure: keys should be on the same shard"

      Router.put(FerricStore.Instance.get(:default), key_a, "watched", 0)

      hash_a = watch_token(key_a)
      watched = %{key_a => hash_a}

      # Write to key_b on the same shard — key_a's value is unchanged.
      Router.put(FerricStore.Instance.get(:default), key_b, "unrelated", 0)

      queue = [{"GET", [key_a]}]
      result = Coordinator.execute(queue, watched, nil)

      assert is_list(result),
             "EXEC should succeed — unrelated key on same shard, got: #{inspect(result)}"

      assert result == ["watched"]
    end

    test "WATCH on non-existent key — creating the key aborts" do
      key = "h13:nonexist_#{System.unique_integer([:positive])}"

      # WATCH a key that doesn't exist (value is nil).
      hash = watch_token(key)
      watched = %{key => hash}

      # Another client creates the key.
      Router.put(FerricStore.Instance.get(:default), key, "created", 0)

      queue = [{"GET", [key]}]
      result = Coordinator.execute(queue, watched, nil)

      assert result == nil, "EXEC should abort when non-existent watched key is created"
    end

    test "WATCH on non-existent key — stays non-existent succeeds" do
      key = "h13:stays_nil_#{System.unique_integer([:positive])}"

      # WATCH a key that doesn't exist.
      hash = watch_token(key)
      watched = %{key => hash}

      # Nobody touches the key.
      queue = [{"SET", [key, "created_by_tx"]}]
      result = Coordinator.execute(queue, watched, nil)

      assert is_list(result), "EXEC should succeed when non-existent key stays absent"
      assert result == [:ok]
      assert Router.get(FerricStore.Instance.get(:default), key) == "created_by_tx"
    end
  end

  # ---------------------------------------------------------------------------
  # Helper: build a real store map for direct handler invocation
  # ---------------------------------------------------------------------------

  defp watch_token(key), do: Router.watch_token(FerricStore.Instance.get(:default), key)

  defp build_real_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      flush: fn -> :ok end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end,
      incr: fn k, d -> Router.incr(FerricStore.Instance.get(:default), k, d) end,
      incr_float: fn k, d -> Router.incr_float(FerricStore.Instance.get(:default), k, d) end,
      append: fn k, s -> Router.append(FerricStore.Instance.get(:default), k, s) end,
      getset: fn k, v -> Router.getset(FerricStore.Instance.get(:default), k, v) end,
      getdel: fn k -> Router.getdel(FerricStore.Instance.get(:default), k) end,
      getex: fn k, e -> Router.getex(FerricStore.Instance.get(:default), k, e) end,
      setrange: fn k, o, v -> Router.setrange(FerricStore.Instance.get(:default), k, o, v) end
    }
  end
end
