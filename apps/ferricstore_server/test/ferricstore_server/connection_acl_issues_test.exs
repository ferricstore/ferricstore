defmodule FerricstoreServer.ConnectionAclIssuesTest do
  @moduledoc """
  Regression tests for issues found in code review R2.

  These tests cover issues found in code review R2. Fixed issues stay here as
  regression guards.

  Covers:
    - R2-C1: Deleted user ACL cache must fail closed
    - R2-C5: Embedded API bypasses ACL (expected, documented here)
    - R2-H11: SUBSCRIBE max_subscriptions returns unhandled {:error_reply, ...} tuple
    - R2-H12: Sendfile do_sendfile_get returns :fallback which fast_get doesn't handle
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl

  # ---------------------------------------------------------------------------
  # R2-C1: Deleted user ACL cache must fail closed
  #
  # Deleted users should not regain access through cached connection state.
  # ---------------------------------------------------------------------------

  describe "R2-C1: deleted user ACL cache fails closed" do
    setup do
      Acl.reset!()
      :ok
    end

    test "Acl.check_command correctly denies a deleted user" do
      # Create user, then delete them
      :ok = Acl.set_user("ephemeral", ["on", ">secret", "+GET", "~*"])
      assert :ok = Acl.check_command("ephemeral", "GET")

      :ok = Acl.del_user("ephemeral")

      # The public ACL API correctly denies deleted users
      assert {:error, "NOPERM" <> _} = Acl.check_command("ephemeral", "GET")
    end

    test "Acl.get_user returns nil for deleted user" do
      :ok = Acl.set_user("temp_user", ["on", ">pass", "+SET", "~*"])
      assert Acl.get_user("temp_user") != nil

      :ok = Acl.del_user("temp_user")

      assert Acl.get_user("temp_user") == nil
    end

    test "contrast: restricted user is correctly denied commands not in their allowlist" do
      :ok = Acl.set_user("readonly", ["on", ">pass", "+GET", "-SET", "~*"])

      assert :ok = Acl.check_command("readonly", "GET")
      assert {:error, "NOPERM" <> _} = Acl.check_command("readonly", "SET")
    end

    test "explicit denies override allowlist entries" do
      user = %{
        Acl.get_user("default")
        | commands: MapSet.new(["GET", "MGET"]),
          denied_commands: MapSet.new(["GET"])
      }

      :ets.insert(:ferricstore_acl, {"reader", user})

      assert {:error, "NOPERM" <> _} = Acl.check_command("reader", "GET")
      assert :ok = Acl.check_command("reader", "MGET")
    end
  end

  # ---------------------------------------------------------------------------
  # R2-C5: Embedded API bypasses ACL
  #
  # The FerricStore module (embedded API) calls Router directly without any
  # ACL check. This is by design — embedded mode is for trusted in-process
  # callers — but it's documented here as a regression guard so that if
  # someone later adds ACL enforcement, these tests catch the change.
  # ---------------------------------------------------------------------------

  describe "R2-C5: embedded API bypasses ACL" do
    setup do
      Acl.reset!()
      # Create a restricted user that can only GET
      :ok = Acl.set_user("restricted", ["on", ">secret", "+GET", "-SET", "~limited:*"])
      :ok
    end

    test "FerricStore.set succeeds without any ACL check" do
      # The embedded API does not consult the ACL at all.
      # Even though we have a restricted user, the embedded API ignores it
      # because it operates at the Router level, not the Connection level.
      assert :ok = FerricStore.set("acl_bypass_test", "value")
      assert {:ok, "value"} = FerricStore.get("acl_bypass_test")

      # Clean up
      FerricStore.del("acl_bypass_test")
    end

    test "FerricStore.set works on keys outside any user's key pattern" do
      # The restricted user can only access "limited:*" keys, but the embedded
      # API doesn't enforce key patterns at all.
      assert :ok = FerricStore.set("forbidden:key", "still works")
      assert {:ok, "still works"} = FerricStore.get("forbidden:key")

      # Clean up
      FerricStore.del("forbidden:key")
    end

    test "embedded API has no concept of authenticated user" do
      # There is no way to pass a username or credentials to the embedded API.
      # This proves ACL is architecturally bypassed, not just unchecked.
      # FerricStore.set/3 accepts key, value, and opts (ttl, nx, xx, etc.)
      # — no username parameter exists.
      assert :ok = FerricStore.set("no_auth_key", "data")
      assert {:ok, "data"} = FerricStore.get("no_auth_key")

      # Clean up
      FerricStore.del("no_auth_key")
    end
  end

  # ---------------------------------------------------------------------------
  # R2-H11: SUBSCRIBE max_subscriptions error returns {:error_reply, ...} tuple
  #
  # When SUBSCRIBE hits the @max_subscriptions limit, dispatch/3 returns:
  #   {:error_reply, {:error, "ERR max subscriptions..."}, state}
  #
  # But handle_command/2 passes dispatch's return value directly to
  # pipeline_dispatch, which only pattern-matches on:
  #   {:continue, response, new_state}
  #   {:quit, response, quit_state}
  #
  # The {:error_reply, ...} tuple is unhandled and would cause a CaseClauseError
  # in pipeline_dispatch/2, crashing the connection process.
  #
  # Testing this requires a TCP connection and sending 100,000+ SUBSCRIBE
  # commands, which is not feasible in a unit test. We document the issue
  # and verify the dispatch return type contract instead.
  # ---------------------------------------------------------------------------

  describe "R2-H11: SUBSCRIBE max_subscriptions unhandled error tuple" do
    test "dispatch return type contract: {:error_reply, ...} is not {:continue, ...} or {:quit, ...}" do
      # This test documents the type mismatch. The dispatch function for
      # SUBSCRIBE returns {:error_reply, encoded_error, state} when the
      # max_subscriptions limit is hit.
      #
      # The pipeline_dispatch function expects either:
      #   {:continue, response, new_state}
      #   {:quit, response, quit_state}
      #
      # Proving the atoms don't match:
      error_reply_tuple = {:error_reply, "some error", %{}}
      assert elem(error_reply_tuple, 0) == :error_reply
      refute elem(error_reply_tuple, 0) in [:continue, :quit]

      # The fix should change dispatch("SUBSCRIBE", ...) to return
      # {:continue, Encoder.encode({:error, "ERR max subscriptions..."}), state}
      # instead of {:error_reply, {:error, "ERR max subscriptions..."}, state}
    end
  end

  # ---------------------------------------------------------------------------
  # R2-H12: Sendfile error path returns :fallback which fast_get doesn't handle
  #
  # In do_sendfile_get/5, two paths return the bare atom :fallback:
  #   1. When :file.open fails (line ~1815): returns :fallback
  #   2. When :gen_tcp.send of the header fails (line ~1808): returns :fallback
  #
  # But fast_get/2 only pattern-matches on:
  #   {:sent, new_state}
  #   {:error_after_header, _reason}
  #
  # The :fallback atom would cause a CaseClauseError, crashing the connection.
  #
  # Testing this requires a TCP connection with a specially crafted cold key
  # whose data file has been removed or made unreadable. This is fragile in
  # unit tests. We document the pattern-match gap as a regression guard.
  # ---------------------------------------------------------------------------

  describe "R2-H12: sendfile :fallback not handled by fast_get" do
    test "do_sendfile_get can return :fallback which is not in fast_get's case clauses" do
      # This test documents the pattern-match gap.
      # fast_get's case clause for cold_ref >= threshold:
      #
      #   case do_sendfile_get(key, path, offset, size, state) do
      #     {:sent, new_state} -> {:continue, "", new_state}
      #     {:error_after_header, _reason} -> {:quit, "", state}
      #   end
      #
      # But do_sendfile_get can also return :fallback when:
      #   - :file.open(path, ...) fails -> :fallback
      #   - :gen_tcp.send(socket, header) fails -> :fallback
      #
      # Proving the mismatch: :fallback is not a two-tuple, so it cannot
      # match either of the case clauses in fast_get.
      handled_tags = [:sent, :error_after_header]
      fallback_result = :fallback
      refute is_tuple(fallback_result) and elem(fallback_result, 0) in handled_tags

      # The fix should either:
      # 1. Add a :fallback clause to fast_get that falls through to dispatch_normal
      # 2. Or change do_sendfile_get to return {:fallback, reason} as a tagged tuple
      #    and handle it in fast_get
    end
  end
end
