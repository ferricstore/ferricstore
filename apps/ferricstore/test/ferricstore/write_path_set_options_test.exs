defmodule Ferricstore.WritePathSetOptionsTest do
  @moduledoc """
  Tests for SET EXAT/PXAT/GET/KEEPTTL options,
  extracted from WritePathOptimizationsTest.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.wait_shards_alive()
    end)
  end

  # Helper: unique key with a given prefix
  defp ukey(prefix), do: "#{prefix}:#{:rand.uniform(9_999_999)}"

  # Helper: builds a store map backed by the real Router
  defp real_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      incr: fn k, d -> Router.incr(FerricStore.Instance.get(:default), k, d) end,
      incr_float: fn k, d -> Router.incr_float(FerricStore.Instance.get(:default), k, d) end,
      append: fn k, s -> Router.append(FerricStore.Instance.get(:default), k, s) end,
      getset: fn k, v -> Router.getset(FerricStore.Instance.get(:default), k, v) end,
      getdel: fn k -> Router.getdel(FerricStore.Instance.get(:default), k) end,
      getex: fn k, e -> Router.getex(FerricStore.Instance.get(:default), k, e) end,
      setrange: fn k, o, v -> Router.setrange(FerricStore.Instance.get(:default), k, o, v) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end
    }
  end

  # =========================================================================
  # SET options: EXAT, PXAT, GET, KEEPTTL
  # =========================================================================

  describe "SET EXAT/PXAT/GET/KEEPTTL" do
    test "quorum SET NX stores value through current write path" do
      store = real_store()
      key = ukey("telemetry_setnx")

      assert :ok = Ferricstore.Commands.Strings.handle("SET", [key, "v", "NX"], store)
      assert Router.get(FerricStore.Instance.get(:default), key) == "v"
    end

    test "SET EXAT with future timestamp sets correct expiry" do
      store = real_store()
      future_ts = div(System.os_time(:millisecond), 1000) + 60

      result =
        Ferricstore.Commands.Strings.handle(
          "SET",
          ["exat:test", "val", "EXAT", "#{future_ts}"],
          store
        )

      assert result == :ok
      assert "val" == store.get.("exat:test")
    end

    test "SET EXAT with past timestamp expires immediately" do
      store = real_store()
      past_ts = div(System.os_time(:millisecond), 1000) - 60

      Ferricstore.Commands.Strings.handle(
        "SET",
        ["exat:past", "val", "EXAT", "#{past_ts}"],
        store
      )

      # Key should be expired immediately
      Process.sleep(10)
      assert nil == store.get.("exat:past")
    end

    test "SET PXAT with future timestamp sets correct expiry" do
      store = real_store()
      future_ms = System.os_time(:millisecond) + 60_000

      result =
        Ferricstore.Commands.Strings.handle(
          "SET",
          ["pxat:test", "val", "PXAT", "#{future_ms}"],
          store
        )

      assert result == :ok
      assert "val" == store.get.("pxat:test")
    end

    test "SET PXAT with past timestamp expires immediately" do
      store = real_store()
      past_ms = System.os_time(:millisecond) - 60_000

      Ferricstore.Commands.Strings.handle(
        "SET",
        ["pxat:past", "val", "PXAT", "#{past_ms}"],
        store
      )

      Process.sleep(10)
      assert nil == store.get.("pxat:past")
    end

    test "SET GET returns old value" do
      store = real_store()
      key = ukey("setget")

      store.put.(key, "old_value", 0)

      result = Ferricstore.Commands.Strings.handle("SET", [key, "new_value", "GET"], store)
      assert result == "old_value"
      assert "new_value" == store.get.(key)
    end

    test "SET GET on non-existent key returns nil" do
      store = real_store()
      key = ukey("setget_nil")

      result = Ferricstore.Commands.Strings.handle("SET", [key, "val", "GET"], store)
      assert result == nil
      assert "val" == store.get.(key)
    end

    test "SET KEEPTTL preserves existing TTL" do
      store = real_store()
      key = ukey("keepttl")

      # Set with a TTL
      expire_at = System.os_time(:millisecond) + 30_000
      store.put.(key, "original", expire_at)

      # Verify TTL is set
      {_val, stored_exp} = store.get_meta.(key)
      assert stored_exp > 0

      # SET with KEEPTTL — should preserve the TTL
      Ferricstore.Commands.Strings.handle("SET", [key, "updated", "KEEPTTL"], store)

      assert "updated" == store.get.(key)
      {_val2, new_exp} = store.get_meta.(key)
      # The TTL should be preserved (approximately the same)
      assert_in_delta new_exp, stored_exp, 1000
    end

    test "SET KEEPTTL on key without TTL keeps no-expiry" do
      store = real_store()
      key = ukey("keepttl_noexp")

      store.put.(key, "original", 0)

      Ferricstore.Commands.Strings.handle("SET", [key, "updated", "KEEPTTL"], store)
      assert "updated" == store.get.(key)

      {_val, exp} = store.get_meta.(key)
      assert exp == 0
    end

    test "SET EXAT + EX returns syntax error (conflicting expiry options)" do
      store = real_store()
      key = ukey("conflict")

      result =
        Ferricstore.Commands.Strings.handle(
          "SET",
          [key, "val", "EX", "10", "EXAT", "9999999"],
          store
        )

      assert {:error, "ERR syntax error"} = result
    end

    test "SET KEEPTTL + EX returns syntax error" do
      store = real_store()
      key = ukey("conflict2")

      result =
        Ferricstore.Commands.Strings.handle("SET", [key, "val", "EX", "10", "KEEPTTL"], store)

      assert {:error, "ERR syntax error"} = result
    end

    test "SET NX only sets if key does not exist" do
      store = real_store()
      key = ukey("setnx")

      result1 = Ferricstore.Commands.Strings.handle("SET", [key, "first", "NX"], store)
      assert result1 == :ok

      result2 = Ferricstore.Commands.Strings.handle("SET", [key, "second", "NX"], store)
      assert result2 == nil

      assert "first" == store.get.(key)
    end

    test "concurrent SET NX serializes the existence check with the write" do
      ctx = FerricStore.Instance.get(:default)
      key = ukey("setnx_race")

      results =
        1..20
        |> Enum.map(fn value ->
          Task.async(fn ->
            Ferricstore.Commands.Strings.handle("SET", [key, "value_#{value}", "NX"], ctx)
          end)
        end)
        |> Enum.map(&Task.await(&1, 10_000))

      assert Enum.count(results, &(&1 == :ok)) == 1
      assert Enum.count(results, &is_nil/1) == 19
      assert Router.get(ctx, key) in Enum.map(1..20, &"value_#{&1}")
    end

    test "concurrent SETNX serializes the existence check with the write" do
      ctx = FerricStore.Instance.get(:default)
      key = ukey("setnx_cmd_race")

      results =
        1..20
        |> Enum.map(fn value ->
          Task.async(fn ->
            Ferricstore.Commands.Strings.handle("SETNX", [key, "value_#{value}"], ctx)
          end)
        end)
        |> Enum.map(&Task.await(&1, 10_000))

      assert Enum.count(results, &(&1 == 1)) == 1
      assert Enum.count(results, &(&1 == 0)) == 19
      assert Router.get(ctx, key) in Enum.map(1..20, &"value_#{&1}")
    end

    test "SET XX only sets if key exists" do
      store = real_store()
      key = ukey("setxx")

      result1 = Ferricstore.Commands.Strings.handle("SET", [key, "first", "XX"], store)
      assert result1 == nil

      store.put.(key, "exists", 0)
      result2 = Ferricstore.Commands.Strings.handle("SET", [key, "updated", "XX"], store)
      assert result2 == :ok
      assert "updated" == store.get.(key)
    end
  end
end
