defmodule Ferricstore.Review.M3PromotedScanColdTest do
  @moduledoc """
  Verifies that HGETALL on promoted keys returns all fields even when
  some entries have been evicted from ETS (value=nil, needs cold read).

  The bug: prefix_scan_entries was called with shard_data_path (shared
  shard dir) for promoted keys. Cold reads built the wrong file path,
  reading from shard_0/00000.log instead of the dedicated dir. Evicted
  entries silently disappeared from scan results.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @test_threshold 5

  setup do
    original = Application.get_env(:ferricstore, :promotion_threshold)

    original_pt =
      try do
        :persistent_term.get(:ferricstore_promotion_threshold)
      rescue
        ArgumentError -> :not_set
      end

    Application.put_env(:ferricstore, :promotion_threshold, @test_threshold)
    :persistent_term.put(:ferricstore_promotion_threshold, @test_threshold)

    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      if original do
        Application.put_env(:ferricstore, :promotion_threshold, original)
      else
        Application.delete_env(:ferricstore, :promotion_threshold)
      end

      case original_pt do
        :not_set -> :persistent_term.erase(:ferricstore_promotion_threshold)
        val -> :persistent_term.put(:ferricstore_promotion_threshold, val)
      end

      ShardHelpers.flush_all_keys()
    end)
  end

  defp real_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      flush: fn -> :ok end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end,
      compound_get: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end
    }
  end

  defp ukey(base), do: "m3_#{base}_#{:rand.uniform(9_999_999)}"

  describe "HGETALL on promoted hash with evicted fields" do
    test "all fields returned even after evicting some from ETS" do
      store = real_store()
      key = ukey("scan_cold")
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{shard_idx}"

      # Create promoted hash
      pairs = Enum.flat_map(1..(@test_threshold + 1), fn i -> ["f_#{i}", "v_#{i}"] end)
      Hash.handle("HSET", [key | pairs], store)

      # Verify all fields present before eviction
      all_before = Hash.handle("HGETALL", [key], store)
      assert length(all_before) == (@test_threshold + 1) * 2

      # Simulate eviction: set value=nil for some compound keys in ETS.
      # This mimics what MemoryGuard.maybe_evict does (line 370):
      #   :ets.update_element(keydir, key, {2, nil})
      for i <- 1..3 do
        compound_key = "H:#{key}\0f_#{i}"
        :ets.update_element(keydir, compound_key, {2, nil})
      end

      # Verify the evicted fields have nil value in ETS
      [{_, val, _, _, _, _, _}] = :ets.lookup(keydir, "H:#{key}\0f_1")
      assert val == nil, "expected nil after eviction"

      # HGETALL should still return all fields — cold reads from dedicated file
      all_after = Hash.handle("HGETALL", [key], store)
      field_count = div(length(all_after), 2)

      assert field_count == @test_threshold + 1,
             "Expected #{@test_threshold + 1} fields, got #{field_count}. " <>
               "Evicted fields were lost from scan results."

      # Verify specific evicted fields are present
      pairs_map = all_after |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end)
      assert pairs_map["f_1"] == "v_1"
      assert pairs_map["f_2"] == "v_2"
      assert pairs_map["f_3"] == "v_3"
    end

    test "HLEN correct after eviction" do
      store = real_store()
      key = ukey("hlen_cold")
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{shard_idx}"

      pairs = Enum.flat_map(1..(@test_threshold + 1), fn i -> ["f_#{i}", "v_#{i}"] end)
      Hash.handle("HSET", [key | pairs], store)

      # Evict half the fields
      for i <- 1..3 do
        :ets.update_element(keydir, "H:#{key}\0f_#{i}", {2, nil})
      end

      count = Hash.handle("HLEN", [key], store)
      assert count == @test_threshold + 1
    end
  end
end
