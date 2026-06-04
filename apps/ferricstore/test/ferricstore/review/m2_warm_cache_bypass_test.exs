defmodule Ferricstore.Review.M2WarmCacheBypassTest do
  @moduledoc """
  Regression test: warm_ets_after_cold_read must respect the hot cache max
  value size threshold. A value larger than the threshold must remain nil
  (cold) in ETS after a cold read — not be promoted to hot.

  Bug scenario: after a cold read in Router.get/1, warm_ets_after_cold_read
  caches the full value in ETS without applying the size limit, bloating ETS
  with large binaries that should stay cold.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  # 100 KB — well above the default 65 536 byte hot cache threshold
  @large_size 100_000

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "warm_ets_after_cold_read does not promote oversized values to ETS" do
    key = "m2_warm_bypass_#{:rand.uniform(999_999)}"
    large_value = :crypto.strong_rand_bytes(@large_size)

    # 1. Write a large value (>65 KB).
    :ok = Router.put(FerricStore.Instance.get(:default), key, large_value)

    # 2. Flush to disk.
    ShardHelpers.flush_all_shards()

    idx = Router.shard_for(FerricStore.Instance.get(:default), key)
    keydir = :"keydir_#{idx}"

    [{^key, nil, _exp, _lfu, file_id, _offset, vsize}] = :ets.lookup(keydir, key)
    assert is_integer(file_id) or match?({:waraft_segment, _}, file_id)
    assert vsize == @large_size

    # Legacy Bitcask cold-read direct-pread path required file_id > 0.
    # WARaft segment locations are already readable and must not be rewritten.
    if is_integer(file_id) and file_id <= 0 do
      data_dir = Application.get_env(:ferricstore, :data_dir, "data")
      shard_path = Ferricstore.DataDir.shard_data_path(data_dir, idx)
      src = Path.join(shard_path, "00000.log")
      dst = Path.join(shard_path, "00001.log")
      unless File.exists?(dst), do: File.ln_s!(src, dst)
      :ets.update_element(keydir, key, {5, 1})
    end

    # 3. Evict value from ETS (simulate LFU eviction → cold entry).
    :ets.update_element(keydir, key, {2, nil})

    # 4. Read via Router.get — cold read triggers warm_ets_after_cold_read.
    assert Router.get(FerricStore.Instance.get(:default), key) == large_value

    # 5. ETS value MUST still be nil. If it holds the full binary, the size
    #    limit was bypassed (the bug).
    [{^key, ets_value_after, _, _, _, _, _}] = :ets.lookup(keydir, key)

    assert ets_value_after == nil,
           "warm_ets_after_cold_read promoted a #{@large_size}-byte value to ETS; " <>
             "expected nil for values exceeding the 65 536-byte threshold"
  end
end
