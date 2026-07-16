defmodule Ferricstore.ReviewR2.M5BloomMetaPersistTest do
  @moduledoc """
  Verifies that bloom metadata persists correctly via BF.INFO.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Bloom
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp real_store_for_key(key) do
    ctx = FerricStore.Instance.get(:default)
    idx = Ferricstore.Store.Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)

    %{
      get: fn k -> Ferricstore.Store.Router.get(ctx, k) end,
      get_meta: fn k -> Ferricstore.Store.Router.get_meta(ctx, k) end,
      put: fn k, v, exp -> Ferricstore.Store.Router.put(ctx, k, v, exp) end,
      delete: fn k -> Ferricstore.Store.Router.delete(ctx, k) end,
      exists?: fn k -> Ferricstore.Store.Router.exists?(ctx, k) end,
      keys: fn -> Ferricstore.Store.Router.keys(ctx) end,
      flush: fn -> :ok end,
      dbsize: fn -> Ferricstore.Store.Router.dbsize(ctx) end,
      prob_dir: fn -> Path.join(shard_path, "prob") end,
      prob_write: fn cmd -> Ferricstore.Store.Router.prob_write(ctx, cmd) end
    }
  end

  describe "bloom metadata" do
    test "BF.RESERVE creates bloom file" do
      key = "bloom_meta_test_#{:rand.uniform(999_999)}"
      store = real_store_for_key(key)

      result = Bloom.handle("BF.RESERVE", [key, "0.01", "1000"], store)
      assert result == :ok

      prob_dir = store.prob_dir.()
      path = Ferricstore.ProbFile.path(prob_dir, key, "bloom")
      assert File.exists?(path)
    end

    test "BF.INFO returns reasonable values after reserve" do
      key = "bloom_info_test_#{:rand.uniform(999_999)}"
      store = real_store_for_key(key)

      Bloom.handle("BF.RESERVE", [key, "0.005", "2000"], store)

      info = Bloom.handle("BF.INFO", [key], store)
      assert is_list(info)

      capacity = find_info_value(info, "Capacity")
      error_rate = find_info_value(info, "Error rate")

      assert is_integer(capacity) and capacity > 0
      assert is_number(error_rate) and error_rate > 0 and error_rate < 1
    end
  end

  defp find_info_value(info, label) when is_list(info) do
    info
    |> Enum.chunk_every(2)
    |> Enum.find_value(fn
      [^label, value] -> value
      _ -> nil
    end)
  end
end
