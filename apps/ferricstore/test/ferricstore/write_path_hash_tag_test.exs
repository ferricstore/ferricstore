defmodule Ferricstore.WritePathHashTagTest do
  @moduledoc """
  Tests for hash tag routing, extracted from WritePathOptimizationsTest.
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

  # =========================================================================
  # Hash tag routing
  # =========================================================================

  describe "hash tags" do
    test "{user:42}:session and {user:42}:profile hash to same shard" do
      assert Router.shard_for(FerricStore.Instance.get(:default), "{user:42}:session") == Router.shard_for(FerricStore.Instance.get(:default), "{user:42}:profile")
    end

    test "no tag hashes on full key" do
      # Keys without braces hash on the full key
      assert Router.extract_hash_tag("nobraces") == nil
    end

    test "empty tag {} hashes on full key" do
      assert Router.extract_hash_tag("{}empty") == nil
    end

    test "unclosed { hashes on full key" do
      assert Router.extract_hash_tag("{unclosed") == nil
    end

    test "nested {{tag}} extracts {tag" do
      # First { at pos 0, first } after that is at pos 5, so tag = "{tag"
      assert Router.extract_hash_tag("{{tag}}") == "{tag"
    end

    test "{a}:x and {b}:x may route to different shards" do
      shard_a = Router.shard_for(FerricStore.Instance.get(:default), "{a}:x")
      shard_b = Router.shard_for(FerricStore.Instance.get(:default), "{b}:x")
      # They hash on different tags, so they *may* differ
      # We can't guarantee they differ with only 4 shards, but we can verify
      # they hash on the tag, not the full key
      assert Router.extract_hash_tag("{a}:x") == "a"
      assert Router.extract_hash_tag("{b}:x") == "b"
      # At least confirm the function works
      assert shard_a in 0..3
      assert shard_b in 0..3
    end

    test "key with { in middle works correctly" do
      assert Router.extract_hash_tag("prefix{tag}suffix") == "tag"
    end

    test "multiple } only uses first one after {" do
      assert Router.extract_hash_tag("{ab}cd}ef") == "ab"
    end

    test "hash tag routing through real writes" do
      k1 = "{ht42}:session_#{:rand.uniform(999_999)}"
      k2 = "{ht42}:profile_#{:rand.uniform(999_999)}"

      Router.put(FerricStore.Instance.get(:default), k1, "sess", 0)
      Router.put(FerricStore.Instance.get(:default), k2, "prof", 0)

      assert Router.shard_for(FerricStore.Instance.get(:default), k1) == Router.shard_for(FerricStore.Instance.get(:default), k2)
      assert "sess" == Router.get(FerricStore.Instance.get(:default), k1)
      assert "prof" == Router.get(FerricStore.Instance.get(:default), k2)
    end
  end
end
