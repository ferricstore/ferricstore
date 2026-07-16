defmodule Ferricstore.Store.PromotionExactCatalogTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Promotion

  test "promotion rejects a missing compound member catalog before touching storage" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-promotion-catalog-#{System.unique_integer([:positive, :monotonic])}"
      )

    shard_path = Path.join(root, "shards/0")
    keydir = :ets.new(:promotion_exact_catalog_keydir, [:set, :public])

    File.mkdir_p!(shard_path)
    File.touch!(Path.join(shard_path, "00000.log"))

    try do
      assert_raise ArgumentError, ~r/compound member catalog is required/, fn ->
        Promotion.promote_collection!(
          :hash,
          "orders",
          shard_path,
          keydir,
          root,
          0,
          nil,
          nil
        )
      end

      assert File.read!(Path.join(shard_path, "00000.log")) == ""
    after
      :ets.delete(keydir)
      File.rm_rf!(root)
    end
  end
end
