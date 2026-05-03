defmodule Ferricstore.PublicBatchReadGuardTest do
  @moduledoc false
  use ExUnit.Case, async: true

  @api_path Path.expand("../../lib/ferricstore.ex", __DIR__)
  @impl_path Path.expand("../../lib/ferricstore/impl.ex", __DIR__)

  test "public plain and compound multi-read APIs use router batch paths" do
    api_source = File.read!(@api_path)
    impl_source = File.read!(@impl_path)

    # Public MGET is often used for cold large values. It must hit the
    # Router.batch_get/2 path so cold reads are submitted as one batch instead
    # of one Router.get/2 and one possible waiter per key.
    assert impl_source =~ "Router.batch_get(ctx, keys)"
    assert api_source =~ "Router.batch_get(ctx, keys)"
    assert api_source =~ "Router.compound_batch_get(ctx, redis_key, compound_keys)"
    assert api_source =~ "Router.compound_batch_get_meta(ctx, redis_key, compound_keys)"
    assert impl_source =~ "Router.compound_batch_get(ctx, redis_key, compound_keys)"
    assert impl_source =~ "Router.compound_batch_get_meta(ctx, redis_key, compound_keys)"

    refute impl_source =~ "Enum.map(keys, &Router.get(ctx, &1))"
    refute api_source =~ "Enum.map(keys, fn key ->\n        Router.get(ctx, key)"
  end
end
