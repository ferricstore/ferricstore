defmodule Ferricstore.Store.RouterCrossShardGuardTest do
  @moduledoc false

  use ExUnit.Case, async: true

  test "router no longer exposes legacy raw op entrypoints" do
    source = Ferricstore.Test.SourceFiles.router_source()

    for helper <- ~w(json_op hll_op bitmap_op geo_op tdigest_op) do
      refute source =~ "def #{helper}("
    end

    refute source =~ "run_bypass_locally"
    refute source =~ "bypass_shard?"
    refute source =~ "forward_bypass_to_leader"
  end

  test "remaining structural cross-shard router calls keep the caller instance context" do
    source = Ferricstore.Test.SourceFiles.router_source()

    assert source =~ ~r/def list_op\(ctx, key, \{:lmove.*CrossShardOp\.execute.*instance: ctx/s
  end
end
