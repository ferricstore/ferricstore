defmodule Ferricstore.StatsResetSamplingTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Stats

  setup do
    Stats.reset()
    on_exit(&Stats.reset/0)
    :ok
  end

  test "reset discards pre-reset sampled hit and miss remainders" do
    ctx = %{FerricStore.Instance.get(:default) | read_sample_rate: 10}

    assert 0 = Stats.sample_keyspace_hits(ctx, 9)
    assert :ok = Stats.sample_keyspace_misses(ctx, 9)
    assert 0 = Stats.keyspace_hits(ctx)
    assert 0 = Stats.keyspace_misses(ctx)

    assert :ok = Stats.reset()

    assert 0 = Stats.sample_keyspace_hits(ctx, 1)
    assert :ok = Stats.sample_keyspace_misses(ctx, 1)
    assert 0 = Stats.keyspace_hits(ctx)
    assert 0 = Stats.keyspace_misses(ctx)
  end
end
