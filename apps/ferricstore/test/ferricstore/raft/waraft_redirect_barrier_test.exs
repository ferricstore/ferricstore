defmodule Ferricstore.Raft.WARaftRedirectBarrierTest do
  use ExUnit.Case, async: false

  alias Ferricstore.ErrorReasons
  alias Ferricstore.Raft.WARaftBackend

  test "redirected success fails closed when local apply cannot be proven" do
    unavailable_shard = 1_000_000

    assert ErrorReasons.write_timeout_unknown() ==
             WARaftBackend.__barrier_redirected_commit_for_test__(
               node(),
               unavailable_shard,
               :ok
             )
  end
end
