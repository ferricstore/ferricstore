defmodule Ferricstore.Store.ListOpQuorumTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp ctx, do: FerricStore.Instance.get(:default)
  defp ukey(base), do: "list_quorum:#{base}_#{:erlang.unique_integer([:positive])}"

  test "LPUSH treats an unswept expired plain value as missing on quorum path" do
    key = ukey("expired_plain_to_list")
    expired_at = Ferricstore.HLC.now_ms() - 1_000

    assert :ok = Router.put(ctx(), key, "old-string", expired_at)

    assert 1 == Router.list_op(ctx(), key, {:lpush, ["fresh"]})
    assert ["fresh"] == Router.list_op(ctx(), key, {:lrange, 0, -1})
  end
end
