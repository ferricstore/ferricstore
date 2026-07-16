defmodule Ferricstore.CrossShardOp.IntentResolverTest do
  use ExUnit.Case, async: true

  alias Ferricstore.CrossShardOp.IntentResolver

  setup do
    intent = %{
      command: :rename,
      keys: %{source: "source"},
      value_hashes: %{"source" => :token},
      status: :executing,
      created_at: 1_000
    }

    %{intent: intent, owner_ref: make_ref()}
  end

  test "retains a stale intent when owner-fenced unlock fails", context do
    unlock = fn _keys, _owner_ref -> {:error, :raft_unavailable} end
    delete = fn _owner_ref -> flunk("intent must remain retryable") end

    assert {:error, :raft_unavailable} =
             IntentResolver.__resolve_stale_intent_for_test__(
               context.owner_ref,
               context.intent,
               20_000,
               unlock,
               delete
             )
  end

  test "deletes a stale intent only after every unlock succeeds", context do
    parent = self()

    unlock = fn keys, owner_ref ->
      send(parent, {:unlock, keys, owner_ref})
      :ok
    end

    delete = fn owner_ref ->
      send(parent, {:delete, owner_ref})
      :ok
    end

    assert :ok =
             IntentResolver.__resolve_stale_intent_for_test__(
               context.owner_ref,
               context.intent,
               20_000,
               unlock,
               delete
             )

    assert_receive {:unlock, ["source"], owner_ref}
    assert owner_ref == context.owner_ref
    assert_receive {:delete, ^owner_ref}
  end
end
