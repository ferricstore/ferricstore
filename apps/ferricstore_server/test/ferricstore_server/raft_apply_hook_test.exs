defmodule FerricstoreServer.RaftApplyHookTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  setup do
    {:ok, _apps} = Application.ensure_all_started(:ferricstore)
    original_ctx = FerricStore.Instance.get(:default)

    on_exit(fn ->
      :persistent_term.put({FerricStore.Instance, :default}, original_ctx)
    end)

    :ok
  end

  test "installing the server hook preserves a previously installed hook" do
    FerricStore.Instance.inject_callbacks(:default,
      raft_apply_hook: fn
        {:enterprise, value} -> {:ok, {:enterprise, value}}
        _command -> {:error, :previous_unknown}
      end
    )

    assert :ok = FerricstoreServer.RaftApplyHook.install_instance(:default)

    hook = FerricStore.Instance.get(:default).raft_apply_hook

    assert hook.({:enterprise, "ok"}) == {:ok, {:enterprise, "ok"}}
    assert hook.(:unknown) == {:error, :previous_unknown}
  end
end
