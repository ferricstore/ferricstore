defmodule Ferricstore.AsyncDurabilityRemovedTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Namespace
  alias Ferricstore.Commands.Server
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Store.Router

  setup do
    NamespaceConfig.reset_all()
    on_exit(fn -> NamespaceConfig.reset_all() end)
    :ok
  end

  def forward_quorum_submit(_event, _measurements, metadata, pid) do
    send(pid, {:quorum_submit, metadata})
  end

  test "namespace config rejects async durability" do
    assert {:error, msg} = NamespaceConfig.set("fast", "durability", "async")
    assert msg =~ "durability"
    assert msg =~ "quorum"
    refute msg =~ "or 'async'"

    assert {:ok, entry} = NamespaceConfig.get("fast")
    assert entry.durability == :quorum
    assert NamespaceConfig.durability_for("fast") == :quorum
  end

  test "config command rejects async durability" do
    assert {:error, msg} =
             Namespace.handle(
               "FERRICSTORE.CONFIG",
               ["SET", "fast", "durability", "async"],
               FerricStore.Instance.get(:default)
             )

    assert msg =~ "durability"
    assert {:ok, entry} = NamespaceConfig.get("fast")
    assert entry.durability == :quorum
  end

  test "router treats even stale async durability modes as quorum" do
    ctx = FerricStore.Instance.get(:default)

    for mode <- [:all_quorum, :all_async, :mixed] do
      assert :quorum ==
               Router.durability_for_key_public(%{ctx | durability_mode: mode}, "fast:key")
    end
  end

  test "instance durability mode cannot be switched back to async" do
    assert %{durability_mode: :all_quorum} =
             FerricStore.Instance.update_durability_mode(:default, :all_async)

    assert %{durability_mode: :all_quorum} = FerricStore.Instance.get(:default)

    assert %{durability_mode: :all_quorum} =
             FerricStore.Instance.update_durability_mode(:default, :mixed)

    assert %{durability_mode: :all_quorum} = FerricStore.Instance.get(:default)
  end

  test "debug command cannot switch the default instance to async durability" do
    assert {:error, msg} = Server.handle("DEBUG", ["SET-DURABILITY", "async"], nil)
    assert msg =~ "async durability"

    assert {:simple, "OK durability_mode=all_quorum"} =
             Server.handle("DEBUG", ["SET-DURABILITY", "quorum"], nil)

    assert FerricStore.Instance.get(:default).durability_mode == :all_quorum
  end

  test "legacy batch_async_put API submits through quorum path" do
    id = {__MODULE__, self(), :quorum_submit}
    parent = self()

    :telemetry.attach(
      id,
      [:ferricstore, :batcher, :quorum_submit],
      &__MODULE__.forward_quorum_submit/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(id) end)

    ctx = %{FerricStore.Instance.get(:default) | durability_mode: :all_async}
    key = "async_removed_batch:#{System.unique_integer([:positive])}"

    assert :ok == Router.batch_async_put(ctx, [{key, "value"}])
    assert_receive {:quorum_submit, %{status: :ok}}, 1_000
    assert "value" == Router.get(FerricStore.Instance.get(:default), key)
  end
end
