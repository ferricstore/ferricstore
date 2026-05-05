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

  test "namespace config no longer accepts any durability field" do
    assert {:error, msg} = NamespaceConfig.set("fast", "durability", "async")
    assert msg =~ "unknown namespace config field"

    assert {:ok, entry} = NamespaceConfig.get("fast")
    refute Map.has_key?(entry, :durability)

    assert {:error, msg} = NamespaceConfig.set("fast", "durability", "quorum")
    assert msg =~ "unknown namespace config field"
  end

  test "config command treats durability as removed field" do
    assert {:error, msg} =
             Namespace.handle(
               "FERRICSTORE.CONFIG",
               ["SET", "fast", "durability", "async"],
               FerricStore.Instance.get(:default)
             )

    assert msg =~ "unknown namespace config field"
    assert {:ok, entry} = NamespaceConfig.get("fast")
    refute Map.has_key?(entry, :durability)
  end

  test "instance context has no durability mode field" do
    ctx = FerricStore.Instance.get(:default)
    refute Map.has_key?(ctx, :durability_mode)
  end

  test "router no longer exposes per-key durability lookup" do
    refute function_exported?(Router, :durability_for_key_public, 2)
  end

  test "debug command cannot set durability mode" do
    assert {:error, msg} = Server.handle("DEBUG", ["SET-DURABILITY", "async"], nil)
    assert msg =~ "removed"

    assert {:error, msg} = Server.handle("DEBUG", ["SET-DURABILITY", "quorum"], nil)
    assert msg =~ "removed"
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

    ctx = FerricStore.Instance.get(:default)
    key = "async_removed_batch:#{System.unique_integer([:positive])}"

    assert :ok == Router.batch_async_put(ctx, [{key, "value"}])
    assert_receive {:quorum_submit, %{status: :ok}}, 1_000
    assert "value" == Router.get(FerricStore.Instance.get(:default), key)
  end

  test "production code has no durability mode plumbing" do
    root = Path.expand("../../lib", __DIR__)

    offenders =
      root
      |> Path.join("**/*.ex")
      |> Path.wildcard()
      |> Enum.flat_map(fn path ->
        source = File.read!(path)

        for token <- [
              "durability_mode",
              "durability_for",
              "default_durability",
              "durability_weakened",
              "namespace_durability",
              ":durability",
              ":ferricstore_durability_mode",
              ":ferricstore_has_async_ns"
            ],
            String.contains?(source, token) do
          {Path.relative_to(path, root), token}
        end
      end)

    assert offenders == []
  end
end
