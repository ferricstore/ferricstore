defmodule Ferricstore.AsyncDurabilityRemovedTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Namespace
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

  test "router no longer exposes async-named batch put API" do
    refute function_exported?(Router, :batch_async_put, 2)
    refute function_exported?(FerricStore, :__async_batch_put_result_list__, 2)
    refute function_exported?(Router, :__install_batch_async_entries_for_test__, 4)
  end

  test "active tests do not call removed async batch APIs" do
    repo_root = Path.expand("../../../..", __DIR__)

    offenders =
      [
        Path.join(repo_root, "apps/ferricstore/test/**/*.exs"),
        Path.join(repo_root, "apps/ferricstore_server/test/**/*.exs")
      ]
      |> Enum.flat_map(&Path.wildcard/1)
      |> Enum.reject(&(&1 == __ENV__.file))
      |> Enum.flat_map(fn path ->
        source = File.read!(path)

        for token <- ["batch_async_put", "__async_batch_put_result_list__"],
            String.contains?(source, token),
            active_test_file?(source) do
          {Path.relative_to(path, repo_root), token}
        end
      end)

    assert offenders == []
  end

  test "active tests do not document removed async durability as supported" do
    repo_root = Path.expand("../../../..", __DIR__)

    stale_claims = [
      "both quorum and async",
      "async namespaces",
      "async path just uses"
    ]

    offenders =
      [
        Path.join(repo_root, "apps/ferricstore/test/**/*.exs"),
        Path.join(repo_root, "apps/ferricstore_server/test/**/*.exs")
      ]
      |> Enum.flat_map(&Path.wildcard/1)
      |> Enum.reject(&(&1 == __ENV__.file))
      |> Enum.flat_map(fn path ->
        source = File.read!(path)

        if active_test_file?(source) do
          downcased = String.downcase(source)

          for token <- stale_claims,
              String.contains?(downcased, token) do
            {Path.relative_to(path, repo_root), token}
          end
        else
          []
        end
      end)

    assert offenders == []
  end

  test "tests no longer keep removed RMW coordinator contract alive" do
    repo_root = Path.expand("../../../..", __DIR__)

    offenders =
      Path.join(repo_root, "apps/ferricstore/test/ferricstore/**/*.exs")
      |> Path.wildcard()
      |> Enum.reject(&String.ends_with?(&1, "application_test.exs"))
      |> Enum.reject(&(&1 == __ENV__.file))
      |> Enum.flat_map(fn path ->
        source = File.read!(path)

        if String.contains?(source, "RmwCoordinator") or
             String.contains?(source, "rmw_coordinator") do
          [Path.relative_to(path, repo_root)]
        else
          []
        end
      end)

    assert offenders == []
  end

  test "batch_put API submits through quorum path" do
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

    assert :ok == Router.batch_put(ctx, [{key, "value"}])
    assert_receive {:quorum_submit, %{status: :ok}}, 1_000
    assert "value" == Router.get(FerricStore.Instance.get(:default), key)
  end

  test "production code and runtime config have no durability mode plumbing" do
    repo_root = Path.expand("../../../..", __DIR__)

    offenders =
      [Path.join(repo_root, "apps/ferricstore/lib/**/*.ex"), Path.join(repo_root, "config/*.exs")]
      |> Enum.flat_map(&Path.wildcard/1)
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
              ":ferricstore_has_async_ns",
              "async namespace",
              "ERR async",
              "ERR async key latch"
            ],
            String.contains?(source, token) do
          {Path.relative_to(path, repo_root), token}
        end
      end)

    assert offenders == []
  end

  test "benchmarks do not reference removed async durability mode" do
    app_root = Path.expand("../..", __DIR__)

    offenders =
      Path.join(app_root, "test/ferricstore/bench/**/*.exs")
      |> Path.wildcard()
      |> Enum.flat_map(fn path ->
        source = File.read!(path)

        for token <- [
              ~s("durability", "async"),
              ~s("durability", "quorum"),
              "durability_for(",
              "durability_for check",
              "async durability",
              "Quorum vs Async",
              "Write async"
            ],
            String.contains?(source, token) do
          {Path.relative_to(path, app_root), token}
        end
      end)

    assert offenders == []
  end

  test "user-facing docs and active benchmark scripts do not advertise async durability" do
    repo_root = Path.expand("../../../..", __DIR__)

    paths =
      [Path.join(repo_root, "README.md")] ++
        Path.wildcard(Path.join(repo_root, "docs/**/*.md")) ++
        Path.wildcard(Path.join(repo_root, "bench/**/*.{exs,sh,md,cfg}"))

    offenders =
      paths
      |> Enum.reject(&String.contains?(&1, "/bench/results/"))
      |> Enum.flat_map(fn path ->
        source = File.read!(path)

        for token <- [
              "async durability",
              "durability mode",
              "durability_for(",
              "Quorum vs Async",
              "read_async",
              "write_async",
              "submit_async"
            ],
            String.contains?(source, token) do
          {Path.relative_to(path, repo_root), token}
        end
      end)

    assert offenders == []
  end

  defp active_test_file?(source), do: not String.contains?(source, "@moduletag skip:")
end
