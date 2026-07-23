defmodule Ferricstore.Flow.Query.IndexCatalogTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.IndexCatalog

  test "loads the bounded launch catalog deterministically" do
    assert {:ok, first} = IndexCatalog.load()
    assert {:ok, second} = IndexCatalog.load()

    assert first.version == 3
    assert first.contract_version == "ferric.flow.query.index-catalog/v1"
    assert first.digest == second.digest
    assert byte_size(first.digest) == 32

    assert Enum.map(first.definitions, & &1.id) == [
             "flow_runs_tenant_updated",
             "flow_runs_tenant_state_updated",
             "flow_runs_tenant_type_updated",
             "flow_runs_tenant_type_state_updated",
             "flow_runs_tenant_type_state_lease_deadline"
           ]

    assert Enum.map(first.definitions, &{&1.id, &1.count_prefixes}) == [
             {"flow_runs_tenant_updated", []},
             {"flow_runs_tenant_state_updated", [1, 2]},
             {"flow_runs_tenant_type_updated", [2]},
             {"flow_runs_tenant_type_state_updated", [3]},
             {"flow_runs_tenant_type_state_lease_deadline", []}
           ]

    assert Enum.all?(first.definitions, fn definition ->
             definition.workloads != [] and
               "WF-SERVICE-API-001" in definition.workloads
           end)
  end

  test "binds the frozen hidden-scope width into every physical definition" do
    assert {:ok, dedicated} = IndexCatalog.load()
    assert {:ok, shared} = IndexCatalog.load(IndexCatalog.default_path(), scope_bytes: 8)

    assert Enum.all?(dedicated.definitions, &(&1.scope_bytes == 0))
    assert Enum.all?(shared.definitions, &(&1.scope_bytes == 8))
    refute dedicated.digest == shared.digest

    dedicated_fingerprints = Enum.map(dedicated.definitions, & &1.fingerprint)
    shared_fingerprints = Enum.map(shared.definitions, & &1.fingerprint)
    refute dedicated_fingerprints == shared_fingerprints
  end

  test "catalog identity is independent of JSON index and workload order" do
    first_path = tmp_path()
    second_path = tmp_path()

    indexes =
      [catalog_index("runs_by_state", "state"), catalog_index("runs_by_type", "type")]
      |> Enum.map(&Map.put(&1, "workloads", ["WF-LIST-002", "WF-LIST-001"]))

    reordered =
      indexes
      |> Enum.reverse()
      |> Enum.map(&Map.update!(&1, "workloads", fn workloads -> Enum.reverse(workloads) end))

    File.write!(first_path, Jason.encode!(catalog(indexes)))
    File.write!(second_path, Jason.encode!(catalog(reordered)))

    on_exit(fn ->
      File.rm(first_path)
      File.rm(second_path)
    end)

    assert {:ok, first} = IndexCatalog.load(first_path)
    assert {:ok, second} = IndexCatalog.load(second_path)

    assert first.digest == second.digest
    assert Enum.map(first.definitions, & &1.id) == ["runs_by_state", "runs_by_type"]
    assert Enum.map(second.definitions, & &1.id) == ["runs_by_type", "runs_by_state"]
  end

  test "rejects malformed fields instead of widening the catalog" do
    path = tmp_path()

    File.write!(
      path,
      Jason.encode!(%{
        "catalog_version" => 1,
        "contract_version" => "ferric.flow.query.index-catalog/v1",
        "indexes" => [
          %{
            "id" => "unsafe",
            "version" => 1,
            "source" => "runs",
            "workloads" => ["WF-LIST-001"],
            "fields" => [
              %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
              %{"name" => "payload", "direction" => "asc", "encoding" => "hashed"}
            ]
          }
        ]
      })
    )

    on_exit(fn -> File.rm(path) end)

    assert {:error, :unsupported_field} = IndexCatalog.load(path)
  end

  test "rejects two physical versions for one logical index" do
    path = tmp_path()

    indexes =
      for version <- [1, 2] do
        %{
          "id" => "runs_by_tenant",
          "version" => version,
          "source" => "runs",
          "workloads" => ["WF-LIST-001"],
          "fields" => [
            %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
            %{"name" => "updated_at_ms", "direction" => "desc", "encoding" => "ordered"}
          ]
        }
      end

    File.write!(
      path,
      Jason.encode!(%{
        "catalog_version" => 2,
        "contract_version" => "ferric.flow.query.index-catalog/v1",
        "indexes" => indexes
      })
    )

    on_exit(fn -> File.rm(path) end)

    assert {:error, :duplicate_query_index_catalog_entry} = IndexCatalog.load(path)
  end

  test "rejects catalog versions outside the replicated unsigned 64-bit contract" do
    path = tmp_path()

    File.write!(
      path,
      Jason.encode!(%{
        "catalog_version" => 0x1_0000_0000_0000_0000,
        "contract_version" => "ferric.flow.query.index-catalog/v1",
        "indexes" => [
          %{
            "id" => "runs_by_tenant",
            "version" => 1,
            "source" => "runs",
            "workloads" => ["WF-LIST-001"],
            "fields" => [
              %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
              %{"name" => "updated_at_ms", "direction" => "desc", "encoding" => "ordered"}
            ]
          }
        ]
      })
    )

    on_exit(fn -> File.rm(path) end)

    assert {:error, :invalid_query_index_catalog} = IndexCatalog.load(path)
  end

  test "rejects counter prefixes that include an ordered range dimension" do
    path = tmp_path()

    File.write!(
      path,
      Jason.encode!(%{
        "catalog_version" => 1,
        "contract_version" => "ferric.flow.query.index-catalog/v1",
        "indexes" => [
          %{
            "id" => "unsafe_counter",
            "version" => 1,
            "source" => "runs",
            "workloads" => ["WF-LIST-001"],
            "count_prefixes" => [2],
            "fields" => [
              %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
              %{"name" => "updated_at_ms", "direction" => "desc", "encoding" => "ordered"}
            ]
          }
        ]
      })
    )

    on_exit(fn -> File.rm(path) end)

    assert {:error, :invalid_index_count_prefixes} = IndexCatalog.load(path)
  end

  defp tmp_path do
    Path.join(
      System.tmp_dir!(),
      "ferricstore_query_index_catalog_#{System.unique_integer([:positive])}.json"
    )
  end

  defp catalog(indexes) do
    %{
      "catalog_version" => 1,
      "contract_version" => "ferric.flow.query.index-catalog/v1",
      "indexes" => indexes
    }
  end

  defp catalog_index(id, field) do
    %{
      "id" => id,
      "version" => 1,
      "source" => "runs",
      "workloads" => ["WF-LIST-001"],
      "fields" => [
        %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
        %{"name" => field, "direction" => "asc", "encoding" => "hashed"},
        %{"name" => "updated_at_ms", "direction" => "desc", "encoding" => "ordered"}
      ]
    }
  end
end
