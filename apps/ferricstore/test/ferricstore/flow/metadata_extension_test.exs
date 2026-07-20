defmodule Ferricstore.Flow.MetadataExtensionTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow.MetadataExtension
  alias FerricStore.Flow.MetadataExtension.Snapshot
  alias Ferricstore.Flow.{StorageScope, SystemMetadata}
  alias Ferricstore.Raft.ApplyContext

  defmodule SharedProvider do
    @behaviour MetadataExtension

    @impl true
    def configure(_opts) do
      {:ok,
       %{
         mode: :shared,
         generation: 7,
         fields: [
           %{
             id: 0x8001,
             version: 1,
             logical_name: "tenant_ref",
             type: :uint64,
             role: :isolation_scope,
             visibility: :hidden,
             mutability: :immutable,
             index: :required_prefix,
             required_in: :shared
           }
         ]
       }}
    end

    @impl true
    def bind_write(_operation, %{"tenant_ref" => tenant_ref}, _snapshot),
      do: {:ok, %{0x8001 => tenant_ref}}

    def bind_write(_operation, _context, _snapshot), do: {:ok, %{}}

    @impl true
    def bind_query(:runs, %{"tenant_ref" => tenant_ref}, _snapshot),
      do: {:ok, {:required, [{0x8001, :eq, tenant_ref}]}}

    def bind_query(_source, _context, _snapshot), do: {:ok, :unscoped}
  end

  defmodule ForgedProvider do
    @behaviour MetadataExtension

    @impl true
    def configure(opts), do: SharedProvider.configure(opts)

    @impl true
    def bind_write(_operation, _context, _snapshot), do: {:ok, %{0x8002 => 42}}

    @impl true
    def bind_query(_source, _context, _snapshot),
      do: {:ok, {:required, [{0x8001, :eq, -1}]}}
  end

  test "dedicated no-op configuration carries no schema or metadata" do
    assert {:ok, %Snapshot{} = snapshot} =
             MetadataExtension.configure(FerricStore.Flow.MetadataExtension.Disabled, [])

    assert snapshot.mode == :dedicated
    assert snapshot.fields == %{}
    assert snapshot.generation == 0
    assert snapshot.schema_digest == <<0::256>>
    assert {:ok, 0} = MetadataExtension.fixed_scope_bytes(snapshot)

    assert ApplyContext.default()
           |> ApplyContext.with_flow_metadata(snapshot)
           |> ApplyContext.valid?()

    assert {:ok, %{}} = MetadataExtension.bind_write(snapshot, :create, %{})
    assert {:ok, :unscoped} = MetadataExtension.bind_query(snapshot, :runs, %{})
  end

  test "shared binding validates and seals required typed scope" do
    assert {:ok, snapshot} = MetadataExtension.configure(SharedProvider, [])

    assert snapshot.mode == :shared
    assert snapshot.generation == 7
    assert {:ok, 8} = MetadataExtension.fixed_scope_bytes(snapshot)

    assert {:ok, %{0x8001 => {1, :uint64, :isolation_scope, 42}} = metadata} =
             MetadataExtension.bind_write(snapshot, :create, %{"tenant_ref" => 42})

    assert {:ok, <<42::unsigned-big-64>>} = SystemMetadata.scope_prefix(metadata)

    assert {:ok, physical_partition} =
             StorageScope.physical_partition_key(%{
               partition_key: "shared-partition",
               system_metadata: metadata
             })

    refute physical_partition == "shared-partition"

    assert {:ok, "shared-partition"} =
             StorageScope.logical_partition_key(%{
               partition_key: physical_partition,
               system_metadata: metadata
             })

    assert {:ok, {:required, [{0x8001, :eq, {:uint64, 42}}]}} =
             MetadataExtension.bind_query(snapshot, :runs, %{"tenant_ref" => 42})

    assert {:error, :flow_scope_required} =
             MetadataExtension.bind_write(snapshot, :create, %{})

    assert {:error, :flow_scope_required} =
             MetadataExtension.bind_query(snapshot, :runs, %{})
  end

  test "dedicated storage scope leaves partition bytes untouched" do
    record = %{partition_key: "dedicated-partition"}

    assert {:ok, "dedicated-partition"} = StorageScope.physical_partition_key(record)
    assert {:ok, "dedicated-partition"} = StorageScope.logical_partition_key(record)
  end

  test "provider output cannot introduce fields or malformed values" do
    assert {:ok, snapshot} = MetadataExtension.configure(ForgedProvider, [])

    assert {:error, :invalid_flow_system_metadata} =
             MetadataExtension.bind_write(snapshot, :create, %{})

    assert {:error, :invalid_flow_system_metadata} =
             MetadataExtension.bind_query(snapshot, :runs, %{})
  end

  test "schema configuration rejects ambiguous and redefined field identities" do
    duplicate = [
      tenant_field(0x8001, "tenant_ref"),
      tenant_field(0x8001, "other_ref")
    ]

    assert {:error, :invalid_flow_metadata_schema} =
             MetadataExtension.validate_configuration(%{
               mode: :shared,
               generation: 1,
               fields: duplicate
             })
  end

  defp tenant_field(id, name) do
    %{
      id: id,
      version: 1,
      logical_name: name,
      type: :uint64,
      role: :isolation_scope,
      visibility: :hidden,
      mutability: :immutable,
      index: :required_prefix,
      required_in: :shared
    }
  end
end
