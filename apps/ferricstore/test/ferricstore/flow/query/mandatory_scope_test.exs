defmodule Ferricstore.Flow.Query.MandatoryScopeTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.Query.MandatoryScope
  alias Ferricstore.Flow.SystemMetadata

  defmodule SharedProvider do
    @behaviour MetadataExtension

    @impl true
    def configure(_opts) do
      {:ok,
       %{
         mode: :shared,
         generation: 9,
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

    @impl true
    def bind_query(:runs, %{"tenant_ref" => tenant_ref}, _snapshot),
      do: {:ok, {:required, [{0x8001, :eq, tenant_ref}]}}

    def bind_query(_source, _context, _snapshot), do: {:error, :flow_scope_required}
  end

  test "dedicated scope adds no storage bytes" do
    assert {:ok, snapshot} =
             MetadataExtension.configure(FerricStore.Flow.MetadataExtension.Disabled, [])

    assert {:ok, scope} =
             MandatoryScope.bind(%{flow_metadata_snapshot: snapshot}, :runs)

    assert scope.mode == :dedicated
    assert scope.generation == 0
    assert :ok = MandatoryScope.validate(scope)
    assert :ok = MandatoryScope.validate_against(scope, snapshot)
    assert MandatoryScope.branch_count(scope) == 1
    assert {:ok, "logical"} = MandatoryScope.physical_partition_key(scope, "logical")
    assert {:ok, "logical"} = MandatoryScope.admission_key(scope, "logical")
    assert {:ok, "logical"} = MandatoryScope.statistics_key(scope, "logical")
    assert {:ok, binding} = MandatoryScope.query_binding(scope, "logical")
    assert byte_size(binding) == 32
    assert :ok = MandatoryScope.verify_record(scope, %{partition_key: "logical"})
  end

  test "shared scope is hidden, canonical, and tenant-partitions physical storage" do
    assert {:ok, snapshot} = MetadataExtension.configure(SharedProvider, [])

    assert {:ok, scope_a} =
             MandatoryScope.bind(
               %{
                 flow_metadata_snapshot: snapshot,
                 request_context: %{"tenant_ref" => 11}
               },
               :runs
             )

    assert {:ok, scope_a_again} =
             MandatoryScope.bind(
               %{
                 flow_metadata_snapshot: snapshot,
                 request_context: %{"tenant_ref" => 11}
               },
               :runs
             )

    assert {:ok, scope_b} =
             MandatoryScope.bind(
               %{
                 flow_metadata_snapshot: snapshot,
                 request_context: %{"tenant_ref" => 22}
               },
               :runs
             )

    assert scope_a.digest == scope_a_again.digest
    refute scope_a.digest == scope_b.digest
    assert :ok = MandatoryScope.validate(scope_a)
    assert :ok = MandatoryScope.validate(scope_b)
    assert :ok = MandatoryScope.validate_against(scope_a, snapshot)
    assert :ok = MandatoryScope.validate_against(scope_b, snapshot)
    refute inspect(scope_a) =~ "tenant_ref"
    refute inspect(scope_a) =~ "branches:"
    refute inspect(scope_a) =~ "prefixes:"

    assert {:ok, partition_a} = MandatoryScope.physical_partition_key(scope_a, "logical")
    assert {:ok, partition_b} = MandatoryScope.physical_partition_key(scope_b, "logical")
    refute partition_a == partition_b

    assert {:ok, admission_a} = MandatoryScope.admission_key(scope_a, "logical")
    assert {:ok, admission_b} = MandatoryScope.admission_key(scope_b, "logical")
    assert admission_a == scope_a.digest
    refute admission_a == admission_b

    assert {:ok, statistics_a} = MandatoryScope.statistics_key(scope_a, "logical")
    assert statistics_a == scope_a.digest

    assert {:ok, binding_a} = MandatoryScope.query_binding(scope_a, "logical")
    assert {:ok, binding_b} = MandatoryScope.query_binding(scope_b, "logical")
    refute binding_a == binding_b

    assert {:ok, derived} = MandatoryScope.derive_keys(scope_a, "logical")
    assert derived.physical_partition_key == partition_a
    assert derived.admission_key == admission_a
    assert derived.statistics_key == statistics_a
    assert derived.query_binding == binding_a

    assert {:ok, other_partition_binding} =
             MandatoryScope.query_binding(scope_a, "other-logical")

    refute binding_a == other_partition_binding

    assert {:ok, metadata_a} = MandatoryScope.single_metadata(scope_a)

    record_a =
      %{partition_key: partition_a}
      |> SystemMetadata.put_record(metadata_a)

    assert :ok = MandatoryScope.verify_record(scope_a, record_a)
    assert {:error, :flow_scope_mismatch} = MandatoryScope.verify_record(scope_b, record_a)
  end

  test "missing shared authority fails closed" do
    assert {:ok, snapshot} = MetadataExtension.configure(SharedProvider, [])

    assert {:error, :flow_scope_required} =
             MandatoryScope.bind(%{flow_metadata_snapshot: snapshot}, :runs)
  end

  test "forged scope values fail closed" do
    scope = dedicated_scope()

    assert {:error, :invalid_flow_mandatory_scope} =
             MandatoryScope.validate(%{scope | digest: :binary.copy(<<1>>, 32)})

    assert {:error, :invalid_flow_mandatory_scope} =
             MandatoryScope.validate(%{scope | prefixes: [<<1>>]})

    assert {:error, :invalid_flow_mandatory_scope} =
             MandatoryScope.validate(%{
               scope
               | branches: [%{0x8001 => {1, :uint64, :isolation_scope, 1}}]
             })
  end

  test "a structurally valid scope is rejected under a different frozen schema" do
    assert {:ok, snapshot} = MetadataExtension.configure(SharedProvider, [])

    assert {:ok, scope} =
             MandatoryScope.bind(
               %{
                 flow_metadata_snapshot: snapshot,
                 request_context: %{"tenant_ref" => 11}
               },
               :runs
             )

    incompatible = %{snapshot | generation: snapshot.generation + 1}

    assert :ok = MandatoryScope.validate(scope)

    assert {:error, :invalid_flow_mandatory_scope} =
             MandatoryScope.validate_against(scope, incompatible)
  end

  defp dedicated_scope do
    MandatoryScope.dedicated()
  end
end
