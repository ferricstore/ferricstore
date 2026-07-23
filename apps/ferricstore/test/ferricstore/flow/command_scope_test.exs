defmodule Ferricstore.Flow.CommandScopeTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.{CommandScope, StorageScope, SystemMetadata}
  alias Ferricstore.Raft.ApplyContext

  defmodule Provider do
    @behaviour MetadataExtension

    @impl true
    def configure(_opts) do
      {:ok,
       %{
         mode: :shared,
         generation: 3,
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
    def bind_query(_source, %{"tenant_ref" => tenant_ref}, _snapshot),
      do: {:ok, {:required, [{0x8001, :eq, tenant_ref}]}}
  end

  setup do
    assert {:ok, snapshot} = MetadataExtension.configure(Provider, [])

    context =
      ApplyContext.default()
      |> ApplyContext.with_flow_metadata(snapshot)

    assert {:ok, ^context} = context |> ApplyContext.encode() |> ApplyContext.decode()

    assert {:ok, metadata} =
             MetadataExtension.bind_write(snapshot, :create, %{"tenant_ref" => 42})

    logical =
      %{id: "run", partition_key: "partition"}
      |> SystemMetadata.put_record(metadata)

    assert {:ok, physical_partition} = StorageScope.physical_partition_key(logical)

    attrs = Map.put(logical, :partition_key, physical_partition)
    {:ok, context: context, attrs: attrs, metadata: metadata}
  end

  test "shared apply accepts only self-contained records matching the frozen schema", %{
    context: context,
    attrs: attrs,
    metadata: metadata
  } do
    state = %{apply_context: context}
    assert :ok = CommandScope.validate(state, attrs)
    assert :ok = CommandScope.validate(state, %{records: [attrs, attrs]})

    assert {:error, "ERR invalid replicated Flow scope"} =
             CommandScope.validate(state, Map.delete(attrs, :system_metadata))

    assert {:error, "ERR invalid replicated Flow scope"} =
             CommandScope.validate(state, %{attrs | partition_key: "partition"})

    forged = Map.put(metadata, 0x8001, {2, :uint64, :isolation_scope, 42})

    assert {:error, "ERR invalid replicated Flow scope"} =
             CommandScope.validate(state, Map.put(attrs, :system_metadata, forged))
  end

  test "dedicated apply rejects hidden metadata and keeps ordinary commands unchanged", %{
    attrs: attrs
  } do
    state = %{apply_context: ApplyContext.default()}
    assert :ok = CommandScope.validate(state, %{id: "run", partition_key: "partition"})
    assert :ok = CommandScope.validate(state, %{id: "auto-run"})
    assert :ok = CommandScope.validate(state, %{records: [%{id: "auto-run"}]})

    for invalid <- [%{}, %{id: 1}, %{id: ""}] do
      assert {:error, "ERR invalid replicated Flow scope"} =
               CommandScope.validate(state, %{records: [invalid]})
    end

    assert {:error, "ERR invalid replicated Flow scope"} =
             CommandScope.validate(state, attrs)
  end
end
