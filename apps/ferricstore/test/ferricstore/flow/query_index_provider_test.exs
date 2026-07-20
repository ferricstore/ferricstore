defmodule FerricStore.Flow.QueryIndexProviderTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.Query.{IndexDefinition, RegisteredIndex, RegistrySnapshot}

  defmodule Provider do
    @behaviour FerricStore.Flow.QueryIndexProvider

    @impl true
    def snapshot(_ctx, shard_index) do
      send(self(), {:snapshot, shard_index})

      definition =
        IndexDefinition.new!(%{
          id: "test_index",
          version: 1,
          fields: [{:partition_key, :asc}, {:updated_at_ms, :desc}]
        })

      {:ok,
       RegistrySnapshot.new!(%{
         epoch: 7,
         catalog_version: 1,
         indexes: [RegisteredIndex.new!(definition, :active)]
       })}
    end
  end

  defmodule RaisingProvider do
    @behaviour FerricStore.Flow.QueryIndexProvider

    @impl true
    def snapshot(_ctx, _shard_index), do: raise("private provider failure")
  end

  defmodule InvalidProvider do
    def snapshot(_ctx, _shard_index), do: {:ok, :invalid}
  end

  defmodule LifecycleProvider do
    @behaviour FerricStore.Flow.QueryIndexProvider

    @impl true
    def snapshot(_ctx, _shard_index) do
      building = definition("building", 1)
      active = definition("active", 1)
      retiring = definition("retiring", 1)

      {:ok,
       RegistrySnapshot.new!(%{
         epoch: 11,
         catalog_version: 2,
         indexes: [
           RegisteredIndex.new!(building, :building),
           RegisteredIndex.new!(active, :active),
           RegisteredIndex.new!(retiring, :retiring)
         ]
       })}
    end

    defp definition(id, version) do
      IndexDefinition.new!(%{
        id: id,
        version: version,
        fields: [{:partition_key, :asc}, {:updated_at_ms, :desc}]
      })
    end
  end

  setup do
    previous = Application.get_env(:ferricstore, FerricStore.Flow.QueryIndexProvider)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, FerricStore.Flow.QueryIndexProvider)
        value -> Application.put_env(:ferricstore, FerricStore.Flow.QueryIndexProvider, value)
      end
    end)

    :ok
  end

  test "validates and freezes a provider in the immutable instance" do
    name = :"query_index_provider_#{System.unique_integer([:positive, :monotonic])}"
    Application.put_env(:ferricstore, FerricStore.Flow.QueryIndexProvider, Provider)

    ctx =
      FerricStore.Instance.build(name,
        shard_count: 1,
        data_dir: Path.join(System.tmp_dir!(), Atom.to_string(name))
      )

    on_exit(fn -> FerricStore.Instance.cleanup(name) end)

    Application.put_env(
      :ferricstore,
      FerricStore.Flow.QueryIndexProvider,
      FerricStore.Flow.QueryIndexProvider.Disabled
    )

    assert ctx.query_index_provider == Provider
    assert {:ok, %RegistrySnapshot{epoch: 7}} =
             FerricStore.Flow.QueryIndexProvider.snapshot(ctx, 0)

    assert_received {:snapshot, 0}
  end

  test "separates projection indexes from query-visible active indexes" do
    ctx = %{query_index_provider: LifecycleProvider}

    assert {:ok, projection} =
             FerricStore.Flow.QueryIndexProvider.projection_definitions(ctx, 0)

    assert Enum.map(projection, & &1.id) == ["building", "active"]

    assert {:ok, [%RegisteredIndex{definition: %IndexDefinition{id: "active"}}]} =
             FerricStore.Flow.QueryIndexProvider.active_indexes(ctx, 0)
  end

  test "normalizes provider failures without leaking their reason" do
    assert {:error, :query_index_provider_failure} =
             FerricStore.Flow.QueryIndexProvider.snapshot(
               %{query_index_provider: RaisingProvider},
               0
             )

    assert {:error, :invalid_query_index_snapshot} =
             FerricStore.Flow.QueryIndexProvider.snapshot(
               %{query_index_provider: InvalidProvider},
               0
             )
  end

  test "rejects a provider that does not implement the contract" do
    assert_raise ArgumentError, ~r/query_index_provider/, fn ->
      FerricStore.Flow.QueryIndexProvider.configured_implementation(query_index_provider: String)
    end
  end
end
