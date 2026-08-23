defmodule Ferricstore.Commands.InvocationTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.Commands.Invocation
  alias Ferricstore.Test.{MockStore, ShardHelpers}

  setup do
    {:ok, _apps} = Application.ensure_all_started(:ferricstore)
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "does not create a durable Flow when the partition catalog write fails" do
    name = "catalog-failure-#{System.unique_integer([:positive, :monotonic])}"
    partition_key = "invocation:test:catalog-failure"
    idempotency_key = "stable-request"
    store = MockStore.make()

    definition = %{
      "name" => name,
      "enabled" => true,
      "partition" => %{"key" => partition_key}
    }

    assert "OK" =
             Invocation.handle(
               "INVOCATION.DEFINITION.PUT",
               [Jason.encode!(definition)],
               store
             )

    original_batch_put = store.compound_batch_put

    failing_store =
      Map.put(store, :compound_batch_put, fn redis_key, entries ->
        if String.starts_with?(redis_key, "f:{f}:invocation:partitions:1:") do
          {:error, :injected_partition_catalog_failure}
        else
          original_batch_put.(redis_key, entries)
        end
      end)

    envelope = %{"attrs" => %{}, "idempotency_key" => idempotency_key}

    assert {:error, _reason} =
             Invocation.handle(
               "INVOCATION.CREATE",
               [name, Jason.encode!(envelope)],
               failing_store
             )

    invocation_id = invocation_id(name, partition_key, idempotency_key)

    assert {:ok, nil} =
             Ferricstore.Flow.get(FerricStore.Instance.get(:default), invocation_id,
               partition_key: partition_key
             )
  end

  test "does not treat an unrelated Flow with an invocation-shaped id as an invocation" do
    name = "identity-check-#{System.unique_integer([:positive, :monotonic])}"
    partition_key = "invocation:test:identity-check"
    store = MockStore.make()

    assert "OK" =
             Invocation.handle(
               "INVOCATION.DEFINITION.PUT",
               [Jason.encode!(%{"name" => name, "enabled" => true})],
               store
             )

    id = invocation_id(name, partition_key, "unrelated-flow")

    assert :ok =
             Ferricstore.Flow.create(FerricStore.Instance.get(:default), id,
               type: "invocation:#{name}",
               partition_key: partition_key,
               attributes: %{"invocation_name" => "different-definition"}
             )

    assert {:ok, %{} = record} =
             Ferricstore.Flow.get(FerricStore.Instance.get(:default), id,
               partition_key: partition_key
             )

    assert record.type == "invocation:#{name}"

    assert is_nil(Invocation.handle("INVOCATION.GET", [id], store))

    matching_id = invocation_id(name, partition_key, "matching-flow")

    assert :ok =
             Ferricstore.Flow.create(FerricStore.Instance.get(:default), matching_id,
               type: "invocation:#{name}",
               partition_key: partition_key,
               attributes: %{"invocation_name" => name}
             )

    assert %{"id" => ^matching_id} = Invocation.handle("INVOCATION.GET", [matching_id], store)
  end

  test "creates and reads an invocation without namespace context or a partition template" do
    name = "default-oss-#{System.unique_integer([:positive, :monotonic])}"
    store = MockStore.make()

    assert "OK" =
             Invocation.handle(
               "INVOCATION.DEFINITION.PUT",
               [Jason.encode!(%{"name" => name, "enabled" => true})],
               store
             )

    assert %{
             "invocation_id" => invocation_id,
             "name" => ^name,
             "partition_key" => partition_key,
             "state" => "queued"
           } =
             Invocation.handle(
               "INVOCATION.CREATE",
               [name, Jason.encode!(%{"attrs" => %{"payload" => %{"message" => "hello"}}})],
               store
             )

    assert is_binary(partition_key) and partition_key != ""
    assert length(String.split(invocation_id, ".")) == 4

    assert Invocation.handle("INVOCATION.PARTITION.LIST", [name], store) == [partition_key]
    assert Invocation.acl_keys("INVOCATION.GET", [invocation_id]) == ["invocation:#{name}"]
  end

  test "uses the authenticated subject for a single-scope identity" do
    name = "single-scope-#{System.unique_integer([:positive, :monotonic])}"
    subject = "worker"
    partition_key = "invocation:#{name}:#{subject}"

    store =
      MockStore.make()
      |> Map.put(:request_context, %{"subject" => subject, "scopes" => []})

    definition = %{
      "name" => name,
      "enabled" => true,
      "partition" => %{"key" => "invocation:{name}:{subject}"}
    }

    assert "OK" =
             Invocation.handle(
               "INVOCATION.DEFINITION.PUT",
               [Jason.encode!(definition)],
               store
             )

    envelope = %{
      "attrs" => %{},
      "idempotency_key" => "stable-request"
    }

    assert %{
             "invocation_id" => invocation_id,
             "partition_key" => ^partition_key,
             "subject" => ^subject
           } =
             Invocation.handle(
               "INVOCATION.CREATE",
               [name, Jason.encode!(envelope)],
               store
             )

    metadata = Invocation.handle("INVOCATION.GET", [invocation_id], store)
    assert metadata["subject"] == subject
    assert length(String.split(invocation_id, ".")) == 4

    assert Invocation.acl_keys("INVOCATION.CREATE", [name, Jason.encode!(envelope)]) == [
             "invocation:#{name}"
           ]
  end

  defp invocation_id(name, partition_key, idempotency_key) do
    token = digest({name, nil, idempotency_key})

    "inv1.#{encode(name)}.#{token}.#{encode(partition_key)}"
  end

  defp encode(value), do: Base.url_encode64(value, padding: false)

  defp digest(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end
end
