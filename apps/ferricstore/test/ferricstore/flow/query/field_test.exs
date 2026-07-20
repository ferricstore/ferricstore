defmodule Ferricstore.Flow.Query.FieldTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.Query.Field

  test "normalizes built-in and bounded metadata field names without creating atoms" do
    before_atoms = :erlang.system_info(:atom_count)

    assert {:ok, :partition_key} = Field.parse("PARTITION_KEY")
    assert {:ok, :updated_at_ms} = Field.parse("updated_at_ms")
    assert {:ok, {:attribute, "region"}} = Field.parse("attribute.region")
    assert {:ok, {:attribute, "Region"}} = Field.parse("ATTRIBUTE.Region")

    assert {:ok, {:state_meta, "running", "worker_pool"}} =
             Field.parse("state_meta.running.worker_pool")

    assert {:ok, {:state_meta, "Running", "workerPool"}} =
             Field.parse("STATE_META.Running.workerPool")

    for id <- 1..100 do
      name = "field_#{id}"
      assert {:ok, {:attribute, ^name}} = Field.parse("attribute.#{name}")
    end

    assert :erlang.system_info(:atom_count) == before_atoms
  end

  test "rejects ambiguous, reserved, or oversized metadata paths" do
    assert {:error, :unsupported_field} = Field.parse("attribute")
    assert {:error, :unsupported_field} = Field.parse("attribute.__private")
    assert {:error, :unsupported_field} = Field.parse("state_meta.running")
    assert {:error, :unsupported_field} = Field.parse("attribute." <> String.duplicate("x", 65))
    assert {:error, :unsupported_field} = Field.parse("unknown")
  end

  test "distinguishes explicit null from a missing record field" do
    record = %{
      id: "run-1",
      partition_key: nil,
      attributes: %{"region" => "eu"},
      state_meta: %{"running" => %{"worker_pool" => "cpu"}}
    }

    assert {:ok, "run-1"} = Field.fetch(record, :run_id)
    assert {:ok, nil} = Field.fetch(record, :partition_key)
    assert {:ok, "eu"} = Field.fetch(record, {:attribute, "region"})
    assert :missing = Field.fetch(record, {:attribute, "absent"})
    assert {:ok, "cpu"} = Field.fetch(record, {:state_meta, "running", "worker_pool"})
    assert :missing = Field.fetch(record, {:state_meta, "failed", "worker_pool"})
  end

  test "reads string-keyed decoded records without atomizing keys" do
    assert {:ok, "run-2"} = Field.fetch(%{"id" => "run-2"}, :run_id)
    assert {:ok, 17} = Field.fetch(%{"updated_at_ms" => 17}, :updated_at_ms)
  end
end
