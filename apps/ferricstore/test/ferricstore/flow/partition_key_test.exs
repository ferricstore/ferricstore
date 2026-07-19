defmodule Ferricstore.Flow.PartitionKeyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.PartitionKey

  test "encodes components with byte lengths" do
    assert PartitionKey.encode_components(["tenant-a", "invoice-1"]) ==
             "fpk:8:tenant-a9:invoice-1"
  end

  test "delimiter bytes cannot make different component lists collide" do
    refute PartitionKey.encode_components(["a:b", "c"]) ==
             PartitionKey.encode_components(["a", "b:c"])
  end

  test "uses byte sizes for arbitrary binary components" do
    assert PartitionKey.encode_components([<<0, 255>>]) == <<"fpk:2:", 0, 255>>
  end
end
