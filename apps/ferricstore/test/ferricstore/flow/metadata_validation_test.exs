defmodule Ferricstore.Flow.MetadataValidationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Attributes, StateMeta}

  test "attributes identify canonical persisted metadata without rebuilding it" do
    canonical = %{
      "binary" => "value",
      "integer" => -1,
      "float" => 1.5,
      "boolean" => true,
      "null" => nil,
      "list" => ["one", "two"]
    }

    assert Attributes.valid_normalized?(canonical)

    for scalar <- [nil, false, true, -1, 1.5, "", :binary.copy("x", 256)] do
      assert Attributes.valid_scalar?(scalar)
    end

    assert Attributes.valid_list?(["one", "two"])
    refute Attributes.valid_scalar?(["one"])
    refute Attributes.valid_scalar?(:binary.copy("x", 257))
    refute Attributes.valid_list?([])
    refute Attributes.valid_list?(["same", "same"])
    refute Attributes.valid_list?(["one", 2])

    refute Attributes.valid_normalized?(%{"empty" => []})
    refute Attributes.valid_normalized?(%{"duplicate" => ["same", "same"]})
    refute Attributes.valid_normalized?(%{" padded" => "value"})
    refute Attributes.valid_normalized?(%{<<0xFF>> => "value"})
    refute Attributes.valid_normalized?(%{atom_key: "value"})
  end

  test "state metadata identifies canonical scalar-only persisted metadata" do
    canonical = %{
      "queued" => %{
        "binary" => "value",
        "integer" => -1,
        "float" => 1.5,
        "boolean" => false,
        "null" => nil
      }
    }

    assert StateMeta.valid_normalized?(canonical)

    refute StateMeta.valid_normalized?(%{"queued" => %{"list" => ["invalid"]}})
    refute StateMeta.valid_normalized?(%{" queued" => %{"worker" => "one"}})
    refute StateMeta.valid_normalized?(%{"queued" => %{" worker" => "one"}})
    refute StateMeta.valid_normalized?(%{<<0xFF>> => %{"worker" => "one"}})
    refute StateMeta.valid_normalized?(%{"queued" => %{<<0xFF>> => "one"}})
  end

  test "normalization rejects names that cannot be addressed by FQL" do
    assert {:error, _reason} = Attributes.normalize(%{<<0xFF>> => "value"})
    assert {:error, _reason} = StateMeta.normalize(%{<<0xFF>> => %{"worker" => "one"}})
    assert {:error, _reason} = StateMeta.normalize(%{"queued" => %{<<0xFF>> => "one"}})
  end
end
