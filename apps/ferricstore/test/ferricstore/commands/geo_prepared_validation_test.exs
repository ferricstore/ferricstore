defmodule Ferricstore.Commands.GeoPreparedValidationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Geo, SortedSet}
  alias Ferricstore.Test.MockStore

  test "prepared GEOADD rejects conflicting flags and invalid coordinates" do
    store = MockStore.make()

    assert {:error, "ERR XX and NX options at the same time are not compatible"} =
             Geo.handle_ast({:geoadd, "geo", [:nx, :xx], [{0.0, 0.0, "member"}]}, store)

    assert {:error, message} =
             Geo.handle_ast({:geoadd, "geo", [], [{181.0, 0.0, "member"}]}, store)

    assert message =~ "invalid longitude,latitude"
    assert 0 == SortedSet.handle("ZCARD", ["geo"], store)
  end

  test "prepared GEOSEARCH rejects malformed geometry before reading storage" do
    store = MockStore.make()

    assert {:error, _message} =
             Geo.handle_ast(
               {:geosearch, "geo", center: {:lonlat, 181.0, 0.0}, shape: {:radius, 1.0}},
               store
             )

    assert {:error, _message} =
             Geo.handle_ast(
               {:geosearch, "geo", center: {:lonlat, 0.0, 0.0}, shape: {:box, -1.0, 1.0}},
               store
             )
  end

  test "prepared GEODIST normalizes units and rejects unsupported units" do
    store = MockStore.make()

    assert 2 ==
             Geo.handle(
               "GEOADD",
               ["geo", "0", "0", "a", "0.01", "0", "b"],
               store
             )

    assert is_binary(Geo.handle_ast({:geodist, "geo", "a", "b", "km"}, store))

    assert {:error, "ERR unsupported unit provided. please use M, KM, FT, MI"} =
             Geo.handle_ast({:geodist, "geo", "a", "b", "bogus"}, store)
  end
end
