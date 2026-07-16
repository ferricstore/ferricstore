defmodule Ferricstore.Commands.GeoSearchValidationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Geo
  alias Ferricstore.Test.MockStore

  test "GEOSEARCH validates center coordinates and positive shape dimensions" do
    store = MockStore.make()

    assert {:error, message} =
             Geo.handle(
               "GEOSEARCH",
               ["geo", "FROMLONLAT", "181", "0", "BYRADIUS", "1", "KM"],
               store
             )

    assert message =~ "invalid longitude,latitude"

    for args <- [
          ["geo", "FROMLONLAT", "0", "0", "BYRADIUS", "0", "KM"],
          ["geo", "FROMLONLAT", "0", "0", "BYRADIUS", "-1", "KM"],
          ["geo", "FROMLONLAT", "0", "0", "BYBOX", "0", "1", "KM"],
          ["geo", "FROMLONLAT", "0", "0", "BYBOX", "1", "-1", "KM"]
        ] do
      assert {:error, _message} = Geo.handle("GEOSEARCH", args, store)
    end
  end

  test "GEOSEARCH BYBOX wraps longitude at the antimeridian" do
    store = MockStore.make()
    assert 1 == Geo.handle("GEOADD", ["geo", "-179", "0", "west"], store)

    assert ["west"] ==
             Geo.handle(
               "GEOSEARCH",
               ["geo", "FROMLONLAT", "179", "0", "BYBOX", "500", "100", "KM"],
               store
             )
  end
end
