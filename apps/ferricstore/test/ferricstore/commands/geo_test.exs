Code.require_file("geo_test/sections/part_01.exs", __DIR__)
Code.require_file("geo_test/sections/part_02.exs", __DIR__)
defmodule Ferricstore.Commands.GeoTest do
  @moduledoc """
  Unit tests for `Ferricstore.Commands.Geo`.

  Tests use the in-process `MockStore` for isolation and speed.
  All tests are async since they use separate Agent-backed stores.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Geo, SortedSet}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # Helper -- seed a sorted set (zset) with geo members into the mock store
  # ===========================================================================

  # Well-known test locations (matching Redis docs)
  @palermo_lng 13.361389
  @palermo_lat 38.115556
  @catania_lng 15.087269
  @catania_lat 37.502669
  @rome_lng 12.496366
  @rome_lat 41.902782

  use Ferricstore.Commands.GeoTest.Sections.Part01

  use Ferricstore.Commands.GeoTest.Sections.Part02

  defp store_with_geo(key, members) do
    store = MockStore.make()

    args =
      Enum.flat_map(members, fn {lng, lat, name} ->
        [to_string(lng), to_string(lat), name]
      end)

    if args != [] do
      Geo.handle("GEOADD", [key | args], store)
    end

    store
  end

  defp store_with_string(key, value) do
    MockStore.make(%{key => {value, 0}})
  end

  # ===========================================================================
  # Geohash encoding/decoding
  # ===========================================================================



  # ===========================================================================
  # GEOADD
  # ===========================================================================


  defp app_path(path), do: Path.expand("../../../#{path}", __DIR__)

  # ===========================================================================
  # GEOPOS
  # ===========================================================================


  # ===========================================================================
  # GEODIST
  # ===========================================================================


  # ===========================================================================
  # GEOHASH
  # ===========================================================================


  # ===========================================================================
  # GEOSEARCH
  # ===========================================================================


  # ===========================================================================
  # GEOSEARCHSTORE
  # ===========================================================================

end
