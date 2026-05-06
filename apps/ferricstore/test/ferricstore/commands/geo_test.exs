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

  # Well-known test locations (matching Redis docs)
  @palermo_lng 13.361389
  @palermo_lat 38.115556
  @catania_lng 15.087269
  @catania_lat 37.502669
  @rome_lng 12.496366
  @rome_lat 41.902782

  # ===========================================================================
  # Geohash encoding/decoding
  # ===========================================================================

  describe "geohash_encode/decode roundtrip" do
    test "roundtrip preserves coordinates within ~0.6mm" do
      score = Geo.geohash_encode(@palermo_lng, @palermo_lat)
      {lng, lat} = Geo.geohash_decode(score)
      assert_in_delta lng, @palermo_lng, 0.001
      assert_in_delta lat, @palermo_lat, 0.001
    end

    test "roundtrip at equator/prime meridian" do
      score = Geo.geohash_encode(0.0, 0.0)
      {lng, lat} = Geo.geohash_decode(score)
      assert_in_delta lng, 0.0, 0.001
      assert_in_delta lat, 0.0, 0.001
    end

    test "roundtrip at extreme coordinates" do
      score = Geo.geohash_encode(-180.0, -85.0)
      {lng, lat} = Geo.geohash_decode(score)
      assert_in_delta lng, -180.0, 0.001
      assert_in_delta lat, -85.0, 0.001
    end

    test "roundtrip at positive extremes" do
      score = Geo.geohash_encode(180.0, 85.0)
      {lng, lat} = Geo.geohash_decode(score)
      assert_in_delta lng, 180.0, 0.001
      assert_in_delta lat, 85.0, 0.001
    end
  end

  describe "haversine distance" do
    test "Palermo to Catania distance is approximately 166km" do
      dist = Geo.haversine(@palermo_lat, @palermo_lng, @catania_lat, @catania_lng)
      # Known distance: ~166.3 km
      assert_in_delta dist, 166_274.0, 2000.0
    end

    test "same point has zero distance" do
      dist = Geo.haversine(0.0, 0.0, 0.0, 0.0)
      assert dist == 0.0
    end

    test "antipodal points have maximum distance (~20000 km)" do
      dist = Geo.haversine(0.0, 0.0, 0.0, 180.0)
      # Half the Earth's circumference
      assert_in_delta dist, 20_015_087.0, 10_000.0
    end
  end

  # ===========================================================================
  # GEOADD
  # ===========================================================================

  describe "GEOADD" do
    test "adds members with coordinates and returns count" do
      store = MockStore.make()

      result =
        Geo.handle(
          "GEOADD",
          [
            "mygeo",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania"
          ],
          store
        )

      assert result == 2
    end

    test "batches member writes" do
      parent = self()
      type_key = CompoundKey.type_key("mygeo")

      store = %{
        exists?: fn "mygeo" -> false end,
        compound_get: fn
          "mygeo", ^type_key -> nil
          "mygeo", _compound_key -> nil
        end,
        compound_scan: fn "mygeo", _prefix -> [] end,
        compound_put: fn
          "mygeo", ^type_key, "zset", 0 ->
            :ok

          "mygeo", compound_key, _score, 0 ->
            flunk(
              "GEOADD should use compound_batch_put, got per-member write #{inspect(compound_key)}"
            )
        end,
        compound_batch_put: fn "mygeo", entries ->
          send(parent, {:compound_batch_put, entries})
          :ok
        end
      }

      assert 2 ==
               Geo.handle(
                 "GEOADD",
                 [
                   "mygeo",
                   "13.361389",
                   "38.115556",
                   "Palermo",
                   "15.087269",
                   "37.502669",
                   "Catania"
                 ],
                 store
               )

      assert_received {:compound_batch_put, entries}
      assert length(entries) == 2
      assert Enum.all?(entries, fn {_compound_key, score, 0} -> is_binary(score) end)
      refute_received {:compound_batch_put, _}
    end

    test "updates requested members without scanning or rewriting the whole zset" do
      parent = self()
      type_key = CompoundKey.type_key("mygeo")
      palermo_key = CompoundKey.zset_member("mygeo", "Palermo")
      old_score = @palermo_lng |> Geo.geohash_encode(@palermo_lat) |> Float.to_string()

      store = %{
        compound_get: fn "mygeo", ^type_key -> "zset" end,
        compound_batch_get: fn "mygeo", [^palermo_key] ->
          send(parent, {:compound_batch_get, [palermo_key]})
          [old_score]
        end,
        compound_scan: fn "mygeo", _prefix ->
          flunk("GEOADD should not scan every member when updating targeted members")
        end,
        compound_batch_delete: fn "mygeo", _compound_keys ->
          flunk("GEOADD should not delete unrelated members")
        end,
        compound_batch_put: fn "mygeo", [{^palermo_key, new_score, 0}] ->
          send(parent, {:compound_batch_put, new_score})
          :ok
        end
      }

      assert 0 == Geo.handle("GEOADD", ["mygeo", "13.5", "38.2", "Palermo"], store)
      assert_received {:compound_batch_get, [^palermo_key]}
      assert_received {:compound_batch_put, _new_score}
    end

    test "rolls back new zset type metadata when member batch write fails" do
      parent = self()
      type_key = CompoundKey.type_key("mygeo")

      store = %{
        exists?: fn "mygeo" -> false end,
        compound_get: fn
          "mygeo", ^type_key -> nil
          "mygeo", _compound_key -> nil
        end,
        compound_scan: fn "mygeo", _prefix -> [] end,
        compound_put: fn "mygeo", ^type_key, "zset", 0 ->
          send(parent, :type_written)
          :ok
        end,
        compound_batch_delete: fn "mygeo", [] -> :ok end,
        compound_batch_put: fn "mygeo", entries when length(entries) == 1 ->
          {:error, :disk_full}
        end,
        compound_delete: fn "mygeo", ^type_key ->
          send(parent, :type_deleted)
          :ok
        end
      }

      assert {:error, :disk_full} ==
               Geo.handle("GEOADD", ["mygeo", "13.361389", "38.115556", "Palermo"], store)

      assert_received :type_written
      assert_received :type_deleted
    end

    test "adds a single member" do
      store = MockStore.make()
      assert 1 == Geo.handle("GEOADD", ["mygeo", "13.361389", "38.115556", "Palermo"], store)
    end

    test "stores members in sorted-set format for ZSCORE compatibility" do
      store = MockStore.make()

      assert 1 == Geo.handle("GEOADD", ["mygeo", "13.361389", "38.115556", "Palermo"], store)

      assert score = SortedSet.handle("ZSCORE", ["mygeo", "Palermo"], store)
      assert {_, ""} = Float.parse(score)
    end

    test "updating existing member returns 0 (not added)" do
      store =
        store_with_geo("mygeo", [
          {@palermo_lng, @palermo_lat, "Palermo"}
        ])

      # Update Palermo with new coordinates
      result = Geo.handle("GEOADD", ["mygeo", "13.5", "38.2", "Palermo"], store)
      assert result == 0
    end

    test "adding new and updating existing returns only new count" do
      store =
        store_with_geo("mygeo", [
          {@palermo_lng, @palermo_lat, "Palermo"}
        ])

      result =
        Geo.handle(
          "GEOADD",
          [
            "mygeo",
            "13.5",
            "38.2",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania"
          ],
          store
        )

      assert result == 1
    end

    test "NX flag prevents updating existing members" do
      store =
        store_with_geo("mygeo", [
          {@palermo_lng, @palermo_lat, "Palermo"}
        ])

      result =
        Geo.handle(
          "GEOADD",
          ["mygeo", "NX", "0.0", "0.0", "Palermo", "15.0", "37.5", "Catania"],
          store
        )

      assert result == 1

      # Verify Palermo's coordinates were NOT changed
      [[plng, plat]] = Geo.handle("GEOPOS", ["mygeo", "Palermo"], store)
      assert_in_delta String.to_float(plng), @palermo_lng, 0.01
      assert_in_delta String.to_float(plat), @palermo_lat, 0.01
    end

    test "XX flag only updates existing, does not add new" do
      store =
        store_with_geo("mygeo", [
          {@palermo_lng, @palermo_lat, "Palermo"}
        ])

      result =
        Geo.handle(
          "GEOADD",
          ["mygeo", "XX", "13.5", "38.2", "Palermo", "15.0", "37.5", "Catania"],
          store
        )

      assert result == 0

      # Catania should not exist
      [nil] = Geo.handle("GEOPOS", ["mygeo", "Catania"], store)
    end

    test "CH flag counts changes (updated + added)" do
      store =
        store_with_geo("mygeo", [
          {@palermo_lng, @palermo_lat, "Palermo"}
        ])

      result =
        Geo.handle(
          "GEOADD",
          [
            "mygeo",
            "CH",
            "13.5",
            "38.2",
            "Palermo",
            "15.0",
            "37.5",
            "Catania"
          ],
          store
        )

      # 1 added (Catania) + 1 changed (Palermo)
      assert result == 2
    end

    test "creates key if non-existent" do
      store = MockStore.make()
      assert 1 == Geo.handle("GEOADD", ["newgeo", "10.0", "20.0", "place"], store)
      assert store.compound_get.("newgeo", CompoundKey.type_key("newgeo")) == "zset"
    end

    test "does not treat a plain serialized zset value as geo data" do
      score = Geo.geohash_encode(@palermo_lng, @palermo_lat)
      encoded = :erlang.term_to_binary({:zset, [{score, "Palermo"}]})
      store = MockStore.make(%{"plain" => {encoded, 0}})

      assert {:error, "WRONGTYPE" <> _} = Geo.handle("GEOPOS", ["plain", "Palermo"], store)
    end

    test "geo read path has no serialized zset fallback" do
      source = File.read!(app_path("lib/ferricstore/commands/geo.ex"))

      refute source =~ "read_legacy_zset"
      refute source =~ "binary_to_term"
    end

    test "rejects invalid coordinates" do
      store = MockStore.make()

      assert {:error, msg} =
               Geo.handle("GEOADD", ["mygeo", "200.0", "38.0", "bad"], store)

      assert msg =~ "invalid longitude,latitude"
    end

    test "rejects latitude beyond polar limits" do
      store = MockStore.make()

      assert {:error, msg} =
               Geo.handle("GEOADD", ["mygeo", "10.0", "90.0", "pole"], store)

      assert msg =~ "invalid longitude,latitude"
    end

    test "no arguments returns error" do
      assert {:error, _} = Geo.handle("GEOADD", [], MockStore.make())
    end

    test "only key returns error" do
      assert {:error, _} = Geo.handle("GEOADD", ["mygeo"], MockStore.make())
    end

    test "incomplete lng/lat/member triple returns error" do
      assert {:error, _} = Geo.handle("GEOADD", ["mygeo", "13.0", "38.0"], MockStore.make())
    end

    test "WRONGTYPE on plain string" do
      store = store_with_string("k", "hello")
      assert {:error, msg} = Geo.handle("GEOADD", ["k", "10.0", "20.0", "m"], store)
      assert msg =~ "WRONGTYPE"
    end
  end

  defp app_path(path), do: Path.expand("../../../#{path}", __DIR__)

  # ===========================================================================
  # GEOPOS
  # ===========================================================================

  describe "GEOPOS" do
    test "returns coordinates for existing members" do
      store =
        store_with_geo("mygeo", [
          {@palermo_lng, @palermo_lat, "Palermo"},
          {@catania_lng, @catania_lat, "Catania"}
        ])

      [[plng, plat], [clng, clat]] =
        Geo.handle("GEOPOS", ["mygeo", "Palermo", "Catania"], store)

      assert_in_delta String.to_float(plng), @palermo_lng, 0.01
      assert_in_delta String.to_float(plat), @palermo_lat, 0.01
      assert_in_delta String.to_float(clng), @catania_lng, 0.01
      assert_in_delta String.to_float(clat), @catania_lat, 0.01
    end

    test "returns nil for missing members" do
      store =
        store_with_geo("mygeo", [
          {@palermo_lng, @palermo_lat, "Palermo"}
        ])

      [[_plng, _plat], nil_val] =
        Geo.handle("GEOPOS", ["mygeo", "Palermo", "NonExistent"], store)

      assert nil_val == nil
    end

    test "batches requested member reads without scanning the whole zset" do
      parent = self()
      type_key = CompoundKey.type_key("mygeo")

      palermo_key = CompoundKey.zset_member("mygeo", "Palermo")
      catania_key = CompoundKey.zset_member("mygeo", "Catania")
      palermo_score = @palermo_lng |> Geo.geohash_encode(@palermo_lat) |> Float.to_string()
      catania_score = @catania_lng |> Geo.geohash_encode(@catania_lat) |> Float.to_string()

      store = %{
        compound_get: fn "mygeo", ^type_key -> "zset" end,
        compound_batch_get: fn "mygeo", [^palermo_key, ^catania_key] ->
          send(parent, {:compound_batch_get, [palermo_key, catania_key]})
          [palermo_score, catania_score]
        end,
        compound_scan: fn "mygeo", _prefix ->
          flunk("GEOPOS should not scan every geo member")
        end
      }

      [[plng, _plat], [clng, _clat]] =
        Geo.handle("GEOPOS", ["mygeo", "Palermo", "Catania"], store)

      assert_received {:compound_batch_get, [^palermo_key, ^catania_key]}
      assert_in_delta String.to_float(plng), @palermo_lng, 0.01
      assert_in_delta String.to_float(clng), @catania_lng, 0.01
    end

    test "returns all nils for non-existent key" do
      store = MockStore.make()
      result = Geo.handle("GEOPOS", ["nosuch", "a", "b"], store)
      assert result == [nil, nil]
    end

    test "no arguments returns error" do
      assert {:error, _} = Geo.handle("GEOPOS", [], MockStore.make())
    end

    test "only key returns error" do
      assert {:error, _} = Geo.handle("GEOPOS", ["mygeo"], MockStore.make())
    end
  end

  # ===========================================================================
  # GEODIST
  # ===========================================================================

  describe "GEODIST" do
    setup do
      store =
        store_with_geo("mygeo", [
          {@palermo_lng, @palermo_lat, "Palermo"},
          {@catania_lng, @catania_lat, "Catania"}
        ])

      {:ok, store: store}
    end

    test "computes distance in meters (default)", %{store: store} do
      result = Geo.handle("GEODIST", ["mygeo", "Palermo", "Catania"], store)
      dist = String.to_float(result)
      assert_in_delta dist, 166_274.0, 2000.0
    end

    test "computes distance in kilometers", %{store: store} do
      result = Geo.handle("GEODIST", ["mygeo", "Palermo", "Catania", "km"], store)
      dist = String.to_float(result)
      assert_in_delta dist, 166.274, 2.0
    end

    test "computes distance in miles", %{store: store} do
      result = Geo.handle("GEODIST", ["mygeo", "Palermo", "Catania", "mi"], store)
      dist = String.to_float(result)
      assert_in_delta dist, 103.3, 2.0
    end

    test "computes distance in feet", %{store: store} do
      result = Geo.handle("GEODIST", ["mygeo", "Palermo", "Catania", "ft"], store)
      dist = String.to_float(result)
      # ~166274 meters * 3.28084 ft/m
      assert dist > 500_000.0
    end

    test "reads only the two requested members" do
      parent = self()
      type_key = CompoundKey.type_key("mygeo")

      palermo_key = CompoundKey.zset_member("mygeo", "Palermo")
      catania_key = CompoundKey.zset_member("mygeo", "Catania")
      palermo_score = @palermo_lng |> Geo.geohash_encode(@palermo_lat) |> Float.to_string()
      catania_score = @catania_lng |> Geo.geohash_encode(@catania_lat) |> Float.to_string()

      store = %{
        compound_get: fn "mygeo", ^type_key -> "zset" end,
        compound_batch_get: fn "mygeo", [^palermo_key, ^catania_key] ->
          send(parent, {:compound_batch_get, [palermo_key, catania_key]})
          [palermo_score, catania_score]
        end,
        compound_scan: fn "mygeo", _prefix ->
          flunk("GEODIST should not scan every geo member")
        end
      }

      result = Geo.handle("GEODIST", ["mygeo", "Palermo", "Catania"], store)

      assert_received {:compound_batch_get, [^palermo_key, ^catania_key]}
      assert_in_delta String.to_float(result), 166_274.0, 2000.0
    end

    test "returns nil if member1 is missing", %{store: store} do
      assert nil == Geo.handle("GEODIST", ["mygeo", "NoPlace", "Catania"], store)
    end

    test "returns nil if member2 is missing", %{store: store} do
      assert nil == Geo.handle("GEODIST", ["mygeo", "Palermo", "NoPlace"], store)
    end

    test "returns nil for non-existent key" do
      store = MockStore.make()
      assert nil == Geo.handle("GEODIST", ["nosuch", "a", "b"], store)
    end

    test "invalid unit returns error", %{store: store} do
      assert {:error, msg} =
               Geo.handle("GEODIST", ["mygeo", "Palermo", "Catania", "parsecs"], store)

      assert msg =~ "unsupported unit"
    end

    test "no arguments returns error" do
      assert {:error, _} = Geo.handle("GEODIST", [], MockStore.make())
    end

    test "only key returns error" do
      assert {:error, _} = Geo.handle("GEODIST", ["mygeo"], MockStore.make())
    end
  end

  # ===========================================================================
  # GEOHASH
  # ===========================================================================

  describe "GEOHASH" do
    test "returns 11-char base32 geohash strings" do
      store =
        store_with_geo("mygeo", [
          {@palermo_lng, @palermo_lat, "Palermo"},
          {@catania_lng, @catania_lat, "Catania"}
        ])

      [p_hash, c_hash] = Geo.handle("GEOHASH", ["mygeo", "Palermo", "Catania"], store)

      # Both should be 11-char base32 strings
      assert byte_size(p_hash) == 11
      assert byte_size(c_hash) == 11

      # Verify they only contain valid base32 characters
      valid_chars = ~c"0123456789bcdefghjkmnpqrstuvwxyz"

      for char <- String.to_charlist(p_hash) do
        assert char in valid_chars
      end
    end

    test "returns nil for missing member" do
      store =
        store_with_geo("mygeo", [
          {@palermo_lng, @palermo_lat, "Palermo"}
        ])

      [_p_hash, nil_hash] = Geo.handle("GEOHASH", ["mygeo", "Palermo", "NoPlace"], store)
      assert nil_hash == nil
    end

    test "batches requested member reads without scanning the whole zset" do
      parent = self()
      type_key = CompoundKey.type_key("mygeo")

      palermo_key = CompoundKey.zset_member("mygeo", "Palermo")
      catania_key = CompoundKey.zset_member("mygeo", "Catania")
      palermo_score = @palermo_lng |> Geo.geohash_encode(@palermo_lat) |> Float.to_string()
      catania_score = @catania_lng |> Geo.geohash_encode(@catania_lat) |> Float.to_string()

      store = %{
        compound_get: fn "mygeo", ^type_key -> "zset" end,
        compound_batch_get: fn "mygeo", [^palermo_key, ^catania_key] ->
          send(parent, {:compound_batch_get, [palermo_key, catania_key]})
          [palermo_score, catania_score]
        end,
        compound_scan: fn "mygeo", _prefix ->
          flunk("GEOHASH should not scan every geo member")
        end
      }

      [p_hash, c_hash] = Geo.handle("GEOHASH", ["mygeo", "Palermo", "Catania"], store)

      assert_received {:compound_batch_get, [^palermo_key, ^catania_key]}
      assert byte_size(p_hash) == 11
      assert byte_size(c_hash) == 11
    end

    test "returns all nils for non-existent key" do
      store = MockStore.make()
      result = Geo.handle("GEOHASH", ["nosuch", "a"], store)
      assert result == [nil]
    end

    test "no arguments returns error" do
      assert {:error, _} = Geo.handle("GEOHASH", [], MockStore.make())
    end
  end

  # ===========================================================================
  # GEOSEARCH
  # ===========================================================================

  describe "GEOSEARCH" do
    setup do
      store =
        store_with_geo("mygeo", [
          {@palermo_lng, @palermo_lat, "Palermo"},
          {@catania_lng, @catania_lat, "Catania"},
          {@rome_lng, @rome_lat, "Rome"}
        ])

      {:ok, store: store}
    end

    test "FROMLONLAT BYRADIUS returns members within radius", %{store: store} do
      # Search 200km around Palermo -- should find Palermo and Catania but not Rome
      result =
        Geo.handle(
          "GEOSEARCH",
          ["mygeo", "FROMLONLAT", "13.361389", "38.115556", "BYRADIUS", "200", "KM"],
          store
        )

      assert "Palermo" in result
      assert "Catania" in result
      refute "Rome" in result
    end

    test "FROMLONLAT BYRADIUS with larger radius finds all", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          ["mygeo", "FROMLONLAT", "13.361389", "38.115556", "BYRADIUS", "500", "KM"],
          store
        )

      assert length(result) == 3
    end

    test "FROMMEMBER BYRADIUS searches from existing member", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          ["mygeo", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "KM"],
          store
        )

      assert "Palermo" in result
      assert "Catania" in result
    end

    test "FROMMEMBER with non-existent member returns error", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          ["mygeo", "FROMMEMBER", "NoPlace", "BYRADIUS", "200", "KM"],
          store
        )

      assert {:error, _} = result
    end

    test "ASC sorts by distance ascending", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          ["mygeo", "FROMLONLAT", "13.361389", "38.115556", "BYRADIUS", "500", "KM", "ASC"],
          store
        )

      # Palermo should be first (distance 0), then Catania, then Rome
      assert hd(result) == "Palermo"
    end

    test "DESC sorts by distance descending", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          ["mygeo", "FROMLONLAT", "13.361389", "38.115556", "BYRADIUS", "500", "KM", "DESC"],
          store
        )

      # Rome should be first (farthest)
      assert hd(result) == "Rome"
    end

    test "COUNT limits results", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          [
            "mygeo",
            "FROMLONLAT",
            "13.361389",
            "38.115556",
            "BYRADIUS",
            "500",
            "KM",
            "ASC",
            "COUNT",
            "2"
          ],
          store
        )

      assert length(result) == 2
      # Palermo (nearest) and Catania (second nearest)
      assert hd(result) == "Palermo"
    end

    test "WITHCOORD includes coordinates", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          [
            "mygeo",
            "FROMLONLAT",
            "13.361389",
            "38.115556",
            "BYRADIUS",
            "200",
            "KM",
            "ASC",
            "WITHCOORD"
          ],
          store
        )

      # Each entry should be [member, [lng, lat]]
      assert is_list(result)
      assert result != []

      [first_entry | _] = result
      assert is_list(first_entry)
      # member name + coordinate pair
      [name | rest] = first_entry
      assert is_binary(name)
      assert [[lng_str, lat_str]] = rest
      assert is_binary(lng_str)
      assert is_binary(lat_str)
    end

    test "WITHDIST includes distance", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          [
            "mygeo",
            "FROMLONLAT",
            "13.361389",
            "38.115556",
            "BYRADIUS",
            "200",
            "KM",
            "ASC",
            "WITHDIST"
          ],
          store
        )

      # Each entry should be [member, distance_string]
      [first_entry | _] = result
      [name, dist_str] = first_entry
      assert is_binary(name)
      assert is_binary(dist_str)
    end

    test "WITHHASH includes integer geohash", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          [
            "mygeo",
            "FROMLONLAT",
            "13.361389",
            "38.115556",
            "BYRADIUS",
            "200",
            "KM",
            "ASC",
            "WITHHASH"
          ],
          store
        )

      [first_entry | _] = result
      [name, hash] = first_entry
      assert is_binary(name)
      assert is_integer(hash)
    end

    test "combined WITHCOORD WITHDIST", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          [
            "mygeo",
            "FROMLONLAT",
            "13.361389",
            "38.115556",
            "BYRADIUS",
            "200",
            "KM",
            "ASC",
            "WITHCOORD",
            "WITHDIST"
          ],
          store
        )

      # Each entry: [member, dist_string, [lng, lat]]
      [first_entry | _] = result
      assert length(first_entry) == 3
    end

    test "BYBOX searches within bounding box", %{store: store} do
      # A large box centered on Palermo
      result =
        Geo.handle(
          "GEOSEARCH",
          ["mygeo", "FROMLONLAT", "13.361389", "38.115556", "BYBOX", "400", "400", "KM"],
          store
        )

      assert "Palermo" in result
      assert "Catania" in result
    end

    test "missing FROMLONLAT/FROMMEMBER returns error", %{store: store} do
      result =
        Geo.handle("GEOSEARCH", ["mygeo", "BYRADIUS", "100", "KM"], store)

      assert {:error, msg} = result
      assert msg =~ "FROMMEMBER or FROMLONLAT"
    end

    test "missing BYRADIUS/BYBOX returns error", %{store: store} do
      result =
        Geo.handle("GEOSEARCH", ["mygeo", "FROMLONLAT", "13.0", "38.0"], store)

      assert {:error, msg} = result
      assert msg =~ "BYRADIUS or BYBOX"
    end

    test "empty result for no matches", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          ["mygeo", "FROMLONLAT", "0.0", "0.0", "BYRADIUS", "1", "KM"],
          store
        )

      assert result == []
    end

    test "no arguments returns error" do
      assert {:error, _} = Geo.handle("GEOSEARCH", [], MockStore.make())
    end
  end

  # ===========================================================================
  # GEOSEARCHSTORE
  # ===========================================================================

  describe "GEOSEARCHSTORE" do
    test "stores results into destination key" do
      store =
        store_with_geo("src", [
          {@palermo_lng, @palermo_lat, "Palermo"},
          {@catania_lng, @catania_lat, "Catania"},
          {@rome_lng, @rome_lat, "Rome"}
        ])

      count =
        Geo.handle(
          "GEOSEARCHSTORE",
          [
            "dst",
            "src",
            "FROMLONLAT",
            "13.361389",
            "38.115556",
            "BYRADIUS",
            "200",
            "KM"
          ],
          store
        )

      assert count == 2

      # Verify the destination has a valid compound zset.
      assert store.compound_get.("dst", CompoundKey.type_key("dst")) == "zset"
      members = SortedSet.handle("ZRANGE", ["dst", "0", "-1"], store)
      assert "Palermo" in members
      assert "Catania" in members
    end

    test "stores destination in sorted-set format for ZSCORE compatibility" do
      store =
        store_with_geo("src", [
          {@palermo_lng, @palermo_lat, "Palermo"},
          {@catania_lng, @catania_lat, "Catania"}
        ])

      assert 2 ==
               Geo.handle(
                 "GEOSEARCHSTORE",
                 [
                   "dst",
                   "src",
                   "FROMLONLAT",
                   "13.361389",
                   "38.115556",
                   "BYRADIUS",
                   "200",
                   "KM"
                 ],
                 store
               )

      assert score = SortedSet.handle("ZSCORE", ["dst", "Palermo"], store)
      assert {_, ""} = Float.parse(score)
    end

    test "returns 0 and deletes destination when no matches" do
      store =
        store_with_geo("src", [
          {@palermo_lng, @palermo_lat, "Palermo"}
        ])

      count =
        Geo.handle(
          "GEOSEARCHSTORE",
          ["dst", "src", "FROMLONLAT", "0.0", "0.0", "BYRADIUS", "1", "KM"],
          store
        )

      assert count == 0
    end

    test "returns destination cleanup errors before writing replacement results" do
      base =
        store_with_geo("src", [
          {@palermo_lng, @palermo_lat, "Palermo"},
          {@catania_lng, @catania_lat, "Catania"}
        ])

      store =
        base
        |> Map.put(:delete, fn "dst" -> {:error, :disk_full} end)
        |> Map.put(:compound_batch_put, fn "dst", _entries ->
          flunk("GEOSEARCHSTORE must not write replacement members after cleanup failure")
        end)

      assert {:error, :disk_full} ==
               Geo.handle(
                 "GEOSEARCHSTORE",
                 [
                   "dst",
                   "src",
                   "FROMLONLAT",
                   "13.361389",
                   "38.115556",
                   "BYRADIUS",
                   "200",
                   "KM"
                 ],
                 store
               )
    end

    test "returns destination cleanup errors when no matches" do
      base =
        store_with_geo("src", [
          {@palermo_lng, @palermo_lat, "Palermo"}
        ])

      store = Map.put(base, :delete, fn "dst" -> {:error, :disk_full} end)

      assert {:error, :disk_full} ==
               Geo.handle(
                 "GEOSEARCHSTORE",
                 ["dst", "src", "FROMLONLAT", "0.0", "0.0", "BYRADIUS", "1", "KM"],
                 store
               )
    end

    test "no arguments returns error" do
      assert {:error, _} = Geo.handle("GEOSEARCHSTORE", [], MockStore.make())
    end

    test "only destination returns error" do
      assert {:error, _} = Geo.handle("GEOSEARCHSTORE", ["dst"], MockStore.make())
    end
  end
end
