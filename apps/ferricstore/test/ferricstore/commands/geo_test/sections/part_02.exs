defmodule Ferricstore.Commands.GeoTest.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Geo, SortedSet}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Test.MockStore

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

    test "options are case-insensitive", %{store: store} do
      result =
        Geo.handle(
          "GEOSEARCH",
          [
            "mygeo",
            "fromlonlat",
            "13.361389",
            "38.115556",
            "byradius",
            "500",
            "km",
            "asc",
            "count",
            "1",
            "any",
            "withdist"
          ],
          store
        )

      assert [[member, _distance]] = result
      assert member in ["Palermo", "Catania", "Rome"]
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
        |> Map.put(:compound_batch_put, fn "dst", entries ->
          if Enum.any?(entries, fn {compound_key, _value, _expire_at_ms} ->
               compound_key == CompoundKey.zset_member("dst", "Palermo")
             end) do
            flunk("GEOSEARCHSTORE must not write replacement members after cleanup failure")
          else
            base.compound_batch_put.("dst", entries)
          end
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

    test "preserves existing destination when cleanup fails after metadata delete" do
      base =
        store_with_geo("src", [
          {@palermo_lng, @palermo_lat, "Palermo"},
          {@catania_lng, @catania_lat, "Catania"}
        ])

      assert 1 == Geo.handle("GEOADD", ["dst", "12.0", "42.0", "Old"], base)
      type_key = CompoundKey.type_key("dst")

      store =
        base
        |> Map.put(:compound_delete, fn
          "dst", ^type_key -> base.compound_delete.("dst", type_key)
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)
        |> Map.put(:compound_delete_prefix, fn "dst", prefix ->
          if prefix == CompoundKey.zset_prefix("dst") do
            {:error, :disk_full}
          else
            base.compound_delete_prefix.("dst", prefix)
          end
        end)
        |> Map.put(:compound_batch_put, fn "dst", entries ->
          if Enum.any?(entries, fn {compound_key, _value, _expire_at_ms} ->
               compound_key == CompoundKey.zset_member("dst", "Palermo")
             end) do
            flunk("GEOSEARCHSTORE must not write replacement members after cleanup failure")
          else
            base.compound_batch_put.("dst", entries)
          end
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

      assert "zset" == base.compound_get.("dst", type_key)
      assert ["Old"] == SortedSet.handle("ZRANGE", ["dst", "0", "-1"], base)
    end

    test "preserves existing destination when replacement member write fails" do
      base =
        store_with_geo("src", [
          {@palermo_lng, @palermo_lat, "Palermo"},
          {@catania_lng, @catania_lat, "Catania"}
        ])

      assert 1 == Geo.handle("GEOADD", ["dst", "12.0", "42.0", "Old"], base)

      store =
        Map.put(base, :compound_batch_put, fn
          "dst", entries ->
            if Enum.any?(entries, fn {compound_key, _value, _expire_at_ms} ->
                 compound_key == CompoundKey.zset_member("dst", "Palermo")
               end) do
              {:error, :disk_full}
            else
              base.compound_batch_put.("dst", entries)
            end

          key, entries ->
            base.compound_batch_put.(key, entries)
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

      assert ["Old"] == SortedSet.handle("ZRANGE", ["dst", "0", "-1"], base)
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
  end
end
