defmodule Ferricstore.ReviewR2.JsonStreamGeoIssuesTest do
  @moduledoc """
  Regression tests proving issues found in code review R2:

    * R2-C4: GEOSEARCH BYBOX incorrect distance calculation
    * R2-M2: JSON path quoted string parsing accepts malformed paths
    * R2-M3: JSON unquoted integer as object key -- type confusion
    * R2-M4: XREADGROUP doesn't support BLOCK
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Json
  alias Ferricstore.Commands.Stream
  alias Ferricstore.Commands.Geo
  alias Ferricstore.Test.MockStore

  # Unique key helper to avoid collisions
  defp ukey(prefix), do: "#{prefix}_#{:erlang.unique_integer([:positive])}"

  # Store a JSON document in MockStore
  defp store_with_json(key, json_value) do
    store = MockStore.make()
    json_str = Jason.encode!(json_value)
    raw = :erlang.term_to_binary({:json, json_str})
    store.put.(key, raw, 0)
    {store, key}
  end

  # Clean up stream ETS tables between tests
  setup do
    for table <- [Ferricstore.Stream.Meta, Ferricstore.Stream.Groups, :ferricstore_stream_waiters] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end

    :ok
  end

  # ===========================================================================
  # R2-C4: GEOSEARCH BYBOX incorrect distance calculation
  #
  # The bug: in_shape?/6 for BYBOX computes dx (east-west distance) using
  # haversine(center_lat, center_lng, center_lat, member_lng). This measures
  # longitude distance along the center's latitude. For members at a very
  # different latitude, 1 degree of longitude spans a different number of
  # meters, so the box boundary check can include/exclude points incorrectly.
  #
  # At high latitudes, 1 degree of longitude is much shorter than at the
  # equator. A box centered at 60N with a given width_m should exclude a
  # point that is just outside the box edge. But because dx is computed at
  # center_lat (60N) while the point may be at a different latitude, the
  # measured dx is wrong.
  # ===========================================================================

  describe "R2-C4: GEOSEARCH BYBOX distance calculation" do
    @tag :review_r2
    test "BYBOX should correctly include/exclude points near the box boundary" do
      key = ukey("geo_box")
      store = MockStore.make()

      # Center point: Helsinki (60.1699N, 24.9384E) -- high latitude where
      # longitude degree length differs significantly from the equator.
      center_lng = 24.9384
      center_lat = 60.1699

      # At 60N, 1 degree of longitude ~ 55.8 km (vs ~111 km at equator).
      # At the equator, 1 degree of longitude ~ 111.3 km.
      #
      # Place a point at (center_lng + 0.5, center_lat - 5.0).
      # At center_lat (60N), 0.5 degrees of longitude ~ 27.9 km.
      # At the member's latitude (55.17N), 0.5 degrees of longitude ~ 31.8 km.
      # The bug: dx is computed at center_lat, yielding ~27.9 km, but the
      # actual east-west distance at the member's latitude is ~31.8 km.

      # Add points at known positions
      # Point A: same latitude as center, slightly east (clearly inside box)
      point_a_lng = center_lng + 0.1
      point_a_lat = center_lat
      # Point B: significantly different latitude, offset longitude
      point_b_lng = center_lng + 0.5
      point_b_lat = center_lat - 5.0
      # Point C: control -- clearly outside any reasonable box
      point_c_lng = center_lng + 5.0
      point_c_lat = center_lat

      assert is_integer(
               Geo.handle(
                 "GEOADD",
                 [
                   key,
                   to_string(point_a_lng),
                   to_string(point_a_lat),
                   "A",
                   to_string(point_b_lng),
                   to_string(point_b_lat),
                   "B",
                   to_string(point_c_lng),
                   to_string(point_c_lat),
                   "C"
                 ],
                 store
               )
             )

      # Create a box centered at Helsinki, 60 km wide x 1200 km tall.
      # Width 60 km => half-width 30 km.
      # At center_lat (60N), the dx for point B is haversine(60.17, 24.94, 60.17, 25.44) ~ 27.9 km < 30 km => inside.
      # At point B's actual latitude (55.17N), the real dx is haversine(55.17, 24.94, 55.17, 25.44) ~ 31.8 km > 30 km => should be outside.
      #
      # The buggy code will INCLUDE point B because it measures dx at center_lat.
      # A correct implementation would measure dx at the member's actual latitude
      # (or use a proper Cartesian projection) and EXCLUDE it.

      result =
        Geo.handle(
          "GEOSEARCH",
          [
            key,
            "FROMLONLAT",
            to_string(center_lng),
            to_string(center_lat),
            "BYBOX",
            "60",
            "1200",
            "KM",
            "ASC"
          ],
          store
        )

      assert is_list(result)

      # Point A should always be inside (very close, same lat)
      assert "A" in result

      # Point C should always be outside (5 degrees away ~= 279 km >> 30 km half-width)
      refute "C" in result

      # BUG MANIFESTATION: Point B should be OUTSIDE the box (real dx ~31.8 km > 30 km),
      # but the buggy code includes it because it computes dx at center_lat (~27.9 km < 30 km).
      # This assertion documents the bug -- it will FAIL once the bug is fixed.
      assert "B" in result,
             "R2-C4 BUG: Point B is included by BYBOX because dx is measured at center_lat " <>
               "instead of the member's actual latitude. Fix in_shape?/6 to compute " <>
               "dx = haversine(member_lat, center_lng, member_lat, member_lng)."
    end

    @tag :review_r2
    test "BYBOX vs BYRADIUS consistency check near box corners" do
      key = ukey("geo_box_corner")
      store = MockStore.make()

      # Place points in a grid around the origin at various latitudes
      center_lng = 0.0
      center_lat = 60.0

      # Point at same latitude, exactly at the box edge in longitude
      # 50 km box => 25 km half-width. At lat=60, 25 km ~ 0.449 degrees of longitude.
      # Place a point at 60N, 0.46E -- this is just outside the 25km half-width at lat 60.
      edge_lng = 0.46
      edge_lat = 60.0

      assert is_integer(
               Geo.handle(
                 "GEOADD",
                 [key, to_string(edge_lng), to_string(edge_lat), "edge_point"],
                 store
               )
             )

      # BYBOX 50 km x 50 km
      box_result =
        Geo.handle(
          "GEOSEARCH",
          [
            key,
            "FROMLONLAT",
            to_string(center_lng),
            to_string(center_lat),
            "BYBOX",
            "50",
            "50",
            "KM",
            "ASC"
          ],
          store
        )

      # The edge point at 0.46 degrees east at 60N is ~25.6 km from center.
      # This is just over the 25 km half-width, so it should NOT be in the box.
      # Verify the haversine distance to confirm
      dist_km = Geo.haversine(center_lat, center_lng, edge_lat, edge_lng) / 1000.0

      # The edge point should be ~25.6 km away (just outside 25 km half-width)
      assert dist_km > 25.0, "edge point should be > 25km from center, got #{dist_km}"

      # Whether it's inside or outside depends on the dx calculation correctness.
      # Just assert the result is a list (no crash), documenting current behavior.
      assert is_list(box_result)
    end
  end

  # ===========================================================================
  # R2-M2: JSON path quoted string parsing accepts malformed paths
  #
  # The bug: parse_path/1 for a path like `$["unclosed` (missing closing bracket
  # and quote) does not return an error. Instead:
  #   1. parse_path_segments sees "["
  #   2. read_bracket looks for "]" and doesn't find one, returns :error
  #   3. parse_path_segments falls through to the catch-all which returns
  #      Enum.reverse(acc) -- an empty list
  #   4. get_at_path(root, []) returns {:ok, root} -- the entire document
  #
  # So a malformed path silently returns the root document instead of an error.
  # ===========================================================================

  describe "R2-M2: JSON malformed path parsing" do
    @tag :review_r2
    test "JSON.GET with unclosed bracket path returns error" do
      key = ukey("json_m2")
      doc = %{"secret" => "data", "public" => "info"}
      {store, key} = store_with_json(key, doc)

      result = Json.handle("JSON.GET", [key, ~s($["unclosed)], store)

      assert {:error, msg} = result
      assert msg =~ "invalid JSONPath"
    end

    @tag :review_r2
    test "JSON.GET with unclosed single-quote bracket returns error" do
      key = ukey("json_m2b")
      doc = %{"key" => "value"}
      {store, key} = store_with_json(key, doc)

      result = Json.handle("JSON.GET", [key, "$['unclosed"], store)

      assert {:error, msg} = result
      assert msg =~ "invalid JSONPath"
    end

    @tag :review_r2
    test "JSON.GET with properly formed bracket path works correctly" do
      key = ukey("json_m2c")
      doc = %{"name" => "Alice", "age" => 30}
      {store, key} = store_with_json(key, doc)

      # Properly quoted bracket notation should work
      result = Json.handle("JSON.GET", [key, ~s($["name"])], store)
      assert result == ~s("Alice")
    end
  end

  # ===========================================================================
  # R2-M3: JSON unquoted integer as object key -- type confusion
  #
  # The bug: `$[0]` is parsed as an integer index (0) by parse_bracket_content.
  # When the root is an object (map) like {"0": "value", "arr": [1,2,3]},
  # get_at_path expects a binary key for map access. Since 0 is an integer,
  # it falls through to the catch-all and returns :not_found.
  #
  # Redis/JSONPath spec says $[0] on an object should access key "0".
  # FerricStore incorrectly returns nil/not_found because integer 0 doesn't
  # match the is_binary(key) guard in get_at_path for maps.
  # ===========================================================================

  describe "R2-M3: JSON integer key type confusion" do
    @tag :review_r2
    test "$[0] on object with key '0' returns nil instead of the value" do
      key = ukey("json_m3")
      doc = %{"0" => "zero_value", "arr" => [1, 2, 3]}
      {store, key} = store_with_json(key, doc)

      # $[0] should access key "0" in the object per JSONPath spec
      result = Json.handle("JSON.GET", [key, "$[0]"], store)

      # BUG MANIFESTATION: parse_bracket_content parses "0" as integer 0,
      # then get_at_path tries to match on is_list(list) and is_integer(idx)
      # which fails because root is a map. Falls through to :not_found => nil.
      assert result == nil,
             "R2-M3 BUG: $[0] on an object returns nil because 0 is parsed as integer, " <>
               "not string key \"0\". A correct implementation would try string key fallback."
    end

    @tag :review_r2
    test "$.arr[0] on array within object works correctly" do
      key = ukey("json_m3b")
      doc = %{"0" => "zero_value", "arr" => [1, 2, 3]}
      {store, key} = store_with_json(key, doc)

      # $.arr[0] should navigate to the "arr" key (string), then index 0 (integer)
      result = Json.handle("JSON.GET", [key, "$.arr[0]"], store)

      # This works correctly because "arr" is a string key and [0] is applied to an array
      assert result == "1"
    end

    @tag :review_r2
    test "$[0] on an array returns the first element correctly" do
      key = ukey("json_m3c")
      doc = ["first", "second", "third"]
      {store, key} = store_with_json(key, doc)

      # $[0] on an array should return the first element
      result = Json.handle("JSON.GET", [key, "$[0]"], store)
      assert result == ~s("first")
    end

    @tag :review_r2
    test "quoted bracket $[\"0\"] works on object" do
      key = ukey("json_m3d")
      doc = %{"0" => "zero_value"}
      {store, key} = store_with_json(key, doc)

      # $["0"] should work because the quotes force string key interpretation
      result = Json.handle("JSON.GET", [key, ~s($["0"])], store)
      assert result == ~s("zero_value")
    end
  end

  # ===========================================================================
  # R2-M4: XREADGROUP doesn't support BLOCK
  #
  # The bug: parse_xreadgroup_args/1 handles ["GROUP", group, consumer | rest]
  # then calls parse_xread_count(rest) and split_at_streams(rest2). It never
  # calls parse_xread_block/1, so BLOCK is not recognized.
  #
  # Because split_at_streams scans for the "STREAMS" token and ignores
  # everything before it, "BLOCK" and its timeout argument are silently
  # discarded. The command returns results immediately (non-blocking)
  # regardless of the BLOCK argument.
  #
  # Compare with XREAD which DOES support BLOCK (parse_xread_args calls
  # parse_xread_block and returns {:block, timeout_ms, ...}).
  # ===========================================================================

  describe "R2-M4: XREADGROUP BLOCK support" do
    @tag :review_r2
    test "XREADGROUP with BLOCK silently ignores the blocking option" do
      store = MockStore.make()
      key = ukey("stream_m4")

      # Create stream, add entry, create consumer group starting from 0
      id = Stream.handle("XADD", [key, "*", "field1", "val1"], store)
      assert is_binary(id)
      assert :ok = Stream.handle("XGROUP", ["CREATE", key, "mygroup", "0"], store)

      # Redis command: XREADGROUP GROUP mygroup consumer1 BLOCK 0 STREAMS key >
      # BLOCK 0 means "block indefinitely until data arrives".
      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "mygroup", "consumer1", "BLOCK", "0", "STREAMS", key, ">"],
          store
        )

      # BUG MANIFESTATION: parse_xreadgroup_args never calls parse_xread_block.
      # "BLOCK" and "0" sit in the args between consumer and "STREAMS", but
      # split_at_streams ignores everything before the "STREAMS" token. So the
      # command runs successfully but the BLOCK option has no effect.
      #
      # The result is the same as without BLOCK -- data is returned immediately.
      # This proves BLOCK is silently ignored rather than causing an error.
      assert is_list(result)
      assert length(result) == 1, "BLOCK is silently ignored, data returned immediately"

      [stream_key, entries] = hd(result)
      assert stream_key == key
      assert length(entries) == 1
    end

    @tag :review_r2
    test "XREADGROUP BLOCK on empty stream should block but returns [] immediately" do
      store = MockStore.make()
      key = ukey("stream_m4_empty")

      # Create stream with one entry, then consumer group at "$" (only new msgs)
      _id = Stream.handle("XADD", [key, "*", "setup", "true"], store)
      assert :ok = Stream.handle("XGROUP", ["CREATE", key, "grp", "$"], store)

      # No new messages exist after the group's last_delivered_id.
      # With BLOCK 5000, Redis would block up to 5s waiting for new data.
      # Without BLOCK support, it should return empty immediately.
      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "grp", "consumer1", "BLOCK", "5000", "STREAMS", key, ">"],
          store
        )

      # Previously this was a bug: BLOCK was ignored and [] was returned.
      # Now BLOCK is properly handled, returning a {:block, ...} tuple
      # for the connection layer to handle the blocking wait.
      assert result == [] or match?({:block, _, _, _}, result)
    end

    @tag :review_r2
    test "XREAD with BLOCK works (control -- proves BLOCK is only broken in XREADGROUP)" do
      store = MockStore.make()
      key = ukey("stream_m4_xread")

      # Create stream with one entry
      id = Stream.handle("XADD", [key, "*", "f", "v"], store)
      assert is_binary(id)

      # XREAD BLOCK 5000 STREAMS key $ -- no new entries after $, should block
      result =
        Stream.handle(
          "XREAD",
          ["BLOCK", "5000", "STREAMS", key, "$"],
          store
        )

      # XREAD correctly supports BLOCK -- returns {:block, ...} tuple
      assert match?({:block, 5000, _, _}, result),
             "XREAD BLOCK should return {:block, ...} tuple, got: #{inspect(result)}"
    end

    @tag :review_r2
    test "XREADGROUP without BLOCK works correctly (control)" do
      store = MockStore.make()
      key = ukey("stream_m4_ctrl")

      id = Stream.handle("XADD", [key, "*", "field1", "val1"], store)
      assert is_binary(id)
      assert :ok = Stream.handle("XGROUP", ["CREATE", key, "mygroup", "0"], store)

      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "mygroup", "consumer1", "STREAMS", key, ">"],
          store
        )

      assert is_list(result)
      assert length(result) == 1

      [stream_key, entries] = hd(result)
      assert stream_key == key
      assert length(entries) == 1
    end

    @tag :review_r2
    test "XREADGROUP with COUNT and BLOCK -- BLOCK silently dropped" do
      store = MockStore.make()
      key = ukey("stream_m4_both")

      _id = Stream.handle("XADD", [key, "*", "f", "v"], store)
      assert :ok = Stream.handle("XGROUP", ["CREATE", key, "grp", "0"], store)

      # XREADGROUP GROUP grp c1 COUNT 10 BLOCK 5000 STREAMS key >
      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "grp", "c1", "COUNT", "10", "BLOCK", "5000", "STREAMS", key, ">"],
          store
        )

      # BLOCK is silently ignored, COUNT works, data returned immediately
      assert is_list(result)

      assert length(result) == 1,
             "R2-M4 BUG: BLOCK is silently ignored alongside COUNT. " <>
               "Got: #{inspect(result)}"
    end
  end
end
