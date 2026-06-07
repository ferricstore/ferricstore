defmodule Ferricstore.Commands.Geo do
  @moduledoc """
  Handles Redis geo commands: GEOADD, GEOPOS, GEODIST, GEOHASH, GEOSEARCH,
  GEOSEARCHSTORE.

  Geo is implemented on top of Sorted Set. Members are stored as compound zset
  entries with geohash-encoded float64 scores -- the same encoding Redis uses,
  making scores wire-compatible. No new data structure is needed.

  ## Geohash encoding

  Coordinates are encoded as 52-bit interleaved geohashes stored as float64
  scores. 26 bits for longitude (-180..180) and 26 bits for latitude (-90..90)
  gives ~0.6mm precision, matching Redis.

  ## Supported commands

    * `GEOADD key [NX|XX] [CH] longitude latitude member [lng lat member ...]`
    * `GEOPOS key member [member ...]`
    * `GEODIST key member1 member2 [M|KM|FT|MI]`
    * `GEOHASH key member [member ...]`
    * `GEOSEARCH key FROMLONLAT lng lat|FROMMEMBER member BYRADIUS radius unit|BYBOX width height unit [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]`
    * `GEOSEARCHSTORE destination source [same GEOSEARCH options]`
  """

  alias Ferricstore.Commands.Geo.Parsing
  alias Ferricstore.Store.{CompoundKey, Ops, TypeRegistry}

  # Earth radius in meters (WGS-84 mean radius)
  @earth_radius_m 6_371_000.0

  # Geohash precision: 52 bits (26 per axis)
  @geohash_bits 52
  @lat_bits div(@geohash_bits, 2)
  @lng_bits div(@geohash_bits, 2)

  # Base32 alphabet for GEOHASH string encoding (standard geohash, not z-base-32)
  @base32_alphabet ~c"0123456789bcdefghjkmnpqrstuvwxyz"

  # Unit conversion factors (to meters)
  @unit_conversions %{
    "M" => 1.0,
    "KM" => 1000.0,
    "FT" => 0.3048,
    "MI" => 1609.344
  }

  @wrongtype_msg "WRONGTYPE Operation against a key holding the wrong kind of value"

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Handles typed Geo command AST terms produced by the Rust RESP parser.
  """
  @spec handle_ast(term(), map()) :: term()
  def handle_ast({tag, {:error, msg}}, _store) when is_atom(tag), do: {:error, msg}

  def handle_ast({:geoadd, key, flags, pairs}, store) do
    do_geoadd(store, key, pairs, flags)
  end

  def handle_ast({:geopos, args}, store), do: geopos_args(args, store)
  def handle_ast({:geohash, args}, store), do: geohash_args(args, store)

  def handle_ast({:geodist, _key, _member1, _member2, {:error, msg}}, _store),
    do: {:error, msg}

  def handle_ast({:geodist, key, member1, member2, unit}, store) do
    with {:ok, [score1, score2]} <- read_zset_member_scores(store, key, [member1, member2]) do
      case {score1, score2} do
        {nil, _} ->
          nil

        {_, nil} ->
          nil

        {score1, score2} when is_float(score1) and is_float(score2) ->
          {lng1, lat1} = geohash_decode(score1)
          {lng2, lat2} = geohash_decode(score2)
          dist_m = haversine(lat1, lng1, lat2, lng2)
          dist = dist_m / @unit_conversions[unit]
          format_distance(dist)
      end
    end
  end

  def handle_ast({:geosearch, _key, {:error, msg}}, _store), do: {:error, msg}

  def handle_ast({:geosearch, key, opts}, store) do
    opts = Map.new(opts)

    with {:ok, center_lng, center_lat} <- resolve_center(opts, store, key),
         {:ok, zset} <- read_zset(store, key) do
      do_geosearch(zset, center_lng, center_lat, opts)
    end
  end

  def handle_ast({:geosearchstore, _destination, _source, {:error, msg}}, _store),
    do: {:error, msg}

  def handle_ast({:geosearchstore, destination, source, opts}, store) do
    opts = Map.new(opts)

    with {:ok, center_lng, center_lat} <- resolve_center(opts, store, source),
         {:ok, zset} <- read_zset(store, source) do
      matches = find_matching_members(zset, center_lng, center_lat, opts)
      sorted = sort_matches(matches, opts)
      limited = apply_count(sorted, opts)

      store_geosearch_results(store, destination, limited)
    end
  end

  @doc """
  Handles a geo command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"GEOADD"`, `"GEODIST"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put`, `delete`, `exists?` callbacks

  ## Returns

  Plain Elixir term: integer, string, list, nil, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # GEOADD key [NX|XX] [CH] longitude latitude member [lng lat member ...]
  # ---------------------------------------------------------------------------

  def handle("GEOADD", [key | rest], store) when rest != [] do
    with {:ok, flags, coord_args} <- Parsing.parse_geoadd_flags(rest),
         {:ok, pairs} <- Parsing.parse_lng_lat_members(coord_args) do
      do_geoadd(store, key, pairs, flags)
    end
  end

  def handle("GEOADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'geoadd' command"}
  end

  # ---------------------------------------------------------------------------
  # GEOPOS key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("GEOPOS", args, store), do: geopos_args(args, store)

  # ---------------------------------------------------------------------------
  # GEODIST key member1 member2 [M|KM|FT|MI]
  # ---------------------------------------------------------------------------

  def handle("GEODIST", [key, member1, member2 | rest], store) do
    unit =
      case rest do
        [] -> "M"
        [u] -> String.upcase(u)
        _ -> nil
      end

    if unit == nil or unit not in Map.keys(@unit_conversions) do
      {:error, "ERR unsupported unit provided. please use M, KM, FT, MI"}
    else
      with {:ok, [score1, score2]} <- read_zset_member_scores(store, key, [member1, member2]) do
        case {score1, score2} do
          {nil, _} ->
            nil

          {_, nil} ->
            nil

          {score1, score2} when is_float(score1) and is_float(score2) ->
            {lng1, lat1} = geohash_decode(score1)
            {lng2, lat2} = geohash_decode(score2)
            dist_m = haversine(lat1, lng1, lat2, lng2)
            dist = dist_m / @unit_conversions[unit]
            format_distance(dist)
        end
      end
    end
  end

  def handle("GEODIST", _args, _store) do
    {:error, "ERR wrong number of arguments for 'geodist' command"}
  end

  # ---------------------------------------------------------------------------
  # GEOHASH key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("GEOHASH", args, store), do: geohash_args(args, store)

  # ---------------------------------------------------------------------------
  # GEOSEARCH key FROMLONLAT lng lat|FROMMEMBER member
  #   BYRADIUS radius M|KM|FT|MI|BYBOX width height M|KM|FT|MI
  #   [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
  # ---------------------------------------------------------------------------

  def handle("GEOSEARCH", [key | rest], store) do
    with {:ok, opts} <- Parsing.parse_geosearch_opts(rest),
         {:ok, center_lng, center_lat} <- resolve_center(opts, store, key),
         {:ok, zset} <- read_zset(store, key) do
      do_geosearch(zset, center_lng, center_lat, opts)
    end
  end

  def handle("GEOSEARCH", _args, _store) do
    {:error, "ERR wrong number of arguments for 'geosearch' command"}
  end

  # ---------------------------------------------------------------------------
  # GEOSEARCHSTORE destination source [same GEOSEARCH options]
  # ---------------------------------------------------------------------------

  def handle("GEOSEARCHSTORE", [destination, source | rest], store) do
    with {:ok, opts} <- Parsing.parse_geosearch_opts(rest),
         {:ok, center_lng, center_lat} <- resolve_center(opts, store, source),
         {:ok, zset} <- read_zset(store, source) do
      matches = find_matching_members(zset, center_lng, center_lat, opts)
      sorted = sort_matches(matches, opts)
      limited = apply_count(sorted, opts)

      store_geosearch_results(store, destination, limited)
    end
  end

  def handle("GEOSEARCHSTORE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'geosearchstore' command"}
  end

  defp geopos_args([key | members], store) when members != [] do
    with {:ok, scores} <- read_zset_member_scores(store, key, members) do
      Enum.map(scores, fn
        nil ->
          nil

        score when is_float(score) ->
          {lng, lat} = geohash_decode(score)
          [format_coord(lng), format_coord(lat)]
      end)
    end
  end

  defp geopos_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'geopos' command"}
  end

  defp geohash_args([key | members], store) when members != [] do
    with {:ok, scores} <- read_zset_member_scores(store, key, members) do
      Enum.map(scores, fn
        nil -> nil
        score when is_float(score) -> encode_geohash_string(score)
      end)
    end
  end

  defp geohash_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'geohash' command"}
  end

  defp read_zset_member_scores(store, key, members) do
    type_key = CompoundKey.type_key(key)

    case Ops.compound_get(store, key, type_key) do
      "zset" ->
        compound_keys = Enum.map(members, &CompoundKey.zset_member(key, &1))

        scores =
          store
          |> Ops.compound_batch_get(key, compound_keys)
          |> Enum.map(&parse_geo_score/1)

        {:ok, scores}

      nil ->
        if Ops.exists?(store, key),
          do: {:error, @wrongtype_msg},
          else: {:ok, missing_scores(members)}

      _other ->
        {:error, @wrongtype_msg}
    end
  end

  defp parse_geo_score(nil), do: nil

  defp parse_geo_score(score_str) when is_binary(score_str) do
    case Float.parse(score_str) do
      {score, ""} -> score
      _ -> 0.0
    end
  end

  defp missing_scores(members), do: Enum.map(members, fn _member -> nil end)

  # ===========================================================================
  # Geohash Encoding/Decoding (public for testing)
  # ===========================================================================

  @doc """
  Encodes longitude and latitude into a 52-bit geohash stored as a float64.

  Interleaves bits of longitude and latitude ranges:
  - longitude: -180 to 180 (26 bits)
  - latitude: -90 to 90 (26 bits)

  ## Parameters

    - `longitude` - Longitude in degrees (-180..180)
    - `latitude` - Latitude in degrees (-90..90)

  ## Returns

  A float64 representing the geohash score.
  """
  @spec geohash_encode(float(), float()) :: float()
  def geohash_encode(longitude, latitude) do
    lng_q = quantize(longitude, -180.0, 180.0, @lng_bits)
    lat_q = quantize(latitude, -90.0, 90.0, @lat_bits)
    interleaved = interleave_bits(lng_q, lat_q, @lng_bits)
    interleaved * 1.0
  end

  @doc """
  Decodes a geohash float64 score back to `{longitude, latitude}`.

  ## Parameters

    - `score` - Float64 geohash score

  ## Returns

  `{longitude, latitude}` tuple with ~0.6mm precision.
  """
  @spec geohash_decode(float()) :: {float(), float()}
  def geohash_decode(score) do
    hash = trunc(score)
    {lng_q, lat_q} = deinterleave_bits(hash, @lng_bits)
    lng = dequantize(lng_q, -180.0, 180.0, @lng_bits)
    lat = dequantize(lat_q, -90.0, 90.0, @lat_bits)
    {lng, lat}
  end

  @doc """
  Computes the haversine distance in meters between two points on Earth.

  ## Parameters

    - `lat1` - Latitude of point 1 in degrees
    - `lng1` - Longitude of point 1 in degrees
    - `lat2` - Latitude of point 2 in degrees
    - `lng2` - Longitude of point 2 in degrees

  ## Returns

  Distance in meters as a float.
  """
  @spec haversine(float(), float(), float(), float()) :: float()
  def haversine(lat1, lng1, lat2, lng2) do
    dlat = deg_to_rad(lat2 - lat1)
    dlng = deg_to_rad(lng2 - lng1)

    a =
      :math.sin(dlat / 2) ** 2 +
        :math.cos(deg_to_rad(lat1)) * :math.cos(deg_to_rad(lat2)) *
          :math.sin(dlng / 2) ** 2

    2 * @earth_radius_m * :math.asin(:math.sqrt(a))
  end

  # ===========================================================================
  # Private -- Sorted set storage helpers
  # ===========================================================================

  # Reads a sorted set from the store, returning [] for missing keys.
  # Returns {:error, msg} on type mismatch.
  @doc false
  @spec read_zset(map(), binary()) :: {:ok, [{float(), binary()}]} | {:error, binary()}
  def read_zset(store, key) do
    case read_compound_zset(store, key) do
      {:ok, zset} -> {:ok, zset}
      :missing -> {:ok, []}
      {:error, _} = err -> err
    end
  end

  defp read_compound_zset(store, key) do
    type_key = CompoundKey.type_key(key)
    prefix = CompoundKey.zset_prefix(key)

    case Ops.compound_get(store, key, type_key) do
      "zset" ->
        {:ok, load_compound_zset(store, key, prefix)}

      nil ->
        if Ops.exists?(store, key), do: {:error, @wrongtype_msg}, else: :missing

      _other ->
        {:error, @wrongtype_msg}
    end
  end

  defp load_compound_zset(store, key, prefix) do
    store
    |> Ops.compound_scan(key, prefix)
    |> parse_compound_zset()
  end

  defp parse_compound_zset(entries) do
    entries
    |> Enum.map(fn {member, score_str} ->
      score =
        case Float.parse(score_str) do
          {score, ""} -> score
          _ -> 0.0
        end

      {score, member}
    end)
    |> Enum.sort()
  end

  defp write_zset(store, key, zset) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           ensure_zset_type(store, key) do
      prefix = CompoundKey.zset_prefix(key)
      new_members = MapSet.new(Enum.map(zset, fn {_score, member} -> member end))

      delete_keys =
        store
        |> Ops.compound_scan(key, prefix)
        |> Enum.flat_map(fn {member, _score} ->
          if MapSet.member?(new_members, member) do
            []
          else
            [CompoundKey.zset_member(key, member)]
          end
        end)

      put_entries =
        Enum.map(zset, fn {score, member} ->
          {CompoundKey.zset_member(key, member), Float.to_string(score), 0}
        end)

      with :ok <- Ops.compound_batch_delete(store, key, delete_keys),
           :ok <- Ops.compound_batch_put(store, key, put_entries) do
        :ok
      else
        {:error, _} = err -> rollback_new_zset_type_marker(key, store, type_status, err)
      end
    end
  end

  defp store_geosearch_results(store, destination, []) do
    case delete_key_data(store, destination) do
      :ok -> 0
      {:error, _} = err -> err
    end
  end

  defp store_geosearch_results(store, destination, limited) do
    new_zset =
      limited
      |> Enum.map(fn {score, member, _dist} -> {score, member} end)
      |> Enum.sort()

    case replace_zset(store, destination, new_zset) do
      :ok -> length(limited)
      {:error, _} = err -> err
    end
  end

  defp replace_zset(store, key, zset) do
    backup = zset_destination_backup(store, key)

    case delete_key_data(store, key) do
      :ok ->
        case write_zset(store, key, zset) do
          :ok -> :ok
          {:error, _} = err -> restore_zset_destination(store, key, backup, err)
        end

      {:error, _} = err ->
        restore_zset_destination(store, key, backup, err)
    end
  end

  defp zset_destination_backup(store, key) do
    case TypeRegistry.get_type(key, store) do
      "zset" ->
        case read_zset(store, key) do
          {:ok, zset} -> {:zset, zset}
          {:error, _} -> :missing
        end

      _other ->
        :missing
    end
  end

  defp restore_zset_destination(store, key, :missing, original_error) do
    case delete_key_data(store, key) do
      :ok -> original_error
      {:error, _} = restore_error -> restore_error
    end
  end

  defp restore_zset_destination(store, key, {:zset, zset}, original_error) do
    case write_zset(store, key, zset) do
      :ok -> original_error
      {:error, _} = restore_error -> restore_error
    end
  end

  defp delete_key_data(store, key) do
    with :ok <- Ops.delete(store, key),
         :ok <- TypeRegistry.delete_type(key, store),
         :ok <- Ops.compound_delete(store, key, CompoundKey.list_meta_key(key)) do
      [
        CompoundKey.hash_prefix(key),
        CompoundKey.list_prefix(key),
        CompoundKey.set_prefix(key),
        CompoundKey.zset_prefix(key)
      ]
      |> Enum.reduce_while(:ok, fn prefix, :ok ->
        case Ops.compound_delete_prefix(store, key, prefix) do
          :ok -> {:cont, :ok}
          {:error, _} = err -> {:halt, err}
        end
      end)
    end
  end

  defp ensure_zset_type(store, key) do
    TypeRegistry.check_or_set_status(key, :zset, store)
  end

  defp rollback_new_zset_type_marker(key, store, {:ok, :created}, write_error) do
    case TypeRegistry.delete_type(key, store) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:geo_zset_type_marker_rollback_failed, write_error, rollback_error}}
    end
  end

  defp rollback_new_zset_type_marker(_key, _store, :ok, write_error), do: write_error

  # ===========================================================================
  # Private -- GEOADD implementation
  # ===========================================================================

  defp do_geoadd(store, key, pairs, flags) do
    members = unique_geoadd_members(pairs)

    with {:ok, type_status, scores} <- read_geoadd_existing_scores(store, key, members) do
      old_map =
        members
        |> Enum.zip(scores)
        |> Map.new()

      {added, changed, new_map} =
        Enum.reduce(pairs, {0, 0, old_map}, fn {lng, lat, member}, {add_acc, ch_acc, map_acc} ->
          score = geohash_encode(lng, lat)

          case Map.get(map_acc, member) do
            nil ->
              if :xx in flags do
                {add_acc, ch_acc, map_acc}
              else
                {add_acc + 1, ch_acc, Map.put(map_acc, member, score)}
              end

            old_score ->
              if :nx in flags do
                {add_acc, ch_acc, map_acc}
              else
                ch = if score != old_score, do: 1, else: 0
                {add_acc, ch_acc + ch, Map.put(map_acc, member, score)}
              end
          end
        end)

      write_entries =
        members
        |> Enum.flat_map(fn member ->
          old_score = Map.get(old_map, member)
          new_score = Map.get(new_map, member)

          if new_score != nil and new_score != old_score do
            [{CompoundKey.zset_member(key, member), Float.to_string(new_score), 0}]
          else
            []
          end
        end)

      case write_geoadd_entries(store, key, write_entries, type_status) do
        :ok -> if :ch in flags, do: added + changed, else: added
        {:error, _} = err -> err
      end
    end
  end

  defp unique_geoadd_members(pairs) do
    pairs
    |> Enum.map(fn {_lng, _lat, member} -> member end)
    |> Enum.uniq()
  end

  defp read_geoadd_existing_scores(store, key, members) do
    type_key = CompoundKey.type_key(key)

    case Ops.compound_get(store, key, type_key) do
      "zset" ->
        compound_keys = Enum.map(members, &CompoundKey.zset_member(key, &1))

        scores =
          store
          |> Ops.compound_batch_get(key, compound_keys)
          |> Enum.map(&parse_geo_score/1)

        {:ok, :ok, scores}

      nil ->
        if Ops.exists?(store, key),
          do: {:error, @wrongtype_msg},
          else: {:ok, :missing, missing_scores(members)}

      _other ->
        {:error, @wrongtype_msg}
    end
  end

  defp write_geoadd_entries(_store, _key, [], _type_status), do: :ok

  defp write_geoadd_entries(store, key, entries, :ok) do
    case Ops.compound_batch_put(store, key, entries) do
      :ok -> :ok
      {:error, _} = err -> rollback_new_zset_type_marker(key, store, :ok, err)
    end
  end

  defp write_geoadd_entries(store, key, entries, :missing) do
    with type_status when type_status in [:ok, {:ok, :created}] <- ensure_zset_type(store, key) do
      case Ops.compound_batch_put(store, key, entries) do
        :ok -> :ok
        {:error, _} = err -> rollback_new_zset_type_marker(key, store, type_status, err)
      end
    end
  end

  # ===========================================================================
  # Private -- Geohash bit manipulation
  # ===========================================================================

  defp quantize(value, min, max, bits) do
    range = max - min
    normalized = (value - min) / range
    max_val = Bitwise.bsl(1, bits) - 1
    trunc(normalized * max_val + 0.5)
  end

  defp dequantize(bits_val, min, max, bits) do
    range = max - min
    max_val = Bitwise.bsl(1, bits)
    min + (bits_val + 0.5) / max_val * range
  end

  defp interleave_bits(lng_q, lat_q, n) do
    Enum.reduce((n - 1)..0//-1, 0, fn i, acc ->
      lng_bit = Bitwise.band(Bitwise.bsr(lng_q, i), 1)
      lat_bit = Bitwise.band(Bitwise.bsr(lat_q, i), 1)
      bit_pos = i * 2

      acc
      |> Bitwise.bor(Bitwise.bsl(lng_bit, bit_pos + 1))
      |> Bitwise.bor(Bitwise.bsl(lat_bit, bit_pos))
    end)
  end

  defp deinterleave_bits(hash, n) do
    Enum.reduce((n - 1)..0//-1, {0, 0}, fn i, {lng_acc, lat_acc} ->
      bit_pos = i * 2
      lng_bit = Bitwise.band(Bitwise.bsr(hash, bit_pos + 1), 1)
      lat_bit = Bitwise.band(Bitwise.bsr(hash, bit_pos), 1)

      {Bitwise.bor(lng_acc, Bitwise.bsl(lng_bit, i)),
       Bitwise.bor(lat_acc, Bitwise.bsl(lat_bit, i))}
    end)
  end

  # ===========================================================================
  # Private -- Geohash base32 string encoding
  # ===========================================================================

  defp encode_geohash_string(score) do
    hash = trunc(score)
    # 11 chars * 5 bits/char = 55 bits. We have 52 bits, so pad with 3 zero bits.
    padded = Bitwise.bsl(hash, 3)

    10..0//-1
    |> Enum.map(fn i ->
      chunk = Bitwise.band(Bitwise.bsr(padded, i * 5), 0x1F)
      Enum.at(@base32_alphabet, chunk)
    end)
    |> List.to_string()
  end

  # ===========================================================================
  # Private -- Haversine helpers
  # ===========================================================================

  defp deg_to_rad(deg), do: deg * :math.pi() / 180.0

  # ===========================================================================
  # Private -- GEOSEARCH execution
  # ===========================================================================

  defp resolve_center(%{center: {:lonlat, lng, lat}}, _store, _key) do
    {:ok, lng, lat}
  end

  defp resolve_center(%{center: {:member, member}}, store, key) do
    with {:ok, [score]} <- read_zset_member_scores(store, key, [member]) do
      case score do
        nil ->
          {:error, "ERR could not decode requested zset member"}

        score when is_float(score) ->
          {lng, lat} = geohash_decode(score)
          {:ok, lng, lat}
      end
    end
  end

  defp find_matching_members(zset, center_lng, center_lat, opts) do
    zset
    |> Enum.reduce([], fn {score, member}, acc ->
      {lng, lat} = geohash_decode(score)
      dist_m = haversine(center_lat, center_lng, lat, lng)

      if in_shape?(dist_m, lng, lat, center_lng, center_lat, opts) do
        [{score, member, dist_m} | acc]
      else
        acc
      end
    end)
    |> Enum.reverse()
  end

  defp in_shape?(dist_m, _lng, _lat, _clng, _clat, %{shape: {:radius, radius_m}}) do
    dist_m <= radius_m
  end

  defp in_shape?(_dist_m, lng, lat, center_lng, center_lat, %{shape: {:box, width_m, height_m}}) do
    # Convert box dimensions from meters to degrees at center latitude.
    # 1 degree latitude ≈ 111,320 meters (constant).
    # 1 degree longitude ≈ 111,320 * cos(latitude) meters (varies with latitude).
    # Then compare member coordinates directly against the degree-based box.
    # This matches Redis behavior and avoids the haversine center-latitude bug.
    lat_half_deg = height_m / 2.0 / 111_320.0
    cos_lat = :math.cos(center_lat * :math.pi() / 180.0)
    lon_half_deg = if cos_lat > 0, do: width_m / 2.0 / (111_320.0 * cos_lat), else: 180.0

    abs(lat - center_lat) <= lat_half_deg and abs(lng - center_lng) <= lon_half_deg
  end

  defp sort_matches(matches, %{sort: :asc}) do
    Enum.sort_by(matches, fn {_score, _member, dist} -> dist end)
  end

  defp sort_matches(matches, %{sort: :desc}) do
    Enum.sort_by(matches, fn {_score, _member, dist} -> dist end, :desc)
  end

  defp sort_matches(matches, _opts), do: matches

  defp apply_count(matches, %{count: count}) do
    Enum.take(matches, count)
  end

  defp apply_count(matches, _opts), do: matches

  defp do_geosearch(zset, center_lng, center_lat, opts) do
    matches = find_matching_members(zset, center_lng, center_lat, opts)
    sorted = sort_matches(matches, opts)
    limited = apply_count(sorted, opts)

    withcoord = Map.get(opts, :withcoord, false)
    withdist = Map.get(opts, :withdist, false)
    withhash = Map.get(opts, :withhash, false)
    unit = Map.get(opts, :unit, "M")

    if withcoord or withdist or withhash do
      Enum.map(limited, fn {score, member, dist_m} ->
        entry = [member]

        entry =
          if withdist do
            dist = dist_m / @unit_conversions[unit]
            entry ++ [format_distance(dist)]
          else
            entry
          end

        entry =
          if withhash do
            entry ++ [trunc(score)]
          else
            entry
          end

        entry =
          if withcoord do
            {lng, lat} = geohash_decode(score)
            entry ++ [[format_coord(lng), format_coord(lat)]]
          else
            entry
          end

        entry
      end)
    else
      Enum.map(limited, fn {_score, member, _dist} -> member end)
    end
  end

  # ===========================================================================
  # Private -- formatting helpers
  # ===========================================================================

  defp format_coord(val) do
    :erlang.float_to_binary(val * 1.0, [:compact, decimals: 4])
  end

  defp format_distance(dist) do
    :erlang.float_to_binary(dist * 1.0, [:compact, decimals: 4])
  end

end
