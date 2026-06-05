defmodule FerricStore.API.Geo do
  @moduledoc false

  import FerricStore.API.Store
  alias Ferricstore.Store.Router
  alias Ferricstore.Commands.Geo

  @type key :: FerricStore.key()
  @type value :: FerricStore.value()
  @type write_error :: FerricStore.write_error()
  @type set_opts :: FerricStore.set_opts()
  @type get_opts :: FerricStore.get_opts()
  @type cas_opts :: FerricStore.cas_opts()
  @type fetch_or_compute_opts :: FerricStore.fetch_or_compute_opts()
  @type zrange_opts :: FerricStore.zrange_opts()

  @doc """
  Adds geospatial members (longitude, latitude, name) to the geo index at `key`.

  Members are stored in a sorted set using geohash-encoded scores,
  enabling radius queries and distance calculations for location-based features.

  ## Parameters

    * `key` - the geo index key
    * `members` - list of `{longitude, latitude, name}` tuples

  ## Returns

    * `{:ok, added_count}` - number of new members added.
    * `{:error, reason}` on failure.

  ## Examples

      iex> FerricStore.geoadd("stores:nyc", [
      ...>   {-73.935242, 40.730610, "brooklyn_store"},
      ...>   {-74.0060, 40.7128, "manhattan_store"}
      ...> ])
      {:ok, 2}

  """
  @spec geoadd(key(), [{number(), number(), binary()}]) ::
          {:ok, non_neg_integer()} | {:error, binary()}
  def geoadd(key, members) when is_list(members) do
    pairs = Enum.map(members, fn {lng, lat, member} -> {lng * 1.0, lat * 1.0, member} end)
    ctx = default_ctx()

    Router.with_key_latch(ctx, key, fn ->
      wrap_result(Geo.handle_ast({:geoadd, key, [], pairs}, build_compound_store(key)))
    end)
  end

  @doc """
  Returns the distance between two geo members.

  ## Parameters

    * `key` - the geo index key
    * `member1` - first member name
    * `member2` - second member name
    * `unit` - distance unit: `"m"` (meters, default), `"km"`, `"mi"`, or `"ft"`

  ## Returns

    * `{:ok, distance_string}` on success.
    * `{:ok, nil}` if either member does not exist.
    * `{:error, reason}` on failure.

  ## Examples

      iex> FerricStore.geodist("stores:nyc", "brooklyn_store", "manhattan_store", "km")
      {:ok, "8.4567"}

  """
  @spec geodist(key(), binary(), binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def geodist(key, member1, member2, unit \\ "m") do
    store = build_compound_store(key)
    result = Geo.handle_ast({:geodist, key, member1, member2, normalize_geo_unit(unit)}, store)
    wrap_result(result)
  end

  @doc """
  Returns geohash strings for the specified members.

  Geohashes are base-32 encoded strings representing a geographic area, useful
  for proximity grouping and prefix-based spatial queries.

  ## Returns

    * `{:ok, [geohash | nil, ...]}` - a geohash per member, or `nil` for missing members.

  ## Examples

      iex> FerricStore.geohash("stores:nyc", ["brooklyn_store", "manhattan_store"])
      {:ok, ["dr5regy3zc0", "dr5regw3pp0"]}

  """
  @spec geohash(key(), [binary()]) :: {:ok, list()} | {:error, binary()}
  def geohash(key, members) when is_list(members) do
    store = build_compound_store(key)
    result = Geo.handle_ast({:geohash, [key | members]}, store)
    wrap_result(result)
  end

  @doc """
  Returns the longitude/latitude positions for the specified members.

  ## Returns

    * `{:ok, [[longitude, latitude] | nil, ...]}` - coordinates per member,
      or `nil` for missing members.

  ## Examples

      iex> FerricStore.geopos("stores:nyc", ["brooklyn_store"])
      {:ok, [["-73.935242", "40.730610"]]}

  """
  @spec geopos(key(), [binary()]) :: {:ok, list()} | {:error, binary()}
  def geopos(key, members) when is_list(members) do
    store = build_compound_store(key)
    result = Geo.handle_ast({:geopos, [key | members]}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # JSON operations
  # ---------------------------------------------------------------------------
end
