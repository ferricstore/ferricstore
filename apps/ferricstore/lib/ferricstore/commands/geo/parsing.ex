defmodule Ferricstore.Commands.Geo.Parsing do
  @moduledoc false

  @unit_conversions %{
    "M" => 1.0,
    "KM" => 1000.0,
    "FT" => 0.3048,
    "MI" => 1609.344
  }

  def parse_geoadd_flags(args) do
    {flags, rest} = take_geoadd_flags(args, [])

    if :nx in flags and :xx in flags do
      {:error, "ERR XX and NX options at the same time are not compatible"}
    else
      {:ok, flags, rest}
    end
  end

  def parse_lng_lat_members(args), do: parse_lng_lat_members(args, [])

  def parse_geosearch_opts(args) do
    parse_geosearch_opts(args, %{})
  end

  defp take_geoadd_flags(["NX" | rest], acc), do: take_geoadd_flags(rest, [:nx | acc])
  defp take_geoadd_flags(["XX" | rest], acc), do: take_geoadd_flags(rest, [:xx | acc])
  defp take_geoadd_flags(["CH" | rest], acc), do: take_geoadd_flags(rest, [:ch | acc])

  defp take_geoadd_flags([opt | rest] = args, acc) when is_binary(opt) do
    case String.upcase(opt) do
      "NX" -> take_geoadd_flags(rest, [:nx | acc])
      "XX" -> take_geoadd_flags(rest, [:xx | acc])
      "CH" -> take_geoadd_flags(rest, [:ch | acc])
      _not_option -> {acc, args}
    end
  end

  defp take_geoadd_flags(rest, acc), do: {acc, rest}

  defp parse_lng_lat_members([], []) do
    {:error, "ERR wrong number of arguments for 'geoadd' command"}
  end

  defp parse_lng_lat_members([], acc), do: {:ok, Enum.reverse(acc)}

  defp parse_lng_lat_members([lng_str, lat_str, member | rest], acc) do
    with {:ok, lng} <- parse_float(lng_str),
         {:ok, lat} <- parse_float(lat_str) do
      if valid_coordinates?(lng, lat) do
        parse_lng_lat_members(rest, [{lng, lat, member} | acc])
      else
        {:error, "ERR invalid longitude,latitude pair #{lng_str},#{lat_str}"}
      end
    end
  end

  defp parse_lng_lat_members([_, _], _acc) do
    {:error, "ERR wrong number of arguments for 'geoadd' command"}
  end

  defp parse_lng_lat_members([_], _acc) do
    {:error, "ERR wrong number of arguments for 'geoadd' command"}
  end

  defp valid_coordinates?(lng, lat) do
    lng >= -180.0 and lng <= 180.0 and lat >= -85.05112878 and lat <= 85.05112878
  end

  defp parse_geosearch_opts([], opts) do
    cond do
      not Map.has_key?(opts, :center) ->
        {:error, "ERR exactly one of FROMMEMBER or FROMLONLAT must be provided"}

      not Map.has_key?(opts, :shape) ->
        {:error, "ERR exactly one of BYRADIUS or BYBOX must be provided"}

      true ->
        {:ok, opts}
    end
  end

  defp parse_geosearch_opts([opt | rest], opts) do
    case String.upcase(opt) do
      "FROMLONLAT" ->
        parse_geosearch_lonlat(rest, opts)

      "FROMMEMBER" ->
        parse_geosearch_member(rest, opts)

      "BYRADIUS" ->
        case rest do
          [radius_str, unit_str | rest] ->
            parse_geosearch_radius(radius_str, unit_str, rest, opts)

          _ ->
            {:error, "ERR syntax error"}
        end

      "BYBOX" ->
        case rest do
          [width_str, height_str, unit_str | rest] ->
            parse_geosearch_box(width_str, height_str, unit_str, rest, opts)

          _ ->
            {:error, "ERR syntax error"}
        end

      "ASC" ->
        parse_geosearch_opts(rest, Map.put(opts, :sort, :asc))

      "DESC" ->
        parse_geosearch_opts(rest, Map.put(opts, :sort, :desc))

      "COUNT" ->
        case rest do
          [count_str | rest] -> parse_geosearch_count(count_str, rest, opts)
          _ -> {:error, "ERR syntax error"}
        end

      "WITHCOORD" ->
        parse_geosearch_opts(rest, Map.put(opts, :withcoord, true))

      "WITHDIST" ->
        parse_geosearch_opts(rest, Map.put(opts, :withdist, true))

      "WITHHASH" ->
        parse_geosearch_opts(rest, Map.put(opts, :withhash, true))

      _unknown ->
        {:error, "ERR syntax error, unexpected '#{opt}'"}
    end
  end

  defp parse_geosearch_lonlat(rest, opts) do
    case rest do
      [lng_str, lat_str | rest] ->
        if Map.has_key?(opts, :center) do
          {:error, "ERR exactly one of FROMMEMBER or FROMLONLAT must be provided"}
        else
          with {:ok, lng} <- parse_float(lng_str),
               {:ok, lat} <- parse_float(lat_str) do
            parse_geosearch_opts(rest, Map.put(opts, :center, {:lonlat, lng, lat}))
          end
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_geosearch_member(rest, opts) do
    case rest do
      [member | rest] ->
        if Map.has_key?(opts, :center) do
          {:error, "ERR exactly one of FROMMEMBER or FROMLONLAT must be provided"}
        else
          parse_geosearch_opts(rest, Map.put(opts, :center, {:member, member}))
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_geosearch_radius(radius_str, unit_str, rest, opts) do
    if Map.has_key?(opts, :shape) do
      {:error, "ERR exactly one of BYRADIUS or BYBOX must be provided"}
    else
      unit = String.upcase(unit_str)

      with {:ok, radius} <- parse_float(radius_str),
           true <- unit in Map.keys(@unit_conversions) do
        radius_m = radius * @unit_conversions[unit]

        opts =
          Map.merge(opts, %{shape: {:radius, radius_m}, unit: unit, raw_radius: radius})

        parse_geosearch_opts(rest, opts)
      else
        false -> {:error, "ERR unsupported unit provided. please use M, KM, FT, MI"}
        err -> err
      end
    end
  end

  defp parse_geosearch_box(width_str, height_str, unit_str, rest, opts) do
    if Map.has_key?(opts, :shape) do
      {:error, "ERR exactly one of BYRADIUS or BYBOX must be provided"}
    else
      unit = String.upcase(unit_str)

      with {:ok, width} <- parse_float(width_str),
           {:ok, height} <- parse_float(height_str),
           true <- unit in Map.keys(@unit_conversions) do
        width_m = width * @unit_conversions[unit]
        height_m = height * @unit_conversions[unit]

        opts =
          Map.merge(opts, %{shape: {:box, width_m, height_m}, unit: unit})

        parse_geosearch_opts(rest, opts)
      else
        false -> {:error, "ERR unsupported unit provided. please use M, KM, FT, MI"}
        err -> err
      end
    end
  end

  defp parse_geosearch_count(count_str, rest, opts) do
    case Integer.parse(count_str) do
      {count, ""} when count > 0 ->
        case rest do
          [any | rest] when is_binary(any) ->
            if String.upcase(any) == "ANY" do
              parse_geosearch_opts(rest, Map.merge(opts, %{count: count, any: true}))
            else
              parse_geosearch_opts([any | rest], Map.merge(opts, %{count: count, any: false}))
            end

          _ ->
            parse_geosearch_opts(rest, Map.merge(opts, %{count: count, any: false}))
        end

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_float(str) do
    case Float.parse(str) do
      {val, ""} ->
        {:ok, val}

      _ ->
        case Integer.parse(str) do
          {val, ""} -> {:ok, val * 1.0}
          _ -> {:error, "ERR value is not a valid float"}
        end
    end
  end
end
