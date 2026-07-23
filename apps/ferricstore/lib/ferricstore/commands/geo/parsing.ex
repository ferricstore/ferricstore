defmodule Ferricstore.Commands.Geo.Parsing do
  @moduledoc false

  @unit_conversions %{
    "M" => 1.0,
    "KM" => 1000.0,
    "FT" => 0.3048,
    "MI" => 1609.344
  }

  @geosearch_option_keys ~w(center shape unit raw_radius sort count any withcoord withdist withhash)a

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

  def validate_geoadd_ast(flags, pairs) when is_list(flags) and is_list(pairs) do
    cond do
      Enum.any?(flags, &(&1 not in [:nx, :xx, :ch])) ->
        {:error, "ERR syntax error"}

      :nx in flags and :xx in flags ->
        {:error, "ERR XX and NX options at the same time are not compatible"}

      pairs == [] ->
        {:error, "ERR wrong number of arguments for 'geoadd' command"}

      true ->
        validate_geoadd_pairs(pairs)
    end
  end

  def validate_geoadd_ast(_flags, _pairs), do: {:error, "ERR syntax error"}

  def validate_geosearch_ast_opts(opts) when is_list(opts) do
    keys =
      Enum.map(opts, fn
        {key, _value} when is_atom(key) -> key
        _invalid -> :invalid
      end)

    cond do
      not Keyword.keyword?(opts) ->
        {:error, "ERR syntax error"}

      Enum.any?(keys, &(&1 not in @geosearch_option_keys)) ->
        {:error, "ERR syntax error"}

      length(keys) != MapSet.size(MapSet.new(keys)) ->
        {:error, "ERR syntax error"}

      true ->
        opts
        |> Map.new()
        |> validate_typed_geosearch_map()
    end
  end

  def validate_geosearch_ast_opts(_opts), do: {:error, "ERR syntax error"}

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

  defp validate_geoadd_pairs(pairs) do
    Enum.reduce_while(pairs, :ok, fn
      {lng, lat, member}, :ok when is_float(lng) and is_float(lat) and is_binary(member) ->
        if valid_coordinates?(lng, lat),
          do: {:cont, :ok},
          else: {:halt, {:error, "ERR invalid longitude,latitude pair #{lng},#{lat}"}}

      _invalid, :ok ->
        {:halt, {:error, "ERR syntax error"}}
    end)
  end

  defp validate_typed_geosearch_map(opts) do
    with :ok <- validate_typed_center(Map.get(opts, :center)),
         :ok <- validate_typed_shape(Map.get(opts, :shape)),
         {:ok, unit} <- validate_typed_unit(Map.get(opts, :unit, "M")),
         :ok <- validate_optional_count(Map.get(opts, :count)),
         :ok <- validate_optional_sort(Map.get(opts, :sort)),
         :ok <- validate_optional_booleans(opts) do
      {:ok, Map.put(opts, :unit, unit)}
    end
  end

  defp validate_typed_center({:lonlat, lng, lat}) when is_float(lng) and is_float(lat) do
    if valid_coordinates?(lng, lat),
      do: :ok,
      else: {:error, "ERR invalid longitude,latitude pair #{lng},#{lat}"}
  end

  defp validate_typed_center({:member, member}) when is_binary(member), do: :ok

  defp validate_typed_center(_center),
    do: {:error, "ERR exactly one of FROMMEMBER or FROMLONLAT must be provided"}

  defp validate_typed_shape({:radius, radius}) when is_float(radius) and radius > 0, do: :ok

  defp validate_typed_shape({:box, width, height})
       when is_float(width) and width > 0 and is_float(height) and height > 0,
       do: :ok

  defp validate_typed_shape(_shape),
    do: {:error, "ERR exactly one of BYRADIUS or BYBOX must be provided"}

  defp validate_typed_unit(unit) when is_binary(unit) do
    normalized = String.upcase(unit)

    if normalized in Map.keys(@unit_conversions),
      do: {:ok, normalized},
      else: {:error, "ERR unsupported unit provided. please use M, KM, FT, MI"}
  end

  defp validate_typed_unit(_unit),
    do: {:error, "ERR unsupported unit provided. please use M, KM, FT, MI"}

  defp validate_optional_count(nil), do: :ok
  defp validate_optional_count(count) when is_integer(count) and count > 0, do: :ok

  defp validate_optional_count(_count),
    do: {:error, "ERR value is not an integer or out of range"}

  defp validate_optional_sort(nil), do: :ok
  defp validate_optional_sort(sort) when sort in [:asc, :desc], do: :ok
  defp validate_optional_sort(_sort), do: {:error, "ERR syntax error"}

  defp validate_optional_booleans(opts) do
    if Enum.all?([:any, :withcoord, :withdist, :withhash], fn key ->
         not Map.has_key?(opts, key) or is_boolean(Map.fetch!(opts, key))
       end) do
      :ok
    else
      {:error, "ERR syntax error"}
    end
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
            if valid_coordinates?(lng, lat) do
              parse_geosearch_opts(rest, Map.put(opts, :center, {:lonlat, lng, lat}))
            else
              {:error, "ERR invalid longitude,latitude pair #{lng_str},#{lat_str}"}
            end
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
        if radius > 0 do
          radius_m = radius * @unit_conversions[unit]

          opts =
            Map.merge(opts, %{shape: {:radius, radius_m}, unit: unit, raw_radius: radius})

          parse_geosearch_opts(rest, opts)
        else
          {:error, "ERR radius must be greater than 0"}
        end
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
        if width > 0 and height > 0 do
          width_m = width * @unit_conversions[unit]
          height_m = height * @unit_conversions[unit]

          opts =
            Map.merge(opts, %{shape: {:box, width_m, height_m}, unit: unit})

          parse_geosearch_opts(rest, opts)
        else
          {:error, "ERR width and height must be greater than 0"}
        end
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
  rescue
    ArgumentError -> {:error, "ERR value is not a valid float"}
    ArithmeticError -> {:error, "ERR value is not a valid float"}
  end
end
