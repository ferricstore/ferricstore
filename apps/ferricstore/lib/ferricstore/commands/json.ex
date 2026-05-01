defmodule Ferricstore.Commands.Json do
  @moduledoc """
  Handles Redis JSON commands: JSON.SET, JSON.GET, JSON.DEL, JSON.NUMINCRBY,
  JSON.TYPE, JSON.STRLEN, JSON.OBJKEYS, JSON.OBJLEN, JSON.ARRAPPEND,
  JSON.ARRLEN, JSON.TOGGLE, JSON.CLEAR, JSON.MGET.

  Starting with Redis 8, JSON is part of Redis Open Source. FerricStore v1
  stores JSON as raw bytes in Bitcask using a type tag:
  `:erlang.term_to_binary({:json, json_string})`. Every `JSON.GET`
  deserializes and evaluates JSONPath. Every `JSON.SET` reads, applies
  mutation, writes back.

  ## JSONPath subset (v1)

    * `$` -- root
    * `$.field` -- object field access
    * `$.field.subfield` -- nested access
    * `$[0]`, `$[1]` -- array index
    * `$.field[0].name` -- mixed access

  ## Supported commands

    * `JSON.SET key path value [NX|XX]` -- set JSON value at path
    * `JSON.GET key [path ...]` -- get JSON value(s) at path(s)
    * `JSON.DEL key [path]` -- delete value at path, returns count deleted
    * `JSON.NUMINCRBY key path value` -- increment number at path
    * `JSON.TYPE key [path]` -- return JSON type at path
    * `JSON.STRLEN key [path]` -- return string length at path
    * `JSON.OBJKEYS key [path]` -- return object keys at path
    * `JSON.OBJLEN key [path]` -- return number of keys in object at path
    * `JSON.ARRAPPEND key path value [value ...]` -- append to array at path
    * `JSON.ARRLEN key [path]` -- return array length at path
    * `JSON.TOGGLE key path` -- toggle boolean at path
    * `JSON.CLEAR key [path]` -- clear container or number to zero
    * `JSON.MGET key [key ...] path` -- get value at path from multiple keys
  """

  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.TypeRegistry

  @wrongtype_msg "WRONGTYPE Operation against a key holding the wrong kind of value"

  @doc """
  Handles a JSON command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"JSON.SET"`, `"JSON.GET"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put`, `delete`, `exists?` callbacks

  ## Returns

  Plain Elixir term: `:ok`, `nil`, integer, string, list, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # JSON.SET key path value [NX|XX]
  # ---------------------------------------------------------------------------

  def handle("JSON.SET", [key, path, value | opts], store) do
    with {:ok, new_value} <- decode_json_value(value),
         {:ok, nx?, xx?} <- parse_set_flags(opts) do
      do_json_set(key, path, new_value, nx?, xx?, store)
    end
  end

  def handle("JSON.SET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'json.set' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.GET key [path ...]
  # ---------------------------------------------------------------------------

  def handle("JSON.GET", [key | paths], store) when paths != [] do
    with_json(key, store, &do_json_get(&1, paths))
  end

  def handle("JSON.GET", [key], store) do
    with_json(key, store, &Jason.encode!/1)
  end

  def handle("JSON.GET", [], _store) do
    {:error, "ERR wrong number of arguments for 'json.get' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.DEL key [path]
  # ---------------------------------------------------------------------------

  def handle("JSON.DEL", [key], store) do
    do_json_del_root(key, store)
  end

  def handle("JSON.DEL", [key, "$"], store) do
    do_json_del_root(key, store)
  end

  def handle("JSON.DEL", [key, path], store) do
    do_json_del_path(key, path, store)
  end

  def handle("JSON.DEL", [], _store) do
    {:error, "ERR wrong number of arguments for 'json.del' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.NUMINCRBY key path value
  # ---------------------------------------------------------------------------

  def handle("JSON.NUMINCRBY", [key, path, incr_str], store) do
    with {:ok, incr} <- parse_number(incr_str),
         {:ok, root} <- read_json_required(key, store) do
      do_numincrby(root, key, path, incr, store)
    end
  end

  def handle("JSON.NUMINCRBY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'json.numincrby' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.TYPE key [path]
  # ---------------------------------------------------------------------------

  def handle("JSON.TYPE", [key], store), do: handle("JSON.TYPE", [key, "$"], store)

  def handle("JSON.TYPE", [key, "$"], store) do
    with_json(key, store, &json_type/1)
  end

  def handle("JSON.TYPE", [key, path], store) do
    with_json_at_path(key, path, store, &json_type/1, _default_on_miss = nil)
  end

  def handle("JSON.TYPE", [], _store) do
    {:error, "ERR wrong number of arguments for 'json.type' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.STRLEN key [path]
  # ---------------------------------------------------------------------------

  def handle("JSON.STRLEN", [key], store), do: handle("JSON.STRLEN", [key, "$"], store)

  def handle("JSON.STRLEN", [key, "$"], store) do
    case read_json(key, store) do
      nil -> nil
      {:error, _} = err -> err
      {:ok, root} when is_binary(root) -> String.length(root)
      {:ok, _} -> {:error, "ERR value at path is not a string"}
    end
  end

  def handle("JSON.STRLEN", [key, path], store) do
    with_json_at_path(key, path, store, &strlen_value/1, nil)
  end

  def handle("JSON.STRLEN", [], _store) do
    {:error, "ERR wrong number of arguments for 'json.strlen' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.OBJKEYS key [path]
  # ---------------------------------------------------------------------------

  def handle("JSON.OBJKEYS", [key], store), do: handle("JSON.OBJKEYS", [key, "$"], store)

  def handle("JSON.OBJKEYS", [key, "$"], store) do
    case read_json(key, store) do
      nil -> nil
      {:error, _} = err -> err
      {:ok, root} when is_map(root) -> Map.keys(root)
      {:ok, _} -> {:error, "ERR value at path is not an object"}
    end
  end

  def handle("JSON.OBJKEYS", [key, path], store) do
    with_json_at_path(key, path, store, &objkeys_value/1, nil)
  end

  def handle("JSON.OBJKEYS", [], _store) do
    {:error, "ERR wrong number of arguments for 'json.objkeys' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.OBJLEN key [path]
  # ---------------------------------------------------------------------------

  def handle("JSON.OBJLEN", [key], store), do: handle("JSON.OBJLEN", [key, "$"], store)

  def handle("JSON.OBJLEN", [key, "$"], store) do
    case read_json(key, store) do
      nil -> nil
      {:error, _} = err -> err
      {:ok, root} when is_map(root) -> map_size(root)
      {:ok, _} -> {:error, "ERR value at path is not an object"}
    end
  end

  def handle("JSON.OBJLEN", [key, path], store) do
    with_json_at_path(key, path, store, &objlen_value/1, nil)
  end

  def handle("JSON.OBJLEN", [], _store) do
    {:error, "ERR wrong number of arguments for 'json.objlen' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.ARRAPPEND key path value [value ...]
  # ---------------------------------------------------------------------------

  def handle("JSON.ARRAPPEND", [key, path | values], store) when values != [] do
    with {:ok, decoded} <- decode_json_values(values),
         {:ok, root} <- read_json_required(key, store) do
      do_arrappend(root, key, path, decoded, store)
    end
  end

  def handle("JSON.ARRAPPEND", _args, _store) do
    {:error, "ERR wrong number of arguments for 'json.arrappend' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.ARRLEN key [path]
  # ---------------------------------------------------------------------------

  def handle("JSON.ARRLEN", [key], store), do: handle("JSON.ARRLEN", [key, "$"], store)

  def handle("JSON.ARRLEN", [key, "$"], store) do
    case read_json(key, store) do
      nil -> nil
      {:error, _} = err -> err
      {:ok, root} when is_list(root) -> length(root)
      {:ok, _} -> {:error, "ERR value at path is not an array"}
    end
  end

  def handle("JSON.ARRLEN", [key, path], store) do
    with_json_at_path(key, path, store, &arrlen_value/1, nil)
  end

  def handle("JSON.ARRLEN", [], _store) do
    {:error, "ERR wrong number of arguments for 'json.arrlen' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.TOGGLE key path
  # ---------------------------------------------------------------------------

  def handle("JSON.TOGGLE", [key, path], store) do
    with {:ok, root} <- read_json_required(key, store) do
      do_toggle(root, key, path, store)
    end
  end

  def handle("JSON.TOGGLE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'json.toggle' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.CLEAR key [path]
  # ---------------------------------------------------------------------------

  def handle("JSON.CLEAR", [key], store), do: handle("JSON.CLEAR", [key, "$"], store)

  def handle("JSON.CLEAR", [key, "$"], store) do
    case read_json(key, store) do
      nil ->
        0

      {:error, _} = err ->
        err

      {:ok, root} ->
        write_json(key, clear_value(root), store)
        1
    end
  end

  def handle("JSON.CLEAR", [key, path], store) do
    do_json_clear_path(key, path, store)
  end

  def handle("JSON.CLEAR", [], _store) do
    {:error, "ERR wrong number of arguments for 'json.clear' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.MGET key [key ...] path
  # ---------------------------------------------------------------------------

  def handle("JSON.MGET", args, store) when length(args) >= 2 do
    {keys, [path]} = Enum.split(args, length(args) - 1)

    case parse_path(path) do
      :error -> {:error, "ERR invalid JSONPath syntax"}
      segments -> mget_many(keys, segments, store)
    end
  end

  def handle("JSON.MGET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'json.mget' command"}
  end

  # ===========================================================================
  # Private — high-level command helpers
  # ===========================================================================

  # Applies a function to the root JSON at key. Returns nil for missing keys.
  defp with_json(key, store, fun) do
    case read_json(key, store) do
      nil -> nil
      {:error, _} = err -> err
      {:ok, root} -> fun.(root)
    end
  end

  # Applies a function to the value at a JSONPath inside a key.
  # Returns `default_on_miss` when the path doesn't exist within the document.
  defp with_json_at_path(key, path, store, fun, default_on_miss) do
    case read_json(key, store) do
      nil -> nil
      {:error, _} = err -> err
      {:ok, root} -> apply_at_path(root, parse_path(path), fun, default_on_miss)
    end
  end

  defp apply_at_path(root, segments, fun, default_on_miss) do
    case get_at_path(root, segments) do
      {:ok, val} -> fun.(val)
      :not_found -> default_on_miss
    end
  end

  # Deletes the entire key if it holds a JSON value.
  defp do_json_del_root(key, store) do
    case read_json(key, store) do
      nil ->
        0

      {:error, _} = err ->
        err

      {:ok, _root} ->
        Ops.delete(store, key)
        1
    end
  end

  # Deletes a nested path within a JSON document.
  defp do_json_del_path(key, path, store) do
    case read_json(key, store) do
      nil -> 0
      {:error, _} = err -> err
      {:ok, root} -> do_delete_path(root, key, path, store)
    end
  end

  defp do_delete_path(root, key, path, store) do
    case delete_at_path(root, parse_path(path)) do
      {:ok, new_root} ->
        write_json(key, new_root, store)
        1

      :not_found ->
        0
    end
  end

  # Performs NUMINCRBY on a loaded root document.
  defp do_numincrby(root, key, path, incr, store) do
    segments = parse_path(path)

    case get_at_path(root, segments) do
      {:ok, current} when is_number(current) ->
        new_val = current + incr
        {:ok, new_root} = set_at_path(root, segments, new_val)
        write_json(key, new_root, store)
        Jason.encode!(new_val)

      {:ok, _} ->
        {:error, "ERR value at path is not a number"}

      :not_found ->
        {:error, "ERR path does not exist"}

      {:error, _} = err ->
        err
    end
  end

  # Performs ARRAPPEND on a loaded root document.
  defp do_arrappend(root, key, path, new_values, store) do
    segments = parse_path(path)

    case get_at_path(root, segments) do
      {:ok, arr} when is_list(arr) ->
        new_arr = arr ++ new_values
        {:ok, new_root} = set_at_path(root, segments, new_arr)
        write_json(key, new_root, store)
        length(new_arr)

      {:ok, _} ->
        {:error, "ERR value at path is not an array"}

      :not_found ->
        {:error, "ERR path does not exist"}

      {:error, _} = err ->
        err
    end
  end

  # Performs TOGGLE on a loaded root document.
  defp do_toggle(root, key, path, store) do
    segments = parse_path(path)

    case get_at_path(root, segments) do
      {:ok, val} when is_boolean(val) ->
        new_val = not val
        {:ok, new_root} = set_at_path(root, segments, new_val)
        write_json(key, new_root, store)
        Jason.encode!(new_val)

      {:ok, _} ->
        {:error, "ERR value at path is not a boolean"}

      :not_found ->
        {:error, "ERR path does not exist"}

      {:error, _} = err ->
        err
    end
  end

  # Clears a value at a nested path.
  defp do_json_clear_path(key, path, store) do
    case read_json(key, store) do
      nil -> 0
      {:error, _} = err -> err
      {:ok, root} -> clear_path_in_root(root, key, path, store)
    end
  end

  defp clear_path_in_root(root, key, path, store) do
    segments = parse_path(path)

    case get_at_path(root, segments) do
      {:ok, val} ->
        {:ok, new_root} = set_at_path(root, segments, clear_value(val))
        write_json(key, new_root, store)
        1

      :not_found ->
        0

      {:error, _} = err ->
        err
    end
  end

  defp mget_many(keys, segments, store) do
    store
    |> Ops.batch_get(keys)
    |> Enum.map(&mget_one_raw(&1, segments))
  end

  defp mget_one_raw(nil, _segments), do: nil

  defp mget_one_raw(raw, segments) do
    case decode_raw_json(raw) do
      {:ok, root} -> mget_encode(root, segments)
      {:error, _} -> nil
    end
  end

  defp mget_encode(root, segments) do
    case get_at_path(root, segments) do
      {:ok, val} -> Jason.encode!(val)
      :not_found -> nil
    end
  end

  # ===========================================================================
  # Private — SET logic
  # ===========================================================================

  defp do_json_set(key, "$", new_value, nx?, xx?, store) do
    case root_json_key_state(key, store) do
      :missing ->
        maybe_write_json(key, new_value, nx?, xx?, false, store)

      {:json, _root} ->
        maybe_write_json(key, new_value, nx?, xx?, true, store)

      :not_json ->
        {:error, "ERR existing key is not a JSON value"}

      :compound ->
        {:error, @wrongtype_msg}
    end
  end

  defp do_json_set(key, path, new_value, nx?, xx?, store) do
    case read_json(key, store) do
      nil -> set_on_missing_key(key, path, new_value, xx?, store)
      {:error, _} = err -> err
      {:ok, root} -> set_on_existing_key(root, key, path, new_value, nx?, xx?, store)
    end
  end

  defp set_on_missing_key(_key, _path, _new_value, true = _xx?, _store), do: nil

  defp set_on_missing_key(key, path, new_value, _xx?, store) do
    case build_from_path(parse_path(path), new_value) do
      {:ok, root} -> write_json(key, root, store)
      :error -> {:error, "ERR cannot create path in empty document"}
    end
  end

  defp set_on_existing_key(root, key, path, new_value, nx?, xx?, store) do
    case parse_path(path) do
      :error -> {:error, "ERR invalid JSONPath syntax"}
      segments -> do_set_on_existing(root, key, segments, new_value, nx?, xx?, store)
    end
  end

  defp do_set_on_existing(root, key, segments, new_value, nx?, xx?, store) do
    path_exists? = get_at_path(root, segments) != :not_found

    if blocked_by_flags?(nx?, xx?, path_exists?) do
      nil
    else
      apply_set_at_path(root, segments, key, new_value, store)
    end
  end

  defp apply_set_at_path(root, segments, key, new_value, store) do
    case set_at_path(root, segments, new_value) do
      {:ok, new_root} -> write_json(key, new_root, store)
      :not_found -> {:error, "ERR path does not exist in the JSON value"}
    end
  end

  defp root_json_key_state(key, store) do
    case Ops.get(store, key) do
      nil -> root_missing_or_compound(key, store)
      raw -> root_raw_json_state(raw)
    end
  end

  defp root_raw_json_state(raw) do
    case decode_raw_json(raw) do
      {:ok, root} -> {:json, root}
      {:error, _} -> :not_json
    end
  end

  defp root_missing_or_compound(key, store) do
    if live_compound_key?(key, store), do: :compound, else: :missing
  end

  defp maybe_write_json(key, new_value, nx?, xx?, exists?, store) do
    if blocked_by_flags?(nx?, xx?, exists?) do
      nil
    else
      write_json(key, new_value, store)
    end
  end

  # Returns true if NX/XX flags block the operation.
  defp blocked_by_flags?(true = _nx?, _xx?, true = _exists?), do: true
  defp blocked_by_flags?(_nx?, true = _xx?, false = _exists?), do: true
  defp blocked_by_flags?(_nx?, _xx?, _exists?), do: false

  # ===========================================================================
  # Private — GET helpers
  # ===========================================================================

  defp do_json_get(root, [path]) do
    case get_at_path(root, parse_path(path)) do
      {:ok, val} -> Jason.encode!(val)
      :not_found -> nil
      {:error, _} = err -> err
    end
  end

  defp do_json_get(root, paths) do
    result = Map.new(paths, &path_to_kv(root, &1))
    Jason.encode!(result)
  end

  defp path_to_kv(root, path) do
    case get_at_path(root, parse_path(path)) do
      {:ok, val} -> {path, val}
      :not_found -> {path, nil}
    end
  end

  # ===========================================================================
  # Private — JSON storage helpers
  # ===========================================================================

  # Reads a JSON value from the store. Returns {:ok, decoded}, nil, or {:error, msg}.
  @spec read_json(binary(), map()) :: {:ok, term()} | nil | {:error, binary()}
  defp read_json(key, store) do
    case Ops.get(store, key) do
      nil -> missing_or_compound_json(key, store)
      raw -> decode_raw_json(raw)
    end
  end

  defp missing_or_compound_json(key, store) do
    if live_compound_key?(key, store), do: {:error, @wrongtype_msg}, else: nil
  end

  defp live_compound_key?(key, store) do
    compound_type_marker?(key, store) and TypeRegistry.get_type(key, store) != "none"
  end

  defp compound_type_marker?(key, store) do
    Ops.has_compound?(store) and
      Ops.compound_get(store, key, CompoundKey.type_key(key)) != nil
  end

  defp decode_raw_json(raw) do
    case decode_stored(raw) do
      {:ok, json_str} -> parse_json_str(json_str)
      :not_json -> {:error, "ERR existing key is not a JSON value"}
    end
  end

  defp parse_json_str(json_str) do
    case Jason.decode(json_str) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, _} -> {:error, "ERR corrupt JSON value"}
    end
  end

  # Like read_json but returns error tuple for missing keys.
  @spec read_json_required(binary(), map()) :: {:ok, term()} | {:error, binary()}
  defp read_json_required(key, store) do
    case read_json(key, store) do
      nil -> {:error, "ERR key does not exist"}
      other -> other
    end
  end

  # Writes a JSON value to the store, wrapping it with the type tag.
  @spec write_json(binary(), term(), map()) :: :ok
  defp write_json(key, value, store) do
    json_str = Jason.encode!(value)
    raw = :erlang.term_to_binary({:json, json_str})
    Ops.put(store, key, raw, 0)
  end

  # Decodes a stored binary. JSON values are stored as `:erlang.term_to_binary({:json, str})`.
  @spec decode_stored(binary()) :: {:ok, binary()} | :not_json
  defp decode_stored(raw) do
    case safe_binary_to_term(raw) do
      {:json, json_str} when is_binary(json_str) -> {:ok, json_str}
      _ -> :not_json
    end
  rescue
    ArgumentError -> :not_json
  end

  defp safe_binary_to_term(bin), do: :erlang.binary_to_term(bin, [:safe])

  # ===========================================================================
  # Private — JSON value decoding
  # ===========================================================================

  defp decode_json_value(str) do
    case Jason.decode(str) do
      {:ok, val} -> {:ok, val}
      {:error, _} -> {:error, "ERR invalid JSON value"}
    end
  end

  defp decode_json_values(values) do
    Enum.reduce_while(values, {:ok, []}, fn v, {:ok, acc} ->
      case Jason.decode(v) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, _} -> {:halt, {:error, "ERR invalid JSON value"}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  # ===========================================================================
  # Private — JSONPath parser
  # ===========================================================================

  # Parses a JSONPath string into a list of path segments.
  # Supports: $, $.field, $.field.subfield, $[0], $.field[0].name
  @spec parse_path(binary()) :: [binary() | non_neg_integer()] | :error
  defp parse_path("$"), do: []
  defp parse_path(<<"$", rest::binary>>), do: parse_path_segments(rest, [])
  defp parse_path(_), do: :error

  defp parse_path_segments(<<>>, acc), do: Enum.reverse(acc)

  defp parse_path_segments(<<".", rest::binary>>, acc) do
    {field, remainder} = read_field(rest)
    parse_path_segments(remainder, [field | acc])
  end

  defp parse_path_segments(<<"[", rest::binary>>, acc) do
    case read_bracket(rest) do
      {:ok, segment, remainder} -> parse_path_segments(remainder, [segment | acc])
      :error -> :error
    end
  end

  defp parse_path_segments(_, acc), do: Enum.reverse(acc)

  # Reads a field name up to the next `.`, `[`, or end of string.
  defp read_field(str) do
    case :binary.match(str, [<<".">>, <<"[">>]) do
      {pos, _len} ->
        {binary_part(str, 0, pos), binary_part(str, pos, byte_size(str) - pos)}

      :nomatch ->
        {str, <<>>}
    end
  end

  # Reads a bracket expression: integer index or quoted string key.
  defp read_bracket(str) do
    case :binary.match(str, <<"]">>) do
      {pos, 1} -> parse_bracket_inner(str, pos)
      _ -> :error
    end
  end

  defp parse_bracket_inner(str, pos) do
    inner = binary_part(str, 0, pos)
    remainder = binary_part(str, pos + 1, byte_size(str) - pos - 1)
    parse_bracket_content(inner, remainder)
  end

  defp parse_bracket_content(<<"\"", _::binary>> = inner, remainder) do
    {:ok, String.slice(inner, 1..-2//1), remainder}
  end

  defp parse_bracket_content(<<"'", _::binary>> = inner, remainder) do
    {:ok, String.slice(inner, 1..-2//1), remainder}
  end

  defp parse_bracket_content(inner, remainder) do
    case Integer.parse(inner) do
      {idx, ""} -> {:ok, idx, remainder}
      _ -> :error
    end
  end

  # ===========================================================================
  # Private — JSONPath traversal
  # ===========================================================================

  # Gets a value at a parsed path within a decoded JSON structure.
  @spec get_at_path(term(), [binary() | non_neg_integer()]) ::
          {:ok, term()} | :not_found
  defp get_at_path(_value, :error), do: {:error, "ERR invalid JSONPath syntax"}
  defp get_at_path(value, []), do: {:ok, value}

  defp get_at_path(map, [key | rest]) when is_map(map) and is_binary(key) do
    case Map.fetch(map, key) do
      {:ok, val} -> get_at_path(val, rest)
      :error -> :not_found
    end
  end

  defp get_at_path(list, [idx | rest]) when is_list(list) and is_integer(idx) do
    actual_idx = normalize_index(idx, length(list))

    if valid_index?(actual_idx, length(list)) do
      get_at_path(Enum.at(list, actual_idx), rest)
    else
      :not_found
    end
  end

  defp get_at_path(_value, [_ | _rest]), do: :not_found

  # Sets a value at a parsed path within a decoded JSON structure.
  @spec set_at_path(term(), [binary() | non_neg_integer()], term()) ::
          {:ok, term()} | :not_found
  defp set_at_path(_current, [], new_value), do: {:ok, new_value}

  defp set_at_path(map, [key | rest], new_value) when is_map(map) and is_binary(key) do
    case set_at_path(Map.get(map, key), rest, new_value) do
      {:ok, updated} -> {:ok, Map.put(map, key, updated)}
      :not_found -> :not_found
    end
  end

  defp set_at_path(list, [idx | rest], new_value) when is_list(list) and is_integer(idx) do
    set_at_list_index(list, idx, rest, new_value)
  end

  # Setting a field on nil creates a new map (auto-vivification for root-level set)
  defp set_at_path(nil, [key | rest], new_value) when is_binary(key) do
    set_at_path(%{}, [key | rest], new_value)
  end

  defp set_at_path(_value, [_ | _rest], _new_value), do: :not_found

  defp set_at_list_index(list, idx, rest, new_value) do
    actual_idx = normalize_index(idx, length(list))

    if valid_index?(actual_idx, length(list)) do
      case set_at_path(Enum.at(list, actual_idx), rest, new_value) do
        {:ok, updated} -> {:ok, List.replace_at(list, actual_idx, updated)}
        :not_found -> :not_found
      end
    else
      :not_found
    end
  end

  # Deletes a value at a parsed path within a decoded JSON structure.
  @spec delete_at_path(term(), [binary() | non_neg_integer()]) ::
          {:ok, term()} | :not_found
  defp delete_at_path(_value, :error), do: {:error, "ERR invalid JSONPath syntax"}
  defp delete_at_path(_value, []), do: :not_found

  defp delete_at_path(map, [key]) when is_map(map) and is_binary(key) do
    if Map.has_key?(map, key), do: {:ok, Map.delete(map, key)}, else: :not_found
  end

  defp delete_at_path(list, [idx]) when is_list(list) and is_integer(idx) do
    actual_idx = normalize_index(idx, length(list))

    if valid_index?(actual_idx, length(list)),
      do: {:ok, List.delete_at(list, actual_idx)},
      else: :not_found
  end

  defp delete_at_path(map, [key | rest]) when is_map(map) and is_binary(key) do
    with {:ok, child} <- Map.fetch(map, key),
         {:ok, updated_child} <- delete_at_path(child, rest) do
      {:ok, Map.put(map, key, updated_child)}
    else
      _ -> :not_found
    end
  end

  defp delete_at_path(list, [idx | rest]) when is_list(list) and is_integer(idx) do
    delete_at_list_index(list, idx, rest)
  end

  defp delete_at_path(_value, [_ | _rest]), do: :not_found

  defp delete_at_list_index(list, idx, rest) do
    actual_idx = normalize_index(idx, length(list))

    if valid_index?(actual_idx, length(list)) do
      case delete_at_path(Enum.at(list, actual_idx), rest) do
        {:ok, updated_child} -> {:ok, List.replace_at(list, actual_idx, updated_child)}
        :not_found -> :not_found
      end
    else
      :not_found
    end
  end

  # ===========================================================================
  # Private — index helpers
  # ===========================================================================

  defp normalize_index(idx, len) when idx < 0, do: len + idx
  defp normalize_index(idx, _len), do: idx

  defp valid_index?(idx, len), do: idx >= 0 and idx < len

  # ===========================================================================
  # Private — path construction for new documents
  # ===========================================================================

  # Builds a nested JSON structure from path segments and a value.
  # E.g., ["a", "b"] with value 1 => %{"a" => %{"b" => 1}}
  @spec build_from_path([binary() | non_neg_integer()], term()) :: {:ok, term()} | :error
  defp build_from_path([], value), do: {:ok, value}

  defp build_from_path([key | rest], value) when is_binary(key) do
    case build_from_path(rest, value) do
      {:ok, inner} -> {:ok, %{key => inner}}
      :error -> :error
    end
  end

  # Cannot auto-create array indices on empty documents
  defp build_from_path([_ | _], _value), do: :error

  # ===========================================================================
  # Private — type helpers
  # ===========================================================================

  @spec json_type(term()) :: binary()
  defp json_type(val) when is_binary(val), do: "string"
  defp json_type(val) when is_integer(val), do: "integer"
  defp json_type(val) when is_float(val), do: "number"
  defp json_type(val) when is_boolean(val), do: "boolean"
  defp json_type(nil), do: "null"
  defp json_type(val) when is_map(val), do: "object"
  defp json_type(val) when is_list(val), do: "array"

  # ===========================================================================
  # Private — value inspection helpers (for with_json_at_path callbacks)
  # ===========================================================================

  defp strlen_value(val) when is_binary(val), do: String.length(val)
  defp strlen_value(_), do: {:error, "ERR value at path is not a string"}

  defp objkeys_value(val) when is_map(val), do: Map.keys(val)
  defp objkeys_value(_), do: {:error, "ERR value at path is not an object"}

  defp objlen_value(val) when is_map(val), do: map_size(val)
  defp objlen_value(_), do: {:error, "ERR value at path is not an object"}

  defp arrlen_value(val) when is_list(val), do: length(val)
  defp arrlen_value(_), do: {:error, "ERR value at path is not an array"}

  # ===========================================================================
  # Private — clear helpers
  # ===========================================================================

  @spec clear_value(term()) :: term()
  defp clear_value(val) when is_map(val), do: %{}
  defp clear_value(val) when is_list(val), do: []
  defp clear_value(val) when is_number(val), do: 0
  defp clear_value(val), do: val

  # ===========================================================================
  # Private — number parsing
  # ===========================================================================

  @spec parse_number(binary()) :: {:ok, number()} | {:error, binary()}
  defp parse_number(str) do
    if String.contains?(str, ".") do
      parse_float(str)
    else
      parse_integer(str)
    end
  end

  defp parse_float(str) do
    case Float.parse(str) do
      {f, ""} -> {:ok, f}
      _ -> {:error, "ERR value is not a number"}
    end
  end

  defp parse_integer(str) do
    case Integer.parse(str) do
      {i, ""} -> {:ok, i}
      _ -> {:error, "ERR value is not a number"}
    end
  end

  # ===========================================================================
  # Private — SET flag parsing
  # ===========================================================================

  @spec parse_set_flags([binary()]) :: {:ok, boolean(), boolean()} | {:error, binary()}
  defp parse_set_flags([]), do: {:ok, false, false}
  defp parse_set_flags(["NX"]), do: {:ok, true, false}
  defp parse_set_flags(["XX"]), do: {:ok, false, true}

  defp parse_set_flags([other]) do
    {:error, "ERR syntax error, option '#{other}' not recognized"}
  end

  defp parse_set_flags(_), do: {:error, "ERR syntax error"}
end
