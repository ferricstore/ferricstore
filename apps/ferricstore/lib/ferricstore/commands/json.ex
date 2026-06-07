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

  alias Ferricstore.Commands.Json.Path, as: JsonPath
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.TypeRegistry

  @wrongtype_msg "WRONGTYPE Operation against a key holding the wrong kind of value"

  @doc """
  Handles typed JSON command AST terms produced by the Rust RESP parser.
  """
  @spec handle_ast(term(), map()) :: term()
  def handle_ast({tag, {:error, msg}}, _store) when is_atom(tag), do: {:error, msg}

  def handle_ast({tag, _key, {:error, msg}}, _store) when is_atom(tag), do: {:error, msg}

  def handle_ast({:json_set, key, path, value, flags}, store) do
    with {:ok, new_value} <- decode_json_value(value),
         {:ok, nx?, xx?} <- ast_set_flags(flags) do
      do_json_set(key, path, new_value, nx?, xx?, store)
    end
  end

  def handle_ast({:json_get, key, paths}, store) do
    with_json(key, store, &do_json_get_specs(&1, paths))
  end

  def handle_ast({:json_del, key, []}, store), do: do_json_del_root(key, store)
  def handle_ast({:json_del, key, path}, store), do: do_json_del_path(key, path, store)

  def handle_ast({:json_numincrby, key, path, incr}, store) do
    with {:ok, root} <- read_json_required(key, store) do
      do_numincrby(root, key, path, incr, store)
    end
  end

  def handle_ast({:json_type, key, []}, store),
    do: with_json_at_path(key, "$", store, &json_type/1, nil)

  def handle_ast({:json_type, key, path}, store) do
    with_json_at_path(key, path, store, &json_type/1, _default_on_miss = nil)
  end

  def handle_ast({:json_strlen, key, []}, store),
    do: with_json_at_path(key, "$", store, &strlen_value/1, nil)

  def handle_ast({:json_strlen, key, path}, store) do
    with_json_at_path(key, path, store, &strlen_value/1, nil)
  end

  def handle_ast({:json_objkeys, key, []}, store),
    do: with_json_at_path(key, "$", store, &objkeys_value/1, nil)

  def handle_ast({:json_objkeys, key, path}, store) do
    with_json_at_path(key, path, store, &objkeys_value/1, nil)
  end

  def handle_ast({:json_objlen, key, []}, store),
    do: with_json_at_path(key, "$", store, &objlen_value/1, nil)

  def handle_ast({:json_objlen, key, path}, store) do
    with_json_at_path(key, path, store, &objlen_value/1, nil)
  end

  def handle_ast({:json_arrappend, key, path, values}, store) do
    with {:ok, decoded} <- decode_json_values(values),
         {:ok, root} <- read_json_required(key, store) do
      do_arrappend(root, key, path, decoded, store)
    end
  end

  def handle_ast({:json_arrlen, key, []}, store),
    do: with_json_at_path(key, "$", store, &arrlen_value/1, nil)

  def handle_ast({:json_arrlen, key, path}, store) do
    with_json_at_path(key, path, store, &arrlen_value/1, nil)
  end

  def handle_ast({:json_toggle, key, path}, store) do
    with {:ok, root} <- read_json_required(key, store) do
      do_toggle(root, key, path, store)
    end
  end

  def handle_ast({:json_clear, key, []}, store), do: do_json_clear_path(key, "$", store)
  def handle_ast({:json_clear, key, path}, store), do: do_json_clear_path(key, path, store)
  def handle_ast({:json_mget, keys, path}, store), do: mget_many(keys, path, store)

  @doc """
  Handles a JSON command.
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

  def handle("JSON.DEL", _args, _store) do
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

  def handle("JSON.TYPE", _args, _store) do
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

  def handle("JSON.STRLEN", _args, _store) do
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

  def handle("JSON.OBJKEYS", _args, _store) do
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

  def handle("JSON.OBJLEN", _args, _store) do
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

  def handle("JSON.ARRLEN", _args, _store) do
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
        write_json_result(key, clear_value(root), store, 1)
    end
  end

  def handle("JSON.CLEAR", [key, path], store) do
    do_json_clear_path(key, path, store)
  end

  def handle("JSON.CLEAR", _args, _store) do
    {:error, "ERR wrong number of arguments for 'json.clear' command"}
  end

  # ---------------------------------------------------------------------------
  # JSON.MGET key [key ...] path
  # ---------------------------------------------------------------------------

  def handle("JSON.MGET", args, store) when length(args) >= 2 do
    {keys, [path]} = Enum.split(args, length(args) - 1)

    case JsonPath.parse(path) do
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
      {:ok, root} -> apply_at_path(root, JsonPath.parse(path), fun, default_on_miss)
    end
  end

  defp apply_at_path(root, segments, fun, default_on_miss) do
    case JsonPath.get(root, segments) do
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
        case Ops.delete(store, key) do
          :ok -> 1
          {:error, _} = err -> err
        end
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
    case JsonPath.delete(root, JsonPath.parse(path)) do
      {:ok, new_root} ->
        write_json_result(key, new_root, store, 1)

      :not_found ->
        0

      {:error, _} = err ->
        err
    end
  end

  # Performs NUMINCRBY on a loaded root document.
  defp do_numincrby(root, key, path, incr, store) do
    segments = JsonPath.parse(path)

    case JsonPath.get(root, segments) do
      {:ok, current} when is_number(current) ->
        new_val = current + incr
        {:ok, new_root} = JsonPath.set(root, segments, new_val)
        write_json_result(key, new_root, store, Jason.encode!(new_val))

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
    segments = JsonPath.parse(path)

    case JsonPath.get(root, segments) do
      {:ok, arr} when is_list(arr) ->
        new_arr = arr ++ new_values
        {:ok, new_root} = JsonPath.set(root, segments, new_arr)
        write_json_result(key, new_root, store, length(new_arr))

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
    segments = JsonPath.parse(path)

    case JsonPath.get(root, segments) do
      {:ok, val} when is_boolean(val) ->
        new_val = not val
        {:ok, new_root} = JsonPath.set(root, segments, new_val)
        write_json_result(key, new_root, store, Jason.encode!(new_val))

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
    segments = JsonPath.parse(path)

    case JsonPath.get(root, segments) do
      {:ok, val} ->
        {:ok, new_root} = JsonPath.set(root, segments, clear_value(val))
        write_json_result(key, new_root, store, 1)

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
    case JsonPath.get(root, segments) do
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
    case JsonPath.parse(path) do
      :error -> {:error, "ERR invalid JSONPath syntax"}
      segments -> build_missing_json_path(key, segments, new_value, store)
    end
  end

  defp build_missing_json_path(key, segments, new_value, store) do
    case JsonPath.build(segments, new_value) do
      {:ok, root} -> write_json(key, root, store)
      :error -> {:error, "ERR cannot create path in empty document"}
    end
  end

  defp set_on_existing_key(root, key, path, new_value, nx?, xx?, store) do
    case JsonPath.parse(path) do
      :error -> {:error, "ERR invalid JSONPath syntax"}
      segments -> do_set_on_existing(root, key, segments, new_value, nx?, xx?, store)
    end
  end

  defp do_set_on_existing(root, key, segments, new_value, nx?, xx?, store) do
    path_exists? = JsonPath.get(root, segments) != :not_found

    if blocked_by_flags?(nx?, xx?, path_exists?) do
      nil
    else
      apply_set_at_path(root, segments, key, new_value, store)
    end
  end

  defp apply_set_at_path(root, segments, key, new_value, store) do
    case JsonPath.set(root, segments, new_value) do
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
    case JsonPath.get(root, JsonPath.parse(path)) do
      {:ok, val} -> Jason.encode!(val)
      :not_found -> nil
      {:error, _} = err -> err
    end
  end

  defp do_json_get(root, paths) do
    result = Map.new(paths, &path_to_kv(root, &1))
    Jason.encode!(result)
  end

  defp do_json_get_specs(root, []), do: Jason.encode!(root)

  defp do_json_get_specs(root, [{_raw_path, segments}]) do
    case JsonPath.get(root, segments) do
      {:ok, val} -> Jason.encode!(val)
      :not_found -> nil
      {:error, _} = err -> err
    end
  end

  defp do_json_get_specs(root, path_specs) do
    result =
      Map.new(path_specs, fn {raw_path, segments} ->
        case JsonPath.get(root, segments) do
          {:ok, val} -> {raw_path, val}
          :not_found -> {raw_path, nil}
        end
      end)

    Jason.encode!(result)
  end

  defp path_to_kv(root, path) do
    case JsonPath.get(root, JsonPath.parse(path)) do
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
  @spec write_json(binary(), term(), map()) :: :ok | {:error, term()}
  defp write_json(key, value, store) do
    json_str = Jason.encode!(value)
    raw = :erlang.term_to_binary({:json, json_str})
    Ops.put(store, key, raw, 0)
  end

  defp write_json_result(key, value, store, success) do
    case write_json(key, value, store) do
      :ok -> success
      {:error, _} = err -> err
    end
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

  defp ast_set_flags([]), do: {:ok, false, false}
  defp ast_set_flags([:nx]), do: {:ok, true, false}
  defp ast_set_flags([:xx]), do: {:ok, false, true}
  defp ast_set_flags({:error, msg}), do: {:error, msg}
  defp ast_set_flags(_), do: {:error, "ERR syntax error"}

  @spec parse_set_flags([binary()]) :: {:ok, boolean(), boolean()} | {:error, binary()}
  defp parse_set_flags([]), do: {:ok, false, false}
  defp parse_set_flags(["NX"]), do: {:ok, true, false}
  defp parse_set_flags(["XX"]), do: {:ok, false, true}

  defp parse_set_flags([opt]) when is_binary(opt) do
    case String.upcase(opt) do
      ^opt -> {:error, "ERR syntax error, option '#{opt}' not recognized"}
      normalized -> parse_set_flags([normalized])
    end
  end

  defp parse_set_flags([other]) do
    {:error, "ERR syntax error, option '#{other}' not recognized"}
  end

  defp parse_set_flags(_), do: {:error, "ERR syntax error"}
end
