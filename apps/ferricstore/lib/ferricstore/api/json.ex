defmodule FerricStore.API.Json do
  @moduledoc false

  import FerricStore.API.Store
  alias Ferricstore.Commands.Json
  alias Ferricstore.Store.Router

  @type key :: FerricStore.key()
  @type value :: FerricStore.value()
  @type write_error :: FerricStore.write_error()
  @type set_opts :: FerricStore.set_opts()
  @type get_opts :: FerricStore.get_opts()
  @type cas_opts :: FerricStore.cas_opts()
  @type fetch_or_compute_opts :: FerricStore.fetch_or_compute_opts()
  @type zrange_opts :: FerricStore.zrange_opts()

  @doc """
  Sets a JSON value at `path` in the document stored at `key`.

  Creates the document if it does not exist (when path is `"$"`). Uses
  JSONPath syntax for nested access. Ideal for storing user preferences,
  feature flags, and nested configuration.

  ## Parameters

    * `key` - the document key
    * `path` - JSONPath expression (e.g. `"$"`, `"$.settings.theme"`)
    * `value` - JSON-encoded string to store

  ## Returns

    * `:ok` on success.
    * `{:error, reason}` on failure.

  ## Examples

      iex> FerricStore.json_set("user:42:prefs", "$", ~s({"theme":"dark","lang":"en"}))
      :ok

      iex> FerricStore.json_set("user:42:prefs", "$.theme", ~s("light"))
      :ok

  """
  @spec json_set(key(), binary(), binary()) :: :ok | {:error, binary()}
  def json_set(key, path, value) do
    case Router.json_set(default_ctx(), key, path, value, []) do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  @doc """
  Gets the JSON value at `path` from the document stored at `key`.

  ## Parameters

    * `key` - the document key
    * `path` - JSONPath expression (default: `"$"` for the root)

  ## Returns

    * `{:ok, json_string}` on success.
    * `{:ok, nil}` if the key does not exist.
    * `{:error, reason}` on failure.

  ## Examples

      iex> FerricStore.json_get("user:42:prefs", "$.theme")
      {:ok, "[\"dark\"]"}

      iex> FerricStore.json_get("user:42:prefs")
      {:ok, "[{\"theme\":\"dark\",\"lang\":\"en\"}]"}

  """
  @spec json_get(key(), binary()) :: {:ok, binary()} | {:error, binary()}
  def json_get(key, path \\ "$") do
    store = build_string_store(key)
    result = Json.handle_ast({:json_get, key, [{path, parse_json_path(path)}]}, store)
    wrap_result(result)
  end

  @doc """
  Deletes the value at `path` from the JSON document at `key`.

  When path is `"$"`, the entire document is deleted.

  ## Returns

    * `{:ok, deleted_count}` - number of paths deleted.
    * `{:error, reason}` on failure.

  ## Examples

      iex> FerricStore.json_del("user:42:prefs", "$.theme")
      {:ok, 1}

  """
  @spec json_del(key(), binary()) :: {:ok, term()} | {:error, binary()}
  def json_del(key, path \\ "$") do
    wrap_result(Router.json_del(default_ctx(), key, path))
  end

  @doc """
  Returns the JSON type of the value at `path` in the document at `key`.

  ## Returns

    * `{:ok, type}` where type is one of `"object"`, `"array"`, `"string"`,
      `"number"`, `"boolean"`, `"null"`.
    * `{:error, reason}` on failure.

  ## Examples

      iex> FerricStore.json_type("user:42:prefs", "$.theme")
      {:ok, ["string"]}

  """
  @spec json_type(key(), binary()) :: {:ok, binary()} | {:error, binary()}
  def json_type(key, path \\ "$") do
    store = build_string_store(key)
    result = Json.handle_ast({:json_type, key, path}, store)
    wrap_result(result)
  end

  @doc """
  Atomically increments a numeric value at `path` in the JSON document at `key`.

  ## Parameters

    * `key` - the document key
    * `path` - JSONPath to a numeric value
    * `increment` - the increment amount as a string (e.g. `"1"`, `"0.5"`)

  ## Returns

    * `{:ok, new_value_string}` on success.
    * `{:error, reason}` if the path is not a number.

  ## Examples

      iex> FerricStore.json_numincrby("config:app", "$.retry_count", "1")
      {:ok, "[4]"}

  """
  @spec json_numincrby(key(), binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def json_numincrby(key, path, increment) do
    case parse_json_number(increment) do
      {:error, _} = err ->
        err

      parsed ->
        default_ctx()
        |> Router.json_numincrby(key, path, parsed)
        |> wrap_result()
    end
  end

  @doc """
  Appends one or more JSON values to the array at `path` in the document at `key`.

  ## Parameters

    * `key` - the document key
    * `path` - JSONPath to an array
    * `values` - list of JSON-encoded strings to append

  ## Returns

    * `{:ok, new_array_length}` on success.
    * `{:error, reason}` if the path is not an array.

  ## Examples

      iex> FerricStore.json_arrappend("user:42:prefs", "$.tags", [~s("vip"), ~s("beta")])
      {:ok, [4]}

  """
  @spec json_arrappend(key(), binary(), [binary()]) :: {:ok, term()} | {:error, binary()}
  def json_arrappend(key, path, values) when is_list(values) do
    default_ctx()
    |> Router.json_arrappend(key, path, values)
    |> wrap_result()
  end

  @doc """
  Returns the length of the JSON array at `path` in the document at `key`.

  ## Examples

      iex> FerricStore.json_arrlen("user:42:prefs", "$.tags")
      {:ok, [4]}

  """
  @spec json_arrlen(key(), binary()) :: {:ok, integer()} | {:error, binary()}
  def json_arrlen(key, path \\ "$") do
    store = build_string_store(key)
    result = Json.handle_ast({:json_arrlen, key, path}, store)
    wrap_result(result)
  end

  @doc """
  Returns the length of the JSON string at `path` in the document at `key`.

  ## Examples

      iex> FerricStore.json_strlen("user:42:prefs", "$.theme")
      {:ok, [4]}

  """
  @spec json_strlen(key(), binary()) :: {:ok, integer()} | {:error, binary()}
  def json_strlen(key, path \\ "$") do
    store = build_string_store(key)
    result = Json.handle_ast({:json_strlen, key, path}, store)
    wrap_result(result)
  end

  @doc """
  Returns the keys of the JSON object at `path` in the document at `key`.

  ## Examples

      iex> FerricStore.json_objkeys("user:42:prefs")
      {:ok, [["theme", "lang"]]}

  """
  @spec json_objkeys(key(), binary()) :: {:ok, list()} | {:error, binary()}
  def json_objkeys(key, path \\ "$") do
    store = build_string_store(key)
    result = Json.handle_ast({:json_objkeys, key, path}, store)
    wrap_result(result)
  end

  @doc """
  Returns the number of keys in the JSON object at `path` in the document at `key`.

  ## Examples

      iex> FerricStore.json_objlen("user:42:prefs")
      {:ok, [2]}

  """
  @spec json_objlen(key(), binary()) :: {:ok, integer()} | {:error, binary()}
  def json_objlen(key, path \\ "$") do
    store = build_string_store(key)
    result = Json.handle_ast({:json_objlen, key, path}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Native: lock, unlock, extend, ratelimit_add
  # ---------------------------------------------------------------------------
end
