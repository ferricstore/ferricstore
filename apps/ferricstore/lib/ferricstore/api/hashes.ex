defmodule FerricStore.API.Hashes do
  @moduledoc false

  import FerricStore.API.Store
  alias Ferricstore.Store.Router
  alias Ferricstore.Commands.Hash

  @type key :: FerricStore.key()
  @type value :: FerricStore.value()
  @type write_error :: FerricStore.write_error()
  @type set_opts :: FerricStore.set_opts()
  @type get_opts :: FerricStore.get_opts()
  @type cas_opts :: FerricStore.cas_opts()
  @type fetch_or_compute_opts :: FerricStore.fetch_or_compute_opts()
  @type zrange_opts :: FerricStore.zrange_opts()

  @doc """
  Sets one or more fields in the hash stored at `key`.

  `fields` is a map of `%{field_name => value}`. Field names and values are
  stored as binaries. If the hash does not exist, a new one is created.
  Existing fields are overwritten.

  ## Examples

      iex> FerricStore.hset("user:42", %{"name" => "alice", "age" => "30"})
      :ok

      iex> FerricStore.hset("user:42", %{"name" => "bob"})
      :ok

  """
  @spec hset(key(), %{binary() => binary()}) :: :ok
  def hset(key, fields) when is_map(fields) do
    store = build_compound_store(key)

    args =
      Enum.flat_map(fields, fn {k, v} -> [to_string(k), to_string(v)] end)

    case Hash.handle_ast({:hset, [key | args]}, store) do
      {:error, _} = err -> err
      _count -> :ok
    end
  end

  @doc """
  Gets the value of a single field from the hash stored at `key`.

  Returns `{:ok, value}` if the field exists, or `{:ok, nil}` if the field
  or the hash does not exist.

  ## Examples

      iex> FerricStore.hset("user:42", %{"name" => "alice", "age" => "30"})
      iex> FerricStore.hget("user:42", "name")
      {:ok, "alice"}

      iex> FerricStore.hget("user:42", "nonexistent_field")
      {:ok, nil}

      iex> FerricStore.hget("no_such_hash", "field")
      {:ok, nil}

  """
  @spec hget(key(), binary()) :: {:ok, binary() | nil}
  def hget(key, field) do
    store = build_compound_store(key)

    case Hash.handle_ast({:hget, key, to_string(field)}, store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Gets all fields and values from the hash stored at `key`.

  Returns `{:ok, map}` where `map` is a `%{field => value}` map. If the key
  does not exist, returns `{:ok, %{}}`.

  ## Examples

      iex> FerricStore.hset("user:42", %{"name" => "alice", "age" => "30"})
      iex> FerricStore.hgetall("user:42")
      {:ok, %{"name" => "alice", "age" => "30"}}

      iex> FerricStore.hgetall("no_such_hash")
      {:ok, %{}}

  """
  @spec hgetall(key()) :: {:ok, %{binary() => binary()}}
  def hgetall(key) do
    store = build_compound_store(key)

    case Hash.handle_ast({:hgetall, key}, store) do
      {:error, _} = err ->
        err

      flat_list ->
        map =
          flat_list
          |> Enum.chunk_every(2)
          |> Map.new(fn [f, v] -> {f, v} end)

        {:ok, map}
    end
  end

  # ---------------------------------------------------------------------------
  # Lists
  #
  # Note: Blocking list commands (BLPOP, BRPOP, BLMOVE, BLMPOP) are only
  # available via TCP/RESP3. The embedded API provides non-blocking variants
  # (lpop, rpop, lmove). For blocking semantics, poll with lpop/rpop or use
  # Phoenix PubSub to subscribe to list-push events.
  # ---------------------------------------------------------------------------


  @doc """
  Deletes one or more fields from the hash stored at `key`.

  Fields that do not exist are ignored. Returns the count of fields actually
  removed.

  ## Examples

      iex> FerricStore.hset("user:42", %{"name" => "alice", "age" => "30", "email" => "a@b.c"})
      iex> FerricStore.hdel("user:42", ["age", "email"])
      {:ok, 2}

      iex> FerricStore.hdel("user:42", ["nonexistent"])
      {:ok, 0}

  """
  @spec hdel(key(), [binary()]) :: {:ok, non_neg_integer()}
  def hdel(key, fields) when is_list(fields) do
    store = build_compound_store(key)
    str_fields = Enum.map(fields, &to_string/1)

    case Hash.handle_ast({:hdel, [key | str_fields]}, store) do
      {:error, _} = err -> err
      count -> {:ok, count}
    end
  end

  @doc """
  Returns whether `field` exists in the hash stored at `key`.

  Returns `true` if the field exists, `false` otherwise. Returns `false`
  if the key itself does not exist.

  ## Examples

      iex> FerricStore.hset("user:42", %{"name" => "alice"})
      iex> FerricStore.hexists("user:42", "name")
      true

      iex> FerricStore.hexists("user:42", "missing")
      false

      iex> FerricStore.hexists("no_such_hash", "field")
      false

  """
  @spec hexists(key(), binary()) :: boolean()
  def hexists(key, field) do
    store = build_compound_store(key)

    case Hash.handle_ast({:hexists, key, to_string(field)}, store) do
      {:error, _} = err -> err
      1 -> true
      0 -> false
    end
  end

  @doc """
  Returns the number of fields in the hash stored at `key`.

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      iex> FerricStore.hset("user:42", %{"name" => "alice", "age" => "30", "email" => "a@b.c"})
      iex> FerricStore.hlen("user:42")
      {:ok, 3}

      iex> FerricStore.hlen("no_such_hash")
      {:ok, 0}

  """
  @spec hlen(key()) :: {:ok, non_neg_integer()}
  def hlen(key) do
    store = build_compound_store(key)

    case Hash.handle_ast({:hlen, key}, store) do
      {:error, _} = err -> err
      count -> {:ok, count}
    end
  end

  @doc """
  Returns all field names from the hash stored at `key`.

  Returns `{:ok, []}` if the key does not exist. The order of returned field
  names is not guaranteed.

  ## Examples

      iex> FerricStore.hset("user:42", %{"name" => "alice", "age" => "30"})
      iex> {:ok, fields} = FerricStore.hkeys("user:42")
      iex> Enum.sort(fields)
      ["age", "name"]

      iex> FerricStore.hkeys("no_such_hash")
      {:ok, []}

  """
  @spec hkeys(key()) :: {:ok, [binary()]}
  def hkeys(key) do
    store = build_compound_store(key)

    case Hash.handle_ast({:hkeys, key}, store) do
      {:error, _} = err -> err
      keys_list -> {:ok, keys_list}
    end
  end

  @doc """
  Returns all field values from the hash stored at `key`.

  Returns `{:ok, []}` if the key does not exist. The order of returned values
  corresponds to the order of fields (not guaranteed to be insertion order).

  ## Examples

      iex> FerricStore.hset("user:42", %{"name" => "alice", "age" => "30"})
      iex> {:ok, vals} = FerricStore.hvals("user:42")
      iex> Enum.sort(vals)
      ["30", "alice"]

      iex> FerricStore.hvals("no_such_hash")
      {:ok, []}

  """
  @spec hvals(key()) :: {:ok, [binary()]}
  def hvals(key) do
    store = build_compound_store(key)

    case Hash.handle_ast({:hvals, key}, store) do
      {:error, _} = err -> err
      vals_list -> {:ok, vals_list}
    end
  end

  @doc """
  Returns values for the specified `fields` from the hash at `key`.

  Returns `nil` for fields that do not exist. The order of returned values
  matches the order of the requested fields.

  ## Examples

      iex> FerricStore.hset("user:42", %{"name" => "alice", "age" => "30"})
      iex> FerricStore.hmget("user:42", ["name", "missing", "age"])
      {:ok, ["alice", nil, "30"]}

      iex> FerricStore.hmget("no_such_hash", ["a", "b"])
      {:ok, [nil, nil]}

  """
  @spec hmget(key(), [binary()]) :: {:ok, [binary() | nil]}
  def hmget(key, fields) when is_list(fields) do
    store = build_compound_store(key)
    str_fields = Enum.map(fields, &to_string/1)

    case Hash.handle_ast({:hmget, [key | str_fields]}, store) do
      {:error, _} = err -> err
      values -> {:ok, values}
    end
  end

  @doc """
  Increments the integer value of `field` in the hash at `key` by `amount`.

  If the field does not exist, it is created with `0` before incrementing.
  Returns `{:error, reason}` if the field value is not a valid integer.

  ## Examples

      iex> FerricStore.hset("user:42", %{"login_count" => "10"})
      iex> FerricStore.hincrby("user:42", "login_count", 5)
      {:ok, 15}

      iex> FerricStore.hincrby("user:42", "new_counter", 1)
      {:ok, 1}

  """
  @spec hincrby(key(), binary(), integer()) :: {:ok, integer()} | {:error, binary()}
  def hincrby(key, field, amount) when is_integer(amount) do
    case Router.hincrby(default_ctx(), key, to_string(field), amount) do
      {:error, _} = err -> err
      new_val -> {:ok, new_val}
    end
  end

  @doc """
  Increments the float value of `field` in the hash at `key` by `amount`.

  If the field does not exist, it is created with `0` before incrementing.
  Returns the new value as a string. Returns `{:error, reason}` if the field
  value is not a valid number.

  ## Examples

      iex> FerricStore.hset("product:99", %{"price" => "10.0"})
      iex> FerricStore.hincrbyfloat("product:99", "price", 2.5)
      {:ok, "12.5"}

      iex> FerricStore.hincrbyfloat("product:99", "discount", 0.15)
      {:ok, "0.15"}

  """
  @spec hincrbyfloat(key(), binary(), float()) :: {:ok, binary()} | {:error, binary()}
  def hincrbyfloat(key, field, amount) when is_number(amount) do
    case Router.hincrbyfloat(default_ctx(), key, to_string(field), amount * 1.0) do
      {:error, _} = err -> err
      result_str -> {:ok, result_str}
    end
  end

  @doc """
  Sets `field` in the hash at `key` only if the field does not already exist.

  Returns `{:ok, true}` if the field was set, `{:ok, false}` if it already
  existed.

  ## Examples

      iex> FerricStore.hsetnx("user:42", "name", "alice")
      {:ok, true}

      iex> FerricStore.hsetnx("user:42", "name", "bob")
      {:ok, false}

  """
  @spec hsetnx(key(), binary(), binary()) :: {:ok, boolean()}
  def hsetnx(key, field, value) do
    store = build_compound_store(key)

    case Hash.handle_ast({:hsetnx, key, to_string(field), to_string(value)}, store) do
      {:error, _} = err -> err
      1 -> {:ok, true}
      0 -> {:ok, false}
    end
  end

  @doc """
  Returns one or more random field names from the hash at `key`.

  Without `count`, returns a single field name or `nil` if the hash is empty.
  With positive `count`, returns up to `count` unique fields. With negative
  `count`, returns `abs(count)` fields with possible duplicates.

  ## Examples

      iex> FerricStore.hset("user:42", %{"name" => "alice", "age" => "30", "email" => "a@b.c"})
      iex> {:ok, field} = FerricStore.hrandfield("user:42")
      iex> field in ["name", "age", "email"]
      true

      iex> {:ok, fields} = FerricStore.hrandfield("user:42", 2)
      iex> length(fields)
      2

      iex> FerricStore.hrandfield("nonexistent")
      {:ok, nil}

  """
  @spec hrandfield(key(), integer() | nil) :: {:ok, binary() | [binary()] | nil}
  def hrandfield(key, count \\ nil) do
    store = build_compound_store(key)

    case count do
      nil ->
        case Hash.handle_ast({:hrandfield, key}, store) do
          {:error, _} = err -> err
          result -> {:ok, result}
        end

      n when is_integer(n) ->
        case Hash.handle_ast({:hrandfield, key, n}, store) do
          {:error, _} = err -> err
          result -> {:ok, result}
        end
    end
  end

  @doc """
  Returns the string length of the value for `field` in the hash at `key`.

  Returns `{:ok, 0}` if the field or the key does not exist.

  ## Examples

      iex> FerricStore.hset("user:42", %{"name" => "alice"})
      iex> FerricStore.hstrlen("user:42", "name")
      {:ok, 5}

      iex> FerricStore.hstrlen("user:42", "missing")
      {:ok, 0}

  """
  @spec hstrlen(key(), binary()) :: {:ok, non_neg_integer()}
  def hstrlen(key, field) do
    store = build_compound_store(key)

    case Hash.handle_ast({:hstrlen, key, to_string(field)}, store) do
      {:error, _} = err -> err
      len -> {:ok, len}
    end
  end

  # ---------------------------------------------------------------------------
  # List extended operations
  # ---------------------------------------------------------------------------
end
