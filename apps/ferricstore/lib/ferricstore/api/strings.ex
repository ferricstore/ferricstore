defmodule FerricStore.API.Strings do
  @moduledoc false

  import FerricStore.API.Store
  alias Ferricstore.HLC
  alias Ferricstore.Commands.Strings
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
  Sets `key` to `value` with optional TTL and condition flags.

  ## Options

    * `:ttl` - Time-to-live in milliseconds (relative). When omitted or `0`,
      the key never expires. Mutually exclusive with `:exat`, `:pxat`, `:keepttl`.
    * `:exat` - Absolute Unix timestamp in seconds at which the key expires.
      Mutually exclusive with `:ttl`, `:pxat`, `:keepttl`.
    * `:pxat` - Absolute Unix timestamp in milliseconds at which the key expires.
      Mutually exclusive with `:ttl`, `:exat`, `:keepttl`.
    * `:nx` - Only set the key if it does not already exist.
    * `:xx` - Only set the key if it already exists.
    * `:get` - Return the old value stored at the key before overwriting.
      When set, the return value changes to `{:ok, old_value}` (or `{:ok, nil}`
      if the key did not exist).
    * `:keepttl` - Retain the existing TTL associated with the key instead of
      clearing it. Mutually exclusive with `:ttl`, `:exat`, `:pxat`.

  ## Examples

      iex> FerricStore.set("user:42:name", "alice")
      :ok

      iex> FerricStore.set("session:abc", "token_data", ttl: :timer.hours(1))
      :ok

      iex> FerricStore.set("cache:page:home", "html", exat: 1711234567)
      :ok

      iex> FerricStore.set("lock:order:99", "owner_1", nx: true)
      :ok

      iex> FerricStore.set("lock:order:99", "owner_2", nx: true)
      nil

      iex> FerricStore.set("counter", "0")
      :ok
      iex> FerricStore.set("counter", "100", get: true)
      {:ok, "0"}

      iex> FerricStore.set("missing", "val", get: true)
      {:ok, nil}

      iex> FerricStore.set("session:abc", "refreshed", keepttl: true)
      :ok

  Returns `{:error, reason}` if the value exceeds the configured
  `max_value_size`.
  """
  @spec set(key(), value(), set_opts()) :: :ok | {:ok, value() | nil} | nil | write_error()
  def set(key, value, opts \\ []) do
    max_value_size =
      Application.get_env(:ferricstore, :max_value_size, 1_048_576)

    if is_binary(value) and byte_size(value) > max_value_size do
      {:error, "ERR value too large (#{byte_size(value)} bytes, max #{max_value_size} bytes)"}
    else
      set_inner(key, value, opts)
    end
  end

  defp set_inner(key, value, opts) do
    ctx = default_ctx()
    ttl = Keyword.get(opts, :ttl, 0)
    exat = Keyword.get(opts, :exat)
    pxat = Keyword.get(opts, :pxat)
    nx? = Keyword.get(opts, :nx, false)
    xx? = Keyword.get(opts, :xx, false)
    get? = Keyword.get(opts, :get, false)
    keepttl? = Keyword.get(opts, :keepttl, false)

    # Determine expire_at_ms from the expiry options (mutually exclusive)
    {expire_at_ms, from_keepttl?} =
      cond do
        keepttl? -> {0, true}
        exat != nil -> {exat * 1000, false}
        pxat != nil -> {pxat, false}
        ttl > 0 -> {HLC.now_ms() + ttl, false}
        true -> {0, false}
      end

    if nx? or xx? or get? or from_keepttl? do
      opts = %{
        expire_at_ms: expire_at_ms,
        nx: nx?,
        xx: xx?,
        get: get?,
        keepttl: from_keepttl?
      }

      case Router.set(ctx, key, value, opts) do
        {:error, _} = err -> err
        result when get? -> {:ok, result}
        result -> result
      end
    else
      Router.put(ctx, key, value, expire_at_ms)
    end
  end

  @doc """
  Gets the value stored at `key`.

  Returns `{:ok, value}` if the key exists and has not expired, or `{:ok, nil}`
  if the key does not exist or has expired.

  ## Examples

      iex> FerricStore.set("user:42:name", "alice")
      :ok
      iex> FerricStore.get("user:42:name")
      {:ok, "alice"}

      iex> FerricStore.get("nonexistent:key")
      {:ok, nil}

  """
  @spec get(key(), get_opts()) :: {:ok, value() | nil}
  def get(key, opts \\ [])

  def get("", _opts) do
    store = build_string_store("")

    case Ferricstore.Store.TypeRegistry.get_type("", store) do
      type when type in ["list", "hash", "set", "zset"] ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      _ ->
        {:ok, Router.get(default_ctx(), "")}
    end
  end

  def get(key, _opts) do
    case Strings.handle_ast({:get, key}, build_string_store(key)) do
      {:error, _} = err -> err
      value -> {:ok, value}
    end
  end

  @doc """
  Deletes one or more keys from the store.

  Accepts a single key or a list of keys. Returns `{:ok, count}` where
  count is the number of keys that were actually deleted.

  ## Examples

      iex> FerricStore.set("k1", "v1")
      iex> FerricStore.del("k1")
      {:ok, 1}

      iex> FerricStore.del("nonexistent")
      {:ok, 0}

      iex> FerricStore.set("a", "1")
      iex> FerricStore.set("b", "2")
      iex> FerricStore.del(["a", "b", "c"])
      {:ok, 2}

  """
  @spec del(key() | [key()]) :: {:ok, non_neg_integer()} | write_error()
  def del(key) when is_binary(key), do: del([key])

  def del(keys) when is_list(keys) do
    store = build_compound_store(hd(keys))

    case Strings.handle_ast({:del, keys}, store) do
      {:error, _} = err -> err
      count -> {:ok, count}
    end
  end

  @doc """
  Increments the integer value stored at `key` by 1.

  If the key does not exist, it is initialized to `0` before incrementing,
  resulting in a value of `1`. Returns `{:error, reason}` if the stored value
  cannot be parsed as an integer.

  ## Examples

      iex> FerricStore.incr("page:views:home")
      {:ok, 1}

      iex> FerricStore.incr("page:views:home")
      {:ok, 2}

      iex> FerricStore.set("name", "alice")
      :ok
      iex> FerricStore.incr("name")
      {:error, "ERR value is not an integer or out of range"}

  """
  @spec incr(key()) :: {:ok, integer()} | write_error()
  def incr(key) do
    incr_by(key, 1)
  end

  @doc """
  Decrements the integer value stored at `key` by 1.

  If the key does not exist, it is initialized to `0` before decrementing,
  resulting in a value of `-1`. Returns `{:error, reason}` if the stored value
  cannot be parsed as an integer.

  ## Examples

      iex> FerricStore.decr("rate_limit:user:42")
      {:ok, -1}

      iex> FerricStore.set("stock:item:99", "10")
      :ok
      iex> FerricStore.decr("stock:item:99")
      {:ok, 9}

  """
  @spec decr(key()) :: {:ok, integer()} | write_error()
  def decr(key) do
    incr_by(key, -1)
  end

  @doc """
  Decrements the integer value stored at `key` by `amount`.

  If the key does not exist, it is initialized to `0` before decrementing.
  Returns `{:error, reason}` if the stored value is not a valid integer.

  ## Examples

      iex> FerricStore.set("stock:item:99", "100")
      :ok
      iex> FerricStore.decr_by("stock:item:99", 10)
      {:ok, 90}

      iex> FerricStore.decr_by("new_counter", 5)
      {:ok, -5}

  """
  @spec decr_by(key(), integer()) :: {:ok, integer()} | write_error()
  def decr_by(key, amount) when is_integer(amount) do
    incr_by(key, -amount)
  end

  @doc """
  Increments the integer value stored at `key` by `amount`.

  If the key does not exist, it is initialized to `0` before incrementing.
  Returns `{:error, reason}` if the stored value is not a valid integer.

  ## Examples

      iex> FerricStore.incr_by("page:views:home", 10)
      {:ok, 10}

      iex> FerricStore.incr_by("page:views:home", 5)
      {:ok, 15}

      iex> FerricStore.set("name", "alice")
      :ok
      iex> FerricStore.incr_by("name", 1)
      {:error, "ERR value is not an integer or out of range"}

  """
  @spec incr_by(key(), integer()) :: {:ok, integer()} | write_error()

  def incr_by(key, amount) when is_integer(amount) do
    ctx = default_ctx()

    case Router.incr(ctx, key, amount) do
      {:ok, result} -> {:ok, result}
      {:error, _} = err -> err
    end
  end

  @doc """
  Increments the numeric value stored at `key` by a floating-point `amount`.

  If the key does not exist, it is initialized to `0.0` before incrementing.
  The new value is returned as a string representation. Returns
  `{:error, reason}` if the stored value is not a valid number.

  ## Examples

      iex> FerricStore.incr_by_float("price:item:99", 3.14)
      {:ok, "3.14"}

      iex> FerricStore.set("balance:user:42", "100.50")
      :ok
      iex> FerricStore.incr_by_float("balance:user:42", -20.25)
      {:ok, "80.25"}

  """
  @spec incr_by_float(key(), float()) :: {:ok, binary()} | write_error()
  def incr_by_float(key, amount) when is_number(amount) do
    ctx = default_ctx()

    case Router.incr_float(ctx, key, amount * 1.0) do
      {:ok, result} -> {:ok, result}
      {:error, _} = err -> err
    end
  end

  @doc """
  Gets values for multiple keys in a single call.

  Returns `{:ok, values}` where `values` is a list in the same order as the
  input keys. Missing or expired keys appear as `nil` in the result list.

  ## Examples

      iex> FerricStore.set("user:1:name", "alice")
      :ok
      iex> FerricStore.set("user:2:name", "bob")
      :ok
      iex> FerricStore.mget(["user:1:name", "user:2:name", "user:3:name"])
      {:ok, ["alice", "bob", nil]}

  """
  @spec mget([key()]) :: {:ok, [value() | nil]}
  def mget(keys) when is_list(keys) do
    ctx = default_ctx()
    values = Router.batch_get(ctx, keys)
    {:ok, values}
  end

  @doc """
  Sets multiple key-value pairs in a single call.

  All pairs are written without expiry. Use `set/3` with `:ttl` if individual
  keys need time-to-live.

  ## Examples

      iex> FerricStore.mset(%{"user:1:name" => "alice", "user:2:name" => "bob"})
      :ok

      iex> FerricStore.get("user:1:name")
      {:ok, "alice"}

  """
  @spec mset(%{key() => value()}) :: :ok
  def mset(pairs) when is_map(pairs) do
    ctx = default_ctx()

    Enum.each(pairs, fn {key, value} ->
      Router.put(ctx, key, value, 0)
    end)

    :ok
  end

  @doc """
  Appends `suffix` to the string value stored at `key`.

  If the key does not exist, it is created with `suffix` as its value.
  Returns the byte length of the string after the append.

  ## Examples

      iex> FerricStore.set("log:request:42", "GET /api")
      :ok
      iex> FerricStore.append("log:request:42", " 200 OK")
      {:ok, 15}

      iex> FerricStore.append("new:key", "hello")
      {:ok, 5}

  """
  @spec append(key(), binary()) :: {:ok, non_neg_integer()}
  def append(key, suffix) do
    ctx = default_ctx()

    case Router.append(ctx, key, suffix) do
      {:ok, len} -> {:ok, len}
      {:error, _} = err -> err
      len when is_integer(len) -> {:ok, len}
    end
  end

  @doc """
  Returns the byte length of the string value stored at `key`.

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      iex> FerricStore.set("user:42:name", "alice")
      :ok
      iex> FerricStore.strlen("user:42:name")
      {:ok, 5}

      iex> FerricStore.strlen("nonexistent:key")
      {:ok, 0}

  """
  @spec strlen(key()) :: {:ok, non_neg_integer()}
  def strlen(key) do
    case Strings.handle_ast({:strlen, key}, build_string_store(key)) do
      {:error, _} = err -> err
      len -> {:ok, len}
    end
  end

  @doc """
  Atomically sets `key` to `value` and returns the previous value.

  Returns `{:ok, old_value}` or `{:ok, nil}` if the key did not previously
  exist. Useful for atomic swap patterns like rotating session tokens.

  ## Examples

      iex> FerricStore.set("session:token", "tok_abc")
      :ok
      iex> FerricStore.getset("session:token", "tok_xyz")
      {:ok, "tok_abc"}

      iex> FerricStore.getset("fresh:key", "first_value")
      {:ok, nil}

  """
  @spec getset(key(), value()) :: {:ok, value() | nil}
  def getset(key, value) do
    ctx = default_ctx()

    case Router.getset(ctx, key, value) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Atomically gets the value of `key` and deletes it.

  Returns `{:ok, value}` or `{:ok, nil}` if the key did not exist. Useful
  for consuming one-time tokens or dequeuing single values.

  ## Examples

      iex> FerricStore.set("otp:user:42", "839201")
      :ok
      iex> FerricStore.getdel("otp:user:42")
      {:ok, "839201"}
      iex> FerricStore.getdel("otp:user:42")
      {:ok, nil}

  """
  @spec getdel(key()) :: {:ok, value() | nil}
  def getdel(key) do
    ctx = default_ctx()

    case Router.getdel(ctx, key) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Gets the value of `key` and optionally updates its expiry.

  When called without options, behaves identically to `get/2`. Pass `:ttl`
  to refresh the expiry on access, or `:persist` to remove it.

  ## Options

    * `:ttl` - New TTL in milliseconds to set on the key.
    * `:persist` - When `true`, removes any existing TTL, making the key persistent.

  ## Examples

      iex> FerricStore.set("session:abc", "data", ttl: 10_000)
      :ok
      iex> FerricStore.getex("session:abc", ttl: 60_000)
      {:ok, "data"}

      iex> FerricStore.getex("session:abc", persist: true)
      {:ok, "data"}

      iex> FerricStore.getex("nonexistent:key")
      {:ok, nil}

  """
  @spec getex(key(), keyword()) :: {:ok, value() | nil}
  def getex(key, opts \\ []) do
    ctx = default_ctx()

    expire_at_ms =
      cond do
        Keyword.get(opts, :persist, false) ->
          0

        ttl = Keyword.get(opts, :ttl) ->
          HLC.now_ms() + ttl

        true ->
          nil
      end

    case expire_at_ms do
      nil ->
        case Strings.handle_ast({:getex, key}, build_string_store(key)) do
          {:error, _} = err -> err
          result -> {:ok, result}
        end

      ms ->
        case Router.getex(ctx, key, ms) do
          {:error, _} = err -> err
          result -> {:ok, result}
        end
    end
  end

  @doc """
  Sets `key` to `value` only if the key does not already exist.

  Returns `{:ok, true}` if the key was created, or `{:ok, false}` if the key
  already existed and the write was skipped.

  ## Examples

      iex> FerricStore.setnx("lock:job:import", "worker_1")
      {:ok, true}

      iex> FerricStore.setnx("lock:job:import", "worker_2")
      {:ok, false}

  """
  @spec setnx(key(), value()) :: {:ok, boolean()}
  def setnx(key, value) do
    case set(key, value, nx: true) do
      :ok -> {:ok, true}
      nil -> {:ok, false}
      {:error, _} = err -> err
    end
  end

  @doc """
  Sets `key` to `value` with a TTL in seconds.

  This is a convenience wrapper equivalent to
  `set(key, value, ttl: seconds * 1_000)`.

  ## Examples

      iex> FerricStore.setex("session:abc", 3600, "token_data")
      :ok

      iex> FerricStore.setex("cache:query:recent", 60, "[\"row1\",\"row2\"]")
      :ok

  """
  @spec setex(key(), pos_integer(), value()) :: :ok
  def setex(key, seconds, value) do
    ctx = default_ctx()
    expire_at_ms = HLC.now_ms() + seconds * 1_000
    Router.put(ctx, key, value, expire_at_ms)
  end

  @doc """
  Sets `key` to `value` with a TTL in milliseconds.

  This is a convenience wrapper equivalent to
  `set(key, value, ttl: milliseconds)`.

  ## Examples

      iex> FerricStore.psetex("rate_limit:user:42", 500, "1")
      :ok

      iex> FerricStore.psetex("debounce:click", 200, "pending")
      :ok

  """
  @spec psetex(key(), pos_integer(), value()) :: :ok
  def psetex(key, milliseconds, value) do
    ctx = default_ctx()
    expire_at_ms = HLC.now_ms() + milliseconds
    Router.put(ctx, key, value, expire_at_ms)
  end

  @doc """
  Returns a substring of the string stored at `key` between byte offsets `start` and `stop` (inclusive).

  Negative offsets count from the end of the string (`-1` is the last byte).
  Returns `{:ok, ""}` if the key does not exist or the range is empty.

  ## Examples

      iex> FerricStore.set("greeting", "Hello, World!")
      :ok
      iex> FerricStore.getrange("greeting", 7, 11)
      {:ok, "World"}

      iex> FerricStore.getrange("greeting", -6, -1)
      {:ok, "orld!"}

      iex> FerricStore.getrange("nonexistent", 0, 10)
      {:ok, ""}

  """
  @spec getrange(key(), integer(), integer()) :: {:ok, binary()}
  def getrange(key, start, stop) do
    case Strings.handle_ast({:getrange, key, start, stop}, build_string_store(key)) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Overwrites part of the string stored at `key` starting at byte `offset`.

  If the key does not exist, or the existing string is shorter than `offset`,
  the value is zero-padded to reach the offset before writing. Returns
  `{:ok, new_byte_length}` with the total length after the write.

  ## Examples

      iex> FerricStore.set("greeting", "Hello World")
      :ok
      iex> FerricStore.setrange("greeting", 6, "Redis")
      {:ok, 11}
      iex> FerricStore.get("greeting")
      {:ok, "Hello Redis"}

      iex> FerricStore.setrange("padded:key", 5, "!")
      {:ok, 6}

  """
  @spec setrange(key(), non_neg_integer(), binary()) :: {:ok, non_neg_integer()}
  def setrange(key, offset, value) do
    ctx = default_ctx()

    case Router.setrange(ctx, key, offset, value) do
      {:ok, len} -> {:ok, len}
      {:error, _} = err -> err
      len when is_integer(len) -> {:ok, len}
    end
  end

  @doc """
  Sets multiple key-value pairs only if none of the given keys already exist.

  This is atomic: either all keys are set, or none are. If any key in the
  map already exists, the entire operation is skipped and `{:ok, false}` is
  returned.

  ## Examples

      iex> FerricStore.msetnx(%{"user:1:email" => "a@test.com", "user:2:email" => "b@test.com"})
      {:ok, true}

      iex> FerricStore.msetnx(%{"user:1:email" => "new@test.com", "user:3:email" => "c@test.com"})
      {:ok, false}

  """
  @spec msetnx(%{key() => value()}) :: {:ok, boolean()}
  def msetnx(pairs) when is_map(pairs) do
    keys = Map.keys(pairs)
    store = default_ctx()

    result =
      Ferricstore.CrossShardOp.execute(
        Enum.map(keys, &{&1, :write}),
        fn unified_store ->
          any_exists =
            Enum.any?(keys, fn k -> Ferricstore.Store.Ops.exists?(unified_store, k) end)

          if any_exists do
            false
          else
            Enum.each(pairs, fn {k, v} -> Ferricstore.Store.Ops.put(unified_store, k, v, 0) end)
            true
          end
        end,
        intent: %{command: :msetnx, keys: %{targets: keys}},
        store: store
      )

    case result do
      {:error, _} = err -> err
      val -> {:ok, val}
    end
  end

  # ---------------------------------------------------------------------------
  # Hash
  # ---------------------------------------------------------------------------
end
