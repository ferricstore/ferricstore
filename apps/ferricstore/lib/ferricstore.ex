defmodule FerricStore do
  @moduledoc """
  Module-based cache instances for FerricStore.

  Each module that calls `use FerricStore` gets its own fully isolated
  cache instance with its own shards, ETS tables, Raft system, and config.

  ## Usage

      defmodule MyApp.Cache do
        use FerricStore,
          data_dir: "/data/cache",
          shard_count: 4,
          max_memory: "1GB"
      end

      # In your supervision tree:
      children = [MyApp.Cache]

      # Then use it:
      MyApp.Cache.set("key", "value")
      {:ok, "value"} = MyApp.Cache.get("key")

  ## Multiple instances

      defmodule MyApp.Sessions do
        use FerricStore,
          data_dir: "/data/sessions",
          shard_count: 2
      end

      MyApp.Cache.set("page:home", html)
      MyApp.Sessions.set("sess:abc", session_data)

  ## Options

    * `:data_dir` — base directory for Bitcask data files (required)
    * `:shard_count` — number of shards (default: 4)
    * `:max_memory_bytes` — maximum memory budget (default: 1GB)
    * `:keydir_max_ram` — maximum ETS keydir memory (default: 256MB)
    * `:eviction_policy` — `:volatile_lfu` | `:allkeys_lfu` | `:noeviction` (default: `:volatile_lfu`)
    * `:hot_cache_max_value_size` — max value size for ETS caching (default: 65536)
    * `:read_sample_rate` — LFU sampling rate (default: 100)
  """

  defmacro __using__(opts) do
    quote do
      use FerricStore.Macro, unquote(opts)
    end
  end

  alias Ferricstore.HLC
  alias Ferricstore.Store.Router

  # Transitional: resolve ctx from the :default instance.
  # Will be removed when all users migrate to `use FerricStore` pattern.
  defp default_ctx do
    FerricStore.Instance.get(:default)
  end

  # ---------------------------------------------------------------------------
  # Readiness
  # ---------------------------------------------------------------------------

  @doc """
  Blocks until FerricStore is fully ready to serve requests.

  Polls `Health.check/0` until all shards are alive and all Raft leaders
  are elected. Returns `:ok` when ready, raises on timeout.

  Call this in your application's `start/2` after FerricStore is in your
  supervision tree, or in test setup, to ensure writes won't fail.

  ## Options

    * `:timeout` - max milliseconds to wait (default: 30_000)
    * `:interval` - polling interval in ms (default: 100)

  ## Examples

      # In your Application.start/2:
      def start(_type, _args) do
        children = [
          {FerricStore, []},
          MyApp.Repo,
          MyAppWeb.Endpoint
        ]
        opts = [strategy: :one_for_one, name: MyApp.Supervisor]
        {:ok, pid} = Supervisor.start_link(children, opts)

        FerricStore.await_ready()
        {:ok, pid}
      end

      # With custom timeout:
      FerricStore.await_ready(timeout: 60_000)

  """
  @spec await_ready(keyword()) :: :ok
  def await_ready(opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 30_000)
    interval = Keyword.get(opts, :interval, 100)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_await_ready(deadline, interval)
  end

  defp do_await_ready(deadline, interval) do
    case Ferricstore.Health.check() do
      %{status: :ok} ->
        :ok

      _ ->
        if System.monotonic_time(:millisecond) > deadline do
          raise "FerricStore not ready within timeout. Check shard health with FerricStore.health()"
        end

        Process.sleep(interval)
        do_await_ready(deadline, interval)
    end
  end

  @doc """
  Returns the current health status without blocking.

  ## Examples

      iex> FerricStore.health()
      %{status: :ok, shard_count: 4, shards: [...], uptime_seconds: 120}

  """
  @spec health() :: Ferricstore.Health.health_result()
  def health do
    Ferricstore.Health.check()
  end

  @doc """
  Returns `true` if FerricStore is ready to serve requests.

  ## Examples

      iex> FerricStore.ready?()
      true

  """
  @spec ready?() :: boolean()
  def ready? do
    Ferricstore.Health.ready?()
  end

  @doc """
  Gracefully shuts down FerricStore, flushing all pending data to disk.

  Flushes Raft batchers, BitcaskWriters, shard pending writes, and
  triggers a WAL rollover. Call before stopping the application to
  ensure zero data loss.

  ## Examples

      FerricStore.shutdown()
      Application.stop(:ferricstore)

  """
  @spec shutdown() :: :ok
  def shutdown do
    Ferricstore.Application.prep_stop(nil)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @type key :: binary()
  @type value :: binary()
  @type write_error :: {:error, binary() | {:timeout, :unknown_outcome}}
  @type set_opts :: [
          ttl: non_neg_integer(),
          exat: pos_integer(),
          pxat: pos_integer(),
          nx: boolean(),
          xx: boolean(),
          get: boolean(),
          keepttl: boolean(),
          cache: atom()
        ]
  @type get_opts :: [cache: atom()]
  @type cas_opts :: [ttl: non_neg_integer()]
  @type fetch_or_compute_opts :: [ttl: pos_integer(), hint: binary()]
  @type zrange_opts :: [withscores: boolean()]

  # ---------------------------------------------------------------------------
  # Strings
  # ---------------------------------------------------------------------------

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
    case Ferricstore.Commands.Strings.handle("GET", [key], build_string_store(key)) do
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

    case Ferricstore.Commands.Strings.handle("DEL", keys, store) do
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

    values =
      Enum.map(keys, fn key ->
        Router.get(ctx, key)
      end)

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
    case Ferricstore.Commands.Strings.handle("STRLEN", [key], build_string_store(key)) do
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
        case Ferricstore.Commands.Strings.handle("GETEX", [key], build_string_store(key)) do
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
    case Ferricstore.Commands.Strings.handle(
           "GETRANGE",
           [key, to_string(start), to_string(stop)],
           build_string_store(key)
         ) do
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

    case Ferricstore.Commands.Hash.handle("HSET", [key | args], store) do
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

    case Ferricstore.Commands.Hash.handle("HGET", [key, to_string(field)], store) do
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

    case Ferricstore.Commands.Hash.handle("HGETALL", [key], store) do
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
  Pushes one or more elements to the left (head) of the list stored at `key`.

  If the key does not exist, a new list is created. Elements are inserted
  left-to-right, so the last element in the list ends up as the leftmost
  element (matching Redis LPUSH semantics).

  ## Examples

      iex> FerricStore.lpush("tasks:queue", ["send_email"])
      {:ok, 1}

      iex> FerricStore.lpush("tasks:queue", ["generate_report", "resize_image"])
      {:ok, 3}

  """
  @spec lpush(key(), [binary()]) :: {:ok, non_neg_integer()} | {:error, binary()}
  def lpush(key, elements) when is_list(elements) do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:lpush, elements})
    |> wrap_result()
  end

  @doc """
  Pushes one or more elements to the right (tail) of the list stored at `key`.

  If the key does not exist, a new list is created.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["send_email"])
      {:ok, 1}

      iex> FerricStore.rpush("tasks:queue", ["generate_report", "resize_image"])
      {:ok, 3}

  """
  @spec rpush(key(), [binary()]) :: {:ok, non_neg_integer()} | {:error, binary()}
  def rpush(key, elements) when is_list(elements) do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:rpush, elements})
    |> wrap_result()
  end

  @doc """
  Pops one or more elements from the left (head) of the list stored at `key`.

  When `count` is 1 (the default), returns a single element. When `count` is
  greater than 1, returns a list of elements. Returns `{:ok, nil}` if the key
  does not exist or the list is empty.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.lpop("tasks:queue")
      {:ok, "task_a"}

      iex> FerricStore.lpop("tasks:queue", 2)
      {:ok, ["task_b", "task_c"]}

      iex> FerricStore.lpop("empty_queue")
      {:ok, nil}

  """
  @spec lpop(key(), pos_integer()) :: {:ok, binary() | [binary()] | nil} | {:error, binary()}
  def lpop(key, count \\ 1) when is_integer(count) and count >= 1 do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:lpop, count})
    |> wrap_result()
  end

  @doc """
  Pops one or more elements from the right (tail) of the list stored at `key`.

  When `count` is 1 (the default), returns a single element. When `count` is
  greater than 1, returns a list of elements. Returns `{:ok, nil}` if the key
  does not exist or the list is empty.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.rpop("tasks:queue")
      {:ok, "task_c"}

      iex> FerricStore.rpop("tasks:queue", 2)
      {:ok, ["task_b", "task_a"]}

      iex> FerricStore.rpop("empty_queue")
      {:ok, nil}

  """
  @spec rpop(key(), pos_integer()) :: {:ok, binary() | [binary()] | nil} | {:error, binary()}
  def rpop(key, count \\ 1) when is_integer(count) and count >= 1 do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:rpop, count})
    |> wrap_result()
  end

  @doc """
  Returns elements from the list stored at `key` within the range `start..stop`.

  Both `start` and `stop` are zero-based, inclusive indices. Negative indices
  count from the end of the list (-1 is the last element).

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.lrange("tasks:queue", 0, -1)
      {:ok, ["task_a", "task_b", "task_c"]}

      iex> FerricStore.lrange("tasks:queue", 1, 1)
      {:ok, ["task_b"]}

      iex> FerricStore.lrange("nonexistent", 0, -1)
      {:ok, []}

  """
  @spec lrange(key(), integer(), integer()) :: {:ok, [binary()]} | {:error, binary()}
  def lrange(key, start, stop) do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:lrange, start, stop})
    |> wrap_result()
  end

  @doc """
  Returns the length of the list stored at `key`.

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.llen("tasks:queue")
      {:ok, 3}

      iex> FerricStore.llen("nonexistent")
      {:ok, 0}

  """
  @spec llen(key()) :: {:ok, non_neg_integer()} | {:error, binary()}
  def llen(key) do
    ctx = default_ctx()

    Router.list_op(ctx, key, :llen)
    |> wrap_result()
  end

  # ---------------------------------------------------------------------------
  # Sets
  # ---------------------------------------------------------------------------

  @doc """
  Adds one or more members to the set stored at `key`.

  If the key does not exist, a new set is created. Members that already exist
  in the set are ignored. Returns the count of members actually added.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      {:ok, 3}

      iex> FerricStore.sadd("article:42:tags", ["rust", "performance"])
      {:ok, 1}

  """
  @spec sadd(key(), [binary()]) :: {:ok, non_neg_integer()}
  def sadd(key, members) when is_list(members) do
    store = build_compound_store(key)

    case Ferricstore.Commands.Set.handle("SADD", [key | members], store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Removes one or more members from the set stored at `key`.

  Members that do not exist in the set are ignored. Returns the count of
  members actually removed.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      iex> FerricStore.srem("article:42:tags", ["rust"])
      {:ok, 1}

      iex> FerricStore.srem("article:42:tags", ["nonexistent"])
      {:ok, 0}

  """
  @spec srem(key(), [binary()]) :: {:ok, non_neg_integer()}
  def srem(key, members) when is_list(members) do
    store = build_compound_store(key)

    case Ferricstore.Commands.Set.handle("SREM", [key | members], store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Returns all members of the set stored at `key`.

  Returns `{:ok, []}` if the key does not exist. The order of returned
  members is not guaranteed.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust"])
      iex> {:ok, members} = FerricStore.smembers("article:42:tags")
      iex> Enum.sort(members)
      ["elixir", "rust"]

      iex> FerricStore.smembers("nonexistent")
      {:ok, []}

  """
  @spec smembers(key()) :: {:ok, [binary()]}
  def smembers(key) do
    store = build_compound_store(key)

    case Ferricstore.Commands.Set.handle("SMEMBERS", [key], store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Checks whether `member` is a member of the set stored at `key`.

  Returns `{:ok, true}` if the member exists, `{:ok, false}` otherwise.
  Returns `{:ok, false}` if the key does not exist.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust"])
      iex> FerricStore.sismember("article:42:tags", "elixir")
      {:ok, true}

      iex> FerricStore.sismember("article:42:tags", "python")
      {:ok, false}

      iex> FerricStore.sismember("nonexistent", "member")
      {:ok, false}

  """
  @spec sismember(key(), binary()) :: {:ok, boolean()} | {:error, binary()}
  def sismember(key, member) do
    store = build_compound_store(key)

    case Ferricstore.Commands.Set.handle("SISMEMBER", [key, member], store) do
      {:error, _} = err -> err
      result -> {:ok, result == 1}
    end
  end

  @doc """
  Returns the number of members in the set stored at `key` (the set cardinality).

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      iex> FerricStore.scard("article:42:tags")
      {:ok, 3}

      iex> FerricStore.scard("nonexistent")
      {:ok, 0}

  """
  @spec scard(key()) :: {:ok, non_neg_integer()}
  def scard(key) do
    store = build_compound_store(key)

    case Ferricstore.Commands.Set.handle("SCARD", [key], store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  # ---------------------------------------------------------------------------
  # Sorted Sets
  # ---------------------------------------------------------------------------

  @doc """
  Adds members with scores to the sorted set stored at `key`.

  `score_member_pairs` is a list of `{score, member}` tuples where `score` is
  a number and `member` is a binary string. If a member already exists, its
  score is updated. Returns the count of new members added (not counting
  score updates).

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])
      {:ok, 2}

      iex> FerricStore.zadd("leaderboard", [{150.0, "alice"}, {300.0, "charlie"}])
      {:ok, 1}

  """
  @spec zadd(key(), [{number(), binary()}]) :: {:ok, non_neg_integer()}
  def zadd(key, score_member_pairs) when is_list(score_member_pairs) do
    store = build_compound_store(key)

    # Build the ZADD args: key score1 member1 score2 member2 ...
    args =
      Enum.flat_map(score_member_pairs, fn {score, member} ->
        [to_string(score * 1.0), member]
      end)

    case Ferricstore.Commands.SortedSet.handle("ZADD", [key | args], store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Returns members in the sorted set stored at `key` within the rank range `start..stop`.

  Indices are zero-based and inclusive. Negative indices count from the end
  (-1 is the last element). Members are ordered by score ascending.

  ## Options

    * `:withscores` - When `true`, returns `{member, score}` tuples instead
      of bare member strings. Defaults to `false`.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zrange("leaderboard", 0, -1)
      {:ok, ["alice", "bob", "charlie"]}

      iex> FerricStore.zrange("leaderboard", 0, 1, withscores: true)
      {:ok, [{"alice", 100.0}, {"bob", 200.0}]}

      iex> FerricStore.zrange("nonexistent", 0, -1)
      {:ok, []}

  """
  @spec zrange(key(), integer(), integer(), zrange_opts()) ::
          {:ok, [binary() | {binary(), float()}]}
  def zrange(key, start, stop, opts \\ []) do
    _ctx = default_ctx()
    store = build_compound_store(key)
    with_scores = Keyword.get(opts, :withscores, false)

    args = [key, to_string(start), to_string(stop)]
    args = if with_scores, do: args ++ ["WITHSCORES"], else: args

    case Ferricstore.Commands.SortedSet.handle("ZRANGE", args, store) do
      {:error, _} = err ->
        err

      result when with_scores and is_list(result) and result != [] ->
        pairs =
          result
          |> Enum.chunk_every(2)
          |> Enum.map(fn [member, score_str] ->
            {score, _} = Float.parse(score_str)
            {member, score}
          end)

        {:ok, pairs}

      result ->
        {:ok, result}
    end
  end

  @doc """
  Returns the score of `member` in the sorted set stored at `key`.

  Returns `{:ok, score}` if the member exists, or `{:ok, nil}` if the member
  or the key does not exist.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])
      iex> FerricStore.zscore("leaderboard", "alice")
      {:ok, 100.0}

      iex> FerricStore.zscore("leaderboard", "unknown")
      {:ok, nil}

  """
  @spec zscore(key(), binary()) :: {:ok, float() | nil}
  def zscore(key, member) do
    store = build_compound_store(key)

    case Ferricstore.Commands.SortedSet.handle("ZSCORE", [key, member], store) do
      {:error, _} = err ->
        err

      nil ->
        {:ok, nil}

      score_str when is_binary(score_str) ->
        {score, _} = Float.parse(score_str)
        {:ok, score}
    end
  end

  @doc """
  Returns the number of members in the sorted set stored at `key`.

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])
      iex> FerricStore.zcard("leaderboard")
      {:ok, 2}

      iex> FerricStore.zcard("nonexistent")
      {:ok, 0}

  """
  @spec zcard(key()) :: {:ok, non_neg_integer()}
  def zcard(key) do
    store = build_compound_store(key)

    case Ferricstore.Commands.SortedSet.handle("ZCARD", [key], store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Removes one or more members from the sorted set stored at `key`.

  Members that do not exist are ignored. Returns the count of members
  actually removed.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])
      iex> FerricStore.zrem("leaderboard", ["alice"])
      {:ok, 1}

      iex> FerricStore.zrem("leaderboard", ["nonexistent"])
      {:ok, 0}

  """
  @spec zrem(key(), [binary()]) :: {:ok, non_neg_integer()}
  def zrem(key, members) when is_list(members) do
    store = build_compound_store(key)

    case Ferricstore.Commands.SortedSet.handle("ZREM", [key | members], store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  # ---------------------------------------------------------------------------
  # Native Commands
  # ---------------------------------------------------------------------------

  @doc """
  Performs an atomic compare-and-swap (optimistic locking) on `key`.

  If the current value of `key` equals `expected`, it is atomically replaced
  with `new_value`. This is the building block for lock-free concurrent updates --
  read the current value, compute the new value, then CAS. If another writer
  changed the value in between, CAS returns `false` and you retry.

  ## Options

    * `:ttl` - Time-to-live in milliseconds for the new value. When omitted,
      the existing TTL is preserved.

  ## Returns

    * `{:ok, true}` if the swap was performed.
    * `{:ok, false}` if the current value did not match `expected` (retry needed).
    * `{:ok, nil}` if the key does not exist.

  ## Examples

      iex> FerricStore.set("inventory:item:99", "10")
      :ok
      iex> FerricStore.cas("inventory:item:99", "10", "9")
      {:ok, true}
      iex> FerricStore.cas("inventory:item:99", "10", "8")
      {:ok, false}

  """
  @spec cas(key(), binary(), binary(), cas_opts()) :: {:ok, true | false | nil}
  def cas(key, expected, new_value, opts \\ []) do
    ctx = default_ctx()
    ttl_ms = Keyword.get(opts, :ttl)

    case Router.cas(ctx, key, expected, new_value, ttl_ms) do
      1 -> {:ok, true}
      0 -> {:ok, false}
      nil -> {:ok, nil}
    end
  end

  @doc """
  Cache-aside pattern with stampede (thundering herd) protection.

  Checks whether `key` has a cached value. If it does, returns
  `{:ok, {:hit, value}}`. If not, returns `{:ok, {:compute, hint}}` to
  indicate that the caller should compute the value and store it via
  `fetch_or_compute_result/3`.

  Only one caller at a time receives `{:compute, hint}` for a given key --
  all other concurrent callers block until the winner stores the computed
  value. This prevents N concurrent cache misses from triggering N
  identical expensive computations (the "stampede" problem).

  ## Options

    * `:ttl` (required) - TTL in milliseconds for the cached value.
    * `:hint` - An opaque string passed back in `{:compute, hint}`. Defaults
      to `""`.

  ## Returns

    * `{:ok, {:hit, value}}` if the value is cached.
    * `{:ok, {:compute, hint}}` if the caller should compute the value.
    * `{:error, reason}` on failure.

  ## Examples

      case FerricStore.fetch_or_compute("dashboard:stats:today", ttl: 30_000) do
        {:ok, {:hit, cached}} ->
          Jason.decode!(cached)

        {:ok, {:compute, _hint}} ->
          stats = DashboardService.compute_stats()
          encoded = Jason.encode!(stats)
          FerricStore.fetch_or_compute_result("dashboard:stats:today", encoded, ttl: 30_000)
          stats
      end

  """
  @spec fetch_or_compute(key(), fetch_or_compute_opts()) ::
          {:ok, {:hit, binary()} | {:compute, binary()}} | {:error, binary()}
  def fetch_or_compute(key, opts) do
    ttl_ms = Keyword.fetch!(opts, :ttl)
    hint = Keyword.get(opts, :hint, "")

    case Ferricstore.FetchOrCompute.fetch_or_compute(key, ttl_ms, hint) do
      {:hit, value} -> {:ok, {:hit, value}}
      {:ok, value} -> {:ok, {:hit, value}}
      {:compute, compute_hint} -> {:ok, {:compute, compute_hint}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Stores the computed value for a `fetch_or_compute/2` cache miss and unblocks waiters.

  Must be called after receiving `{:ok, {:compute, hint}}` from `fetch_or_compute/2`.
  Stores the value in the cache and wakes all concurrent callers that were blocked
  waiting for the computation to complete.

  ## Options

    * `:ttl` (required) - TTL in milliseconds for the cached value.

  ## Returns

    * `:ok` on success.

  ## Examples

      iex> FerricStore.fetch_or_compute_result("dashboard:stats:today", "cached_value", ttl: 30_000)
      :ok

  """
  @spec fetch_or_compute_result(key(), binary(), keyword()) :: :ok | {:error, binary()}
  def fetch_or_compute_result(key, value, opts) do
    ttl_ms = Keyword.fetch!(opts, :ttl)
    Ferricstore.FetchOrCompute.fetch_or_compute_result(key, value, ttl_ms)
  end

  # ---------------------------------------------------------------------------
  # Generic Key Operations
  # ---------------------------------------------------------------------------

  @doc """
  Checks whether `key` exists in the store and has not expired.

  ## Examples

      iex> FerricStore.set("user:42:name", "alice")
      :ok
      iex> FerricStore.exists("user:42:name")
      true

      iex> FerricStore.exists("nonexistent:key")
      false

  """
  @spec exists(key()) :: boolean()
  def exists(key) do
    Ferricstore.Commands.Strings.handle("EXISTS", [key], build_compound_store(key)) == 1
  end

  @doc """
  Returns all keys matching `pattern` (glob-style).

  The pattern supports glob-style wildcards:

    * `*` - matches any sequence of characters
    * `?` - matches any single character

  ## Examples

      iex> FerricStore.set("user:1:name", "alice")
      :ok
      iex> FerricStore.set("user:2:name", "bob")
      :ok
      iex> FerricStore.set("order:1", "pending")
      :ok
      iex> {:ok, user_keys} = FerricStore.keys("user:*")
      iex> Enum.sort(user_keys)
      ["user:1:name", "user:2:name"]

      iex> {:ok, all} = FerricStore.keys()
      iex> length(all) >= 3
      true

  """
  @spec keys(binary()) :: {:ok, [binary()]}
  def keys(pattern \\ "*") do
    ctx = default_ctx()
    alias Ferricstore.Store.CompoundKey

    all_keys = Router.keys(ctx)
    match_all? = pattern == "*"

    visible = CompoundKey.user_visible_keys(all_keys)

    results =
      if match_all? do
        visible
      else
        Enum.filter(visible, &Ferricstore.GlobMatcher.match?(&1, pattern))
      end

    {:ok, results}
  end

  @doc """
  Returns the total number of user-visible keys in the store.

  Internal compound keys (used by hashes, lists, sets, and sorted sets)
  are excluded from the count.

  ## Examples

      iex> FerricStore.set("key:a", "1")
      :ok
      iex> FerricStore.set("key:b", "2")
      :ok
      iex> {:ok, count} = FerricStore.dbsize()
      iex> count >= 2
      true

  """
  @spec dbsize() :: {:ok, non_neg_integer()}
  def dbsize do
    {:ok, matched_keys} = keys()
    {:ok, length(matched_keys)}
  end

  @doc """
  Deletes all keys from the store.

  ## Returns

    * `:ok`

  ## Examples

      :ok = FerricStore.flushdb()

  """
  @spec flushdb() :: :ok
  def flushdb do
    ctx = default_ctx()
    shard_count = ctx.shard_count

    for i <- 0..(shard_count - 1) do
      shard = Router.shard_name(ctx, i)

      raw_keys =
        try do
          :ets.foldl(fn {key, _, _, _, _, _, _}, acc -> [key | acc] end, [], :"keydir_#{i}")
        rescue
          ArgumentError -> []
        end

      Enum.each(raw_keys, fn key ->
        try do
          GenServer.call(shard, {:delete, key}, 10_000)
        catch
          :exit, _ -> :ok
        end
      end)

      # Clear probabilistic structure registries for this shard.
      # These are local mmap handles / NIF resources not managed by Raft.
      clear_shard_registries(i)
    end

    :ok
  end

  defp clear_shard_registries(_shard_index) do
    # Close mmap handles and delete files for probabilistic structures.
    # create_table on an existing table calls close_all + delete_all_objects.
    #
    # NOTE: This runs locally on the node executing FLUSHDB. In single-node
    # mode, this is sufficient. In multi-node mode, followers clear keys via
    # Raft but NOT registries — a {:flush_registries} Raft command would be
    # needed to propagate registry cleanup to all nodes.
    # Legacy registry tables removed — prob structures use stateless pread/pwrite NIFs now.
  end

  # ---------------------------------------------------------------------------
  # TTL
  # ---------------------------------------------------------------------------

  @doc """
  Sets a TTL (in milliseconds) on an existing key.

  The key will be automatically deleted after `ttl_ms` milliseconds have
  elapsed. Returns `{:ok, false}` if the key does not exist.

  ## Examples

      iex> FerricStore.set("session:abc", "data")
      :ok
      iex> FerricStore.expire("session:abc", :timer.minutes(30))
      {:ok, true}

      iex> FerricStore.expire("nonexistent:key", 5_000)
      {:ok, false}

  """
  @spec expire(key(), non_neg_integer()) :: {:ok, boolean()}
  def expire(key, ttl_ms) when is_integer(ttl_ms) and ttl_ms > 0 do
    case Ferricstore.Commands.Expiry.handle(
           "PEXPIRE",
           [key, to_string(ttl_ms)],
           build_compound_store(key)
         ) do
      1 -> {:ok, true}
      0 -> {:ok, false}
      {:error, _} = err -> err
    end
  end

  @doc """
  Returns the remaining time-to-live in milliseconds for `key`.

  Returns `{:ok, ms}` if the key has a TTL set, or `{:ok, nil}` if the key
  has no expiry or does not exist.

  ## Examples

      iex> FerricStore.set("session:abc", "data", ttl: 60_000)
      :ok
      iex> {:ok, ms} = FerricStore.ttl("session:abc")
      iex> ms > 0 and ms <= 60_000
      true

      iex> FerricStore.set("permanent:key", "data")
      :ok
      iex> FerricStore.ttl("permanent:key")
      {:ok, nil}

      iex> FerricStore.ttl("nonexistent:key")
      {:ok, nil}

  """
  @spec ttl(key()) :: {:ok, non_neg_integer() | nil}
  def ttl(key) do
    case Ferricstore.Commands.Expiry.handle("PTTL", [key], build_compound_store(key)) do
      ttl_ms when ttl_ms < 0 -> {:ok, nil}
      ttl_ms -> {:ok, ttl_ms}
    end
  end

  # ---------------------------------------------------------------------------
  # Key Operations (copy, rename, renamenx, type, randomkey)
  # ---------------------------------------------------------------------------

  @doc """
  Copies the value (and its TTL) from `source` to `destination`.

  By default, returns an error if the destination already exists. Pass
  `:replace` to overwrite.

  ## Options

    * `:replace` - When `true`, overwrites `destination` if it already exists.

  ## Examples

      iex> FerricStore.set("user:42:name", "alice")
      :ok
      iex> FerricStore.copy("user:42:name", "user:42:name:backup")
      {:ok, true}

      iex> FerricStore.copy("user:42:name", "user:42:name:backup")
      {:ok, false}

      iex> FerricStore.copy("user:42:name", "user:42:name:backup", replace: true)
      {:ok, true}

      iex> FerricStore.copy("nonexistent", "dst")
      {:error, "ERR no such key"}

  """
  @spec copy(key(), key(), keyword()) :: {:ok, boolean()} | {:error, binary()}
  def copy(source, destination, opts \\ []) do
    replace = Keyword.get(opts, :replace, false)
    args = [source, destination] ++ if replace, do: ["REPLACE"], else: []

    case Ferricstore.Commands.Generic.handle("COPY", args, %{}) do
      1 -> {:ok, true}
      0 -> {:ok, false}
      {:error, _} = err -> err
    end
  end

  @doc """
  Renames `source` to `destination`, overwriting `destination` if it exists.

  The value and TTL are transferred to the new key name, and the source key
  is deleted. Returns `{:error, reason}` if the source does not exist.

  ## Examples

      iex> FerricStore.set("temp:upload:abc", "file_data")
      :ok
      iex> FerricStore.rename("temp:upload:abc", "file:abc")
      :ok
      iex> FerricStore.get("file:abc")
      {:ok, "file_data"}
      iex> FerricStore.exists("temp:upload:abc")
      false

      iex> FerricStore.rename("nonexistent", "dst")
      {:error, "ERR no such key"}

  """
  @spec rename(key(), key()) :: :ok | {:error, binary()}
  def rename(source, destination) do
    Ferricstore.Commands.Generic.handle(
      "RENAME",
      [source, destination],
      %{}
    )
  end

  @doc """
  Renames `source` to `destination` only if `destination` does not already exist.

  Unlike `rename/2`, this will not overwrite an existing destination key.
  The value and TTL are transferred on success.

  ## Examples

      iex> FerricStore.set("temp:import:1", "data")
      :ok
      iex> FerricStore.renamenx("temp:import:1", "import:1")
      {:ok, true}

      iex> FerricStore.set("import:2", "existing")
      :ok
      iex> FerricStore.set("temp:import:2", "new_data")
      :ok
      iex> FerricStore.renamenx("temp:import:2", "import:2")
      {:ok, false}

      iex> FerricStore.renamenx("nonexistent", "dst")
      {:error, "ERR no such key"}

  """
  @spec renamenx(key(), key()) :: {:ok, boolean()} | {:error, binary()}
  def renamenx(source, destination) do
    case Ferricstore.Commands.Generic.handle(
           "RENAMENX",
           [source, destination],
           %{}
         ) do
      1 -> {:ok, true}
      0 -> {:ok, false}
      {:error, _} = err -> err
    end
  end

  @doc """
  Returns the data type of the value stored at `key`.

  The returned type string reflects the underlying data structure: `"string"`,
  `"hash"`, `"list"`, `"set"`, `"zset"`, `"stream"`, or `"none"` if the key
  does not exist.

  ## Examples

      iex> FerricStore.set("user:42:name", "alice")
      :ok
      iex> FerricStore.type("user:42:name")
      {:ok, "string"}

      iex> FerricStore.hset("user:42", %{"name" => "alice"})
      :ok
      iex> FerricStore.type("user:42")
      {:ok, "hash"}

      iex> FerricStore.type("nonexistent:key")
      {:ok, "none"}

  """
  @spec type(key()) :: {:ok, binary()}
  def type(key) do
    ctx = default_ctx()

    # Check compound key type registry first (for hash/set/zset stored via compound keys)
    store = build_compound_store(key)
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    compound_type = store.compound_get.(key, type_key)

    cond do
      compound_type != nil ->
        {:ok, compound_type}

      # Check for list metadata key (lists use compound keys, no type marker)
      store.compound_get.(key, Ferricstore.Store.CompoundKey.list_meta_key(key)) != nil ->
        {:ok, "list"}

      true ->
        case Router.get(ctx, key) do
          nil ->
            {:ok, "none"}

          value when is_binary(value) ->
            detected =
              try do
                case :erlang.binary_to_term(value) do
                  {:list, _} -> "list"
                  _ -> nil
                end
              rescue
                ArgumentError -> nil
              end

            if detected do
              {:ok, detected}
            else
              {:ok, "string"}
            end

          _ ->
            {:ok, "string"}
        end
    end
  end

  @doc """
  Returns a random key from the store, or `{:ok, nil}` if the store is empty.

  Returns a random key from the store.

  ## Examples

      iex> FerricStore.set("key:a", "1")
      :ok
      iex> {:ok, key} = FerricStore.randomkey()
      iex> is_binary(key)
      true

      iex> # When the store is empty:
      iex> FerricStore.randomkey()
      {:ok, nil}

  """
  @spec randomkey() :: {:ok, key() | nil}
  def randomkey do
    {:ok, all_keys} = keys()

    case all_keys do
      [] -> {:ok, nil}
      _ -> {:ok, Enum.random(all_keys)}
    end
  end

  # ---------------------------------------------------------------------------
  # TTL extended: persist, pexpire, pexpireat, expireat, expiretime, pexpiretime, pttl
  # ---------------------------------------------------------------------------

  @doc """
  Removes the TTL from `key`, making it persist indefinitely.

  Returns `{:ok, true}` if an expiry was removed, or `{:ok, false}` if the
  key does not exist or already has no TTL.

  ## Examples

      iex> FerricStore.set("session:abc", "data", ttl: 60_000)
      :ok
      iex> FerricStore.persist("session:abc")
      {:ok, true}
      iex> FerricStore.ttl("session:abc")
      {:ok, nil}

      iex> FerricStore.persist("permanent:key")
      {:ok, false}

      iex> FerricStore.persist("nonexistent:key")
      {:ok, false}

  """
  @spec persist(key()) :: {:ok, boolean()}
  def persist(key) do
    case Ferricstore.Commands.Expiry.handle("PERSIST", [key], build_compound_store(key)) do
      1 -> {:ok, true}
      0 -> {:ok, false}
      {:error, _} = err -> err
    end
  end

  @doc """
  Sets a TTL in milliseconds on an existing key.

  This is an alias for `expire/2` -- both accept milliseconds.

  ## Examples

      iex> FerricStore.set("rate_limit:user:42", "3")
      :ok
      iex> FerricStore.pexpire("rate_limit:user:42", 30_000)
      {:ok, true}

      iex> FerricStore.pexpire("nonexistent:key", 5_000)
      {:ok, false}

  """
  @spec pexpire(key(), non_neg_integer()) :: {:ok, boolean()}
  def pexpire(key, ttl_ms), do: expire(key, ttl_ms)

  @doc """
  Sets the key to expire at the given absolute Unix timestamp (in seconds).

  The key will be automatically deleted when the system clock reaches the
  specified timestamp. Returns `{:ok, false}` if the key does not exist.

  ## Examples

      iex> FerricStore.set("event:promo", "active")
      :ok
      iex> FerricStore.expireat("event:promo", 1_700_000_000)
      {:ok, true}

      iex> FerricStore.expireat("nonexistent:key", 1_700_000_000)
      {:ok, false}

  """
  @spec expireat(key(), non_neg_integer()) :: {:ok, boolean()}
  def expireat(key, unix_ts_seconds) do
    case Ferricstore.Commands.Expiry.handle(
           "EXPIREAT",
           [key, to_string(unix_ts_seconds)],
           build_compound_store(key)
         ) do
      1 -> {:ok, true}
      0 -> {:ok, false}
      {:error, _} = err -> err
    end
  end

  @doc """
  Sets the key to expire at the given absolute Unix timestamp (in milliseconds).

  Like `expireat/2` but with millisecond precision. Returns `{:ok, false}`
  if the key does not exist.

  ## Examples

      iex> FerricStore.set("event:flash_sale", "active")
      :ok
      iex> FerricStore.pexpireat("event:flash_sale", 1_700_000_000_000)
      {:ok, true}

      iex> FerricStore.pexpireat("nonexistent:key", 1_700_000_000_000)
      {:ok, false}

  """
  @spec pexpireat(key(), non_neg_integer()) :: {:ok, boolean()}
  def pexpireat(key, unix_ts_ms) do
    case Ferricstore.Commands.Expiry.handle(
           "PEXPIREAT",
           [key, to_string(unix_ts_ms)],
           build_compound_store(key)
         ) do
      1 -> {:ok, true}
      0 -> {:ok, false}
      {:error, _} = err -> err
    end
  end

  @doc """
  Returns the absolute Unix timestamp (in seconds) at which `key` will expire.

  Returns `{:ok, -1}` if the key exists but has no associated expiry, and
  `{:ok, -2}` if the key does not exist.

  ## Examples

      iex> FerricStore.set("session:abc", "data", ttl: 60_000)
      :ok
      iex> {:ok, ts} = FerricStore.expiretime("session:abc")
      iex> ts > 0
      true

      iex> FerricStore.set("permanent:key", "data")
      :ok
      iex> FerricStore.expiretime("permanent:key")
      {:ok, -1}

      iex> FerricStore.expiretime("nonexistent:key")
      {:ok, -2}

  """
  @spec expiretime(key()) :: {:ok, integer()}
  def expiretime(key) do
    {:ok, Ferricstore.Commands.Generic.handle("EXPIRETIME", [key], build_compound_store(key))}
  end

  @doc """
  Returns the absolute Unix timestamp (in milliseconds) at which `key` will expire.

  Like `expiretime/1` but with millisecond precision. Returns `{:ok, -1}`
  if the key has no expiry, and `{:ok, -2}` if it does not exist.

  ## Examples

      iex> FerricStore.set("session:abc", "data", ttl: 60_000)
      :ok
      iex> {:ok, ts_ms} = FerricStore.pexpiretime("session:abc")
      iex> ts_ms > 0
      true

      iex> FerricStore.set("permanent:key", "data")
      :ok
      iex> FerricStore.pexpiretime("permanent:key")
      {:ok, -1}

      iex> FerricStore.pexpiretime("nonexistent:key")
      {:ok, -2}

  """
  @spec pexpiretime(key()) :: {:ok, integer()}
  def pexpiretime(key) do
    {:ok, Ferricstore.Commands.Generic.handle("PEXPIRETIME", [key], build_compound_store(key))}
  end

  @doc """
  Returns the remaining time-to-live in milliseconds for `key`.

  This is an alias for `ttl/1` -- both return millisecond precision.
  Returns `{:ok, nil}` if the key has no expiry or does not exist.

  ## Examples

      iex> FerricStore.set("cache:result", "data", ttl: 30_000)
      :ok
      iex> {:ok, ms} = FerricStore.pttl("cache:result")
      iex> ms > 0 and ms <= 30_000
      true

      iex> FerricStore.pttl("nonexistent:key")
      {:ok, nil}

  """
  @spec pttl(key()) :: {:ok, non_neg_integer() | nil}
  def pttl(key), do: ttl(key)

  # ---------------------------------------------------------------------------
  # Bitmap operations
  # ---------------------------------------------------------------------------

  @doc """
  Sets or clears the bit at `offset` in the string value stored at `key`.

  Returns the original bit value at that position.

  ## Examples

      {:ok, 0} = FerricStore.setbit("key", 7, 1)

  """
  @spec setbit(key(), non_neg_integer(), 0 | 1) :: {:ok, 0 | 1} | {:error, binary()}
  def setbit(key, offset, bit_value) when bit_value in [0, 1] do
    cond do
      offset < 0 ->
        {:error, "ERR bit offset is not an integer or out of range"}

      offset > 4_294_967_295 ->
        {:error, "ERR bit offset is not an integer or out of range"}

      true ->
        wrap_result(Router.setbit(default_ctx(), key, offset, bit_value))
    end
  end

  @doc """
  Returns the bit value at `offset` in the string value stored at `key`.

  Returns `{:ok, 0}` for nonexistent keys or out-of-range offsets.

  ## Examples

      {:ok, 1} = FerricStore.getbit("key", 7)

  """
  @spec getbit(key(), non_neg_integer()) :: {:ok, 0 | 1}
  def getbit(key, offset) do
    store = build_string_store(key)
    result = Ferricstore.Commands.Bitmap.handle("GETBIT", [key, to_string(offset)], store)
    wrap_result(result)
  end

  @doc """
  Counts the number of set bits (1s) in the string value stored at `key`.

  ## Options

    * `:start` - Start byte offset (default: 0).
    * `:stop` - Stop byte offset (default: -1, meaning end of string).

  ## Returns

    * `{:ok, count}` on success.

  ## Examples

      {:ok, count} = FerricStore.bitcount("key")

  """
  @spec bitcount(key(), keyword()) :: {:ok, non_neg_integer()}
  def bitcount(key, opts \\ []) do
    store = build_string_store(key)
    start = Keyword.get(opts, :start)
    stop = Keyword.get(opts, :stop)

    args =
      if start != nil and stop != nil do
        [key, to_string(start), to_string(stop)]
      else
        [key]
      end

    result = Ferricstore.Commands.Bitmap.handle("BITCOUNT", args, store)
    wrap_result(result)
  end

  @doc """
  Performs a bitwise operation between strings stored at `source_keys` and
  stores the result in `dest_key`.

  ## Parameters

    * `op` - `:and`, `:or`, `:xor`, or `:not`
    * `dest_key` - Destination key.
    * `source_keys` - List of source keys.

  ## Returns

    * `{:ok, byte_length}` - Length of the result string.

  ## Examples

      {:ok, 3} = FerricStore.bitop(:and, "dest", ["key1", "key2"])

  """
  @spec bitop(atom(), key(), [key()]) :: {:ok, non_neg_integer()}
  def bitop(op, dest_key, source_keys) when is_atom(op) and is_list(source_keys) do
    op_str = op |> Atom.to_string() |> String.upcase()
    wrap_result(Router.bitmap_op(default_ctx(), "BITOP", [op_str, dest_key | source_keys]))
  end

  @doc """
  Finds the first bit set to `bit_value` (0 or 1) in the string at `key`.

  ## Options

    * `:start` - Start byte offset.
    * `:stop` - Stop byte offset.

  ## Returns

    * `{:ok, position}` - Bit position, or -1 if not found within a bounded range.

  ## Examples

      {:ok, 8} = FerricStore.bitpos("key", 1)

  """
  @spec bitpos(key(), 0 | 1, keyword()) :: {:ok, integer()}
  def bitpos(key, bit_value, opts \\ []) when bit_value in [0, 1] do
    store = build_string_store(key)
    start = Keyword.get(opts, :start)
    stop = Keyword.get(opts, :stop)

    args =
      cond do
        start != nil and stop != nil ->
          [key, to_string(bit_value), to_string(start), to_string(stop)]

        start != nil ->
          [key, to_string(bit_value), to_string(start)]

        true ->
          [key, to_string(bit_value)]
      end

    result = Ferricstore.Commands.Bitmap.handle("BITPOS", args, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Hash extended operations
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

    case Ferricstore.Commands.Hash.handle("HDEL", [key | str_fields], store) do
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

    case Ferricstore.Commands.Hash.handle("HEXISTS", [key, to_string(field)], store) do
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

    case Ferricstore.Commands.Hash.handle("HLEN", [key], store) do
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

    case Ferricstore.Commands.Hash.handle("HKEYS", [key], store) do
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

    case Ferricstore.Commands.Hash.handle("HVALS", [key], store) do
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

    case Ferricstore.Commands.Hash.handle("HMGET", [key | str_fields], store) do
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

    case Ferricstore.Commands.Hash.handle(
           "HSETNX",
           [key, to_string(field), to_string(value)],
           store
         ) do
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
        case Ferricstore.Commands.Hash.handle("HRANDFIELD", [key], store) do
          {:error, _} = err -> err
          result -> {:ok, result}
        end

      n when is_integer(n) ->
        case Ferricstore.Commands.Hash.handle(
               "HRANDFIELD",
               [key, Integer.to_string(n)],
               store
             ) do
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

    case Ferricstore.Commands.Hash.handle("HSTRLEN", [key, to_string(field)], store) do
      {:error, _} = err -> err
      len -> {:ok, len}
    end
  end

  # ---------------------------------------------------------------------------
  # List extended operations
  # ---------------------------------------------------------------------------

  @doc """
  Returns the element at `index` in the list stored at `key`.

  Negative indices count from the end (-1 is the last element). Returns
  `{:ok, nil}` for out-of-range indices or nonexistent keys.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.lindex("tasks:queue", 0)
      {:ok, "task_a"}

      iex> FerricStore.lindex("tasks:queue", -1)
      {:ok, "task_c"}

      iex> FerricStore.lindex("tasks:queue", 99)
      {:ok, nil}

  """
  @spec lindex(key(), integer()) :: {:ok, binary() | nil} | {:error, binary()}
  def lindex(key, index) do
    ctx = default_ctx()

    Router.list_op(ctx, key, {:lindex, index})
    |> wrap_result()
  end

  @doc """
  Sets the element at `index` in the list stored at `key`.

  Returns `:ok` on success, or `{:error, reason}` if the index is out of range
  or the key does not exist.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.lset("tasks:queue", 1, "task_b_updated")
      :ok

      iex> FerricStore.lset("tasks:queue", 99, "value")
      {:error, "ERR index out of range"}

  """
  @spec lset(key(), integer(), binary()) :: :ok | {:error, binary()}
  def lset(key, index, element) do
    ctx = default_ctx()

    result =
      Router.list_op(ctx, key, {:lset, index, element})

    case result do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  @doc """
  Removes occurrences of `element` from the list at `key`.

  The `count` argument controls the direction and number of removals:

    * `count > 0` - Remove up to `count` occurrences scanning from head to tail.
    * `count < 0` - Remove up to `abs(count)` occurrences scanning from tail to head.
    * `count == 0` - Remove all occurrences.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["retry", "send", "retry", "retry"])
      iex> FerricStore.lrem("tasks:queue", 0, "retry")
      {:ok, 3}

      iex> FerricStore.rpush("tasks:queue", ["a", "b", "a"])
      iex> FerricStore.lrem("tasks:queue", 1, "a")
      {:ok, 1}

  """
  @spec lrem(key(), integer(), binary()) :: {:ok, non_neg_integer()}
  def lrem(key, count, element) do
    ctx = default_ctx()

    case Router.list_op(ctx, key, {:lrem, count, element}) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Inserts `element` before or after `pivot` in the list at `key`.

  Returns `{:ok, new_length}` if the pivot was found, or `{:ok, -1}` if the
  pivot was not found. Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["task_a", "task_b", "task_c"])
      iex> FerricStore.linsert("tasks:queue", :before, "task_b", "task_new")
      {:ok, 4}

      iex> FerricStore.linsert("tasks:queue", :after, "task_c", "task_last")
      {:ok, 5}

      iex> FerricStore.linsert("tasks:queue", :before, "nonexistent", "x")
      {:ok, -1}

  """
  @spec linsert(key(), :before | :after, binary(), binary()) :: {:ok, integer()}
  def linsert(key, direction, pivot, element) when direction in [:before, :after] do
    ctx = default_ctx()

    case Router.list_op(ctx, key, {:linsert, direction, pivot, element}) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Atomically moves an element from one list to another.

  Pops from `from_dir` of `source` and pushes to `to_dir` of `destination`.
  Returns `{:ok, nil}` if the source list is empty or does not exist.

  ## Examples

      iex> FerricStore.rpush("inbox", ["msg_a", "msg_b"])
      iex> FerricStore.lmove("inbox", "processing", :left, :right)
      {:ok, "msg_a"}

      iex> FerricStore.lmove("empty_list", "dst", :left, :right)
      {:ok, nil}

  """
  @spec lmove(key(), key(), :left | :right, :left | :right) :: {:ok, binary() | nil}
  def lmove(source, destination, from_dir, to_dir)
      when from_dir in [:left, :right] and to_dir in [:left, :right] do
    from = from_dir |> Atom.to_string() |> String.upcase()
    to = to_dir |> Atom.to_string() |> String.upcase()

    Ferricstore.Commands.List.handle("LMOVE", [source, destination, from, to], %{})
    |> wrap_result()
  end

  @doc """
  Finds the position of `element` in the list at `key`.

  Returns the zero-based index of the first match, or `{:ok, nil}` if not
  found. When `:count` is specified, returns a list of indices.

  ## Options

    * `:rank` - Skip the first N-1 matches and return starting from the Nth
      (default: 1). Negative rank searches from tail.
    * `:count` - Return up to N positions. 0 means all. When given, always
      returns a list.
    * `:maxlen` - Limit scan to the first N elements (default: 0, no limit).

  ## Examples

      iex> FerricStore.rpush("tasks:queue", ["retry", "send", "retry", "process"])
      iex> FerricStore.lpos("tasks:queue", "retry")
      {:ok, 0}

      iex> FerricStore.lpos("tasks:queue", "retry", count: 0)
      {:ok, [0, 2]}

      iex> FerricStore.lpos("tasks:queue", "missing")
      {:ok, nil}

  """
  @spec lpos(key(), binary(), keyword()) :: {:ok, integer() | [integer()] | nil}
  def lpos(key, element, opts \\ []) do
    ctx = default_ctx()
    rank = Keyword.get(opts, :rank, 1)
    count = Keyword.get(opts, :count)
    maxlen = Keyword.get(opts, :maxlen, 0)

    case Router.list_op(ctx, key, {:lpos, element, rank, count, maxlen}) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  # ---------------------------------------------------------------------------
  # Set extended operations
  # ---------------------------------------------------------------------------

  @doc """
  Returns the membership status of multiple members in the set at `key`.

  Returns a list of 1s and 0s corresponding to each member, in the same order
  as the input list.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      iex> FerricStore.smismember("article:42:tags", ["elixir", "python", "database"])
      {:ok, [1, 0, 1]}

      iex> FerricStore.smismember("nonexistent", ["a", "b"])
      {:ok, [0, 0]}

  """
  @spec smismember(key(), [binary()]) :: {:ok, [0 | 1]}
  def smismember(key, members) when is_list(members) do
    store = build_compound_store(key)

    results =
      Enum.map(members, fn member ->
        compound_key = Ferricstore.Store.CompoundKey.set_member(key, member)
        if store.compound_get.(key, compound_key) != nil, do: 1, else: 0
      end)

    {:ok, results}
  end

  @doc """
  Returns one or more random members from the set at `key` without removing them.

  Without `count`, returns a single member or `nil` for empty/nonexistent sets.
  With positive `count`, returns up to `count` unique members. With negative
  `count`, returns `abs(count)` members with possible duplicates.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      iex> {:ok, member} = FerricStore.srandmember("article:42:tags")
      iex> member in ["elixir", "rust", "database"]
      true

      iex> {:ok, members} = FerricStore.srandmember("article:42:tags", 2)
      iex> length(members)
      2

      iex> FerricStore.srandmember("nonexistent")
      {:ok, nil}

  """
  @spec srandmember(key(), integer() | nil) :: {:ok, binary() | [binary()] | nil} | write_error()
  def srandmember(key, count \\ nil) do
    store = build_compound_store(key)

    args = if is_nil(count), do: [key], else: [key, to_string(count)]

    "SRANDMEMBER"
    |> Ferricstore.Commands.Set.handle(args, store)
    |> wrap_result()
  end

  @doc """
  Removes and returns one or more random members from the set at `key`.

  Without `count`, returns a single member or `nil` for empty/nonexistent sets.
  With `count`, returns a list of up to `count` removed members.

  ## Examples

      iex> FerricStore.sadd("article:42:tags", ["elixir", "rust", "database"])
      iex> {:ok, tag} = FerricStore.spop("article:42:tags")
      iex> tag in ["elixir", "rust", "database"]
      true

      iex> {:ok, tags} = FerricStore.spop("article:42:tags", 2)
      iex> length(tags)
      2

      iex> FerricStore.spop("nonexistent")
      {:ok, nil}

  """
  @spec spop(key(), non_neg_integer() | nil) :: {:ok, binary() | [binary()] | nil} | write_error()
  def spop(key, count \\ nil) do
    store = build_compound_store(key)

    args = if is_nil(count), do: [key], else: [key, to_string(count)]

    "SPOP"
    |> Ferricstore.Commands.Set.handle(args, store)
    |> wrap_result()
  end

  @doc """
  Returns the set difference: members in the first set that are not in any of the other sets.

  Handles cross-shard keys transparently. Returns `{:ok, []}` if the first
  key does not exist.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react", "tailwind"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> {:ok, diff} = FerricStore.sdiff(["frontend:tags", "backend:tags"])
      iex> Enum.sort(diff)
      ["react", "tailwind"]

  """
  @spec sdiff([key()]) :: {:ok, [binary()]}
  def sdiff([]), do: {:ok, []}

  def sdiff(keys) when is_list(keys) do
    result = Ferricstore.Commands.Set.handle("SDIFF", keys, build_compound_store(hd(keys)))
    wrap_result(result)
  end

  @doc """
  Returns the set intersection: members common to all given sets.

  Handles cross-shard keys transparently. Returns `{:ok, []}` if any key
  does not exist.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react", "tailwind"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> FerricStore.sinter(["frontend:tags", "backend:tags"])
      {:ok, ["elixir"]}

  """
  @spec sinter([key()]) :: {:ok, [binary()]}
  def sinter([]), do: {:ok, []}

  def sinter(keys) when is_list(keys) do
    result = Ferricstore.Commands.Set.handle("SINTER", keys, build_compound_store(hd(keys)))
    wrap_result(result)
  end

  @doc """
  Returns the set union: all unique members across all given sets.

  Handles cross-shard keys transparently.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> {:ok, union} = FerricStore.sunion(["frontend:tags", "backend:tags"])
      iex> Enum.sort(union)
      ["elixir", "postgres", "react"]

  """
  @spec sunion([key()]) :: {:ok, [binary()]}
  def sunion([]), do: {:ok, []}

  def sunion(keys) when is_list(keys) do
    result = Ferricstore.Commands.Set.handle("SUNION", keys, build_compound_store(hd(keys)))
    wrap_result(result)
  end

  @doc """
  Computes the set difference of the given keys and stores the result in `destination`.

  Any existing value at `destination` is overwritten. Returns the number of
  elements in the resulting set.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react", "tailwind"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> FerricStore.sdiffstore("frontend_only:tags", ["frontend:tags", "backend:tags"])
      {:ok, 2}

  """
  @spec sdiffstore(key(), [key()]) :: {:ok, non_neg_integer()}
  def sdiffstore(destination, keys) when is_list(keys) do
    result = Ferricstore.Commands.Set.handle("SDIFFSTORE", [destination | keys], %{})
    wrap_result(result)
  end

  @doc """
  Computes the set intersection of the given keys and stores the result in `destination`.

  Any existing value at `destination` is overwritten. Returns the number of
  elements in the resulting set.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> FerricStore.sinterstore("shared:tags", ["frontend:tags", "backend:tags"])
      {:ok, 1}

  """
  @spec sinterstore(key(), [key()]) :: {:ok, non_neg_integer()}
  def sinterstore(destination, keys) when is_list(keys) do
    result = Ferricstore.Commands.Set.handle("SINTERSTORE", [destination | keys], %{})
    wrap_result(result)
  end

  @doc """
  Computes the set union of the given keys and stores the result in `destination`.

  Any existing value at `destination` is overwritten. Returns the number of
  elements in the resulting set.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres"])
      iex> FerricStore.sunionstore("all:tags", ["frontend:tags", "backend:tags"])
      {:ok, 3}

  """
  @spec sunionstore(key(), [key()]) :: {:ok, non_neg_integer()}
  def sunionstore(destination, keys) when is_list(keys) do
    result = Ferricstore.Commands.Set.handle("SUNIONSTORE", [destination | keys], %{})
    wrap_result(result)
  end

  @doc """
  Returns the cardinality of the intersection of all given sets.

  More efficient than `sinter/1` when you only need the count, not the
  actual members.

  ## Options

    * `:limit` - Stop counting after reaching this limit (0 means no limit,
      default: 0). Useful for early termination on large sets.

  ## Examples

      iex> FerricStore.sadd("frontend:tags", ["elixir", "react", "tailwind"])
      iex> FerricStore.sadd("backend:tags", ["elixir", "postgres", "tailwind"])
      iex> FerricStore.sintercard(["frontend:tags", "backend:tags"])
      {:ok, 2}

      iex> FerricStore.sintercard(["frontend:tags", "backend:tags"], limit: 1)
      {:ok, 1}

  """
  @spec sintercard([key()], keyword()) :: {:ok, non_neg_integer()}
  def sintercard(keys, opts \\ []) when is_list(keys) do
    numkeys = Integer.to_string(length(keys))
    limit = Keyword.get(opts, :limit, 0)

    args =
      [numkeys | keys] ++
        if limit > 0, do: ["LIMIT", Integer.to_string(limit)], else: []

    store =
      case keys do
        [first | _] -> build_compound_store(first)
        [] -> build_compound_store("")
      end

    result = Ferricstore.Commands.Set.handle("SINTERCARD", args, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Sorted Set extended operations
  # ---------------------------------------------------------------------------

  @doc """
  Returns the rank of `member` in the sorted set at `key` (ascending score order).

  Rank is 0-based (the member with the lowest score has rank 0). Returns
  `{:ok, nil}` if the member or key does not exist.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zrank("leaderboard", "alice")
      {:ok, 0}

      iex> FerricStore.zrank("leaderboard", "charlie")
      {:ok, 2}

      iex> FerricStore.zrank("leaderboard", "unknown")
      {:ok, nil}

  """
  @spec zrank(key(), binary()) :: {:ok, non_neg_integer() | nil}
  def zrank(key, member) do
    store = build_compound_store(key)
    result = Ferricstore.Commands.SortedSet.handle("ZRANK", [key, member], store)
    wrap_result(result)
  end

  @doc """
  Returns the reverse rank of `member` in the sorted set at `key` (descending score order).

  Rank is 0-based (the member with the highest score has rank 0). Returns
  `{:ok, nil}` if the member or key does not exist.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zrevrank("leaderboard", "charlie")
      {:ok, 0}

      iex> FerricStore.zrevrank("leaderboard", "alice")
      {:ok, 2}

      iex> FerricStore.zrevrank("leaderboard", "unknown")
      {:ok, nil}

  """
  @spec zrevrank(key(), binary()) :: {:ok, non_neg_integer() | nil}
  def zrevrank(key, member) do
    store = build_compound_store(key)
    result = Ferricstore.Commands.SortedSet.handle("ZREVRANK", [key, member], store)
    wrap_result(result)
  end

  @doc """
  Returns members with scores between `min` and `max` (inclusive by default).

  Use "-inf" and "+inf" for unbounded ranges. Prefix a bound with "(" for
  exclusive (e.g., "(100" means score > 100).

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zrangebyscore("leaderboard", "100", "200")
      {:ok, ["alice", "bob"]}

      iex> FerricStore.zrangebyscore("leaderboard", "-inf", "+inf")
      {:ok, ["alice", "bob", "charlie"]}

      iex> FerricStore.zrangebyscore("leaderboard", "(200", "+inf")
      {:ok, ["charlie"]}

  """
  @spec zrangebyscore(key(), binary(), binary(), keyword()) :: {:ok, [binary()]}
  def zrangebyscore(key, min, max, _opts \\ []) do
    store = build_compound_store(key)
    result = Ferricstore.Commands.SortedSet.handle("ZRANGEBYSCORE", [key, min, max], store)
    wrap_result(result)
  end

  @doc """
  Counts members in the sorted set at `key` with scores between `min` and `max`.

  Use "-inf" and "+inf" for unbounded ranges. Prefix a bound with "(" for
  exclusive.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zcount("leaderboard", "100", "200")
      {:ok, 2}

      iex> FerricStore.zcount("leaderboard", "-inf", "+inf")
      {:ok, 3}

  """
  @spec zcount(key(), binary(), binary()) :: {:ok, non_neg_integer()}
  def zcount(key, min, max) do
    store = build_compound_store(key)
    result = Ferricstore.Commands.SortedSet.handle("ZCOUNT", [key, min, max], store)
    wrap_result(result)
  end

  @doc """
  Increments the score of `member` in the sorted set at `key` by `increment`.

  Creates the member with the given increment as score if it does not exist.
  Returns the new score as a string.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}])
      iex> FerricStore.zincrby("leaderboard", 50.0, "alice")
      {:ok, "150.0"}

      iex> FerricStore.zincrby("leaderboard", 25.0, "newcomer")
      {:ok, "25.0"}

  """
  @spec zincrby(key(), number(), binary()) :: {:ok, binary()} | {:error, binary()}
  def zincrby(key, increment, member) do
    wrap_result(Router.zincrby(default_ctx(), key, increment * 1.0, member))
  end

  @doc """
  Returns one or more random members from the sorted set at `key`.

  Without `count`, returns a single member or `nil` for empty/nonexistent keys.
  With positive `count`, returns up to `count` unique members. With negative
  `count`, returns `abs(count)` members with possible duplicates.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> {:ok, member} = FerricStore.zrandmember("leaderboard")
      iex> member in ["alice", "bob", "charlie"]
      true

      iex> {:ok, members} = FerricStore.zrandmember("leaderboard", 2)
      iex> length(members)
      2

      iex> FerricStore.zrandmember("nonexistent")
      {:ok, nil}

  """
  @spec zrandmember(key(), integer() | nil) :: {:ok, binary() | [binary()] | nil}
  def zrandmember(key, count \\ nil) do
    store = build_compound_store(key)

    case count do
      nil ->
        result = Ferricstore.Commands.SortedSet.handle("ZRANDMEMBER", [key], store)
        wrap_result(result)

      n ->
        result = Ferricstore.Commands.SortedSet.handle("ZRANDMEMBER", [key, to_string(n)], store)
        wrap_result(result)
    end
  end

  @doc """
  Removes and returns up to `count` members with the lowest scores.

  Returns `{:ok, []}` if the key does not exist or the sorted set is empty.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zpopmin("leaderboard", 1)
      {:ok, [{"alice", 100.0}]}

      iex> FerricStore.zpopmin("leaderboard", 2)
      {:ok, [{"bob", 200.0}, {"charlie", 300.0}]}

  """
  @spec zpopmin(key(), pos_integer()) :: {:ok, [{binary(), float()}]}
  def zpopmin(key, count \\ 1) do
    store = build_compound_store(key)
    result = Ferricstore.Commands.SortedSet.handle("ZPOPMIN", [key, to_string(count)], store)

    case result do
      {:error, _} = err ->
        err

      flat when is_list(flat) ->
        pairs =
          flat
          |> Enum.chunk_every(2)
          |> Enum.map(fn [member, score_str] ->
            {score, _} = Float.parse(score_str)
            {member, score}
          end)

        {:ok, pairs}
    end
  end

  @doc """
  Removes and returns up to `count` members with the highest scores.

  Returns `{:ok, []}` if the key does not exist or the sorted set is empty.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zpopmax("leaderboard", 1)
      {:ok, [{"charlie", 300.0}]}

      iex> FerricStore.zpopmax("leaderboard", 2)
      {:ok, [{"bob", 200.0}, {"alice", 100.0}]}

  """
  @spec zpopmax(key(), pos_integer()) :: {:ok, [{binary(), float()}]}
  def zpopmax(key, count \\ 1) do
    store = build_compound_store(key)
    result = Ferricstore.Commands.SortedSet.handle("ZPOPMAX", [key, to_string(count)], store)

    case result do
      {:error, _} = err ->
        err

      flat when is_list(flat) ->
        pairs =
          flat
          |> Enum.chunk_every(2)
          |> Enum.map(fn [member, score_str] ->
            {score, _} = Float.parse(score_str)
            {member, score}
          end)

        {:ok, pairs}
    end
  end

  @doc """
  Returns scores for multiple members in the sorted set at `key`.

  Returns `nil` for members that do not exist. The order of returned scores
  matches the order of the input members.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])
      iex> FerricStore.zmscore("leaderboard", ["alice", "unknown", "bob"])
      {:ok, [100.0, nil, 200.0]}

  """
  @spec zmscore(key(), [binary()]) :: {:ok, [float() | nil]}
  def zmscore(key, members) when is_list(members) do
    store = build_compound_store(key)
    result = Ferricstore.Commands.SortedSet.handle("ZMSCORE", [key | members], store)

    case result do
      {:error, _} = err ->
        err

      scores when is_list(scores) ->
        parsed =
          Enum.map(scores, fn
            nil ->
              nil

            score_str when is_binary(score_str) ->
              {score, _} = Float.parse(score_str)
              score
          end)

        {:ok, parsed}
    end
  end

  # ---------------------------------------------------------------------------
  # Streams
  # ---------------------------------------------------------------------------

  @doc """
  Appends an entry to the stream at `key` with an auto-generated ID.

  `fields` is a flat list of field-value pairs: `["field1", "val1", "field2", "val2"]`.
  Streams are append-only logs ideal for event sourcing, activity feeds, and audit trails.

  ## Returns

    * `{:ok, entry_id}` where `entry_id` is a `"timestamp-seq"` string.
    * `{:error, reason}` on failure.

  ## Examples

      iex> FerricStore.xadd("events:user:42", ["action", "login", "ip", "10.0.0.1"])
      {:ok, "1711234567890-0"}

      iex> FerricStore.xadd("activity:feed", ["type", "comment", "body", "looks great!"])
      {:ok, "1711234567891-0"}

  """
  @spec xadd(key(), [binary()]) :: {:ok, binary()} | {:error, binary()}
  def xadd(key, fields) when is_list(fields) do
    store = build_stream_store(key)
    result = Ferricstore.Commands.Stream.handle("XADD", [key, "*" | fields], store)
    wrap_result(result)
  end

  @doc """
  Returns the number of entries in the stream at `key`.

  ## Returns

    * `{:ok, length}` on success.

  ## Examples

      iex> FerricStore.xlen("events:user:42")
      {:ok, 5}

  """
  @spec xlen(key()) :: {:ok, non_neg_integer()}
  def xlen(key) do
    store = build_stream_store(key)
    result = Ferricstore.Commands.Stream.handle("XLEN", [key], store)
    wrap_result(result)
  end

  @doc """
  Returns entries from the stream at `key` in forward (oldest-first) order between `start` and `stop`.

  Use `"-"` for the minimum and `"+"` for the maximum stream IDs.

  ## Options

    * `:count` - Maximum number of entries to return.

  ## Returns

    * `{:ok, entries}` where entries is a list of `{id, [field, value, ...]}` tuples.

  ## Examples

      iex> FerricStore.xrange("events:user:42", "-", "+", count: 10)
      {:ok, [{"1711234567890-0", ["action", "login", "ip", "10.0.0.1"]}]}

      iex> FerricStore.xrange("activity:feed", "-", "+")
      {:ok, [{"1711234567891-0", ["type", "comment", "body", "looks great!"]}]}

  """
  @spec xrange(key(), binary(), binary(), keyword()) :: {:ok, [tuple()]}
  def xrange(key, start, stop, opts \\ []) do
    store = build_stream_store(key)
    count = Keyword.get(opts, :count)
    args = [key, start, stop]
    args = if count, do: args ++ ["COUNT", to_string(count)], else: args
    result = Ferricstore.Commands.Stream.handle("XRANGE", args, store)
    wrap_result(result)
  end

  @doc """
  Returns entries from the stream at `key` in reverse (newest-first) order between `stop` and `start`.

  ## Options

    * `:count` - Maximum number of entries to return.

  ## Returns

    * `{:ok, entries}` where entries is a list of `{id, [field, value, ...]}` tuples.

  ## Examples

      iex> FerricStore.xrevrange("events:user:42", "+", "-", count: 5)
      {:ok, [{"1711234567890-0", ["action", "login", "ip", "10.0.0.1"]}]}

  """
  @spec xrevrange(key(), binary(), binary(), keyword()) :: {:ok, [tuple()]}
  def xrevrange(key, stop, start, opts \\ []) do
    store = build_stream_store(key)
    count = Keyword.get(opts, :count)
    args = [key, stop, start]
    args = if count, do: args ++ ["COUNT", to_string(count)], else: args
    result = Ferricstore.Commands.Stream.handle("XREVRANGE", args, store)
    wrap_result(result)
  end

  @doc """
  Trims the stream at `key` to a maximum number of entries, evicting the oldest.

  Useful for capping event logs and activity feeds to prevent unbounded growth.

  ## Options

    * `:maxlen` (required) - Maximum number of entries to keep.

  ## Returns

    * `{:ok, trimmed_count}` - the number of entries removed.

  ## Examples

      iex> FerricStore.xtrim("events:user:42", maxlen: 1000)
      {:ok, 5}

  """
  @spec xtrim(key(), keyword()) :: {:ok, non_neg_integer()}
  def xtrim(key, opts) do
    store = build_stream_store(key)
    maxlen = Keyword.fetch!(opts, :maxlen)

    result =
      Ferricstore.Commands.Stream.handle("XTRIM", [key, "MAXLEN", to_string(maxlen)], store)

    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Bloom Filter operations
  # ---------------------------------------------------------------------------

  @doc """
  Creates a Bloom filter with specific error rate and capacity.

  ## Examples

      :ok = FerricStore.bf_reserve("filter", 0.01, 1000)

  """
  @spec bf_reserve(key(), float(), pos_integer()) :: :ok | {:error, binary()}
  def bf_reserve(key, error_rate, capacity) do
    store = build_prob_store(key)

    Ferricstore.Commands.Bloom.handle(
      "BF.RESERVE",
      [key, to_string(error_rate), to_string(capacity)],
      store
    )
  end

  @doc """
  Adds an element to the Bloom filter at `key`, auto-creating if needed.

  ## Returns

    * `{:ok, 1}` if the element was added.
    * `{:ok, 0}` if the element was already present.

  ## Examples

      {:ok, 1} = FerricStore.bf_add("filter", "hello")

  """
  @spec bf_add(key(), binary()) :: {:ok, 0 | 1}
  def bf_add(key, element) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Bloom.handle("BF.ADD", [key, element], store)
    wrap_result(result)
  end

  @doc """
  Adds multiple elements to the Bloom filter at `key`.

  ## Returns

    * `{:ok, [0 | 1, ...]}` for each element.

  """
  @spec bf_madd(key(), [binary()]) :: {:ok, [0 | 1]}
  def bf_madd(key, elements) when is_list(elements) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Bloom.handle("BF.MADD", [key | elements], store)
    wrap_result(result)
  end

  @doc """
  Checks if an element may exist in the Bloom filter at `key`.

  ## Returns

    * `{:ok, 1}` if the element may exist.
    * `{:ok, 0}` if the element definitely does not exist.

  """
  @spec bf_exists(key(), binary()) :: {:ok, 0 | 1}
  def bf_exists(key, element) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Bloom.handle("BF.EXISTS", [key, element], store)
    wrap_result(result)
  end

  @doc """
  Checks if multiple elements may exist in the Bloom filter at `key`.

  ## Returns

    * `{:ok, [0 | 1, ...]}` for each element.

  """
  @spec bf_mexists(key(), [binary()]) :: {:ok, [0 | 1]}
  def bf_mexists(key, elements) when is_list(elements) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Bloom.handle("BF.MEXISTS", [key | elements], store)
    wrap_result(result)
  end

  @doc """
  Returns the approximate number of unique elements added to the Bloom filter at `key`.

  ## Returns

    * `{:ok, count}` on success.

  ## Examples

      iex> FerricStore.bf_card("emails:seen")
      {:ok, 42}

  """
  @spec bf_card(key()) :: {:ok, non_neg_integer()}
  def bf_card(key) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Bloom.handle("BF.CARD", [key], store)
    wrap_result(result)
  end

  @doc """
  Returns metadata about the Bloom filter at `key` (capacity, error rate, size, etc.).

  ## Returns

    * `{:ok, info_list}` - flat key-value list of filter properties.
    * `{:error, reason}` if the filter does not exist.

  ## Examples

      iex> FerricStore.bf_info("emails:seen")
      {:ok, ["Capacity", 100000, "Size", 120048, "Number of filters", 1, "Number of items inserted", 42, "Expansion rate", 2]}

  """
  @spec bf_info(key()) :: {:ok, list()} | {:error, binary()}
  def bf_info(key) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Bloom.handle("BF.INFO", [key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Cuckoo Filter operations
  # ---------------------------------------------------------------------------

  @doc """
  Creates a Cuckoo filter with the specified capacity.

  A Cuckoo filter is similar to a Bloom filter but supports deletion and counting.
  Use it when you need probabilistic membership checks with the ability to remove
  items later (e.g., tracking active sessions that can be revoked).

  ## Parameters

    * `key` - the Cuckoo filter key
    * `capacity` - expected number of elements

  ## Examples

      iex> FerricStore.cf_reserve("sessions:active", 50_000)
      :ok

  """
  @spec cf_reserve(key(), pos_integer()) :: :ok | {:error, binary()}
  def cf_reserve(key, capacity) do
    store = build_prob_store(key)
    Ferricstore.Commands.Cuckoo.handle("CF.RESERVE", [key, to_string(capacity)], store)
  end

  @doc """
  Adds an element to the Cuckoo filter at `key`, auto-creating if needed.

  Unlike Bloom filters, duplicate insertions increase the count for the element.

  ## Returns

    * `{:ok, 1}` on success.
    * `{:error, reason}` if the filter is full and cannot accommodate the element.

  ## Examples

      iex> FerricStore.cf_add("sessions:active", "sess_abc123")
      {:ok, 1}

  """
  @spec cf_add(key(), binary()) :: {:ok, 0 | 1} | {:error, binary()}
  def cf_add(key, element) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.ADD", [key, element], store)
    wrap_result(result)
  end

  @doc """
  Adds an element to the Cuckoo filter only if it is not already present.

  ## Returns

    * `{:ok, 1}` if the element was newly added.
    * `{:ok, 0}` if the element already exists.
    * `{:error, reason}` if the filter is full.

  ## Examples

      iex> FerricStore.cf_addnx("sessions:active", "sess_abc123")
      {:ok, 1}

      iex> FerricStore.cf_addnx("sessions:active", "sess_abc123")
      {:ok, 0}

  """
  @spec cf_addnx(key(), binary()) :: {:ok, 0 | 1} | {:error, binary()}
  def cf_addnx(key, element) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.ADDNX", [key, element], store)
    wrap_result(result)
  end

  @doc """
  Deletes one occurrence of an element from the Cuckoo filter at `key`.

  This is the key advantage of Cuckoo filters over Bloom filters -- elements
  can be removed. Only deletes one occurrence if the element was added multiple times.

  ## Returns

    * `{:ok, 1}` if the element was deleted.
    * `{:ok, 0}` if the element was not found.

  ## Examples

      iex> FerricStore.cf_del("sessions:active", "sess_abc123")
      {:ok, 1}

  """
  @spec cf_del(key(), binary()) :: {:ok, 0 | 1}
  def cf_del(key, element) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.DEL", [key, element], store)
    wrap_result(result)
  end

  @doc """
  Checks if an element may exist in the Cuckoo filter at `key`.

  ## Returns

    * `{:ok, 1}` if the element probably exists.
    * `{:ok, 0}` if the element definitely does not exist.

  ## Examples

      iex> FerricStore.cf_exists("sessions:active", "sess_abc123")
      {:ok, 1}

  """
  @spec cf_exists(key(), binary()) :: {:ok, 0 | 1}
  def cf_exists(key, element) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.EXISTS", [key, element], store)
    wrap_result(result)
  end

  @doc """
  Checks multiple elements against the Cuckoo filter at `key` in a single call.

  ## Returns

    * `{:ok, [0 | 1, ...]}` - `1` for probably present, `0` for definitely absent,
      one per element.

  ## Examples

      iex> FerricStore.cf_mexists("sessions:active", ["sess_abc123", "sess_unknown"])
      {:ok, [1, 0]}

  """
  @spec cf_mexists(key(), [binary()]) :: {:ok, [0 | 1]}
  def cf_mexists(key, elements) when is_list(elements) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.MEXISTS", [key | elements], store)
    wrap_result(result)
  end

  @doc """
  Returns the approximate number of times an element was added to the Cuckoo filter.

  ## Returns

    * `{:ok, count}` - estimated insertion count for the element.

  ## Examples

      iex> FerricStore.cf_count("sessions:active", "sess_abc123")
      {:ok, 1}

  """
  @spec cf_count(key(), binary()) :: {:ok, non_neg_integer()}
  def cf_count(key, element) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.COUNT", [key, element], store)
    wrap_result(result)
  end

  @doc """
  Returns metadata about the Cuckoo filter at `key` (size, bucket count, etc.).

  ## Returns

    * `{:ok, info_list}` - flat key-value list of filter properties.
    * `{:error, reason}` if the filter does not exist.

  ## Examples

      iex> FerricStore.cf_info("sessions:active")
      {:ok, ["Size", 1024, "Number of buckets", 512, "Number of filters", 1, "Number of items inserted", 3, "Number of items deleted", 0, "Bucket size", 2, "Expansion rate", 1, "Max iterations", 20]}

  """
  @spec cf_info(key()) :: {:ok, list()} | {:error, binary()}
  def cf_info(key) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.Cuckoo.handle("CF.INFO", [key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Count-Min Sketch operations
  # ---------------------------------------------------------------------------

  @doc """
  Creates a Count-Min Sketch with the given `width` and `depth` dimensions.

  A Count-Min Sketch is a probabilistic structure for approximate frequency counting.
  It uses sub-linear space and answers "how many times has X been seen?" with bounded
  over-estimation. Ideal for view counts, click tracking, and frequency analysis
  where exact counts are not required.

  ## Parameters

    * `key` - the CMS key
    * `width` - number of counters per hash function (larger = more accurate)
    * `depth` - number of hash functions (larger = lower error probability)

  ## Examples

      iex> FerricStore.cms_initbydim("page:views", 2000, 5)
      :ok

  """
  @spec cms_initbydim(key(), pos_integer(), pos_integer()) :: :ok | {:error, binary()}
  def cms_initbydim(key, width, depth) do
    store = build_prob_store(key)

    Ferricstore.Commands.CMS.handle(
      "CMS.INITBYDIM",
      [key, to_string(width), to_string(depth)],
      store
    )
  end

  @doc """
  Creates a Count-Min Sketch with a target error rate and over-estimation probability.

  The sketch dimensions (width/depth) are computed automatically from the error bounds.

  ## Parameters

    * `key` - the CMS key
    * `error` - acceptable error rate as a fraction (e.g. `0.001` for 0.1%)
    * `probability` - probability of exceeding the error rate (e.g. `0.01` for 1%)

  ## Examples

      iex> FerricStore.cms_initbyprob("click:tracking", 0.001, 0.01)
      :ok

  """
  @spec cms_initbyprob(key(), float(), float()) :: :ok | {:error, binary()}
  def cms_initbyprob(key, error, probability) do
    store = build_prob_store(key)

    Ferricstore.Commands.CMS.handle(
      "CMS.INITBYPROB",
      [key, to_string(error), to_string(probability)],
      store
    )
  end

  @doc """
  Increments the count for one or more elements in the Count-Min Sketch.

  ## Parameters

    * `key` - the CMS key
    * `pairs` - list of `{element, increment}` tuples

  ## Returns

    * `{:ok, [new_count, ...]}` - estimated count after increment, one per element.
    * `{:error, reason}` if the sketch does not exist.

  ## Examples

      iex> FerricStore.cms_incrby("page:views", [{"homepage", 1}, {"about", 3}])
      {:ok, [1, 3]}

  """
  @spec cms_incrby(key(), [{binary(), pos_integer()}]) ::
          {:ok, [non_neg_integer()]} | {:error, binary()}
  def cms_incrby(key, pairs) when is_list(pairs) do
    store = build_prob_store(key)
    args = Enum.flat_map(pairs, fn {element, count} -> [element, to_string(count)] end)
    result = Ferricstore.Commands.CMS.handle("CMS.INCRBY", [key | args], store)
    wrap_result(result)
  end

  @doc """
  Queries the estimated frequency count for one or more elements in the Count-Min Sketch.

  Counts may be over-estimated but never under-estimated.

  ## Returns

    * `{:ok, [count, ...]}` - estimated count per element.
    * `{:error, reason}` if the sketch does not exist.

  ## Examples

      iex> FerricStore.cms_query("page:views", ["homepage", "about", "unknown"])
      {:ok, [42, 7, 0]}

  """
  @spec cms_query(key(), [binary()]) :: {:ok, [non_neg_integer()]} | {:error, binary()}
  def cms_query(key, elements) when is_list(elements) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.CMS.handle("CMS.QUERY", [key | elements], store)
    wrap_result(result)
  end

  @doc """
  Returns metadata about the Count-Min Sketch at `key` (width, depth, total count).

  ## Returns

    * `{:ok, info_list}` - flat key-value list of sketch properties.
    * `{:error, reason}` if the sketch does not exist.

  ## Examples

      iex> FerricStore.cms_info("page:views")
      {:ok, ["width", 2000, "depth", 5, "count", 49]}

  """
  @spec cms_info(key()) :: {:ok, list()} | {:error, binary()}
  def cms_info(key) do
    store = build_prob_store(key)
    result = Ferricstore.Commands.CMS.handle("CMS.INFO", [key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # TopK operations
  # ---------------------------------------------------------------------------

  @doc """
  Creates a Top-K tracker that maintains the `k` most frequent elements.

  A Top-K structure uses the Heavy Keeper algorithm to efficiently track the
  most popular items in a data stream with bounded memory. Ideal for trending
  topics, popular search queries, and hot product tracking.

  ## Parameters

    * `key` - the Top-K tracker key
    * `k` - number of top elements to track

  ## Examples

      iex> FerricStore.topk_reserve("trending:searches", 10)
      :ok

  """
  @spec topk_reserve(key(), pos_integer()) :: :ok | {:error, binary()}
  def topk_reserve(key, k) do
    store = build_topk_store(key)
    Ferricstore.Commands.TopK.handle("TOPK.RESERVE", [key, to_string(k)], store)
  end

  @doc """
  Adds one or more elements to the Top-K tracker, updating frequency counts.

  If an element displaces another from the top-k, the displaced element is returned.

  ## Returns

    * `{:ok, [displaced | nil, ...]}` - `nil` if no element was displaced,
      or the name of the displaced element, one per input.

  ## Examples

      iex> FerricStore.topk_add("trending:searches", ["elixir", "rust", "golang"])
      {:ok, [nil, nil, nil]}

  """
  @spec topk_add(key(), [binary()]) :: {:ok, list()} | {:error, binary()}
  def topk_add(key, elements) when is_list(elements) do
    store = build_topk_store(key)
    result = Ferricstore.Commands.TopK.handle("TOPK.ADD", [key | elements], store)
    wrap_result(result)
  end

  @doc """
  Checks whether elements are currently in the Top-K set.

  ## Returns

    * `{:ok, [0 | 1, ...]}` - `1` if the element is in the top-k, `0` otherwise.

  ## Examples

      iex> FerricStore.topk_query("trending:searches", ["elixir", "obscure-lang"])
      {:ok, [1, 0]}

  """
  @spec topk_query(key(), [binary()]) :: {:ok, list()} | {:error, binary()}
  def topk_query(key, elements) when is_list(elements) do
    store = build_topk_store(key)
    result = Ferricstore.Commands.TopK.handle("TOPK.QUERY", [key | elements], store)
    wrap_result(result)
  end

  @doc """
  Returns the current Top-K elements, ordered by estimated frequency (descending).

  ## Returns

    * `{:ok, [element, ...]}` - the top-k element names.
    * `{:error, reason}` if the tracker does not exist.

  ## Examples

      iex> FerricStore.topk_list("trending:searches")
      {:ok, ["elixir", "rust", "golang"]}

  """
  @spec topk_list(key()) :: {:ok, [binary()]} | {:error, binary()}
  def topk_list(key) do
    store = build_topk_store(key)
    result = Ferricstore.Commands.TopK.handle("TOPK.LIST", [key], store)
    wrap_result(result)
  end

  @doc """
  Returns metadata about the Top-K tracker at `key` (k, width, depth, decay).

  ## Returns

    * `{:ok, info_list}` - flat key-value list of tracker properties.
    * `{:error, reason}` if the tracker does not exist.

  ## Examples

      iex> FerricStore.topk_info("trending:searches")
      {:ok, ["k", 10, "width", 8, "depth", 7, "decay", "0.9"]}

  """
  @spec topk_info(key()) :: {:ok, list()} | {:error, binary()}
  def topk_info(key) do
    store = build_topk_store(key)
    result = Ferricstore.Commands.TopK.handle("TOPK.INFO", [key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # T-Digest operations
  # ---------------------------------------------------------------------------

  @doc """
  Creates a T-Digest structure at `key` for estimating quantiles and percentiles.

  A T-Digest compactly summarizes a distribution of numeric values, enabling
  accurate estimation of percentiles (p50, p95, p99) with bounded memory.
  Ideal for latency monitoring, response time analysis, and SLA tracking.

  ## Examples

      iex> FerricStore.tdigest_create("api:latency:ms")
      :ok

  """
  @spec tdigest_create(key()) :: :ok | {:error, binary()}
  def tdigest_create(key) do
    Router.tdigest_op(default_ctx(), "TDIGEST.CREATE", [key])
  end

  @doc """
  Adds one or more numeric observations to the T-Digest at `key`.

  ## Parameters

    * `key` - the T-Digest key
    * `values` - list of numeric values to add

  ## Examples

      iex> FerricStore.tdigest_add("api:latency:ms", [12.5, 45.0, 3.2, 89.1, 150.0])
      :ok

  """
  @spec tdigest_add(key(), [number()]) :: :ok | {:error, binary()}
  def tdigest_add(key, values) when is_list(values) do
    value_strs = Enum.map(values, &to_string(&1 * 1.0))
    Router.tdigest_op(default_ctx(), "TDIGEST.ADD", [key | value_strs])
  end

  @doc """
  Estimates the values at the given quantile points (0.0 to 1.0).

  For example, quantile `0.5` is the median, `0.95` is the 95th percentile.

  ## Returns

    * `{:ok, [value, ...]}` - estimated value at each quantile.
    * `{:error, reason}` if the digest does not exist.

  ## Examples

      iex> FerricStore.tdigest_quantile("api:latency:ms", [0.5, 0.95, 0.99])
      {:ok, ["45.0", "150.0", "150.0"]}

  """
  @spec tdigest_quantile(key(), [float()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_quantile(key, quantiles) when is_list(quantiles) do
    store = build_tdigest_store(key)
    qs = Enum.map(quantiles, &to_string(&1 * 1.0))
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.QUANTILE", [key | qs], store)
    wrap_result(result)
  end

  @doc """
  Estimates the cumulative distribution function (CDF) at the given values.

  Returns the fraction of observations less than or equal to each value.
  For example, a CDF of `0.95` at value `100` means 95% of observations
  were <= 100.

  ## Returns

    * `{:ok, [fraction, ...]}` - CDF value (0.0 to 1.0) at each input.

  ## Examples

      iex> FerricStore.tdigest_cdf("api:latency:ms", [50.0, 100.0])
      {:ok, ["0.6", "0.8"]}

  """
  @spec tdigest_cdf(key(), [number()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_cdf(key, values) when is_list(values) do
    store = build_tdigest_store(key)
    vs = Enum.map(values, &to_string(&1 * 1.0))
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.CDF", [key | vs], store)
    wrap_result(result)
  end

  @doc """
  Returns the minimum value observed in the T-Digest at `key`.

  ## Examples

      iex> FerricStore.tdigest_min("api:latency:ms")
      {:ok, "3.2"}

  """
  @spec tdigest_min(key()) :: {:ok, binary()} | {:error, binary()}
  def tdigest_min(key) do
    store = build_tdigest_store(key)
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.MIN", [key], store)
    wrap_result(result)
  end

  @doc """
  Returns the maximum value observed in the T-Digest at `key`.

  ## Examples

      iex> FerricStore.tdigest_max("api:latency:ms")
      {:ok, "150.0"}

  """
  @spec tdigest_max(key()) :: {:ok, binary()} | {:error, binary()}
  def tdigest_max(key) do
    store = build_tdigest_store(key)
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.MAX", [key], store)
    wrap_result(result)
  end

  @doc """
  Returns metadata about the T-Digest at `key` (compression, total observations, etc.).

  ## Returns

    * `{:ok, info_list}` - flat key-value list of digest properties.
    * `{:error, reason}` if the digest does not exist.

  ## Examples

      iex> FerricStore.tdigest_info("api:latency:ms")
      {:ok, ["Compression", 100, "Capacity", 610, "Merged nodes", 5, "Unmerged nodes", 0, "Merged weight", "5.0", "Unmerged weight", "0.0", "Total compressions", 1]}

  """
  @spec tdigest_info(key()) :: {:ok, list()} | {:error, binary()}
  def tdigest_info(key) do
    store = build_tdigest_store(key)
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.INFO", [key], store)
    wrap_result(result)
  end

  @doc """
  Resets the T-Digest at `key`, discarding all observations.

  ## Examples

      iex> FerricStore.tdigest_reset("api:latency:ms")
      :ok

  """
  @spec tdigest_reset(key()) :: :ok | {:error, binary()}
  def tdigest_reset(key) do
    Router.tdigest_op(default_ctx(), "TDIGEST.RESET", [key])
  end

  @doc """
  Computes the trimmed mean of values between quantile bounds `lo` and `hi`.

  A trimmed mean excludes outliers by only averaging values within the specified
  quantile range. For example, `tdigest_trimmed_mean(key, 0.1, 0.9)` averages
  the middle 80% of the distribution.

  ## Parameters

    * `key` - the T-Digest key
    * `lo` - lower quantile bound (0.0 to 1.0)
    * `hi` - upper quantile bound (0.0 to 1.0)

  ## Examples

      iex> FerricStore.tdigest_trimmed_mean("api:latency:ms", 0.1, 0.9)
      {:ok, "45.5"}

  """
  @spec tdigest_trimmed_mean(key(), float(), float()) :: {:ok, binary()} | {:error, binary()}
  def tdigest_trimmed_mean(key, lo, hi) do
    store = build_tdigest_store(key)

    result =
      Ferricstore.Commands.TDigest.handle(
        "TDIGEST.TRIMMED_MEAN",
        [key, to_string(lo * 1.0), to_string(hi * 1.0)],
        store
      )

    wrap_result(result)
  end

  @doc """
  Estimates the rank (number of observations less than or equal to) for each value.

  ## Returns

    * `{:ok, [rank, ...]}` - estimated rank per value.

  ## Examples

      iex> FerricStore.tdigest_rank("api:latency:ms", [50.0, 100.0])
      {:ok, [3, 4]}

  """
  @spec tdigest_rank(key(), [number()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_rank(key, values) when is_list(values) do
    store = build_tdigest_store(key)
    vs = Enum.map(values, &to_string(&1 * 1.0))
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.RANK", [key | vs], store)
    wrap_result(result)
  end

  @doc """
  Estimates the reverse rank (number of observations greater than) for each value.

  ## Returns

    * `{:ok, [reverse_rank, ...]}` - estimated reverse rank per value.

  ## Examples

      iex> FerricStore.tdigest_revrank("api:latency:ms", [50.0, 100.0])
      {:ok, [2, 1]}

  """
  @spec tdigest_revrank(key(), [number()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_revrank(key, values) when is_list(values) do
    store = build_tdigest_store(key)
    vs = Enum.map(values, &to_string(&1 * 1.0))
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.REVRANK", [key | vs], store)
    wrap_result(result)
  end

  @doc """
  Estimates the value at each given rank (0-based position in sorted order).

  ## Returns

    * `{:ok, [value, ...]}` - estimated value at each rank.

  ## Examples

      iex> FerricStore.tdigest_byrank("api:latency:ms", [0, 2, 4])
      {:ok, ["3.2", "45.0", "150.0"]}

  """
  @spec tdigest_byrank(key(), [integer()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_byrank(key, ranks) when is_list(ranks) do
    store = build_tdigest_store(key)
    rs = Enum.map(ranks, &to_string/1)
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.BYRANK", [key | rs], store)
    wrap_result(result)
  end

  @doc """
  Estimates the value at each given reverse rank (0 = largest, 1 = second largest, etc.).

  ## Returns

    * `{:ok, [value, ...]}` - estimated value at each reverse rank.

  ## Examples

      iex> FerricStore.tdigest_byrevrank("api:latency:ms", [0, 1])
      {:ok, ["150.0", "89.1"]}

  """
  @spec tdigest_byrevrank(key(), [integer()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_byrevrank(key, ranks) when is_list(ranks) do
    store = build_tdigest_store(key)
    rs = Enum.map(ranks, &to_string/1)
    result = Ferricstore.Commands.TDigest.handle("TDIGEST.BYREVRANK", [key | rs], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Geo operations
  # ---------------------------------------------------------------------------

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
    args =
      Enum.flat_map(members, fn {lng, lat, member} ->
        [to_string(lng * 1.0), to_string(lat * 1.0), member]
      end)

    wrap_result(Router.geo_op(default_ctx(), "GEOADD", [key | args]))
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
    result = Ferricstore.Commands.Geo.handle("GEODIST", [key, member1, member2, unit], store)
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
    result = Ferricstore.Commands.Geo.handle("GEOHASH", [key | members], store)
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
    result = Ferricstore.Commands.Geo.handle("GEOPOS", [key | members], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # JSON operations
  # ---------------------------------------------------------------------------

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
    case Router.json_op(default_ctx(), "JSON.SET", [key, path, value]) do
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
    result = Ferricstore.Commands.Json.handle("JSON.GET", [key, path], store)
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
    wrap_result(Router.json_op(default_ctx(), "JSON.DEL", [key, path]))
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
    result = Ferricstore.Commands.Json.handle("JSON.TYPE", [key, path], store)
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
    wrap_result(Router.json_op(default_ctx(), "JSON.NUMINCRBY", [key, path, increment]))
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
    wrap_result(Router.json_op(default_ctx(), "JSON.ARRAPPEND", [key, path | values]))
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
    result = Ferricstore.Commands.Json.handle("JSON.ARRLEN", [key, path], store)
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
    result = Ferricstore.Commands.Json.handle("JSON.STRLEN", [key, path], store)
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
    result = Ferricstore.Commands.Json.handle("JSON.OBJKEYS", [key, path], store)
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
    result = Ferricstore.Commands.Json.handle("JSON.OBJLEN", [key, path], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Native: lock, unlock, extend, ratelimit_add
  # ---------------------------------------------------------------------------

  @doc """
  Acquires a distributed mutex lock on `key` with the given `owner` identity and TTL.

  Only one owner can hold a lock at a time. If the lock is already held by a
  different owner, returns an error. Use `unlock/2` to release and `extend/3`
  to renew the TTL before expiry.

  ## Parameters

    * `key` - the lock key (e.g. `"lock:order:123"`)
    * `owner` - unique owner identifier (e.g. a UUID or node name)
    * `ttl_ms` - lock duration in milliseconds (auto-expires as a safety net)

  ## Returns

    * `:ok` if the lock was acquired.
    * `{:error, reason}` if the lock is held by another owner.

  ## Examples

      iex> FerricStore.lock("lock:order:123", "worker_abc", 30_000)
      :ok

      iex> FerricStore.lock("lock:order:123", "worker_xyz", 30_000)
      {:error, "ERR lock is held by another owner"}

  """
  @spec lock(key(), binary(), pos_integer()) :: :ok | {:error, binary()}
  def lock(key, owner, ttl_ms) do
    ctx = default_ctx()

    case Router.lock(ctx, key, owner, ttl_ms) do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  @doc """
  Releases the lock on `key`, but only if it is currently held by `owner`.

  This ensures that a lock holder cannot accidentally release someone else's lock
  (e.g. after a timeout and re-acquisition by another process).

  ## Returns

    * `{:ok, 1}` if the lock was released.
    * `{:error, reason}` if the lock is not held by `owner`.

  ## Examples

      iex> FerricStore.unlock("lock:order:123", "worker_abc")
      {:ok, 1}

  """
  @spec unlock(key(), binary()) :: {:ok, 1} | {:error, binary()}
  def unlock(key, owner) do
    ctx = default_ctx()

    case Router.unlock(ctx, key, owner) do
      1 -> {:ok, 1}
      {:error, _} = err -> err
    end
  end

  @doc """
  Extends the TTL of a lock on `key`, but only if it is currently held by `owner`.

  Call this periodically to prevent lock expiry while a long-running operation
  is still in progress.

  ## Returns

    * `{:ok, 1}` if the TTL was extended.
    * `{:error, reason}` if the lock is not held by `owner`.

  ## Examples

      iex> FerricStore.extend("lock:order:123", "worker_abc", 30_000)
      {:ok, 1}

  """
  @spec extend(key(), binary(), pos_integer()) :: {:ok, 1} | {:error, binary()}
  def extend(key, owner, ttl_ms) do
    ctx = default_ctx()

    case Router.extend(ctx, key, owner, ttl_ms) do
      1 -> {:ok, 1}
      {:error, _} = err -> err
    end
  end

  @doc """
  Records `count` events against the sliding-window rate limiter at `key`.

  Uses a sliding window algorithm to track request counts within a time window.
  Returns the current count and whether the limit has been exceeded. Ideal for
  API rate limiting, abuse prevention, and throttling.

  ## Parameters

    * `key` - the rate limit key (e.g. `"ratelimit:api:user:42"`)
    * `window_ms` - sliding window duration in milliseconds
    * `max` - maximum allowed events within the window
    * `count` - number of events to record (default: 1)

  ## Returns

    * `{:ok, [allowed, current_count]}` where `allowed` is `1` (allowed) or `0`
      (rate limit exceeded), and `current_count` is the total events in the window.

  ## Examples

      iex> FerricStore.ratelimit_add("ratelimit:api:user:42", 60_000, 100)
      {:ok, [1, 1]}

      iex> FerricStore.ratelimit_add("ratelimit:api:user:42", 60_000, 100, 5)
      {:ok, [1, 6]}

  """
  @spec ratelimit_add(key(), pos_integer(), pos_integer(), pos_integer()) :: {:ok, list()}
  def ratelimit_add(key, window_ms, max, count \\ 1) do
    ctx = default_ctx()
    result = Router.ratelimit_add(ctx, key, window_ms, max, count)
    {:ok, result}
  end

  # ---------------------------------------------------------------------------
  # HyperLogLog operations
  # ---------------------------------------------------------------------------

  @doc """
  Adds elements to the HyperLogLog at `key` for approximate cardinality counting.

  A HyperLogLog uses ~12KB of memory to estimate the number of unique elements
  in a set with a standard error of 0.81%. Ideal for counting unique visitors,
  distinct IPs, or unique events without storing every value.

  ## Returns

    * `{:ok, true}` if the internal registers were modified (new unique element likely).
    * `{:ok, false}` if the registers were not modified.
    * `{:error, reason}` on failure.

  ## Examples

      iex> FerricStore.pfadd("visitors:2024-03-28", ["user_1", "user_2", "user_3"])
      {:ok, true}

  """
  @spec pfadd(key(), [binary()]) :: {:ok, boolean()} | {:error, binary()}
  def pfadd(key, elements) when is_list(elements) do
    case Router.hll_op(default_ctx(), "PFADD", [key | elements]) do
      1 -> {:ok, true}
      0 -> {:ok, false}
      {:error, _} = err -> err
    end
  end

  @doc """
  Returns the approximate number of unique elements across one or more HyperLogLogs.

  When given multiple keys, computes the cardinality of their union without
  modifying the underlying structures.

  ## Returns

    * `{:ok, count}` - estimated unique element count.
    * `{:error, reason}` on failure.

  ## Examples

      iex> FerricStore.pfcount(["visitors:2024-03-28"])
      {:ok, 3}

      iex> FerricStore.pfcount(["visitors:2024-03-27", "visitors:2024-03-28"])
      {:ok, 5}

  """
  @spec pfcount([key()]) :: {:ok, non_neg_integer()} | {:error, binary()}
  def pfcount(keys) when is_list(keys) do
    store = build_string_store(hd(keys))
    result = Ferricstore.Commands.HyperLogLog.handle("PFCOUNT", keys, store)
    wrap_result(result)
  end

  @doc """
  Merges multiple HyperLogLog keys into `dest_key`.

  The resulting HyperLogLog approximates the cardinality of the union of all
  source sets. Useful for computing weekly/monthly unique counts from daily ones.

  ## Returns

    * `:ok` on success.
    * `{:error, reason}` on failure.

  ## Examples

      iex> FerricStore.pfmerge("visitors:2024-w13", ["visitors:2024-03-25", "visitors:2024-03-26", "visitors:2024-03-27"])
      :ok

  """
  @spec pfmerge(key(), [key()]) :: :ok | {:error, binary()}
  def pfmerge(dest_key, source_keys) when is_list(source_keys) do
    case Router.hll_op(default_ctx(), "PFMERGE", [dest_key | source_keys]) do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  # ---------------------------------------------------------------------------
  # Multi/Tx
  # ---------------------------------------------------------------------------

  @doc """
  Executes a sequence of commands atomically as a transaction.

  The provided function receives a `FerricStore.Tx` accumulator and should
  pipe commands into it. All commands execute in order and results are returned.

  ## Examples

      {:ok, [:ok, {:ok, "v1"}]} = FerricStore.multi(fn tx ->
        tx
        |> FerricStore.Tx.set("k1", "v1")
        |> FerricStore.Tx.get("k1")
      end)

  """
  @spec multi((FerricStore.Tx.t() -> FerricStore.Tx.t())) :: {:ok, [term()]} | {:error, binary()}
  def multi(fun) when is_function(fun, 1) do
    tx = fun.(FerricStore.Tx.new())

    case FerricStore.Tx.execute(tx) do
      {:error, _} = err -> err
      results when is_list(results) -> {:ok, results}
    end
  end

  # ---------------------------------------------------------------------------
  # Server: ping, echo, flushall
  # ---------------------------------------------------------------------------

  @doc """
  Health check that returns `{:ok, "PONG"}`.

  ## Examples

      iex> FerricStore.ping()
      {:ok, "PONG"}

  """
  @spec ping() :: {:ok, binary()}
  def ping, do: {:ok, "PONG"}

  @doc """
  Echoes back the given message, useful for connection testing.

  ## Examples

      iex> FerricStore.echo("hello")
      {:ok, "hello"}

  """
  @spec echo(binary()) :: {:ok, binary()}
  def echo(message) when is_binary(message), do: {:ok, message}

  @doc """
  Deletes all keys from the store.

  Alias for `flushdb/0`.

  ## Examples

      iex> FerricStore.flushall()
      :ok

  """
  @spec flushall() :: :ok
  def flushall, do: flushdb()

  # ---------------------------------------------------------------------------
  # Pipeline
  # ---------------------------------------------------------------------------

  @doc """
  Batches multiple commands into a single group-commit entry.

  The provided function receives a `FerricStore.Pipe` accumulator and should
  pipe commands into it. All commands are executed atomically on completion.

  ## Examples

      results = FerricStore.pipeline(fn pipe ->
        pipe
        |> FerricStore.Pipe.set("key1", "val1")
        |> FerricStore.Pipe.set("key2", "val2")
        |> FerricStore.Pipe.incr("counter")
      end)

  ## Returns

    * `{:ok, results}` - a list of results for each piped command, in order.

  """
  @spec pipeline((FerricStore.Pipe.t() -> FerricStore.Pipe.t())) :: {:ok, [term()]}
  def pipeline(fun) when is_function(fun, 1) do
    pipe = fun.(FerricStore.Pipe.new())
    results = FerricStore.Pipe.execute(pipe)
    {:ok, results}
  end

  @doc """
  Batch GET: takes a list of keys, returns a list of values (nil for missing).

  Goes directly to `Router.batch_get` — single HLC timestamp, zero GenServer,
  zero Pipe struct overhead. Designed for erpc callers.
  """
  @spec batch_get([binary()]) :: [binary() | nil]
  def batch_get(keys) when is_list(keys) do
    ctx = FerricStore.Instance.get(:default)
    Ferricstore.Store.Router.batch_get(ctx, keys)
  end

  @doc """
  Packed binary batch GET — minimal distribution overhead.

  Input: single binary with packed keys: `<<count::32, key_len::16, key::binary, ...>>`
  Output: single binary with packed values: `<<val_len::32, val::binary, ...>>`
  where val_len=0xFFFFFFFF means nil.

  One flat binary over distribution instead of a list of N binaries —
  eliminates per-element external term format encoding.
  """
  @spec packed_batch_get(binary()) :: binary()
  def packed_batch_get(packed_keys) when is_binary(packed_keys) do
    ctx = FerricStore.Instance.get(:default)
    <<count::32, rest::binary>> = packed_keys
    keys = unpack_keys(rest, count, [])
    values = Ferricstore.Store.Router.batch_get(ctx, keys)
    pack_values(values, [])
  end

  defp unpack_keys(_rest, 0, acc), do: Enum.reverse(acc)

  defp unpack_keys(<<len::16, key::binary-size(len), rest::binary>>, n, acc) do
    unpack_keys(rest, n - 1, [key | acc])
  end

  defp pack_values([], acc), do: IO.iodata_to_binary(Enum.reverse(acc))

  defp pack_values([nil | rest], acc) do
    pack_values(rest, [<<0xFFFFFFFF::32>> | acc])
  end

  defp pack_values([value | rest], acc) when is_binary(value) do
    pack_values(rest, [<<byte_size(value)::32, value::binary>> | acc])
  end

  @doc """
  Batch SET: takes a list of `{key, value}` pairs, returns a list of results.

  Routes through `Router.batch_quorum_put` or `batch_async_put` based on
  namespace durability. Designed for erpc callers.
  """
  @spec batch_set([{binary(), binary()}]) :: [:ok | write_error()]
  def batch_set(kv_pairs) when is_list(kv_pairs) do
    ctx = FerricStore.Instance.get(:default)

    case ctx.durability_mode do
      :all_async ->
        batch_async_put_result_list(ctx, kv_pairs)

      :all_quorum ->
        Ferricstore.Store.Router.batch_quorum_put(ctx, kv_pairs)

      :mixed ->
        indexed = Enum.with_index(kv_pairs)

        {async_kvs, quorum_kvs} =
          Enum.split_with(indexed, fn {{k, _v}, _i} ->
            Ferricstore.Store.Router.durability_for_key_public(ctx, k) == :async
          end)

        async_results =
          if async_kvs != [] do
            async_values =
              batch_async_put_result_list(ctx, Enum.map(async_kvs, fn {{k, v}, _} -> {k, v} end))

            Enum.zip(Enum.map(async_kvs, fn {_, i} -> i end), async_values)
          else
            []
          end

        quorum_results =
          if quorum_kvs != [] do
            results =
              Ferricstore.Store.Router.batch_quorum_put(
                ctx,
                Enum.map(quorum_kvs, fn {{k, v}, _} -> {k, v} end)
              )

            Enum.zip(Enum.map(quorum_kvs, fn {_, i} -> i end), results)
          else
            []
          end

        (async_results ++ quorum_results)
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.map(&elem(&1, 1))
    end
  end

  # ---------------------------------------------------------------------------
  # Private — result wrapping helper
  # ---------------------------------------------------------------------------

  defp wrap_result({:error, _} = err), do: err
  defp wrap_result(result), do: {:ok, result}

  defp batch_async_put_result_list(ctx, kv_pairs) do
    case Ferricstore.Store.Router.batch_async_put(ctx, kv_pairs) do
      :ok -> List.duplicate(:ok, length(kv_pairs))
      {:error, _reason} = error -> List.duplicate(error, length(kv_pairs))
    end
  end

  # ---------------------------------------------------------------------------
  # Private — string store builder for bitmap/json/hyperloglog operations
  # ---------------------------------------------------------------------------

  defp build_string_store(_key) do
    ctx = default_ctx()

    %{
      get: fn k -> Router.get(ctx, k) end,
      get_meta: fn k -> Router.get_meta(ctx, k) end,
      put: fn k, v, exp -> Router.put(ctx, k, v, exp) end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn k -> Router.exists?(ctx, k) end,
      keys: fn -> Router.keys(ctx) end,
      incr: fn k, d -> Router.incr(ctx, k, d) end,
      incr_float: fn k, d -> Router.incr_float(ctx, k, d) end,
      append: fn k, s -> Router.append(ctx, k, s) end,
      getset: fn k, v -> Router.getset(ctx, k, v) end,
      getdel: fn k -> Router.getdel(ctx, k) end,
      getex: fn k, e -> Router.getex(ctx, k, e) end,
      setrange: fn k, o, v -> Router.setrange(ctx, k, o, v) end,
      compound_get: fn redis_key, compound_key ->
        Router.compound_get(ctx, redis_key, compound_key)
      end,
      compound_get_meta: fn redis_key, compound_key ->
        Router.compound_get_meta(ctx, redis_key, compound_key)
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        Router.compound_put(ctx, redis_key, compound_key, value, expire_at_ms)
      end,
      compound_delete: fn redis_key, compound_key ->
        Router.compound_delete(ctx, redis_key, compound_key)
      end,
      compound_scan: fn redis_key, prefix -> Router.compound_scan(ctx, redis_key, prefix) end,
      compound_count: fn redis_key, prefix -> Router.compound_count(ctx, redis_key, prefix) end,
      compound_delete_prefix: fn redis_key, prefix ->
        Router.compound_delete_prefix(ctx, redis_key, prefix)
      end
    }
  end

  # ---------------------------------------------------------------------------
  # Private — stream store builder
  # ---------------------------------------------------------------------------

  defp build_stream_store(key) do
    build_string_store(key)
  end

  # ---------------------------------------------------------------------------
  # Private — probabilistic structure store builder
  # ---------------------------------------------------------------------------

  defp build_prob_store(key) do
    ctx = default_ctx()
    # Probabilistic structures route writes through Raft and reads via
    # stateless pread NIFs. The store needs prob_dir and prob_write.
    index = Router.shard_for(ctx, key)
    ensure_prob_registry_tables(index)

    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, index)

    %{
      get: fn k -> Router.get(ctx, k) end,
      get_meta: fn k -> Router.get_meta(ctx, k) end,
      put: fn k, v, exp -> Router.put(ctx, k, v, exp) end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn k -> Router.exists?(ctx, k) end,
      keys: fn -> Router.keys(ctx) end,
      prob_dir: fn -> Path.join(shard_data_path, "prob") end,
      prob_dir_for_key: fn key ->
        idx = Router.shard_for(ctx, key)
        sp = Ferricstore.DataDir.shard_data_path(data_dir, idx)
        Path.join(sp, "prob")
      end,
      prob_write: fn cmd -> Router.prob_write(ctx, cmd) end
    }
  end

  defp ensure_prob_registry_tables(_index), do: :ok

  # ---------------------------------------------------------------------------
  # Private — TopK store builder
  # ---------------------------------------------------------------------------

  defp build_topk_store(key) do
    ctx = default_ctx()
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    index = Router.shard_for(ctx, key)
    shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, index)

    %{
      get: fn key ->
        case Router.get(ctx, key) do
          nil ->
            nil

          bin when is_binary(bin) ->
            try do
              :erlang.binary_to_term(bin)
            rescue
              ArgumentError -> bin
            end
        end
      end,
      put: fn key, val, exp ->
        encoded = if is_tuple(val), do: :erlang.term_to_binary(val), else: val
        Router.put(ctx, key, encoded, exp)
      end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn k -> Router.exists?(ctx, k) end,
      keys: fn -> Router.keys(ctx) end,
      # Route topk writes through Raft so all replicas materialize the same
      # mmap file and follower TOPK.LIST/QUERY work. Without prob_write the
      # command falls back to applying locally on the originating node only.
      prob_write: fn cmd -> Router.prob_write(ctx, cmd) end,
      prob_dir: fn ->
        prob_dir = Path.join(shard_data_path, "prob")
        Ferricstore.FS.mkdir_p!(prob_dir)
        prob_dir
      end,
      prob_dir_for_key: fn key ->
        idx = Router.shard_for(ctx, key)
        sp = Ferricstore.DataDir.shard_data_path(data_dir, idx)
        prob_dir = Path.join(sp, "prob")
        Ferricstore.FS.mkdir_p!(prob_dir)
        prob_dir
      end
    }
  end

  # ---------------------------------------------------------------------------
  # Private — TDigest store builder
  # ---------------------------------------------------------------------------
  # TDigest RMW commands (TDIGEST.ADD/RESET/MERGE/CREATE) are now serialized
  # by Raft — Router.tdigest_op submits {:tdigest_op, cmd, args} to the
  # state machine, which calls TDigest.handle/3 with a state-machine-scoped
  # store. Concurrent callers are serialized by Raft log order, so no latch
  # is needed. Read-only tdigest commands (quantile/cdf/info/etc.) still
  # run in the caller process via build_tdigest_store below.

  defp build_tdigest_store(_resolved_key) do
    ctx = default_ctx()

    %{
      get: fn key ->
        case Router.get(ctx, key) do
          nil ->
            nil

          bin when is_binary(bin) ->
            try do
              case :erlang.binary_to_term(bin) do
                {:tdigest, _, _} = tuple -> tuple
                _ -> bin
              end
            rescue
              ArgumentError -> bin
            end
        end
      end,
      put: fn key, val, exp ->
        encoded =
          if is_tuple(val) and tuple_size(val) >= 1 and elem(val, 0) == :tdigest do
            :erlang.term_to_binary(val)
          else
            val
          end

        Router.put(ctx, key, encoded, exp)
      end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn key ->
        Router.get(ctx, key) != nil
      end,
      keys: fn -> Router.keys(ctx) end
    }
  end

  # ---------------------------------------------------------------------------
  # Private — compound key store builder for set/sorted-set operations
  # ---------------------------------------------------------------------------

  # Builds the store map expected by Commands.Set and Commands.SortedSet.
  # The store maps compound key operations to the correct shard GenServer
  # using the Redis key for routing (all sub-keys for one Redis key live
  # on the same shard).
  defp build_compound_store(_key) do
    ctx = default_ctx()

    %{
      get: fn k -> Router.get(ctx, k) end,
      get_meta: fn k -> Router.get_meta(ctx, k) end,
      put: fn k, v, exp -> Router.put(ctx, k, v, exp) end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn k -> Router.exists?(ctx, k) end,
      keys: fn -> Router.keys(ctx) end,
      # Compound ops route through Router so they get the same not_leader →
      # forward + read-your-write barrier as plain Router.put. Going direct
      # to the local shard skips that and silently loses writes when the
      # local node isn't the leader for this key's shard.
      compound_get: fn redis_key, compound_key ->
        Router.compound_get(ctx, redis_key, compound_key)
      end,
      compound_get_meta: fn redis_key, compound_key ->
        Router.compound_get_meta(ctx, redis_key, compound_key)
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        Router.compound_put(ctx, redis_key, compound_key, value, expire_at_ms)
      end,
      compound_delete: fn redis_key, compound_key ->
        Router.compound_delete(ctx, redis_key, compound_key)
      end,
      compound_scan: fn redis_key, prefix -> Router.compound_scan(ctx, redis_key, prefix) end,
      compound_count: fn redis_key, prefix -> Router.compound_count(ctx, redis_key, prefix) end,
      compound_delete_prefix: fn redis_key, prefix ->
        Router.compound_delete_prefix(ctx, redis_key, prefix)
      end
    }
  end
end

defmodule FerricStore.Pipe do
  @moduledoc """
  Pipeline accumulator for batching multiple FerricStore commands.

  Used with `FerricStore.pipeline/1` to batch multiple operations into a single
  Raft entry per shard. Commands are accumulated in reverse order and on execute,
  converted to RESP tuples and dispatched through the Coordinator. Single-shard
  pipelines commit in one Raft round-trip; cross-shard pipelines use the
  anchor-shard mechanism.

  Results are normalized to match the FerricStore public API format (e.g.
  `{:ok, value}` for GET, `:ok` for DEL) rather than raw Dispatcher values.

  ## Usage

      FerricStore.pipeline(fn pipe ->
        pipe
        |> FerricStore.Pipe.set("key1", "val1")
        |> FerricStore.Pipe.set("key2", "val2")
        |> FerricStore.Pipe.incr("counter")
      end)

  """

  @type command ::
          {:set, binary(), binary(), keyword()}
          | {:get, binary()}
          | {:del, binary()}
          | {:incr, binary()}
          | {:incr_by, binary(), integer()}
          | {:hset, binary(), map()}
          | {:hget, binary(), binary()}
          | {:lpush, binary(), [binary()]}
          | {:rpush, binary(), [binary()]}
          | {:sadd, binary(), [binary()]}
          | {:zadd, binary(), [{number(), binary()}]}
          | {:expire, binary(), non_neg_integer()}

  @type t :: %__MODULE__{commands: [command()]}

  defstruct commands: []

  @doc "Creates a new empty pipeline."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Adds a SET command to the pipeline."
  @spec set(t(), binary(), binary(), keyword()) :: t()
  def set(%__MODULE__{} = pipe, key, value, opts \\ []) do
    %{pipe | commands: [{:set, key, value, opts} | pipe.commands]}
  end

  @doc "Adds a GET command to the pipeline."
  @spec get(t(), binary()) :: t()
  def get(%__MODULE__{} = pipe, key) do
    %{pipe | commands: [{:get, key} | pipe.commands]}
  end

  @doc "Adds a DEL command to the pipeline."
  @spec del(t(), binary()) :: t()
  def del(%__MODULE__{} = pipe, key) do
    %{pipe | commands: [{:del, key} | pipe.commands]}
  end

  @doc "Adds an INCR command to the pipeline."
  @spec incr(t(), binary()) :: t()
  def incr(%__MODULE__{} = pipe, key) do
    %{pipe | commands: [{:incr, key} | pipe.commands]}
  end

  @doc "Adds an INCRBY command to the pipeline."
  @spec incr_by(t(), binary(), integer()) :: t()
  def incr_by(%__MODULE__{} = pipe, key, amount) do
    %{pipe | commands: [{:incr_by, key, amount} | pipe.commands]}
  end

  @doc "Adds an HSET command to the pipeline."
  @spec hset(t(), binary(), map()) :: t()
  def hset(%__MODULE__{} = pipe, key, fields) do
    %{pipe | commands: [{:hset, key, fields} | pipe.commands]}
  end

  @doc "Adds an HGET command to the pipeline."
  @spec hget(t(), binary(), binary()) :: t()
  def hget(%__MODULE__{} = pipe, key, field) do
    %{pipe | commands: [{:hget, key, field} | pipe.commands]}
  end

  @doc "Adds an LPUSH command to the pipeline."
  @spec lpush(t(), binary(), [binary()]) :: t()
  def lpush(%__MODULE__{} = pipe, key, elements) do
    %{pipe | commands: [{:lpush, key, elements} | pipe.commands]}
  end

  @doc "Adds an RPUSH command to the pipeline."
  @spec rpush(t(), binary(), [binary()]) :: t()
  def rpush(%__MODULE__{} = pipe, key, elements) do
    %{pipe | commands: [{:rpush, key, elements} | pipe.commands]}
  end

  @doc "Adds a SADD command to the pipeline."
  @spec sadd(t(), binary(), [binary()]) :: t()
  def sadd(%__MODULE__{} = pipe, key, members) do
    %{pipe | commands: [{:sadd, key, members} | pipe.commands]}
  end

  @doc "Adds a ZADD command to the pipeline."
  @spec zadd(t(), binary(), [{number(), binary()}]) :: t()
  def zadd(%__MODULE__{} = pipe, key, score_member_pairs) do
    %{pipe | commands: [{:zadd, key, score_member_pairs} | pipe.commands]}
  end

  @doc "Adds an EXPIRE command to the pipeline."
  @spec expire(t(), binary(), non_neg_integer()) :: t()
  def expire(%__MODULE__{} = pipe, key, ttl_ms) do
    %{pipe | commands: [{:expire, key, ttl_ms} | pipe.commands]}
  end

  @doc """
  Executes all accumulated pipeline commands as a single batch Raft entry
  per shard via the Coordinator.

  This is called internally by `FerricStore.pipeline/1`. Commands are converted
  to RESP-style tuples and dispatched through `Ferricstore.Transaction.Coordinator`,
  which groups them by shard and submits each group as a single `{:batch}` or
  `{:tx_execute}` Raft entry. Single-shard pipelines commit in one Raft round-trip;
  cross-shard pipelines use the anchor-shard mechanism.

  Results are returned in the original command order.
  """
  @spec execute(t()) :: [term()] | FerricStore.write_error()
  def execute(%__MODULE__{commands: []}), do: []

  def execute(%__MODULE__{commands: commands}) do
    ordered = Enum.reverse(commands)
    ctx = FerricStore.Instance.get(:default)

    case classify_batch(ordered) do
      :all_gets ->
        keys = Enum.map(ordered, fn {:get, k} -> k end)
        values = Ferricstore.Store.Router.batch_get(ctx, keys)
        pipeline_get_results(ctx, keys, values)

      :all_sets ->
        kv_pairs = Enum.map(ordered, fn {:set, k, v, _opts} -> {k, v} end)
        execute_batch_sets(ctx, ordered, kv_pairs)

      {:mixed_get_set, _} ->
        execute_mixed_get_set(ctx, ordered)

      :complex ->
        queue = Enum.map(ordered, &to_resp_command/1)
        raw_results = Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)

        ordered
        |> Enum.zip(raw_results)
        |> Enum.map(fn {cmd, raw} -> normalize_result(cmd, raw) end)
    end
  end

  defp classify_batch(commands) do
    classify_batch(commands, nil, MapSet.new(), MapSet.new())
  end

  defp classify_batch([], kind, _written, _read), do: kind || :complex

  defp classify_batch([{:get, key} | rest], kind, written, read) do
    if MapSet.member?(written, key) do
      :complex
    else
      new_kind =
        case kind do
          nil -> :all_gets
          :all_gets -> :all_gets
          :all_sets -> {:mixed_get_set, true}
          {:mixed_get_set, _} -> {:mixed_get_set, true}
          _ -> :complex
        end

      if new_kind == :complex,
        do: :complex,
        else: classify_batch(rest, new_kind, written, MapSet.put(read, key))
    end
  end

  defp classify_batch([{:set, key, _v, opts} | rest], kind, written, read) do
    if opts != [] or MapSet.member?(read, key) do
      :complex
    else
      new_kind =
        case kind do
          nil -> :all_sets
          :all_sets -> :all_sets
          :all_gets -> {:mixed_get_set, true}
          {:mixed_get_set, _} -> {:mixed_get_set, true}
          _ -> :complex
        end

      if new_kind == :complex,
        do: :complex,
        else: classify_batch(rest, new_kind, MapSet.put(written, key), read)
    end
  end

  defp classify_batch(_, _, _, _), do: :complex

  defp batch_async_put_result_list(ctx, kv_pairs) do
    case Ferricstore.Store.Router.batch_async_put(ctx, kv_pairs) do
      :ok -> List.duplicate(:ok, length(kv_pairs))
      {:error, _reason} = error -> List.duplicate(error, length(kv_pairs))
    end
  end

  defp execute_batch_sets(ctx, _ordered, kv_pairs) do
    case ctx.durability_mode do
      :all_async ->
        batch_async_put_result_list(ctx, kv_pairs)

      :all_quorum ->
        Ferricstore.Store.Router.batch_quorum_put(ctx, kv_pairs)

      :mixed ->
        indexed = Enum.with_index(kv_pairs)

        {async_kvs, quorum_kvs} =
          Enum.split_with(indexed, fn {{k, _v}, _i} ->
            Ferricstore.Store.Router.durability_for_key_public(ctx, k) == :async
          end)

        async_results =
          if async_kvs != [] do
            async_values =
              batch_async_put_result_list(ctx, Enum.map(async_kvs, fn {{k, v}, _} -> {k, v} end))

            Enum.zip(Enum.map(async_kvs, fn {_, i} -> i end), async_values)
          else
            []
          end

        quorum_results =
          if quorum_kvs != [] do
            results =
              Ferricstore.Store.Router.batch_quorum_put(
                ctx,
                Enum.map(quorum_kvs, fn {{k, v}, _} -> {k, v} end)
              )

            Enum.zip(Enum.map(quorum_kvs, fn {_, i} -> i end), results)
          else
            []
          end

        (async_results ++ quorum_results)
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.map(&elem(&1, 1))
    end
  end

  defp execute_mixed_get_set(ctx, ordered) do
    indexed = Enum.with_index(ordered)
    get_ops = for {{:get, key}, i} <- indexed, do: {i, key}
    set_ops = for {{:set, key, value, _}, i} <- indexed, do: {i, key, value}

    set_results =
      if set_ops != [] do
        kv_pairs = Enum.map(set_ops, fn {_i, k, v} -> {k, v} end)

        results =
          case ctx.durability_mode do
            :all_async ->
              batch_async_put_result_list(ctx, kv_pairs)

            :all_quorum ->
              Ferricstore.Store.Router.batch_quorum_put(ctx, kv_pairs)

            :mixed ->
              {async_ops, quorum_ops} =
                Enum.split_with(set_ops, fn {_i, k, _v} ->
                  Ferricstore.Store.Router.durability_for_key_public(ctx, k) == :async
                end)

              async_r =
                if async_ops != [] do
                  batch_async_put_result_list(
                    ctx,
                    Enum.map(async_ops, fn {_, k, v} -> {k, v} end)
                  )
                else
                  []
                end

              quorum_r =
                if quorum_ops != [] do
                  Ferricstore.Store.Router.batch_quorum_put(
                    ctx,
                    Enum.map(quorum_ops, fn {_, k, v} -> {k, v} end)
                  )
                else
                  []
                end

              recombine_results(async_ops, async_r, quorum_ops, quorum_r)
          end

        set_ops
        |> Enum.zip(results)
        |> Map.new(fn {{i, _, _}, r} -> {i, r} end)
      else
        %{}
      end

    get_results =
      if get_ops != [] do
        keys = Enum.map(get_ops, &elem(&1, 1))
        values = Ferricstore.Store.Router.batch_get(ctx, keys)
        results = pipeline_get_results(ctx, keys, values)

        get_ops
        |> Enum.zip(results)
        |> Map.new(fn {{i, _}, result} -> {i, result} end)
      else
        %{}
      end

    count = length(ordered)

    for i <- 0..(count - 1) do
      Map.get(get_results, i) || Map.get(set_results, i)
    end
  end

  defp recombine_results(async_ops, async_results, quorum_ops, quorum_results) do
    async_map = async_ops |> Enum.zip(async_results) |> Map.new(fn {{i, _, _}, r} -> {i, r} end)

    quorum_map =
      quorum_ops |> Enum.zip(quorum_results) |> Map.new(fn {{i, _, _}, r} -> {i, r} end)

    combined = Map.merge(async_map, quorum_map)

    (async_ops ++ quorum_ops)
    |> Enum.sort_by(fn {i, _, _} -> i end)
    |> Enum.map(fn {i, _, _} -> Map.fetch!(combined, i) end)
  end

  defp pipeline_get_results(ctx, keys, values) do
    keys
    |> Enum.zip(values)
    |> Enum.map(fn {key, value} -> pipeline_get_result(ctx, key, value) end)
  end

  defp pipeline_get_result(_ctx, _key, value) when value != nil, do: {:ok, value}

  defp pipeline_get_result(ctx, key, nil) do
    if pipeline_compound_data_structure_key?(ctx, key) do
      {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
    else
      {:ok, nil}
    end
  end

  defp pipeline_compound_data_structure_key?(ctx, key) do
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    list_meta_key = Ferricstore.Store.CompoundKey.list_meta_key(key)

    Ferricstore.Store.Router.compound_get(ctx, key, type_key) != nil or
      Ferricstore.Store.Router.compound_get(ctx, key, list_meta_key) != nil
  end

  # The Coordinator returns raw Dispatcher results (RESP-level values).
  # Pipeline callers expect the same format as FerricStore public API calls.
  # This maps Dispatcher results back to the public API format.
  defp normalize_result({:get, _}, {:error, _} = err), do: err
  defp normalize_result({:get, _}, value), do: {:ok, value}

  defp normalize_result({:hget, _, _}, {:error, _} = err), do: err
  defp normalize_result({:hget, _, _}, value), do: {:ok, value}

  defp normalize_result({:del, _}, {:error, _} = err), do: err
  defp normalize_result({:del, _}, _count), do: :ok

  defp normalize_result({:hset, _, _}, {:error, _} = err), do: err
  defp normalize_result({:hset, _, _}, _count), do: :ok

  defp normalize_result({:lpush, _, _}, {:error, _} = err), do: err
  defp normalize_result({:lpush, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:rpush, _, _}, {:error, _} = err), do: err
  defp normalize_result({:rpush, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:sadd, _, _}, {:error, _} = err), do: err
  defp normalize_result({:sadd, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:zadd, _, _}, {:error, _} = err), do: err
  defp normalize_result({:zadd, _, _}, count) when is_integer(count), do: {:ok, count}

  defp normalize_result({:expire, _, _}, {:error, _} = err), do: err
  defp normalize_result({:expire, _, _}, 1), do: {:ok, true}
  defp normalize_result({:expire, _, _}, 0), do: {:ok, false}

  # SET, INCR, INCR_BY already return the correct format from Dispatcher
  defp normalize_result(_, result), do: result

  defp to_resp_command({:set, key, value, opts}) do
    args = [key, value]

    args =
      case Keyword.get(opts, :ttl) do
        nil -> args
        0 -> args
        ms -> args ++ ["PX", Integer.to_string(ms)]
      end

    args =
      case Keyword.get(opts, :ex) do
        nil -> args
        seconds -> args ++ ["EX", Integer.to_string(seconds)]
      end

    args =
      case Keyword.get(opts, :px) do
        nil -> args
        ms -> args ++ ["PX", Integer.to_string(ms)]
      end

    args =
      if Keyword.get(opts, :nx, false), do: args ++ ["NX"], else: args

    args =
      if Keyword.get(opts, :xx, false), do: args ++ ["XX"], else: args

    {"SET", args}
  end

  defp to_resp_command({:get, key}), do: {"GET", [key]}
  defp to_resp_command({:del, key}), do: {"DEL", [key]}
  defp to_resp_command({:incr, key}), do: {"INCR", [key]}
  defp to_resp_command({:incr_by, key, amount}), do: {"INCRBY", [key, Integer.to_string(amount)]}

  defp to_resp_command({:hset, key, fields}) do
    flat = Enum.flat_map(fields, fn {k, v} -> [to_string(k), to_string(v)] end)
    {"HSET", [key | flat]}
  end

  defp to_resp_command({:hget, key, field}), do: {"HGET", [key, field]}
  defp to_resp_command({:lpush, key, elements}), do: {"LPUSH", [key | elements]}
  defp to_resp_command({:rpush, key, elements}), do: {"RPUSH", [key | elements]}
  defp to_resp_command({:sadd, key, members}), do: {"SADD", [key | members]}

  defp to_resp_command({:zadd, key, pairs}) do
    flat =
      Enum.flat_map(pairs, fn {score, member} ->
        [to_string(score), member]
      end)

    {"ZADD", [key | flat]}
  end

  defp to_resp_command({:expire, key, ttl_ms}) do
    {"PEXPIRE", [key, Integer.to_string(ttl_ms)]}
  end
end

defmodule FerricStore.Tx do
  @moduledoc """
  Transaction accumulator for executing multiple FerricStore commands atomically.

  Used with `FerricStore.multi/1` to batch multiple operations. Commands are
  accumulated in reverse order and executed sequentially when the transaction
  completes.

  ## Usage

      FerricStore.multi(fn tx ->
        tx
        |> FerricStore.Tx.set("key1", "val1")
        |> FerricStore.Tx.get("key1")
      end)

  """

  @type command ::
          {:set, binary(), binary(), keyword()}
          | {:get, binary()}
          | {:del, binary()}
          | {:incr, binary()}
          | {:incr_by, binary(), integer()}
          | {:hset, binary(), map()}
          | {:hget, binary(), binary()}
          | {:lpush, binary(), [binary()]}
          | {:rpush, binary(), [binary()]}
          | {:sadd, binary(), [binary()]}
          | {:zadd, binary(), [{number(), binary()}]}
          | {:expire, binary(), non_neg_integer()}

  @type t :: %__MODULE__{commands: [command()]}

  defstruct commands: []

  @doc "Creates a new empty transaction."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Adds a SET command to the transaction."
  @spec set(t(), binary(), binary(), keyword()) :: t()
  def set(%__MODULE__{} = tx, key, value, opts \\ []) do
    %{tx | commands: [{:set, key, value, opts} | tx.commands]}
  end

  @doc "Adds a GET command to the transaction."
  @spec get(t(), binary()) :: t()
  def get(%__MODULE__{} = tx, key) do
    %{tx | commands: [{:get, key} | tx.commands]}
  end

  @doc "Adds a DEL command to the transaction."
  @spec del(t(), binary()) :: t()
  def del(%__MODULE__{} = tx, key) do
    %{tx | commands: [{:del, key} | tx.commands]}
  end

  @doc "Adds an INCR command to the transaction."
  @spec incr(t(), binary()) :: t()
  def incr(%__MODULE__{} = tx, key) do
    %{tx | commands: [{:incr, key} | tx.commands]}
  end

  @doc "Adds an INCRBY command to the transaction."
  @spec incr_by(t(), binary(), integer()) :: t()
  def incr_by(%__MODULE__{} = tx, key, amount) do
    %{tx | commands: [{:incr_by, key, amount} | tx.commands]}
  end

  @doc "Adds an HSET command to the transaction."
  @spec hset(t(), binary(), map()) :: t()
  def hset(%__MODULE__{} = tx, key, fields) do
    %{tx | commands: [{:hset, key, fields} | tx.commands]}
  end

  @doc "Adds an HGET command to the transaction."
  @spec hget(t(), binary(), binary()) :: t()
  def hget(%__MODULE__{} = tx, key, field) do
    %{tx | commands: [{:hget, key, field} | tx.commands]}
  end

  @doc "Adds an LPUSH command to the transaction."
  @spec lpush(t(), binary(), [binary()]) :: t()
  def lpush(%__MODULE__{} = tx, key, elements) do
    %{tx | commands: [{:lpush, key, elements} | tx.commands]}
  end

  @doc "Adds an RPUSH command to the transaction."
  @spec rpush(t(), binary(), [binary()]) :: t()
  def rpush(%__MODULE__{} = tx, key, elements) do
    %{tx | commands: [{:rpush, key, elements} | tx.commands]}
  end

  @doc "Adds a SADD command to the transaction."
  @spec sadd(t(), binary(), [binary()]) :: t()
  def sadd(%__MODULE__{} = tx, key, members) do
    %{tx | commands: [{:sadd, key, members} | tx.commands]}
  end

  @doc "Adds a ZADD command to the transaction."
  @spec zadd(t(), binary(), [{number(), binary()}]) :: t()
  def zadd(%__MODULE__{} = tx, key, score_member_pairs) do
    %{tx | commands: [{:zadd, key, score_member_pairs} | tx.commands]}
  end

  @doc "Adds an EXPIRE command to the transaction."
  @spec expire(t(), binary(), non_neg_integer()) :: t()
  def expire(%__MODULE__{} = tx, key, ttl_ms) do
    %{tx | commands: [{:expire, key, ttl_ms} | tx.commands]}
  end

  @doc """
  Executes all accumulated transaction commands atomically.

  Groups commands by shard. If all target a single shard, dispatches them
  as a batch to the shard GenServer (atomic, no interleaving). If commands
  span multiple shards, returns a CROSSSLOT error.
  """
  @spec execute(t()) :: [term()] | FerricStore.write_error()
  def execute(%__MODULE__{commands: []}), do: []

  def execute(%__MODULE__{commands: commands}) do
    queue =
      commands
      |> Enum.reverse()
      |> Enum.map(&to_resp_command/1)

    Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
  end

  defp to_resp_command({:set, key, value, opts}) do
    args = [key, value]

    args =
      case Keyword.get(opts, :ex) do
        nil -> args
        seconds -> args ++ ["EX", Integer.to_string(seconds)]
      end

    args =
      case Keyword.get(opts, :px) do
        nil -> args
        ms -> args ++ ["PX", Integer.to_string(ms)]
      end

    args =
      if Keyword.get(opts, :nx, false), do: args ++ ["NX"], else: args

    args =
      if Keyword.get(opts, :xx, false), do: args ++ ["XX"], else: args

    {"SET", args}
  end

  defp to_resp_command({:get, key}), do: {"GET", [key]}
  defp to_resp_command({:del, key}), do: {"DEL", [key]}
  defp to_resp_command({:incr, key}), do: {"INCR", [key]}
  defp to_resp_command({:incr_by, key, amount}), do: {"INCRBY", [key, Integer.to_string(amount)]}

  defp to_resp_command({:hset, key, fields}) do
    flat = Enum.flat_map(fields, fn {k, v} -> [to_string(k), to_string(v)] end)
    {"HSET", [key | flat]}
  end

  defp to_resp_command({:hget, key, field}), do: {"HGET", [key, field]}
  defp to_resp_command({:lpush, key, elements}), do: {"LPUSH", [key | elements]}
  defp to_resp_command({:rpush, key, elements}), do: {"RPUSH", [key | elements]}
  defp to_resp_command({:sadd, key, members}), do: {"SADD", [key | members]}

  defp to_resp_command({:zadd, key, pairs}) do
    flat =
      Enum.flat_map(pairs, fn {score, member} ->
        [to_string(score), member]
      end)

    {"ZADD", [key | flat]}
  end

  defp to_resp_command({:expire, key, ttl_ms}) do
    {"PEXPIRE", [key, Integer.to_string(ttl_ms)]}
  end
end
