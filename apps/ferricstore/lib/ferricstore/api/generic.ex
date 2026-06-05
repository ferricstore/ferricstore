defmodule FerricStore.API.Generic do
  @moduledoc false

  import FerricStore.API.Store
  alias Ferricstore.Commands.{Expiry, Generic, Strings}
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
    Strings.handle_ast({:exists, [key]}, build_compound_store(key)) == 1
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
  @spec flushdb() :: :ok | {:error, term()}
  def flushdb do
    FerricStore.Impl.flushdb(default_ctx())
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
    case Expiry.handle_ast({:pexpire, key, ttl_ms}, build_compound_store(key)) do
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
    case Expiry.handle_ast({:pttl, key}, build_compound_store(key)) do
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

    case Generic.handle_ast({:copy, source, destination, replace}, %{}) do
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
    Generic.handle_ast({:rename, source, destination}, %{})
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
    case Generic.handle_ast({:renamenx, source, destination}, %{}) do
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
        case Ferricstore.Stats.with_cache_tracking_disabled(fn -> Router.get(ctx, key) end) do
          nil ->
            {:ok, "none"}

          value when is_binary(value) ->
            detected =
              try do
                case :erlang.binary_to_term(value, [:safe]) do
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
    case Expiry.handle_ast({:persist, key}, build_compound_store(key)) do
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
    case Expiry.handle_ast({:expireat, key, unix_ts_seconds}, build_compound_store(key)) do
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
    case Expiry.handle_ast({:pexpireat, key, unix_ts_ms}, build_compound_store(key)) do
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
    {:ok, Generic.handle_ast({:expiretime, key}, build_compound_store(key))}
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
    {:ok, Generic.handle_ast({:pexpiretime, key}, build_compound_store(key))}
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
end
