defmodule FerricStore.API.Locks do
  @moduledoc false

  import FerricStore.API.Store
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
end
