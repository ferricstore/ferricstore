defmodule FerricStore.API.HyperLogLog do
  @moduledoc false

  import FerricStore.API.Store
  alias Ferricstore.Store.Router
  alias Ferricstore.Commands.HyperLogLog

  @type key :: FerricStore.key()
  @type value :: FerricStore.value()
  @type write_error :: FerricStore.write_error()
  @type set_opts :: FerricStore.set_opts()
  @type get_opts :: FerricStore.get_opts()
  @type cas_opts :: FerricStore.cas_opts()
  @type fetch_or_compute_opts :: FerricStore.fetch_or_compute_opts()
  @type zrange_opts :: FerricStore.zrange_opts()

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
    case Router.pfadd(default_ctx(), key, elements) do
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
    result = HyperLogLog.handle_ast({:pfcount, keys}, store)
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
    ctx = default_ctx()

    result =
      Router.with_key_latch(ctx, dest_key, fn ->
        HyperLogLog.handle_ast(
          {:pfmerge, [dest_key | source_keys]},
          build_string_store(dest_key)
        )
      end)

    case result do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  # ---------------------------------------------------------------------------
  # Multi/Tx
  # ---------------------------------------------------------------------------
end
