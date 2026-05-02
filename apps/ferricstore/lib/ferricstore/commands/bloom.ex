defmodule Ferricstore.Commands.Bloom do
  @moduledoc """
  Handles Redis-compatible Bloom filter commands.

  Write commands (BF.RESERVE, BF.ADD, BF.MADD) route through Raft via
  `store.prob_write` so that mutations are replicated to follower nodes.
  Read commands (BF.EXISTS, BF.MEXISTS, BF.CARD, BF.INFO) use stateless
  pread NIFs directly on the local file — no Raft, no mmap, no resource
  caching.

  ## Supported Commands

    * `BF.RESERVE key error_rate capacity` -- creates a new Bloom filter
    * `BF.ADD key element` -- adds an element (auto-creates with defaults if missing)
    * `BF.MADD key element [element ...]` -- adds multiple elements
    * `BF.EXISTS key element` -- checks if an element may exist
    * `BF.MEXISTS key element [element ...]` -- checks multiple elements
    * `BF.CARD key` -- returns the number of elements added
    * `BF.INFO key` -- returns filter metadata
  """

  alias Ferricstore.Bitcask.{Async, NIF}
  alias Ferricstore.Commands.ProbType

  @prob_read_timeout_ms 5_000
  @default_error_rate 0.01
  @default_capacity 100

  # -------------------------------------------------------------------
  # Public command handler
  # -------------------------------------------------------------------

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # BF.RESERVE key error_rate capacity
  # ---------------------------------------------------------------------------

  def handle("BF.RESERVE", [key, error_rate_str, capacity_str], store) do
    with {:ok, error_rate} <- parse_float(error_rate_str, "error_rate"),
         {:ok, capacity} <- parse_pos_integer(capacity_str, "capacity"),
         :ok <- validate_error_rate(error_rate),
         :ok <- check_bloom_not_exists(key, store) do
      do_bloom_reserve(key, capacity, error_rate, store)
    end
  end

  def handle("BF.RESERVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.reserve' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.ADD key element — write through Raft
  # ---------------------------------------------------------------------------

  def handle("BF.ADD", [key, element], store) do
    with :ok <- ProbType.check_expected(key, :bloom, store) do
      auto_params = default_auto_create_params()
      result = do_prob_write(store, {:bloom_add, key, element, auto_params})
      normalize_add_result(result)
    end
  end

  def handle("BF.ADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.add' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.MADD key element [element ...] — write through Raft
  # ---------------------------------------------------------------------------

  def handle("BF.MADD", [key | elements], store) when elements != [] do
    with :ok <- ProbType.check_expected(key, :bloom, store) do
      auto_params = default_auto_create_params()
      result = do_prob_write(store, {:bloom_madd, key, elements, auto_params})
      normalize_add_result(result)
    end
  end

  def handle("BF.MADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.madd' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.EXISTS key element — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("BF.EXISTS", [key, element], store) do
    with :ok <- ProbType.check_expected(key, :bloom, store) do
      path = prob_path(store, key, "bloom")

      case await_nif(fn proxy, corr_id ->
             NIF.bloom_file_exists_async(proxy, corr_id, path, element)
           end) do
        {:ok, result} ->
          result

        {:error, "enoent"} ->
          0

        {:error, :timeout} ->
          {:error, "ERR timeout"}

        {:error, reason} ->
          {:error, "ERR bloom exists failed: #{reason}"}
      end
    end
  end

  def handle("BF.EXISTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.exists' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.MEXISTS key element [element ...] — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("BF.MEXISTS", [key | elements], store) when elements != [] do
    with :ok <- ProbType.check_expected(key, :bloom, store) do
      path = prob_path(store, key, "bloom")

      case await_nif(fn proxy, corr_id ->
             NIF.bloom_file_mexists_async(proxy, corr_id, path, elements)
           end) do
        {:ok, results} ->
          results

        {:error, "enoent"} ->
          List.duplicate(0, length(elements))

        {:error, :timeout} ->
          {:error, "ERR timeout"}

        {:error, reason} ->
          {:error, "ERR bloom mexists failed: #{reason}"}
      end
    end
  end

  def handle("BF.MEXISTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.mexists' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.CARD key — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("BF.CARD", [key], store) do
    with :ok <- ProbType.check_expected(key, :bloom, store) do
      path = prob_path(store, key, "bloom")

      case await_nif(fn proxy, corr_id ->
             NIF.bloom_file_card_async(proxy, corr_id, path)
           end) do
        {:ok, count} ->
          count

        {:error, "enoent"} ->
          0

        {:error, :timeout} ->
          {:error, "ERR timeout"}

        {:error, reason} ->
          {:error, "ERR bloom card failed: #{reason}"}
      end
    end
  end

  def handle("BF.CARD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.card' command"}
  end

  # ---------------------------------------------------------------------------
  # BF.INFO key — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("BF.INFO", [key], store) do
    with :ok <- ProbType.check_expected(key, :bloom, store) do
      path = prob_path(store, key, "bloom")

      case await_nif(fn proxy, corr_id ->
             NIF.bloom_file_info_async(proxy, corr_id, path)
           end) do
        {:ok, {num_bits, count, num_hashes}} ->
          # Try to get capacity/error_rate from stored metadata
          {capacity, error_rate} = recover_bloom_meta(key, store, num_bits, num_hashes)

          [
            "Capacity",
            capacity,
            "Size",
            count,
            "Number of filters",
            1,
            "Number of items inserted",
            count,
            "Expansion rate",
            0,
            "Error rate",
            error_rate,
            "Number of hash functions",
            num_hashes,
            "Number of bits",
            num_bits
          ]

        {:error, "enoent"} ->
          {:error, "ERR not found"}

        {:error, :timeout} ->
          {:error, "ERR timeout"}

        {:error, reason} ->
          {:error, "ERR bloom info failed: #{reason}"}
      end
    end
  end

  def handle("BF.INFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bf.info' command"}
  end

  # ---------------------------------------------------------------------------
  # Deletion (called from DEL / UNLINK handlers)
  # ---------------------------------------------------------------------------

  @spec nif_delete(binary(), map()) :: :ok
  def nif_delete(key, store) do
    path = prob_path(store, key, "bloom")
    _ = Ferricstore.FS.rm(path)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Optimal sizing (public for tests)
  # ---------------------------------------------------------------------------

  @doc false
  @spec optimal_num_bits(pos_integer(), float()) :: pos_integer()
  def optimal_num_bits(capacity, error_rate) do
    m = -1.0 * capacity * :math.log(error_rate) / :math.pow(:math.log(2), 2)
    max(1, ceil(m))
  end

  @doc false
  @spec optimal_num_hashes(pos_integer(), pos_integer()) :: pos_integer()
  def optimal_num_hashes(num_bits, capacity) do
    k = num_bits / capacity * :math.log(2)
    max(1, round(k))
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp await_nif(submit_fun), do: Async.await(submit_fun, @prob_read_timeout_ms)

  defp check_bloom_not_exists(key, store) do
    case ProbType.check_create(key, :bloom, store) do
      :ok -> if bloom_file_exists?(key, store), do: {:error, "ERR item exists"}, else: :ok
      {:error, :exists} -> {:error, "ERR item exists"}
      {:error, _} = error -> error
    end
  end

  defp do_bloom_reserve(key, capacity, error_rate, store) do
    {num_bits, num_hashes} = compute_params(capacity, error_rate)

    meta =
      {:bloom_meta,
       %{
         path: prob_path(store, key, "bloom"),
         num_bits: num_bits,
         num_hashes: num_hashes,
         capacity: capacity,
         error_rate: error_rate
       }}

    result = do_prob_write(store, {:bloom_create, key, num_bits, num_hashes, meta})

    case result do
      {:ok, _} -> :ok
      :ok -> :ok
      other -> other
    end
  end

  defp compute_params(capacity, error_rate) do
    num_bits = optimal_num_bits(capacity, error_rate)
    num_hashes = optimal_num_hashes(num_bits, capacity)
    {num_bits, num_hashes}
  end

  defp default_auto_create_params do
    {num_bits, num_hashes} = compute_params(@default_capacity, @default_error_rate)

    %{
      num_bits: num_bits,
      num_hashes: num_hashes,
      capacity: @default_capacity,
      error_rate: @default_error_rate
    }
  end

  defp prob_path(store, key, ext) do
    safe = Base.url_encode64(key, padding: false)
    prob_dir = resolve_prob_dir(store, key)
    Path.join(prob_dir, "#{safe}.#{ext}")
  end

  defp resolve_prob_dir(%{prob_dir: prob_dir_fn}, _key) when is_function(prob_dir_fn),
    do: prob_dir_fn.()

  defp resolve_prob_dir(%{prob_dir_for_key: f}, key) when is_function(f), do: f.(key)
  defp resolve_prob_dir(%{bloom_registry: %{dir: dir}}, _key), do: dir

  defp resolve_prob_dir(%FerricStore.Instance{} = ctx, key) do
    idx = Ferricstore.Store.Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
    Path.join(shard_path, "prob")
  end

  defp resolve_prob_dir(_store, key) do
    ctx = FerricStore.Instance.get(:default)
    idx = Ferricstore.Store.Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
    Path.join(shard_path, "prob")
  end

  defp bloom_file_exists?(key, store) do
    case Map.get(store, :exists?) do
      nil ->
        path = prob_path(store, key, "bloom")
        Ferricstore.FS.exists?(path)

      exists_fn ->
        exists_fn.(key)
    end
  end

  defp normalize_add_result({:ok, result}), do: result

  defp normalize_add_result({:error, reason}),
    do: {:error, "ERR bloom add failed: #{inspect(reason)}"}

  defp normalize_add_result(:ok), do: :ok
  defp normalize_add_result(other), do: other

  defp do_prob_write(%FerricStore.Instance{} = ctx, command) do
    Ferricstore.Store.Router.prob_write(ctx, command)
  end

  defp do_prob_write(store, command) do
    case Map.get(store, :prob_write) do
      nil -> apply_prob_locally(store, command)
      write_fn -> write_fn.(command)
    end
  end

  # Direct NIF application for test stores without Raft. Every path that
  # creates a prob file fsyncs the containing dir so the filename entry
  # is durable — matches the Raft state-machine path.
  defp apply_prob_locally(store, {:bloom_create, key, num_bits, num_hashes, meta}) do
    path = prob_path(store, key, "bloom")
    ensure_prob_dir(Path.dirname(path))
    result = NIF.bloom_file_create(path, num_bits, num_hashes)
    _ = NIF.v2_fsync_dir(Path.dirname(path))
    register_bloom_meta(result, store, key, meta)
    result
  end

  defp apply_prob_locally(store, {:bloom_add, key, element, auto_params}) do
    path = prob_path(store, key, "bloom")
    dir = Path.dirname(path)
    ensure_prob_dir(dir)
    created? = not Ferricstore.FS.exists?(path)

    if created? do
      if auto_params do
        %{num_bits: nb, num_hashes: nh} = auto_params
        result = NIF.bloom_file_create(path, nb, nh)
        _ = NIF.v2_fsync_dir(dir)
        register_bloom_meta(result, store, key, {:bloom_meta, Map.put(auto_params, :path, path)})
      end
    end

    NIF.bloom_file_add(path, element)
  end

  defp apply_prob_locally(store, {:bloom_madd, key, elements, auto_params}) do
    path = prob_path(store, key, "bloom")
    dir = Path.dirname(path)
    ensure_prob_dir(dir)
    created? = not Ferricstore.FS.exists?(path)

    if created? do
      if auto_params do
        %{num_bits: nb, num_hashes: nh} = auto_params
        result = NIF.bloom_file_create(path, nb, nh)
        _ = NIF.v2_fsync_dir(dir)
        register_bloom_meta(result, store, key, {:bloom_meta, Map.put(auto_params, :path, path)})
      end
    end

    NIF.bloom_file_madd(path, elements)
  end

  defp register_bloom_meta({:error, _}, _store, _key, _meta), do: :ok

  defp register_bloom_meta(_result, store, key, meta), do: ProbType.register(store, key, meta)

  # Creates the prob dir if missing and fsyncs its parent so the new
  # directory entry is durable. No-op if the dir already exists.
  defp ensure_prob_dir(dir) do
    unless Ferricstore.FS.dir?(dir) do
      Ferricstore.FS.mkdir_p!(dir)
      _ = NIF.v2_fsync_dir(Path.dirname(dir))
    end

    :ok
  end

  defp recover_bloom_meta(key, store, num_bits, num_hashes) do
    case fetch_stored_meta(key, store) do
      {capacity, error_rate} -> {capacity, error_rate}
      nil -> derive_bloom_params(num_bits, num_hashes)
    end
  end

  defp fetch_stored_meta(key, store) do
    case Map.get(store, :get) do
      nil -> nil
      get_fn -> parse_stored_meta(get_fn.(key))
    end
  end

  defp parse_stored_meta(nil), do: nil

  defp parse_stored_meta(value) when is_binary(value) do
    try do
      case :erlang.binary_to_term(value) do
        {:bloom_meta, %{capacity: c, error_rate: e}} -> {c, e}
        _ -> nil
      end
    rescue
      _ -> nil
    end
  end

  defp parse_stored_meta(_), do: nil

  defp derive_bloom_params(num_bits, num_hashes) do
    capacity =
      if num_hashes > 0,
        do: max(1, round(num_bits * :math.log(2) / num_hashes)),
        else: @default_capacity

    error_rate =
      if capacity > 0,
        do: :math.exp(-num_bits * :math.pow(:math.log(2), 2) / capacity),
        else: @default_error_rate

    {capacity, error_rate}
  end

  # ---------------------------------------------------------------------------
  # Private: input validation
  # ---------------------------------------------------------------------------

  @spec parse_float(binary(), binary()) :: {:ok, float()} | {:error, binary()}
  defp parse_float(str, name) do
    case Float.parse(str) do
      {val, ""} ->
        {:ok, val}

      {val, ".0"} ->
        {:ok, val}

      _ ->
        case Integer.parse(str) do
          {int_val, ""} -> {:ok, int_val / 1}
          _ -> {:error, "ERR bad #{name} value"}
        end
    end
  end

  @spec parse_pos_integer(binary(), binary()) :: {:ok, pos_integer()} | {:error, binary()}
  defp parse_pos_integer(str, name) do
    case Integer.parse(str) do
      {val, ""} when val > 0 -> {:ok, val}
      _ -> {:error, "ERR bad #{name} value"}
    end
  end

  @spec validate_error_rate(float()) :: :ok | {:error, binary()}
  defp validate_error_rate(rate) when rate > 0.0 and rate < 1.0, do: :ok
  defp validate_error_rate(_), do: {:error, "ERR (0 < error rate range < 1)"}
end
