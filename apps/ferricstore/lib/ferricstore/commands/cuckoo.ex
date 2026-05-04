defmodule Ferricstore.Commands.Cuckoo do
  @moduledoc """
  Handles Redis-compatible Cuckoo filter commands.

  Write commands (CF.RESERVE, CF.ADD, CF.ADDNX, CF.DEL) route through
  Raft via `store.prob_write`. Read commands (CF.EXISTS, CF.MEXISTS,
  CF.COUNT, CF.INFO) use stateless pread NIFs on local files.
  """

  alias Ferricstore.Bitcask.{Async, NIF}
  alias Ferricstore.Commands.ProbType

  @prob_read_timeout_ms 5_000
  @default_capacity 1024
  @bucket_size 4

  # -------------------------------------------------------------------
  # Public command handler
  # -------------------------------------------------------------------

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # CF.RESERVE key capacity
  # ---------------------------------------------------------------------------

  def handle("CF.RESERVE", [key, capacity_str], store) do
    with {:ok, capacity} <- parse_pos_integer(capacity_str, "capacity"),
         :ok <- check_cuckoo_not_exists(key, store) do
      store
      |> do_prob_write({:cuckoo_create, key, capacity, @bucket_size})
      |> normalize_create_result()
    end
  end

  def handle("CF.RESERVE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.reserve' command"}

  # ---------------------------------------------------------------------------
  # CF.ADD key element — write through Raft
  # ---------------------------------------------------------------------------

  def handle("CF.ADD", [key, element], store) do
    with :ok <- ProbType.check_expected(key, :cuckoo, store) do
      auto_params = %{capacity: @default_capacity, bucket_size: @bucket_size}
      result = do_prob_write(store, {:cuckoo_add, key, element, auto_params})

      case result do
        {:ok, 1} -> 1
        {:ok, _} -> 1
        :ok -> 1
        {:error, {:fsync_dir_failed, _phase, _reason} = reason} -> {:error, reason}
        {:error, _} -> {:error, "ERR filter is full"}
        other -> other
      end
    end
  end

  def handle("CF.ADD", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.add' command"}

  # ---------------------------------------------------------------------------
  # CF.ADDNX key element — write through Raft
  # ---------------------------------------------------------------------------

  def handle("CF.ADDNX", [key, element], store) do
    with :ok <- ProbType.check_expected(key, :cuckoo, store) do
      auto_params = %{capacity: @default_capacity, bucket_size: @bucket_size}
      result = do_prob_write(store, {:cuckoo_addnx, key, element, auto_params})

      case result do
        {:ok, n} when n in [0, 1] -> n
        {:error, {:fsync_dir_failed, _phase, _reason} = reason} -> {:error, reason}
        {:error, _} -> {:error, "ERR filter is full"}
        other -> other
      end
    end
  end

  def handle("CF.ADDNX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.addnx' command"}

  # ---------------------------------------------------------------------------
  # CF.DEL key element — write through Raft
  # ---------------------------------------------------------------------------

  def handle("CF.DEL", [key, element], store) do
    with :ok <- ProbType.check_expected(key, :cuckoo, store) do
      path = prob_path(store, key, "cuckoo")

      if Ferricstore.FS.exists?(path) do
        result = do_prob_write(store, {:cuckoo_del, key, element})

        case result do
          {:ok, n} -> n
          {:error, reason} -> {:error, "ERR cuckoo del failed: #{inspect(reason)}"}
          other -> other
        end
      else
        0
      end
    end
  end

  def handle("CF.DEL", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.del' command"}

  # ---------------------------------------------------------------------------
  # CF.EXISTS key element — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("CF.EXISTS", [key, element], store) do
    path = prob_path(store, key, "cuckoo")

    case await_nif(fn proxy, corr_id ->
           NIF.cuckoo_file_exists_async(proxy, corr_id, path, element)
         end) do
      {:ok, result} ->
        result

      {:error, "enoent"} ->
        missing_or_wrongtype(key, store, 0)

      {:error, :timeout} ->
        {:error, "ERR timeout"}

      {:error, reason} ->
        {:error, "ERR cuckoo exists failed: #{reason}"}
    end
  end

  def handle("CF.EXISTS", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.exists' command"}

  # ---------------------------------------------------------------------------
  # CF.MEXISTS key element [element ...] — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("CF.MEXISTS", [key | elements], store) when elements != [] do
    path = prob_path(store, key, "cuckoo")

    case await_nif(fn proxy, corr_id ->
           NIF.cuckoo_file_mexists_async(proxy, corr_id, path, elements)
         end) do
      {:ok, results} ->
        results

      {:error, "enoent"} ->
        missing_or_wrongtype(key, store, List.duplicate(0, length(elements)))

      {:error, :timeout} ->
        {:error, "ERR timeout"}

      {:error, reason} ->
        {:error, "ERR cuckoo mexists failed: #{reason}"}
    end
  end

  def handle("CF.MEXISTS", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.mexists' command"}

  # ---------------------------------------------------------------------------
  # CF.COUNT key element — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("CF.COUNT", [key, element], store) do
    path = prob_path(store, key, "cuckoo")

    case await_nif(fn proxy, corr_id ->
           NIF.cuckoo_file_count_async(proxy, corr_id, path, element)
         end) do
      {:ok, count} ->
        count

      {:error, "enoent"} ->
        missing_or_wrongtype(key, store, 0)

      {:error, :timeout} ->
        {:error, "ERR timeout"}

      {:error, reason} ->
        {:error, "ERR cuckoo count failed: #{reason}"}
    end
  end

  def handle("CF.COUNT", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.count' command"}

  # ---------------------------------------------------------------------------
  # CF.INFO key — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("CF.INFO", [key], store) do
    path = prob_path(store, key, "cuckoo")

    case await_nif(fn proxy, corr_id ->
           NIF.cuckoo_file_info_async(proxy, corr_id, path)
         end) do
      {:ok,
       {num_buckets, bucket_size, fingerprint_size, num_items, num_deletes, total_slots,
        max_kicks}} ->
        [
          "Size",
          total_slots,
          "Number of buckets",
          num_buckets,
          "Number of filters",
          1,
          "Number of items inserted",
          num_items,
          "Number of items deleted",
          num_deletes,
          "Bucket size",
          bucket_size,
          "Fingerprint size",
          fingerprint_size,
          "Max iterations",
          max_kicks,
          "Expansion rate",
          0
        ]

      {:error, "enoent"} ->
        missing_or_wrongtype(key, store, {:error, "ERR not found"})

      {:error, :timeout} ->
        {:error, "ERR timeout"}

      {:error, reason} ->
        {:error, "ERR cuckoo info failed: #{reason}"}
    end
  end

  def handle("CF.INFO", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cf.info' command"}

  # ---------------------------------------------------------------------------
  # Deletion
  # ---------------------------------------------------------------------------

  @spec nif_delete(binary(), map()) :: :ok | {:error, term()}
  def nif_delete(key, store) do
    path = prob_path(store, key, "cuckoo")

    case Ferricstore.FS.rm(path) do
      :ok -> prob_fsync_dir(Path.dirname(path), :delete_prob_file)
      {:error, {:not_found, _msg}} -> :ok
      {:error, reason} -> {:error, {:delete_prob_file_failed, reason}}
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp await_nif(submit_fun), do: Async.await(submit_fun, @prob_read_timeout_ms)

  defp missing_or_wrongtype(key, store, missing_result) do
    case ProbType.check_expected(key, :cuckoo, store) do
      :ok -> missing_result
      {:error, _} = error -> error
    end
  end

  defp prob_path(store, key, ext) do
    safe = Base.url_encode64(key, padding: false)
    prob_dir = resolve_prob_dir(store, key)
    Path.join(prob_dir, "#{safe}.#{ext}")
  end

  defp resolve_prob_dir(%{prob_dir: prob_dir_fn}, _key) when is_function(prob_dir_fn),
    do: prob_dir_fn.()

  defp resolve_prob_dir(%{prob_dir_for_key: f}, key) when is_function(f), do: f.(key)
  defp resolve_prob_dir(%{cuckoo_registry: %{dir: dir}}, _key), do: dir

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

  defp do_prob_write(%FerricStore.Instance{} = ctx, command) do
    Ferricstore.Store.Router.prob_write(ctx, command)
  end

  defp do_prob_write(store, command) do
    case Map.get(store, :prob_write) do
      nil -> apply_prob_locally(store, command)
      write_fn -> write_fn.(command)
    end
  end

  defp apply_prob_locally(store, {:cuckoo_create, key, capacity, bucket_size}) do
    path = prob_path(store, key, "cuckoo")
    dir = Path.dirname(path)

    with :ok <- ensure_prob_dir(dir),
         {:ok, _resource} = result <- NIF.cuckoo_file_create(path, capacity, bucket_size),
         :ok <- prob_fsync_dir(dir, :prob_file_dir) do
      register_cuckoo_meta(result, store, key, capacity)
      result
    end
  end

  defp apply_prob_locally(store, {:cuckoo_add, key, element, auto_params}) do
    path = prob_path(store, key, "cuckoo")
    dir = Path.dirname(path)

    with :ok <- ensure_prob_dir(dir),
         :ok <- maybe_auto_create_cuckoo(store, key, path, dir, auto_params) do
      NIF.cuckoo_file_add(path, element)
    end
  end

  defp apply_prob_locally(store, {:cuckoo_addnx, key, element, auto_params}) do
    path = prob_path(store, key, "cuckoo")
    dir = Path.dirname(path)

    with :ok <- ensure_prob_dir(dir),
         :ok <- maybe_auto_create_cuckoo(store, key, path, dir, auto_params) do
      NIF.cuckoo_file_addnx(path, element)
    end
  end

  defp apply_prob_locally(store, {:cuckoo_del, key, element}) do
    path = prob_path(store, key, "cuckoo")
    NIF.cuckoo_file_del(path, element)
  end

  defp maybe_auto_create_cuckoo(store, key, path, dir, auto_params) do
    cond do
      Ferricstore.FS.exists?(path) ->
        :ok

      is_map(auto_params) ->
        %{capacity: cap, bucket_size: bs} = auto_params

        with {:ok, _resource} = result <- NIF.cuckoo_file_create(path, cap, bs),
             :ok <- prob_fsync_dir(dir, :prob_file_dir) do
          register_cuckoo_meta(result, store, key, cap)
          :ok
        end

      true ->
        :ok
    end
  end

  # Creates the prob dir if missing and fsyncs its parent so the new
  # directory entry is durable.
  defp ensure_prob_dir(dir) do
    if Ferricstore.FS.dir?(dir) do
      :ok
    else
      Ferricstore.FS.mkdir_p!(dir)
      prob_fsync_dir(Path.dirname(dir), :create_prob_dir)
    end
  end

  defp prob_fsync_dir(path, phase) do
    result =
      case Process.get(:ferricstore_prob_command_fsync_dir_hook) do
        fun when is_function(fun, 1) -> fun.(path)
        _ -> NIF.v2_fsync_dir(path)
      end

    case result do
      :ok -> :ok
      {:error, reason} -> {:error, {:fsync_dir_failed, phase, reason}}
    end
  end

  # Checks if a cuckoo filter key already exists. Uses store.exists? when
  # available (checks Bitcask metadata), falls back to file check.
  defp cuckoo_file_exists?(key, store) do
    case Map.get(store, :exists?) do
      nil ->
        path = prob_path(store, key, "cuckoo")
        Ferricstore.FS.exists?(path)

      exists_fn ->
        exists_fn.(key)
    end
  end

  defp check_cuckoo_not_exists(key, store) do
    case ProbType.check_create(key, :cuckoo, store) do
      :ok -> if cuckoo_file_exists?(key, store), do: {:error, "ERR item exists"}, else: :ok
      {:error, :exists} -> {:error, "ERR item exists"}
      {:error, _} = error -> error
    end
  end

  defp register_cuckoo_meta(_result, store, key, capacity) do
    ProbType.register(store, key, {:cuckoo_meta, %{capacity: capacity}})
  end

  defp normalize_create_result({:ok, _}), do: :ok
  defp normalize_create_result(:ok), do: :ok
  defp normalize_create_result(other), do: other

  @spec parse_pos_integer(binary(), binary()) :: {:ok, pos_integer()} | {:error, binary()}
  defp parse_pos_integer(str, name) do
    case Integer.parse(str) do
      {val, ""} when val > 0 -> {:ok, val}
      _ -> {:error, "ERR bad #{name} value"}
    end
  end
end
