defmodule Ferricstore.Commands.CMS do
  @moduledoc """
  Count-Min Sketch commands routed through Raft for replication.

  Write commands (CMS.INITBYDIM, CMS.INITBYPROB, CMS.INCRBY, CMS.MERGE)
  route through Raft via `store.prob_write`. Read commands (CMS.QUERY,
  CMS.INFO) use stateless pread NIFs on local files.
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Commands.ProbType

  # -------------------------------------------------------------------
  # Public command handler
  # -------------------------------------------------------------------

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("CMS.INITBYDIM", [key, width_str, depth_str], store) do
    with {:ok, width} <- parse_pos_integer(width_str, "width"),
         {:ok, depth} <- parse_pos_integer(depth_str, "depth"),
         :ok <- check_not_exists(key, store) do
      result = do_prob_write(store, {:cms_create, key, width, depth})

      case result do
        {:ok, _} -> :ok
        :ok -> :ok
        other -> other
      end
    end
  end

  def handle("CMS.INITBYDIM", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.initbydim' command"}

  def handle("CMS.INITBYPROB", [key, error_str, prob_str], store) do
    with {:ok, error} <- parse_pos_float(error_str, "error"),
         {:ok, prob} <- parse_prob_float(prob_str, "probability"),
         :ok <- check_not_exists(key, store) do
      width = ceil(:math.exp(1) / error)
      depth = ceil(:math.log(1.0 / prob))
      result = do_prob_write(store, {:cms_create, key, width, depth})

      case result do
        {:ok, _} -> :ok
        :ok -> :ok
        other -> other
      end
    end
  end

  def handle("CMS.INITBYPROB", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.initbyprob' command"}

  def handle("CMS.INCRBY", [key | rest], store) when rest != [] do
    with :ok <- ProbType.check_expected(key, :cms, store),
         {:ok, pairs} <- parse_element_count_pairs(rest) do
      result = do_prob_write(store, {:cms_incrby, key, pairs})
      normalize_result(result)
    end
  end

  def handle("CMS.INCRBY", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.incrby' command"}

  # CMS.QUERY — local stateless pread (async)
  def handle("CMS.QUERY", [key | elements], store) when elements != [] do
    with :ok <- ProbType.check_expected(key, :cms, store) do
      path = prob_path(store, key, "cms")
      corr_id = System.unique_integer([:positive, :monotonic])
      :ok = NIF.cms_file_query_async(self(), corr_id, path, elements)

      receive do
        {:tokio_complete, ^corr_id, :ok, counts} -> counts
        {:tokio_complete, ^corr_id, :error, "enoent"} -> {:error, "ERR CMS: key does not exist"}
        {:tokio_complete, ^corr_id, :error, reason} -> {:error, "ERR CMS query failed: #{reason}"}
      after
        5000 -> {:error, "ERR timeout"}
      end
    end
  end

  def handle("CMS.QUERY", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.query' command"}

  def handle("CMS.MERGE", [dst, numkeys_str | rest], store) do
    with {:ok, numkeys} <- parse_pos_integer(numkeys_str, "numkeys"),
         {:ok, src_keys, weights} <- parse_merge_args(rest, numkeys),
         :ok <- ProbType.check_expected(dst, :cms, store),
         :ok <- check_source_types(src_keys, store),
         {:ok, first_w, first_d} <- get_first_sketch_dims(store, src_keys),
         :ok <- validate_sketch_dims(store, src_keys, first_w, first_d) do
      create_params = %{width: first_w, depth: first_d}
      src_paths = Enum.map(src_keys, &prob_path(store, &1, "cms"))
      result = do_prob_write(store, {:cms_merge, dst, src_paths, weights, create_params})
      normalize_result(result)
    end
  end

  def handle("CMS.MERGE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.merge' command"}

  # CMS.INFO — local stateless pread (async)
  def handle("CMS.INFO", [key], store) do
    with :ok <- ProbType.check_expected(key, :cms, store) do
      path = prob_path(store, key, "cms")
      corr_id = System.unique_integer([:positive, :monotonic])
      :ok = NIF.cms_file_info_async(self(), corr_id, path)

      receive do
        {:tokio_complete, ^corr_id, :ok, {width, depth, count}} ->
          ["width", width, "depth", depth, "count", count]

        {:tokio_complete, ^corr_id, :error, "enoent"} ->
          {:error, "ERR CMS: key does not exist"}

        {:tokio_complete, ^corr_id, :error, reason} ->
          {:error, "ERR CMS info failed: #{reason}"}
      after
        5000 -> {:error, "ERR timeout"}
      end
    end
  end

  def handle("CMS.INFO", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cms.info' command"}

  # ---------------------------------------------------------------------------
  # Deletion
  # ---------------------------------------------------------------------------

  @spec nif_delete(binary(), map()) :: :ok
  def nif_delete(key, store) do
    path = prob_path(store, key, "cms")
    _ = Ferricstore.FS.rm(path)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp prob_path(store, key, ext) do
    safe = Base.url_encode64(key, padding: false)
    prob_dir = resolve_prob_dir(store, key)
    Path.join(prob_dir, "#{safe}.#{ext}")
  end

  defp resolve_prob_dir(%{prob_dir: prob_dir_fn}, _key) when is_function(prob_dir_fn),
    do: prob_dir_fn.()

  defp resolve_prob_dir(%{prob_dir_for_key: f}, key) when is_function(f), do: f.(key)
  defp resolve_prob_dir(%{cms_registry: %{dir: dir}}, _key), do: dir

  defp resolve_prob_dir(_store, key) do
    ctx = FerricStore.Instance.get(:default)
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    idx = Ferricstore.Store.Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, idx)
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

  defp apply_prob_locally(store, {:cms_create, key, width, depth}) do
    path = prob_path(store, key, "cms")
    dir = Path.dirname(path)
    ensure_prob_dir(dir)
    result = NIF.cms_file_create(path, width, depth)
    _ = NIF.v2_fsync_dir(dir)
    register_cms_meta(result, store, key, width, depth)
    result
  end

  defp apply_prob_locally(store, {:cms_incrby, key, items}) do
    path = prob_path(store, key, "cms")
    NIF.cms_file_incrby(path, items)
  end

  # src_paths are pre-resolved absolute paths from the handler
  defp apply_prob_locally(store, {:cms_merge, dst_key, src_paths, weights, create_params}) do
    dst_path = prob_path(store, dst_key, "cms")
    dir = Path.dirname(dst_path)
    ensure_prob_dir(dir)

    unless Ferricstore.FS.exists?(dst_path) do
      %{width: w, depth: d} = create_params
      result = NIF.cms_file_create(dst_path, w, d)
      _ = NIF.v2_fsync_dir(dir)
      register_cms_meta(result, store, dst_key, w, d)
    end

    NIF.cms_file_merge(dst_path, src_paths, weights)
  end

  # Creates the prob dir if missing and fsyncs its parent so the new
  # directory entry is durable.
  defp ensure_prob_dir(dir) do
    unless Ferricstore.FS.dir?(dir) do
      Ferricstore.FS.mkdir_p!(dir)
      _ = NIF.v2_fsync_dir(Path.dirname(dir))
    end

    :ok
  end

  defp check_not_exists(key, store) do
    case ProbType.check_create(key, :cms, store) do
      :ok -> if cms_file_exists?(key, store), do: {:error, "ERR item already exists"}, else: :ok
      {:error, :exists} -> {:error, "ERR item already exists"}
      {:error, _} = error -> error
    end
  end

  defp cms_file_exists?(key, store) do
    case Map.get(store, :exists?) do
      nil -> Ferricstore.FS.exists?(prob_path(store, key, "cms"))
      exists_fn -> exists_fn.(key)
    end
  end

  defp cms_file_info_for_key(store, key) do
    path = prob_path(store, key, "cms")
    NIF.cms_file_info(path)
  end

  defp normalize_result({:ok, result}), do: result
  defp normalize_result(:ok), do: :ok
  defp normalize_result({:error, _} = err), do: err
  defp normalize_result(other), do: other

  defp parse_pos_integer(str, label) do
    case Integer.parse(str) do
      {n, ""} when n > 0 -> {:ok, n}
      {_n, ""} -> {:error, "ERR #{label} must be a positive integer"}
      _ -> {:error, "ERR #{label} is not an integer or out of range"}
    end
  end

  defp parse_pos_float(str, label) do
    case Float.parse(str) do
      {f, ""} when f > 0 -> {:ok, f}
      {_f, ""} -> {:error, "ERR #{label} must be a positive number"}
      _ -> {:error, "ERR #{label} is not a valid number"}
    end
  end

  defp get_first_sketch_dims(store, src_keys) do
    case cms_file_info_for_key(store, hd(src_keys)) do
      {:ok, {w, d, _}} -> {:ok, w, d}
      {:error, _} = err -> err
    end
  end

  defp validate_sketch_dims(store, src_keys, first_w, first_d) do
    all_valid =
      Enum.all?(src_keys, fn k ->
        case cms_file_info_for_key(store, k) do
          {:ok, {w, d, _}} -> w == first_w and d == first_d
          _ -> false
        end
      end)

    if all_valid, do: :ok, else: {:error, "ERR CMS: width/depth of src sketches must be equal"}
  end

  defp check_source_types(src_keys, store) do
    Enum.reduce_while(src_keys, :ok, fn key, :ok ->
      case ProbType.check_expected(key, :cms, store) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp register_cms_meta({:error, _}, _store, _key, _width, _depth), do: :ok

  defp register_cms_meta(_result, store, key, width, depth) do
    ProbType.register(store, key, {:cms_meta, %{width: width, depth: depth}})
  end

  defp parse_prob_float(str, label) do
    case Float.parse(str) do
      {f, ""} when f > 0 and f < 1 -> {:ok, f}
      {_f, ""} -> {:error, "ERR #{label} must be between 0 and 1 exclusive"}
      _ -> {:error, "ERR #{label} is not a valid number"}
    end
  end

  defp parse_element_count_pairs(args) do
    if rem(length(args), 2) != 0 do
      {:error, "ERR wrong number of arguments for 'cms.incrby' command"}
    else
      do_parse_pairs(args)
    end
  end

  defp do_parse_pairs(args) do
    result =
      args
      |> Enum.chunk_every(2)
      |> Enum.reduce_while([], fn [element, count_str], acc ->
        case parse_count(count_str) do
          {:ok, count} -> {:cont, [{element, count} | acc]}
          {:error, _} = err -> {:halt, err}
        end
      end)

    case result do
      {:error, _} = err -> err
      list -> {:ok, Enum.reverse(list)}
    end
  end

  defp parse_count(str) do
    case Integer.parse(str) do
      {count, ""} when count >= 1 -> {:ok, count}
      _ -> {:error, "ERR CMS: invalid count value"}
    end
  end

  defp parse_merge_args(args, numkeys) when length(args) < numkeys do
    _ = numkeys
    {:error, "ERR wrong number of arguments for 'cms.merge' command"}
  end

  defp parse_merge_args(args, numkeys) do
    {src_keys, remaining} = Enum.split(args, numkeys)
    parse_merge_weights(src_keys, remaining, numkeys)
  end

  defp parse_merge_weights(src_keys, [], numkeys),
    do: {:ok, src_keys, List.duplicate(1, numkeys)}

  defp parse_merge_weights(src_keys, ["WEIGHTS" | weight_strs], numkeys) do
    if length(weight_strs) != numkeys do
      {:error, "ERR wrong number of weights for 'cms.merge' command"}
    else
      parse_weight_values(src_keys, weight_strs)
    end
  end

  defp parse_merge_weights(_src_keys, _remaining, _numkeys),
    do: {:error, "ERR syntax error in 'cms.merge' command"}

  defp parse_weight_values(src_keys, weight_strs) do
    weights =
      Enum.reduce_while(weight_strs, [], fn w_str, acc ->
        case Integer.parse(w_str) do
          {w, ""} -> {:cont, [w | acc]}
          _ -> {:halt, :error}
        end
      end)

    case weights do
      :error -> {:error, "ERR CMS: invalid weight value"}
      ws -> {:ok, src_keys, Enum.reverse(ws)}
    end
  end
end
