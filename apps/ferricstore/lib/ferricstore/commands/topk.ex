defmodule Ferricstore.Commands.TopK do
  @moduledoc """
  Handles Top-K commands routed through Raft for replication.

  Write commands (TOPK.RESERVE, TOPK.ADD, TOPK.INCRBY) route through
  Raft via `store.prob_write`. Read commands (TOPK.QUERY, TOPK.LIST,
  TOPK.COUNT, TOPK.INFO) use stateless pread NIFs on local files.
  """

  alias Ferricstore.Bitcask.{Async, NIF}
  alias Ferricstore.Commands.ProbType
  alias Ferricstore.Store.Ops

  @prob_read_timeout_ms 5_000
  @default_width 8
  @default_depth 7
  @default_decay 0.9

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # TOPK.RESERVE key k [width depth decay]
  # ---------------------------------------------------------------------------

  def handle("TOPK.RESERVE", [key, k_str], store) do
    do_reserve(key, k_str, @default_width, @default_depth, @default_decay, store)
  end

  def handle("TOPK.RESERVE", [key, k_str, width_str, depth_str, decay_str], store) do
    with {:ok, width} <- parse_pos_integer(width_str, "width"),
         {:ok, depth} <- parse_pos_integer(depth_str, "depth"),
         {:ok, decay} <- parse_decay(decay_str) do
      do_reserve(key, k_str, width, depth, decay, store)
    end
  end

  def handle("TOPK.RESERVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.reserve' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.ADD key element [element ...] — write through Raft
  # ---------------------------------------------------------------------------

  def handle("TOPK.ADD", [key | elements], store) when elements != [] do
    with :ok <- ProbType.check_expected(key, :topk, store) do
      result = do_prob_write(store, {:topk_add, key, elements})
      normalize_result(result)
    end
  end

  def handle("TOPK.ADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.add' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.INCRBY key element count [element count ...] — write through Raft
  # ---------------------------------------------------------------------------

  def handle("TOPK.INCRBY", [key | rest], store) when rest != [] do
    with :ok <- ProbType.check_expected(key, :topk, store),
         {:ok, pairs} <- parse_element_count_pairs(rest) do
      result = do_prob_write(store, {:topk_incrby, key, pairs})
      normalize_result(result)
    end
  end

  def handle("TOPK.INCRBY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.incrby' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.QUERY key element [element ...] — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("TOPK.QUERY", [key | elements], store) when elements != [] do
    path = prob_path(store, key, "topk")

    case await_nif(fn proxy, corr_id ->
           NIF.topk_file_query_v2_async(proxy, corr_id, path, elements)
         end) do
      {:ok, result} ->
        result

      {:error, "enoent"} ->
        missing_or_wrongtype(key, store, {:error, "ERR TOPK: key does not exist"})

      {:error, :timeout} ->
        {:error, "ERR timeout"}

      {:error, reason} ->
        {:error, "ERR TOPK: #{reason}"}
    end
  end

  def handle("TOPK.QUERY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.query' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.LIST key [WITHCOUNT] — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("TOPK.LIST", [key], store) do
    path = prob_path(store, key, "topk")

    case await_nif(fn proxy, corr_id ->
           NIF.topk_file_list_v2_async(proxy, corr_id, path)
         end) do
      {:ok, result} ->
        result

      {:error, "enoent"} ->
        missing_or_wrongtype(key, store, {:error, "ERR TOPK: key does not exist"})

      {:error, :timeout} ->
        {:error, "ERR timeout"}

      {:error, reason} ->
        {:error, "ERR topk list failed: #{reason}"}
    end
  end

  def handle("TOPK.LIST", [key, withcount], store) when is_binary(withcount) do
    if String.upcase(withcount) != "WITHCOUNT" do
      {:error, "ERR syntax error"}
    else
      do_list_with_count(key, store)
    end
  end

  def handle("TOPK.LIST", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.list' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.COUNT key element [element ...] — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("TOPK.COUNT", [key | elements], store) when elements != [] do
    path = prob_path(store, key, "topk")

    case await_nif(fn proxy, corr_id ->
           NIF.topk_file_count_v2_async(proxy, corr_id, path, elements)
         end) do
      {:ok, result} ->
        result

      {:error, "enoent"} ->
        missing_or_wrongtype(key, store, {:error, "ERR TOPK: key does not exist"})

      {:error, :timeout} ->
        {:error, "ERR timeout"}

      {:error, reason} ->
        {:error, "ERR TOPK: #{reason}"}
    end
  end

  def handle("TOPK.COUNT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.count' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.INFO key — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("TOPK.INFO", [key], store) do
    path = prob_path(store, key, "topk")

    case await_nif(fn proxy, corr_id ->
           NIF.topk_file_info_v2_async(proxy, corr_id, path)
         end) do
      {:ok, {k, width, depth, decay}} ->
        ["k", k, "width", width, "depth", depth, "decay", decay]

      {:error, "enoent"} ->
        missing_or_wrongtype(key, store, {:error, "ERR TOPK: key does not exist"})

      {:error, :timeout} ->
        {:error, "ERR timeout"}

      {:error, reason} ->
        {:error, "ERR TOPK: #{reason}"}
    end
  end

  def handle("TOPK.INFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.info' command"}
  end

  # ---------------------------------------------------------------------------
  # Deletion
  # ---------------------------------------------------------------------------

  @spec nif_delete(binary(), map()) :: :ok
  def nif_delete(key, store) do
    path = prob_path(store, key, "topk")
    _ = Ferricstore.FS.rm(path)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp do_list_with_count(key, store) do
    path = prob_path(store, key, "topk")

    # First: get list (async, await)
    items =
      case await_nif(fn proxy, corr_id ->
             NIF.topk_file_list_v2_async(proxy, corr_id, path)
           end) do
        {:ok, result} ->
          result

        {:error, "enoent"} ->
          missing_or_wrongtype(key, store, {:error, "ERR TOPK: key does not exist"})

        {:error, :timeout} ->
          {:error, "ERR timeout"}

        {:error, reason} ->
          {:error, "ERR topk list failed: #{reason}"}
      end

    case items do
      {:error, _} = err ->
        err

      items when is_list(items) ->
        # Second: get counts (async, await)
        case await_nif(fn proxy, corr_id ->
               NIF.topk_file_count_v2_async(proxy, corr_id, path, items)
             end) do
          {:ok, counts} ->
            Enum.zip(items, counts) |> Enum.flat_map(fn {elem, count} -> [elem, count] end)

          {:error, "enoent"} ->
            {:error, "ERR TOPK: key does not exist"}

          {:error, :timeout} ->
            {:error, "ERR timeout"}

          {:error, reason} ->
            {:error, "ERR topk list failed: #{reason}"}
        end
    end
  end

  defp do_reserve(key, k_str, width, depth, decay, store) do
    with {:ok, k} <- parse_pos_integer(k_str, "k"),
         :ok <- check_not_exists(key, store) do
      store
      |> do_prob_write({:topk_create, key, k, width, depth, decay * 1.0})
      |> maybe_register_topk(store, key)
    end
  end

  defp maybe_register_topk({:ok, _}, store, key), do: do_register_topk(store, key)
  defp maybe_register_topk(:ok, store, key), do: do_register_topk(store, key)
  defp maybe_register_topk(other, _store, _key), do: other

  defp await_nif(submit_fun), do: Async.await(submit_fun, @prob_read_timeout_ms)

  defp missing_or_wrongtype(key, store, missing_result) do
    case ProbType.check_expected(key, :topk, store) do
      :ok -> missing_result
      {:error, _} = error -> error
    end
  end

  defp do_register_topk(%FerricStore.Instance{}, _key) do
    # When using Instance struct, prob commands go through Raft and
    # the state machine's apply clause stores metadata via do_put.
    # No separate Ops.put needed.
    :ok
  end

  defp do_register_topk(store, key) do
    if is_nil(Map.get(store, :prob_write)) do
      path = prob_path(store, key, "topk")
      ProbType.register(store, key, {:topk_meta, %{path: path}})
    end

    :ok
  end

  defp check_not_exists(key, store) do
    case ProbType.check_create(key, :topk, store) do
      :ok -> if Ops.exists?(store, key), do: {:error, "ERR item already exists"}, else: :ok
      {:error, :exists} -> {:error, "ERR item already exists"}
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

  defp apply_prob_locally(store, {:topk_create, key, k, width, depth, decay}) do
    path = prob_path(store, key, "topk")
    dir = Path.dirname(path)
    created_dir? = not Ferricstore.FS.dir?(dir)
    Ferricstore.FS.mkdir_p!(dir)

    if created_dir? do
      _ = NIF.v2_fsync_dir(Path.dirname(dir))
    end

    result = NIF.topk_file_create_v2(path, k, width, depth, decay)
    # Fsync the prob dir so the new filename entry is durable. Matches
    # the Raft state-machine path's `prob_fsync_dir` after every
    # *_file_create.
    _ = NIF.v2_fsync_dir(dir)
    result
  end

  defp apply_prob_locally(store, {:topk_add, key, elements}) do
    path = prob_path(store, key, "topk")
    NIF.topk_file_add_v2(path, elements)
  end

  defp apply_prob_locally(store, {:topk_incrby, key, pairs}) do
    path = prob_path(store, key, "topk")
    NIF.topk_file_incrby_v2(path, pairs)
  end

  defp normalize_result({:ok, result}), do: result
  defp normalize_result(:ok), do: :ok
  defp normalize_result({:error, :enoent}), do: {:error, "ERR TOPK: key does not exist"}
  defp normalize_result({:error, _} = err), do: err
  defp normalize_result(other), do: other

  defp parse_pos_integer(str, label) do
    case Integer.parse(str) do
      {n, ""} when n > 0 -> {:ok, n}
      {_n, ""} -> {:error, "ERR #{label} must be a positive integer"}
      _ -> {:error, "ERR #{label} is not an integer or out of range"}
    end
  end

  defp parse_decay(str) do
    case Float.parse(str) do
      {f, ""} when f >= 0.0 and f <= 1.0 -> {:ok, f}
      {_f, ""} -> {:error, "ERR decay must be between 0 and 1"}
      _ -> {:error, "ERR decay is not a valid number"}
    end
  end

  defp parse_element_count_pairs(args) do
    if rem(length(args), 2) != 0 do
      {:error, "ERR wrong number of arguments for 'topk.incrby' command"}
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
      _ -> {:error, "ERR TOPK: invalid count value"}
    end
  end
end
