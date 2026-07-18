defmodule Ferricstore.Commands.TopK do
  @moduledoc """
  Handles Top-K commands routed through Raft for replication.

  Write commands (TOPK.RESERVE, TOPK.ADD, TOPK.INCRBY) route through
  Raft via `store.prob_write`. Read commands (TOPK.QUERY, TOPK.LIST,
  TOPK.COUNT, TOPK.INFO) use stateless pread NIFs on local files.
  """

  alias Ferricstore.Bitcask.{Async, NIF}
  alias Ferricstore.Commands.ProbType
  alias Ferricstore.ProbFile
  alias Ferricstore.Store.Ops

  @prob_read_timeout_ms 5_000
  @default_width 8
  @default_depth 7
  @max_topk_k 100_000
  @max_topk_cms_counters 1_048_576
  @max_topk_element_bytes 252
  @max_int64 9_223_372_036_854_775_807
  @max_batch_items 10_000

  @spec handle_ast(term(), map()) :: term()
  def handle_ast({tag, {:error, msg}}, _store) when is_atom(tag), do: {:error, msg}
  def handle_ast({tag, _key, {:error, msg}}, _store) when is_atom(tag), do: {:error, msg}

  def handle_ast({:topk_reserve, key, k, width, depth}, store) do
    with :ok <- validate_topk_reserve(k, width, depth),
         :ok <- check_not_exists(key, store) do
      store
      |> do_prob_write({:topk_create, key, k, width, depth})
      |> maybe_register_topk(store, key)
    end
  end

  def handle_ast({:topk_reserve, _key, _k, _width, _depth, _decay}, _store),
    do: {:error, "ERR TOPK.RESERVE decay is not supported"}

  def handle_ast({:topk_add, args}, store), do: topk_add_args(args, store)

  def handle_ast({:topk_incrby, key, pairs}, store) do
    with :ok <- validate_topk_pairs(pairs),
         :ok <- ProbType.check_expected(key, :topk, store) do
      result = do_prob_write(store, {:topk_incrby, key, pairs})
      normalize_result(result)
    end
  end

  def handle_ast({:topk_query, args}, store), do: topk_query_args(args, store)
  def handle_ast({:topk_count, args}, store), do: topk_count_args(args, store)
  def handle_ast({:topk_info, args}, store), do: topk_info_args(args, store)
  def handle_ast({:topk_list, key, false}, store), do: topk_list_key(key, store)
  def handle_ast({:topk_list, key, true}, store), do: do_list_with_count(key, store)

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # TOPK.RESERVE key k [width depth]
  # ---------------------------------------------------------------------------

  def handle("TOPK.RESERVE", [key, k_str], store) do
    do_reserve(key, k_str, @default_width, @default_depth, store)
  end

  def handle("TOPK.RESERVE", [key, k_str, width_str, depth_str], store) do
    with {:ok, width} <- parse_pos_integer(width_str, "width"),
         {:ok, depth} <- parse_pos_integer(depth_str, "depth") do
      do_reserve(key, k_str, width, depth, store)
    end
  end

  def handle("TOPK.RESERVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.reserve' command"}
  end

  # ---------------------------------------------------------------------------
  # TOPK.ADD key element [element ...] — write through Raft
  # ---------------------------------------------------------------------------

  def handle("TOPK.ADD", args, store), do: topk_add_args(args, store)

  # ---------------------------------------------------------------------------
  # TOPK.INCRBY key element count [element count ...] — write through Raft
  # ---------------------------------------------------------------------------

  def handle("TOPK.INCRBY", [key | rest], store) when rest != [] do
    with :ok <- validate_raw_pair_batch(rest),
         {:ok, pairs} <- parse_element_count_pairs(rest),
         :ok <- validate_topk_pairs(pairs),
         :ok <- ProbType.check_expected(key, :topk, store) do
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

  def handle("TOPK.QUERY", args, store), do: topk_query_args(args, store)

  # ---------------------------------------------------------------------------
  # TOPK.LIST key [WITHCOUNT] — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("TOPK.LIST", [key], store), do: topk_list_key(key, store)

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

  def handle("TOPK.COUNT", args, store), do: topk_count_args(args, store)

  # ---------------------------------------------------------------------------
  # TOPK.INFO key — local stateless pread
  # ---------------------------------------------------------------------------

  def handle("TOPK.INFO", args, store), do: topk_info_args(args, store)

  defp topk_add_args([key | elements], store) when elements != [] do
    with :ok <- validate_topk_elements(elements),
         :ok <- ProbType.check_expected(key, :topk, store) do
      result = do_prob_write(store, {:topk_add, key, elements})
      normalize_result(result)
    end
  end

  defp topk_add_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.add' command"}
  end

  defp topk_query_args([key | elements], store) when elements != [] do
    with :ok <- validate_topk_elements(elements),
         :ok <- topk_read_status(key, store) do
      do_topk_query(key, elements, store)
    end
  end

  defp topk_query_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.query' command"}
  end

  defp do_topk_query(key, elements, store) do
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

  defp topk_list_key(key, store) do
    with :ok <- topk_read_status(key, store) do
      do_topk_list(key, store)
    end
  end

  defp do_topk_list(key, store) do
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

  defp topk_count_args([key | elements], store) when elements != [] do
    with :ok <- validate_topk_elements(elements),
         :ok <- topk_read_status(key, store) do
      do_topk_count(key, elements, store)
    end
  end

  defp topk_count_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.count' command"}
  end

  defp do_topk_count(key, elements, store) do
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

  defp topk_info_args([key], store) do
    with :ok <- topk_read_status(key, store) do
      do_topk_info(key, store)
    end
  end

  defp topk_info_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'topk.info' command"}
  end

  defp do_topk_info(key, store) do
    path = prob_path(store, key, "topk")

    case await_nif(fn proxy, corr_id ->
           NIF.topk_file_info_v2_async(proxy, corr_id, path)
         end) do
      {:ok, {k, width, depth}} ->
        ["k", k, "width", width, "depth", depth]

      {:error, "enoent"} ->
        missing_or_wrongtype(key, store, {:error, "ERR TOPK: key does not exist"})

      {:error, :timeout} ->
        {:error, "ERR timeout"}

      {:error, reason} ->
        {:error, "ERR TOPK: #{reason}"}
    end
  end

  # ---------------------------------------------------------------------------
  # Deletion
  # ---------------------------------------------------------------------------

  @spec nif_delete(binary(), map()) :: :ok | {:error, term()}
  def nif_delete(key, store) do
    path = prob_path(store, key, "topk")

    case Ferricstore.FS.rm(path) do
      :ok -> prob_fsync_dir(Path.dirname(path), :delete_prob_file)
      {:error, {:not_found, _msg}} -> :ok
      {:error, reason} -> {:error, {:delete_prob_file_failed, reason}}
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp do_list_with_count(key, store) do
    with :ok <- topk_read_status(key, store) do
      do_list_with_count_after_type_check(key, store)
    end
  end

  defp do_list_with_count_after_type_check(key, store) do
    path = prob_path(store, key, "topk")

    case await_nif(fn proxy, corr_id ->
           NIF.topk_file_list_with_count_async(proxy, corr_id, path)
         end) do
      {:ok, result} ->
        case normalize_list_with_count_response(result) do
          {:ok, response} -> response
          {:error, _message} = error -> error
        end

      {:error, "enoent"} ->
        missing_or_wrongtype(key, store, {:error, "ERR TOPK: key does not exist"})

      {:error, :timeout} ->
        {:error, "ERR timeout"}

      {:error, reason} ->
        {:error, "ERR topk list failed: #{reason}"}
    end
  end

  @doc false
  @spec normalize_list_with_count_response(term()) :: {:ok, list()} | {:error, binary()}
  def normalize_list_with_count_response(response) when is_list(response) do
    if valid_list_with_count_response?(response) do
      {:ok, response}
    else
      invalid_count_response()
    end
  end

  def normalize_list_with_count_response(_response), do: invalid_count_response()

  defp valid_list_with_count_response?([]), do: true

  defp valid_list_with_count_response?([item, count | rest])
       when is_binary(item) and is_integer(count) and count >= 0,
       do: valid_list_with_count_response?(rest)

  defp valid_list_with_count_response?(_response), do: false

  defp invalid_count_response, do: {:error, "ERR TOPK: invalid count response"}

  defp do_reserve(key, k_str, width, depth, store) do
    with {:ok, k} <- parse_pos_integer(k_str, "k"),
         :ok <- validate_topk_reserve(k, width, depth),
         :ok <- check_not_exists(key, store) do
      store
      |> do_prob_write({:topk_create, key, k, width, depth})
      |> maybe_register_topk(store, key)
    end
  end

  defp maybe_register_topk({:ok, _}, store, key) do
    case do_register_topk(store, key) do
      :ok ->
        :ok

      {:error, _reason} = error ->
        ProbType.rollback_created_file(prob_path(store, key, "topk"))
        error
    end
  end

  defp maybe_register_topk(:ok, store, key), do: do_register_topk(store, key)
  defp maybe_register_topk(other, _store, _key), do: other

  defp await_nif(submit_fun), do: Async.await(submit_fun, @prob_read_timeout_ms)

  defp missing_or_wrongtype(key, store, missing_result) do
    case ProbType.check_create(key, :topk, store) do
      :ok -> missing_result
      {:error, :exists} -> missing_result
      {:error, _} = error -> error
    end
  end

  defp topk_read_status(key, store) do
    case ProbType.check_create(key, :topk, store) do
      {:error, :exists} -> :ok
      :ok -> {:error, "ERR TOPK: key does not exist"}
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
    else
      :ok
    end
  end

  defp check_not_exists(key, store) do
    case ProbType.check_create(key, :topk, store) do
      :ok -> if Ops.exists?(store, key), do: {:error, "ERR item already exists"}, else: :ok
      {:error, :exists} -> {:error, "ERR item already exists"}
      {:error, _} = error -> error
    end
  end

  defp prob_path(store, key, ext) do
    prob_dir = resolve_prob_dir(store, key)
    ProbFile.path(prob_dir, key, ext)
  end

  defp resolve_prob_dir(%{prob_dir_for_key: f}, key) when is_function(f), do: f.(key)

  defp resolve_prob_dir(%{prob_dir: prob_dir_fn}, _key) when is_function(prob_dir_fn),
    do: prob_dir_fn.()

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

  defp apply_prob_locally(store, {:topk_create, key, k, width, depth}) do
    path = prob_path(store, key, "topk")
    dir = Path.dirname(path)

    with :ok <- ensure_prob_dir(dir),
         {:ok, _resource} = result <- NIF.topk_file_create_v2(path, k, width, depth) do
      ProbType.finalize_created_file(path, result, fn ->
        prob_fsync_dir(dir, :prob_file_dir)
      end)
    end
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

  defp validate_topk_reserve(k, width, depth)
       when is_integer(k) and k > 0 and is_integer(width) and width > 0 and is_integer(depth) and
              depth > 0 do
    cond do
      k > @max_topk_k ->
        {:error, "ERR k must be <= #{@max_topk_k}"}

      width > div(@max_topk_cms_counters, depth) ->
        {:error, "ERR TopK counter count exceeds #{@max_topk_cms_counters}"}

      true ->
        :ok
    end
  end

  defp validate_topk_reserve(_k, _width, _depth),
    do: {:error, "ERR invalid TopK reserve parameters"}

  defp validate_topk_elements([_ | _] = elements) do
    with :ok <- validate_batch_count(length(elements)) do
      if Enum.all?(elements, &(is_binary(&1) and byte_size(&1) <= @max_topk_element_bytes)) do
        :ok
      else
        {:error, "ERR TopK element exceeds #{@max_topk_element_bytes} bytes"}
      end
    end
  end

  defp validate_topk_elements(_elements), do: {:error, "ERR invalid TopK elements"}

  defp validate_topk_pairs([_ | _] = pairs) do
    with :ok <- validate_batch_count(length(pairs)) do
      if Enum.all?(pairs, fn
           {element, count}
           when is_binary(element) and byte_size(element) <= @max_topk_element_bytes and
                  is_integer(count) and count > 0 and count <= @max_int64 ->
             true

           _other ->
             false
         end) do
        :ok
      else
        {:error, "ERR TOPK: invalid element/count pairs"}
      end
    end
  end

  defp validate_topk_pairs(_pairs), do: {:error, "ERR TOPK: invalid element/count pairs"}

  defp validate_raw_pair_batch(args), do: validate_batch_count(div(length(args) + 1, 2))

  defp validate_batch_count(count) when count <= @max_batch_items, do: :ok

  defp validate_batch_count(_count),
    do: {:error, "ERR TopK batch exceeds maximum of #{@max_batch_items} items"}

  defp parse_pos_integer(str, label) do
    case Integer.parse(str) do
      {n, ""} when n > 0 -> {:ok, n}
      {_n, ""} -> {:error, "ERR #{label} must be a positive integer"}
      _ -> {:error, "ERR #{label} is not an integer or out of range"}
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
      {count, ""} when count >= 1 and count <= @max_int64 -> {:ok, count}
      _ -> {:error, "ERR TOPK: invalid count value"}
    end
  end
end
