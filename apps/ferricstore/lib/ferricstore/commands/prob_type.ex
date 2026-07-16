defmodule Ferricstore.Commands.ProbType do
  @moduledoc false

  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.TypeRegistry
  alias Ferricstore.TermCodec
  alias Ferricstore.Bitcask.NIF

  @wrongtype "WRONGTYPE Operation against a key holding the wrong kind of value"
  @max_prob_meta_value_size 8_192
  @max_bloom_bits 8_589_934_592
  @max_bloom_hashes 1_024
  @max_cms_depth 1_024
  @max_cms_counters 16_777_216
  @max_cuckoo_capacity 268_435_456
  @max_topk_k 100_000
  @max_topk_counters 1_048_576

  @spec check_expected(binary(), atom(), map()) :: :ok | {:error, binary()}
  def check_expected(key, expected, store) do
    case stored_type(key, store) do
      {:error, _reason} = error -> error
      nil -> :ok
      ^expected -> :ok
      _other -> {:error, @wrongtype}
    end
  end

  @spec check_create(binary(), atom(), map()) :: :ok | {:error, :exists | binary()}
  def check_create(key, expected, store) do
    case stored_type(key, store) do
      {:error, _reason} = error -> error
      nil -> :ok
      ^expected -> {:error, :exists}
      _other -> {:error, @wrongtype}
    end
  end

  @spec register(map(), binary(), {atom(), map()}) :: :ok | {:error, term()}
  def register(%FerricStore.Instance{}, _key, _meta), do: :ok
  def register(%{prob_write: write_fn}, _key, _meta) when is_function(write_fn), do: :ok

  def register(store, key, meta) when is_map(store) do
    if Map.has_key?(store, :put) do
      Ops.put(store, key, TermCodec.encode(meta), 0)
    else
      :ok
    end
  end

  def register(_store, _key, _meta), do: :ok

  @doc false
  @spec finalize_created_file(binary(), {:ok, term()}, (-> :ok | {:error, term()})) ::
          {:ok, term()} | {:error, term()}
  def finalize_created_file(path, {:ok, _resource} = created, commit) when is_function(commit, 0) do
    case commit.() do
      :ok ->
        created

      {:error, _reason} = error ->
        rollback_created_file(path)
        error
    end
  end

  @doc false
  @spec rollback_created_file(binary()) :: :ok
  def rollback_created_file(path) do
    case Ferricstore.FS.rm(path) do
      :ok ->
        _ = fsync_rollback_dir(Path.dirname(path))
        :ok

      {:error, {:not_found, _message}} ->
        :ok

      {:error, _reason} ->
        :ok
    end
  end

  defp fsync_rollback_dir(dir) do
    case Process.get(:ferricstore_prob_command_fsync_dir_hook) do
      fun when is_function(fun, 1) -> fun.(dir)
      _ -> NIF.v2_fsync_dir(dir)
    end
  end

  defp stored_type(key, store) do
    case raw_value_type(key, store) do
      {:error, _reason} = error -> error
      nil -> registry_type(key, store)
      type -> type
    end
  end

  defp raw_value_type(key, store) do
    case large_metadata_value?(key, store) do
      {:error, _reason} = error ->
        error

      true ->
        :other

      false ->
        if has_get?(store) do
          case Ops.get(store, key) do
            {:error, {:storage_read_failed, _reason}} = failure ->
              ReadResult.command_error(failure)

            value ->
              decode_raw_type(value)
          end
        end
    end
  end

  defp large_metadata_value?(key, store) do
    case metadata_value_size(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      size when is_integer(size) and size > @max_prob_meta_value_size -> true
      _ -> false
    end
  end

  defp metadata_value_size(%FerricStore.Instance{} = store, key), do: Ops.value_size(store, key)

  defp metadata_value_size(%Ferricstore.Store.LocalTxStore{} = store, key),
    do: Ops.value_size(store, key)

  defp metadata_value_size(%{value_size: value_size}, key) when is_function(value_size, 1),
    do: value_size.(key)

  defp metadata_value_size(_store, _key), do: :unknown

  defp decode_raw_type(nil), do: nil

  defp decode_raw_type(value) when is_binary(value) do
    case TermCodec.decode(value) do
      {:ok, term} -> decode_term_type(term)
      {:error, :invalid_external_term} -> :string
    end
  end

  defp decode_raw_type(value), do: decode_term_type(value)

  @doc false
  @spec metadata_type(term()) :: :bloom | :cms | :cuckoo | :topk | :other
  def metadata_type({:bloom_meta, metadata}) when is_map(metadata) do
    if valid_bloom_metadata?(metadata), do: :bloom, else: :other
  end

  def metadata_type({:cms_meta, metadata}) when is_map(metadata) do
    if valid_cms_metadata?(metadata), do: :cms, else: :other
  end

  def metadata_type({:cuckoo_meta, metadata}) when is_map(metadata) do
    if valid_cuckoo_metadata?(metadata), do: :cuckoo, else: :other
  end

  def metadata_type({:topk_meta, metadata}) when is_map(metadata) do
    if valid_topk_metadata?(metadata), do: :topk, else: :other
  end

  def metadata_type({:topk_path, path}) when is_binary(path) and byte_size(path) > 0, do: :topk
  def metadata_type(_term), do: :other

  defp decode_term_type(nil), do: nil
  defp decode_term_type(term), do: metadata_type(term)

  defp valid_bloom_metadata?(%{path: path} = metadata) when map_size(metadata) == 1,
    do: valid_path?(path)

  defp valid_bloom_metadata?(%{capacity: capacity, error_rate: error_rate} = metadata) do
    keys = Map.keys(metadata)

    Enum.all?(keys, &(&1 in [:path, :num_bits, :num_hashes, :capacity, :error_rate])) and
      is_integer(capacity) and capacity > 0 and is_float(error_rate) and error_rate > 0.0 and
      error_rate < 1.0 and valid_optional_path?(metadata) and valid_bloom_dimensions?(metadata)
  end

  defp valid_bloom_metadata?(_metadata), do: false

  defp valid_bloom_dimensions?(metadata) do
    case {Map.fetch(metadata, :num_bits), Map.fetch(metadata, :num_hashes)} do
      {:error, :error} ->
        true

      {{:ok, num_bits}, {:ok, num_hashes}} ->
        is_integer(num_bits) and num_bits > 0 and num_bits <= @max_bloom_bits and
          is_integer(num_hashes) and num_hashes > 0 and num_hashes <= @max_bloom_hashes

      _ ->
        false
    end
  end

  defp valid_cms_metadata?(%{path: path} = metadata) when map_size(metadata) == 1,
    do: valid_path?(path)

  defp valid_cms_metadata?(%{width: width, depth: depth} = metadata)
       when map_size(metadata) == 2 and is_integer(width) and width > 0 and is_integer(depth) and
              depth > 0 and depth <= @max_cms_depth,
       do: width <= div(@max_cms_counters, depth)

  defp valid_cms_metadata?(_metadata), do: false

  defp valid_cuckoo_metadata?(%{path: path} = metadata) when map_size(metadata) == 1,
    do: valid_path?(path)

  defp valid_cuckoo_metadata?(%{capacity: capacity} = metadata)
       when map_size(metadata) == 1 and is_integer(capacity),
       do: capacity > 0 and capacity <= @max_cuckoo_capacity

  defp valid_cuckoo_metadata?(_metadata), do: false

  defp valid_topk_metadata?(%{path: path} = metadata) when map_size(metadata) == 1,
    do: valid_path?(path)

  defp valid_topk_metadata?(
         %{
           path: path,
           k: k,
           width: width,
           depth: depth
         } = metadata
       )
       when map_size(metadata) == 4 and is_integer(k) and k > 0 and k <= @max_topk_k and
              is_integer(width) and width > 0 and is_integer(depth) and depth > 0 and
              depth <= @max_topk_counters,
       do: valid_path?(path) and width <= div(@max_topk_counters, depth)

  defp valid_topk_metadata?(_metadata), do: false

  defp valid_optional_path?(metadata) do
    case Map.fetch(metadata, :path) do
      :error -> true
      {:ok, path} -> valid_path?(path)
    end
  end

  defp valid_path?(path), do: is_binary(path) and byte_size(path) > 0

  defp registry_type(key, store) do
    if has_type_registry?(store) do
      case TypeRegistry.get_type(key, store) do
        {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
        "none" -> nil
        _type -> :other
      end
    end
  end

  defp has_get?(%FerricStore.Instance{}), do: true
  defp has_get?(%Ferricstore.Store.LocalTxStore{}), do: true
  defp has_get?(store) when is_map(store), do: Map.has_key?(store, :get)
  defp has_get?(_store), do: false

  defp has_type_registry?(%FerricStore.Instance{}), do: true
  defp has_type_registry?(%Ferricstore.Store.LocalTxStore{}), do: true

  defp has_type_registry?(store) when is_map(store) do
    Map.has_key?(store, :compound_get) and Map.has_key?(store, :get)
  end

  defp has_type_registry?(_store), do: false
end
