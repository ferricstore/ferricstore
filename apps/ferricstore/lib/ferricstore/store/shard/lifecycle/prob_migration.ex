defmodule Ferricstore.Store.Shard.Lifecycle.ProbMigration do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Shard.Lifecycle

  require Logger

  @spec migrate_prob_files(binary(), :ets.tid(), non_neg_integer(), term()) :: :ok
  @doc false
  def migrate_prob_files(shard_data_path, keydir, index, instance_ctx \\ nil) do
    prob_dir = Path.join(shard_data_path, "prob")

    case Ferricstore.FS.ls(prob_dir) do
      {:ok, files} ->
        migrated =
          Enum.reduce(files, 0, fn filename, count ->
            migrate_prob_file(prob_dir, filename, keydir, index, count, instance_ctx)
          end)

        if migrated > 0 do
          Logger.info("Shard: migrated #{migrated} existing prob file(s) to Raft metadata")
        end

      {:error, {:not_found, _}} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Shard #{index}: migrate_prob_files failed to list #{prob_dir}: #{inspect(reason)}"
        )

        raise "migrate_prob_files failed to list #{prob_dir}: #{inspect(reason)}"
    end
  end

  @spec migrate_prob_file(
          binary(),
          binary(),
          :ets.tid(),
          non_neg_integer(),
          non_neg_integer(),
          term()
        ) ::
          non_neg_integer()
  @doc false
  def migrate_prob_file(prob_dir, filename, keydir, shard_index, count, instance_ctx \\ nil) do
    path = Path.join(prob_dir, filename)

    cond do
      String.ends_with?(filename, ".bloom") ->
        key = filename |> String.trim_trailing(".bloom")
        migrate_if_missing(keydir, shard_index, key, path, :bloom_meta, count, instance_ctx)

      String.ends_with?(filename, ".cms") ->
        key = filename |> String.trim_trailing(".cms")
        migrate_if_missing(keydir, shard_index, key, path, :cms_meta, count, instance_ctx)

      String.ends_with?(filename, ".cuckoo") ->
        key = filename |> String.trim_trailing(".cuckoo")
        migrate_if_missing(keydir, shard_index, key, path, :cuckoo_meta, count, instance_ctx)

      String.ends_with?(filename, ".topk") ->
        key = filename |> String.trim_trailing(".topk")
        migrate_if_missing(keydir, shard_index, key, path, :topk_meta, count, instance_ctx)

      true ->
        count
    end
  end

  # Writes a metadata marker into ETS if the key doesn't already have one.
  # The key in the filename may be Base64-encoded (new) or sanitized (old).
  # We try to decode as Base64 first; if that fails, treat the filename
  # stem as the literal key.
  @spec migrate_if_missing(
          :ets.tid(),
          non_neg_integer(),
          binary(),
          binary(),
          atom(),
          non_neg_integer()
        ) :: non_neg_integer()
  @doc false
  def migrate_if_missing(
        keydir,
        shard_index,
        filename_key,
        path,
        type,
        count,
        instance_ctx \\ nil
      ) do
    key =
      case Base.url_decode64(filename_key, padding: false) do
        {:ok, decoded} -> decoded
        :error -> filename_key
      end

    case :ets.lookup(keydir, key) do
      [{^key, _val, _exp, _lfu, _fid, _off, _vsize}] ->
        # Already has an ETS entry — no migration needed
        count

      [] ->
        # No ETS entry — write a metadata marker
        meta = build_prob_meta(type, path, key)
        meta_bin = :erlang.term_to_binary(meta)
        Lifecycle.track_binary_add(shard_index, key, meta_bin, instance_ctx)
        :ets.insert(keydir, {key, meta_bin, 0, 0, 0, 0, byte_size(meta_bin)})
        count + 1
    end
  rescue
    ArgumentError -> count
  end

  @spec build_prob_meta(atom(), binary(), binary()) :: {atom(), map()}
  @doc false
  def build_prob_meta(:bloom_meta, path, _key) do
    # Try to read bloom header for capacity/error_rate derivation
    case NIF.bloom_file_info(path) do
      {:ok, {num_bits, _count, num_hashes}} ->
        capacity =
          if num_hashes > 0,
            do: max(1, round(num_bits * :math.log(2) / num_hashes)),
            else: 100

        error_rate =
          if capacity > 0,
            do: :math.exp(-num_bits * :math.pow(:math.log(2), 2) / capacity),
            else: 0.01

        {:bloom_meta,
         %{
           path: path,
           num_bits: num_bits,
           num_hashes: num_hashes,
           capacity: capacity,
           error_rate: error_rate
         }}

      _ ->
        {:bloom_meta, %{path: path}}
    end
  end

  def build_prob_meta(:cms_meta, path, _key) do
    case NIF.cms_file_info(path) do
      {:ok, {width, depth, _count}} ->
        {:cms_meta, %{width: width, depth: depth}}

      _ ->
        {:cms_meta, %{path: path}}
    end
  end

  def build_prob_meta(:cuckoo_meta, path, _key) do
    case NIF.cuckoo_file_info(path) do
      {:ok, {num_buckets, _bs, _fp, _ni, _nd, _ts, _mk}} ->
        {:cuckoo_meta, %{capacity: num_buckets}}

      _ ->
        {:cuckoo_meta, %{path: path}}
    end
  end

  def build_prob_meta(:topk_meta, path, _key) do
    case NIF.topk_file_info_v2(path) do
      {k, width, depth, decay} ->
        {:topk_meta, %{path: path, k: k, width: width, depth: depth, decay: decay}}

      _ ->
        {:topk_meta, %{path: path}}
    end
  end
end
