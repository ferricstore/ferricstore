defmodule Ferricstore.DataDir do
  @moduledoc """
  Manages the FerricStore on-disk directory layout (spec section 2B.4).

  The canonical directory structure under `data_dir` is:

  ```
  data_dir/
    data/shard_0/ ... shard_N/      (shared Bitcask per shard)
    dedicated/shard_0/ ... shard_N/ (for future promoted collections)
    blob/shard_0/ ... shard_N/      (large-value side-channel blobs)
    prob/shard_0/ ... shard_N/      (probabilistic structure files)
    waraft/                         (WARaft consensus segment logs)
    registry/                        (merge scheduler state)
    hints/                           (hot cache warm-up files)
  ```

  ## Usage

  Call `ensure_layout!/2` during application startup to create all placeholder
  directories. Then use `shard_data_path/2` wherever a per-shard Bitcask path
  is needed.
  """

  alias Ferricstore.Bitcask.NIF

  require Logger

  @waraft_storage_root "ferricstore_waraft_backend"
  @top_level_dirs ~w(data dedicated blob prob waraft registry hints)
  @sharded_dirs ~w(data dedicated blob prob)

  @doc """
  Creates the full directory layout under `data_dir`.

  For each of `data/`, `dedicated/`, `blob/`, and `prob/`,
  per-shard subdirectories `shard_0` through `shard_{N-1}` are created.
  `registry/` and `hints/` are top-level directories without per-shard
  subdirectories.

  This function is idempotent -- calling it multiple times is safe.

  ## Parameters

    * `data_dir` -- the root data directory
    * `shard_count` -- number of shards (default: 4)

  ## Returns

    `:ok`

  ## Raises

    Raises on filesystem errors (e.g. permission denied).
  """
  @spec ensure_layout!(binary(), pos_integer()) :: :ok
  def ensure_layout!(data_dir, shard_count \\ 4)

  def ensure_layout!(data_dir, shard_count)
      when is_binary(data_dir) and is_integer(shard_count) and shard_count > 0 do
    created_dirs = maybe_create_dir([], data_dir, :create_root)

    created_dirs =
      Enum.reduce(@top_level_dirs, created_dirs, fn dir, acc ->
        maybe_create_dir(acc, Path.join(data_dir, dir), :create_top_level_dir)
      end)

    created_dirs =
      Enum.reduce(@sharded_dirs, created_dirs, fn dir, acc ->
        Enum.reduce(0..(shard_count - 1), acc, fn i, shard_acc ->
          maybe_create_dir(shard_acc, Path.join([data_dir, dir, "shard_#{i}"]), :create_shard_dir)
        end)
      end)

    created_dirs =
      Enum.reduce(1..shard_count, created_dirs, fn partition, acc ->
        root = Path.join([data_dir, "waraft", "#{@waraft_storage_root}.#{partition}"])

        acc
        |> maybe_create_dir(root, :create_waraft_partition_dir)
        |> maybe_create_dir(Path.join(root, "segment_log"), :create_waraft_segment_log_dir)
      end)

    fsync_created_dir_parents!(created_dirs)

    Logger.debug("DataDir layout ensured under #{data_dir} (#{shard_count} shards)")

    :ok
  end

  def ensure_layout!(_data_dir, _shard_count) do
    raise ArgumentError, "data_dir must be a binary and shard_count must be a positive integer"
  end

  defp maybe_create_dir(created_dirs, path, phase) do
    created? = not Ferricstore.FS.dir?(path)
    Ferricstore.FS.mkdir_p!(path)

    if created? do
      [{phase, path} | created_dirs]
    else
      created_dirs
    end
  end

  defp fsync_created_dir_parents!(created_dirs) do
    created_dirs
    |> Enum.reverse()
    |> unique_parent_fsyncs()
    |> Enum.each(fn {phase, created_path, parent_path} ->
      case fsync_dir(parent_path) do
        :ok ->
          :ok

        {:error, reason} ->
          raise "DataDir layout fsync failed during #{phase} for #{parent_path} " <>
                  "after creating #{created_path}: #{inspect(reason)}"
      end
    end)
  end

  defp unique_parent_fsyncs(created_dirs) do
    {fsyncs, _seen} =
      Enum.reduce(created_dirs, {[], MapSet.new()}, fn {phase, created_path}, {acc, seen} ->
        parent_path = Path.dirname(created_path)

        if MapSet.member?(seen, parent_path) do
          {acc, seen}
        else
          {[{phase, created_path, parent_path} | acc], MapSet.put(seen, parent_path)}
        end
      end)

    Enum.reverse(fsyncs)
  end

  defp fsync_dir(path) do
    case Process.get(:ferricstore_data_dir_fsync_dir_hook) do
      fun when is_function(fun, 1) -> fun.(path)
      _ -> NIF.v2_fsync_dir(path)
    end
  end

  @doc """
  Returns the canonical Bitcask data path for a given shard index.

  ## Parameters

    * `data_dir` -- the root data directory
    * `shard_index` -- zero-based shard index

  ## Examples

      iex> Ferricstore.DataDir.shard_data_path("/tmp/fs_new", 0)
      "/tmp/fs_new/data/shard_0"

  """
  @spec shard_data_path(binary(), non_neg_integer()) :: binary()
  def shard_data_path(data_dir, shard_index) do
    shard_path(data_dir, "data", shard_index)
  end

  @doc """
  Returns the canonical large-value blob path for a given shard index.

  This is intentionally outside the shared Bitcask shard directory. Large blob
  side-channel files must be copied and cleaned as shard-owned storage during
  cluster setup/join, but should not be scanned as normal Bitcask records.
  """
  @spec blob_shard_path(binary(), non_neg_integer()) :: binary()
  def blob_shard_path(data_dir, shard_index) do
    shard_path(data_dir, "blob", shard_index)
  end

  defp shard_path(data_dir, class, shard_index)
       when is_binary(data_dir) and is_binary(class) and is_integer(shard_index) and
              shard_index >= 0 do
    prefix = trim_trailing_slashes(data_dir)

    base =
      cond do
        prefix != "" -> prefix <> "/" <> class
        absolute_path?(data_dir) -> "/" <> class
        true -> class
      end

    base <> "/shard_" <> Integer.to_string(shard_index)
  end

  defp trim_trailing_slashes(path) do
    trim_trailing_slashes(path, byte_size(path))
  end

  defp trim_trailing_slashes(_path, 0), do: ""

  defp trim_trailing_slashes(path, size) do
    cond do
      :binary.at(path, size - 1) == ?/ ->
        trim_trailing_slashes(path, size - 1)

      size == byte_size(path) ->
        path

      true ->
        :binary.part(path, 0, size)
    end
  end

  defp absolute_path?(<<?/, _::binary>>), do: true
  defp absolute_path?(_path), do: false

  @doc """
  Returns the FerricStore root data directory for a shard Bitcask path.

  Accepts only the canonical layout:

    * `root/data/shard_N` -> `root`
  """
  @spec root_from_shard_path(binary()) :: binary()
  def root_from_shard_path(shard_data_path) do
    parent = Path.dirname(shard_data_path)
    basename = Path.basename(shard_data_path)

    if Path.basename(parent) == "data" and canonical_shard_basename?(basename) do
      Path.dirname(parent)
    else
      raise ArgumentError,
            "expected canonical shard data path root/data/shard_N, got: #{inspect(shard_data_path)}"
    end
  end

  defp canonical_shard_basename?("shard_" <> index) do
    case Integer.parse(index) do
      {value, ""} when value >= 0 -> Integer.to_string(value) == index
      _invalid -> false
    end
  end

  defp canonical_shard_basename?(_basename), do: false

  @doc """
  Returns the list of top-level subdirectory names that make up the layout.

  Useful for testing and introspection.
  """
  @spec top_level_dirs() :: [binary()]
  def top_level_dirs, do: @top_level_dirs
end
