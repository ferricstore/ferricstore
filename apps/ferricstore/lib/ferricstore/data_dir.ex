defmodule Ferricstore.DataDir do
  @moduledoc """
  Manages the FerricStore on-disk directory layout (spec section 2B.4).

  The canonical directory structure under `data_dir` is:

  ```
  data_dir/
    data/shard_0/ ... shard_N/      (shared Bitcask per shard)
    dedicated/shard_0/ ... shard_N/ (for future promoted collections)
    prob/shard_0/ ... shard_N/      (probabilistic structure files)
    raft/shard_0/ ... shard_N/      (Raft WAL - managed by ra)
    registry/                        (merge scheduler state)
    hints/                           (hot cache warm-up files)
  ```

  ## Backward compatibility

  Before this layout was introduced, shard data was stored directly under
  `data_dir/shard_N/`. For backward compatibility, `shard_data_path/2` checks
  whether the legacy `data_dir/shard_N/` directory already exists. If it does,
  that path is returned so that existing data is not orphaned. For fresh
  installs (where no legacy directory exists), the new canonical path
  `data_dir/data/shard_N/` is used.

  ## Usage

  Call `ensure_layout!/2` during application startup to create all placeholder
  directories. Then use `shard_data_path/2` wherever a per-shard Bitcask path
  is needed.
  """

  require Logger

  @top_level_dirs ~w(data dedicated prob raft registry hints)

  @doc """
  Creates the full directory layout under `data_dir`.

  For each of `data/`, `dedicated/`, `prob/`, and `raft/`,
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
  def ensure_layout!(data_dir, shard_count \\ 4) do
    Ferricstore.FS.mkdir_p!(data_dir)

    # Directories that get per-shard subdirectories
    sharded_dirs = ~w(data dedicated prob raft)

    for dir <- sharded_dirs, i <- 0..(shard_count - 1) do
      Ferricstore.FS.mkdir_p!(Path.join([data_dir, dir, "shard_#{i}"]))
    end

    # Top-level-only directories (no per-shard subdivision)
    for dir <- ~w(registry hints) do
      Ferricstore.FS.mkdir_p!(Path.join(data_dir, dir))
    end

    Logger.debug("DataDir layout ensured under #{data_dir} (#{shard_count} shards)")

    :ok
  end

  @doc """
  Returns the Bitcask data path for a given shard index, respecting backward
  compatibility.

  If the legacy path `data_dir/shard_N/` already exists on disk, it is
  returned to avoid orphaning existing data. Otherwise the canonical path
  `data_dir/data/shard_N/` is returned.

  ## Parameters

    * `data_dir` -- the root data directory
    * `shard_index` -- zero-based shard index

  ## Examples

      # Legacy directory exists on disk:
      iex> Ferricstore.DataDir.shard_data_path("/tmp/fs", 0)
      "/tmp/fs/shard_0"

      # Fresh install (no legacy directory):
      iex> Ferricstore.DataDir.shard_data_path("/tmp/fs_new", 0)
      "/tmp/fs_new/data/shard_0"

  """
  @spec shard_data_path(binary(), non_neg_integer()) :: binary()
  def shard_data_path(data_dir, shard_index) do
    legacy_path = Path.join(data_dir, "shard_#{shard_index}")

    if Ferricstore.FS.dir?(legacy_path) do
      legacy_path
    else
      Path.join([data_dir, "data", "shard_#{shard_index}"])
    end
  end

  @doc """
  Returns the FerricStore root data directory for a shard Bitcask path.

  Handles both supported layouts:

    * canonical: `root/data/shard_N` -> `root`
    * legacy: `root/shard_N` -> `root`
  """
  @spec root_from_shard_path(binary()) :: binary()
  def root_from_shard_path(shard_data_path) do
    parent = Path.dirname(shard_data_path)

    if Path.basename(parent) == "data" do
      Path.dirname(parent)
    else
      parent
    end
  end

  @doc """
  Returns the list of top-level subdirectory names that make up the layout.

  Useful for testing and introspection.
  """
  @spec top_level_dirs() :: [binary()]
  def top_level_dirs, do: @top_level_dirs
end
