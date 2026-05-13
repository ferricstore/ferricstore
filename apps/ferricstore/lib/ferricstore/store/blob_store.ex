defmodule Ferricstore.Store.BlobStore do
  @moduledoc """
  Content-addressed side-channel blob storage for large values.

  This module is intentionally not wired into normal SET/Raft yet. It provides
  the correctness primitive needed for that later path: a large payload is
  stored once under `data_dir/blob/shard_N/`, while Bitcask/Raft can store the
  fixed-size `BlobRef`.
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.BlobRef

  @type reason :: term()

  @doc """
  Stores `payload` under its content-addressed path and returns the small ref.

  If the same complete blob already exists, this returns without rewriting it.
  That keeps fanout-style workloads from writing identical large payload bytes
  once per workflow/key.
  """
  @spec put(binary(), non_neg_integer(), binary()) :: {:ok, BlobRef.t()} | {:error, reason()}
  def put(data_dir, shard_index, payload)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_binary(payload) do
    ref = BlobRef.from_payload(payload)
    path = BlobRef.path(data_dir, shard_index, ref)

    case existing_complete?(path, byte_size(payload)) do
      true -> {:ok, ref}
      false -> write_atomic(path, payload, ref)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Reads and validates a blob by ref."
  @spec get(binary(), non_neg_integer(), BlobRef.t()) :: {:ok, binary()} | {:error, reason()}
  def get(data_dir, shard_index, %BlobRef{} = ref)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    path = BlobRef.path(data_dir, shard_index, ref)

    with {:ok, payload} <- File.read(path),
         :ok <- verify_size(ref, payload),
         :ok <- verify_checksum(ref, payload) do
      {:ok, payload}
    end
  end

  defp existing_complete?(path, expected_size) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: ^expected_size}} -> true
      {:ok, _other} -> false
      {:error, :enoent} -> false
      {:error, reason} -> {:error, reason}
    end
  end

  defp write_atomic(path, payload, ref) do
    dir = Path.dirname(path)
    tmp_path = tmp_path(path)

    result =
      with :ok <- Ferricstore.FS.mkdir_p(dir),
           :ok <- fsync_parent_after_mkdir(dir),
           :ok <- File.write(tmp_path, payload, [:binary]),
           :ok <- fsync_file(tmp_path),
           :ok <- Ferricstore.FS.rename(tmp_path, path),
           :ok <- fsync_dir(dir) do
        {:ok, ref}
      end

    case result do
      {:ok, ^ref} = ok ->
        ok

      {:error, _reason} = error ->
        cleanup_tmp(tmp_path)
        error
    end
  end

  defp tmp_path(path) do
    basename = Path.basename(path)
    suffix = System.unique_integer([:positive, :monotonic])
    Path.join(Path.dirname(path), ".#{basename}.#{suffix}.tmp")
  end

  defp fsync_parent_after_mkdir(dir) do
    # `mkdir_p` is idempotent. Fsyncing the parent each time is conservative
    # and keeps the first write durable even when the shard prefix dir is new.
    fsync_dir(Path.dirname(dir))
  end

  defp fsync_file(path), do: normalize_fsync(NIF.v2_fsync(path))
  defp fsync_dir(path), do: normalize_fsync(NIF.v2_fsync_dir(path))

  defp normalize_fsync(:ok), do: :ok
  defp normalize_fsync({:error, reason}), do: {:error, reason}

  defp verify_size(%BlobRef{size: size}, payload) do
    if byte_size(payload) == size do
      :ok
    else
      {:error, :size_mismatch}
    end
  end

  defp verify_checksum(%BlobRef{} = ref, payload) do
    if BlobRef.verify_payload?(ref, payload) do
      :ok
    else
      {:error, :checksum_mismatch}
    end
  end

  defp cleanup_tmp(tmp_path) do
    case Ferricstore.FS.rm(tmp_path) do
      :ok -> :ok
      {:error, {:not_found, _message}} -> :ok
      {:error, _reason} -> :ok
    end
  end
end
