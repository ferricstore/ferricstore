defmodule Ferricstore.ReplicationMode do
  @moduledoc false

  @state_file "cluster_state.term"
  @pt_key {__MODULE__, :current}

  @type mode :: :raft

  @spec current() :: mode()
  def current do
    :persistent_term.get(@pt_key, :raft)
  end

  @spec put_current(mode()) :: :ok
  def put_current(:raft) do
    :persistent_term.put(@pt_key, :raft)
    :ok
  end

  @spec raft?() :: boolean()
  def raft?, do: current() == :raft

  @spec resolve!(binary(), pos_integer()) :: mode()
  def resolve!(data_dir, shard_count) do
    case read(data_dir) do
      {:ok, %{replication_mode: mode, shard_count: ^shard_count}}
      when mode in [:raft] ->
        mode

      {:ok, %{replication_mode: mode, shard_count: ^shard_count}} ->
        raise "unsupported cluster_state replication_mode=#{inspect(mode)}; standalone promotion mode was removed"

      {:ok, %{replication_mode: _mode, shard_count: other}} ->
        raise "cluster_state shard_count mismatch: marker=#{inspect(other)} runtime=#{shard_count}"

      {:error, :enoent} ->
        :raft

      {:error, reason} ->
        raise "failed to read cluster_state marker: #{inspect(reason)}"
    end
  end

  @spec marker_path(binary()) :: binary()
  def marker_path(data_dir), do: Path.join(data_dir, @state_file)

  @spec read(binary()) :: {:ok, map()} | {:error, term()}
  def read(data_dir) do
    path = marker_path(data_dir)

    case File.read(path) do
      {:ok, binary} -> decode(binary)
      {:error, :enoent} -> {:error, :enoent}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec write!(binary(), map()) :: :ok
  def write!(data_dir, attrs) when is_map(attrs) do
    Ferricstore.FS.mkdir_p!(data_dir)

    data =
      attrs
      |> Map.put_new(:version, 1)
      |> put_cluster_id(data_dir)
      |> Map.put_new(:node, node())
      |> Map.put_new(:updated_at_ms, System.system_time(:millisecond))

    payload = :erlang.term_to_binary({:ferricstore_cluster_state, data})
    checksum = :crypto.hash(:sha256, payload)
    encoded = :erlang.term_to_binary({:ferricstore_cluster_state_v1, payload, checksum})
    path = marker_path(data_dir)
    tmp_path = path <> ".tmp"

    :ok =
      File.open!(tmp_path, [:write, :binary], fn io ->
        :ok = IO.binwrite(io, encoded)
        :ok = :file.sync(io)
      end)

    File.rename!(tmp_path, path)
    fsync_dir!(data_dir)
    :ok
  end

  @spec mark_raft!(binary(), pos_integer(), non_neg_integer(), map()) :: :ok
  def mark_raft!(data_dir, shard_count, epoch, barrier_indices) do
    write!(data_dir, %{
      replication_mode: :raft,
      promotion_epoch: epoch,
      shard_count: shard_count,
      barrier_indices: barrier_indices
    })

    put_current(:raft)
  end

  defp decode(binary) do
    case :erlang.binary_to_term(binary, [:safe]) do
      {:ferricstore_cluster_state_v1, payload, checksum}
      when is_binary(payload) and is_binary(checksum) ->
        if :crypto.hash(:sha256, payload) == checksum do
          case :erlang.binary_to_term(payload, [:safe]) do
            {:ferricstore_cluster_state, data} when is_map(data) -> {:ok, data}
            other -> {:error, {:invalid_payload, other}}
          end
        else
          {:error, :checksum_mismatch}
        end

      other ->
        {:error, {:invalid_marker, other}}
    end
  rescue
    error -> {:error, {:decode_failed, error}}
  end

  defp cluster_id do
    Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end

  defp put_cluster_id(%{cluster_id: cluster_id} = data, _data_dir) when is_binary(cluster_id) do
    data
  end

  defp put_cluster_id(data, data_dir) do
    Map.put(data, :cluster_id, existing_cluster_id(data_dir) || cluster_id())
  end

  defp existing_cluster_id(data_dir) do
    case read(data_dir) do
      {:ok, %{cluster_id: cluster_id}} when is_binary(cluster_id) -> cluster_id
      {:ok, _} -> nil
      {:error, :enoent} -> nil
      {:error, reason} -> raise "failed to preserve existing cluster_id: #{inspect(reason)}"
      _ -> nil
    end
  end

  defp fsync_dir!(path) do
    case Ferricstore.Bitcask.NIF.v2_fsync_dir(path) do
      :ok -> :ok
      {:error, reason} -> raise "failed to fsync cluster_state directory: #{inspect(reason)}"
    end
  end
end
