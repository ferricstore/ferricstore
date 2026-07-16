defmodule Ferricstore.ReplicationMode do
  @moduledoc false

  alias Ferricstore.TermCodec

  @state_file "cluster_state.term"
  @pt_key {__MODULE__, :current}
  @max_marker_bytes 1_048_576

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

    case Ferricstore.FS.read_nofollow(path, @max_marker_bytes) do
      {:ok, binary} -> decode(binary)
      {:error, {:not_found, _reason}} -> {:error, :enoent}
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
      |> normalize_marker_data()

    payload = TermCodec.encode({:ferricstore_cluster_state, data})
    checksum = :crypto.hash(:sha256, payload)
    encoded = TermCodec.encode({:ferricstore_cluster_state_v1, payload, checksum})

    if byte_size(encoded) > @max_marker_bytes do
      raise ArgumentError, "cluster_state marker exceeds #{@max_marker_bytes} bytes"
    end

    path = marker_path(data_dir)
    tmp_path = unique_marker_tmp_path(path)

    :ok =
      File.open!(tmp_path, [:write, :binary], fn io ->
        :ok = IO.binwrite(io, encoded)
        :ok = :file.sync(io)
      end)

    Ferricstore.FS.rename!(tmp_path, path)
    fsync_dir!(data_dir)
    :ok
  end

  defp unique_marker_tmp_path(path) do
    suffix =
      [node(), self(), System.unique_integer([:positive, :monotonic])]
      |> :erlang.term_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    path <> "." <> suffix <> ".tmp"
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
    case TermCodec.decode(binary) do
      {:ok, {:ferricstore_cluster_state_v1, payload, checksum}}
      when is_binary(payload) and is_binary(checksum) and byte_size(checksum) == 32 ->
        if :crypto.hash(:sha256, payload) == checksum do
          case TermCodec.decode(payload) do
            {:ok, {:ferricstore_cluster_state, data}} when is_map(data) -> {:ok, data}
            _invalid -> {:error, :invalid_payload}
          end
        else
          {:error, :checksum_mismatch}
        end

      _invalid ->
        {:error, :invalid_marker}
    end
  rescue
    error -> {:error, {:decode_failed, error}}
  end

  defp cluster_id do
    Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end

  defp normalize_marker_data(%{node: node_name} = data) when is_atom(node_name) do
    Map.put(data, :node, Atom.to_string(node_name))
  end

  defp normalize_marker_data(data), do: data

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
