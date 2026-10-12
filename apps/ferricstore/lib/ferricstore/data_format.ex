defmodule Ferricstore.DataFormat do
  @moduledoc """
  The on-disk format of a data directory, recorded in its `data_format` file.

    * Format 1 - Flow keys carry full 32-byte SHA-256 digests (43 characters).
      Data directories created before data formats existed are format 1.
    * Format 2 - Flow keys carry the first 16 bytes (22 characters).

  A node runs in its data directory's format (see `Ferricstore.Flow.Digest`), so
  format 1 directories keep working unchanged; `FerricStore.Upgrade` converts
  one to format 2. Every member of a cluster must use the same format.
  """

  alias Ferricstore.Flow.Digest
  alias Ferricstore.ReplicationMode

  @marker "data_format"
  @latest 2
  @max_marker_bytes 16
  @probe_timeout_ms 5_000

  @type format :: 1 | 2

  @spec latest() :: format()
  def latest, do: @latest

  @doc "The format this node runs in."
  @spec current() :: format()
  def current, do: Digest.format()

  @doc false
  @spec put_current(format()) :: :ok
  def put_current(format), do: Digest.put_format(format)

  @spec marker_path(binary()) :: binary()
  def marker_path(data_dir), do: Path.join(data_dir, @marker)

  @doc """
  Returns the data directory's format, recording it on first boot. A directory
  that booted before formats existed (it has a cluster_state marker) is format 1;
  a new one takes the `:data_format` setting, default #{@latest}.
  """
  @spec resolve!(binary()) :: format()
  def resolve!(data_dir), do: resolve!(data_dir, &configured_format/0)

  @doc false
  # Flow keys are derived with the node-wide format, so another instance's data
  # dir must be in it; a new one takes it.
  @spec ensure_node_format!(binary()) :: :ok
  def ensure_node_format!(data_dir) do
    current = current()

    case resolve!(data_dir, fn -> current end) do
      ^current ->
        :ok

      other ->
        raise "data dir #{data_dir} is data_format #{other} but this node runs data_format #{current}; use FerricStore.Upgrade to convert it"
    end
  end

  defp resolve!(data_dir, new_dir_format) do
    case read(data_dir) do
      {:ok, format} ->
        check_configured!(format)
        format

      :missing ->
        format = if booted_before?(data_dir), do: 1, else: new_dir_format.()
        write!(data_dir, format)
        format

      {:error, reason} ->
        raise "unreadable data_format marker in #{data_dir}: #{inspect(reason)}"
    end
  end

  @doc false
  # A joining node writes keys in its own format, so it must match ours. A
  # release without data formats writes format 1 keys.
  @spec validate_join_target(node(), (node() -> {:ok, term()} | {:error, term()}) | nil) ::
          :ok | {:error, term()}
  def validate_join_target(target_node, probe \\ nil) do
    current = current()

    case target_format(target_node, probe) do
      {:ok, ^current} -> :ok
      {:ok, other} -> {:error, {:data_format_mismatch, target_node, other, current}}
      {:error, reason} -> {:error, {:data_format_probe_failed, target_node, reason}}
    end
  end

  defp target_format(target_node, nil), do: normalize_probe(remote_format(target_node))
  defp target_format(target_node, probe), do: normalize_probe(probe.(target_node))

  defp normalize_probe({:ok, format}) when format in [1, 2], do: {:ok, format}
  defp normalize_probe({:error, {:exception, :undef, _stack}}), do: {:ok, 1}
  defp normalize_probe({:error, _reason} = error), do: error
  defp normalize_probe(other), do: {:error, {:invalid_data_format_probe, other}}

  defp remote_format(target_node) do
    {:ok, :erpc.call(target_node, __MODULE__, :current, [], @probe_timeout_ms)}
  catch
    :error, {:exception, _reason, _stack} = exception -> {:error, exception}
    :error, {:erpc, reason} -> {:error, reason}
    kind, reason -> {:error, {kind, reason}}
  end

  defp read(data_dir) do
    case Ferricstore.FS.read_nofollow(marker_path(data_dir), @max_marker_bytes) do
      {:ok, "1\n"} -> {:ok, 1}
      {:ok, "2\n"} -> {:ok, 2}
      {:ok, other} -> {:error, {:invalid_data_format, other}}
      {:error, {:not_found, _reason}} -> :missing
      {:error, reason} -> {:error, reason}
    end
  end

  defp write!(data_dir, format) do
    Ferricstore.FS.mkdir_p!(data_dir)

    case Ferricstore.FS.atomic_replace_nofollow(
           marker_path(data_dir),
           Integer.to_string(format) <> "\n",
           @max_marker_bytes
         ) do
      :ok ->
        :ok

      {:error, reason} ->
        raise "cannot write data_format marker in #{data_dir}: #{inspect(reason)}"
    end
  end

  defp booted_before?(data_dir) do
    match?({:ok, _state}, ReplicationMode.read(data_dir)) or
      File.exists?(ReplicationMode.marker_path(data_dir))
  end

  defp configured_format do
    case Application.get_env(:ferricstore, :data_format, @latest) do
      format when format in [1, 2] -> format
      other -> raise "invalid :data_format setting #{inspect(other)}; expected 1 or 2"
    end
  end

  # The setting only chooses a new directory's format; contradicting a recorded
  # one is a misconfiguration, not a request to convert.
  defp check_configured!(format) do
    case Application.get_env(:ferricstore, :data_format) do
      nil ->
        :ok

      ^format ->
        :ok

      other ->
        raise "data_format setting #{inspect(other)} does not match the data dir's format #{format}; use FerricStore.Upgrade to convert"
    end
  end
end
