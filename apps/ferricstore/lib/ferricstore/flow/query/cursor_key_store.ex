defmodule Ferricstore.Flow.Query.CursorKeyStore do
  @moduledoc false

  use GenServer

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.FS

  @key_bytes 32
  @key_filename "flow-query-cursor.key"
  @call_timeout 1_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    ctx = Keyword.get(opts, :instance_ctx)

    case validate_context(ctx) do
      :ok ->
        GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, server_name(ctx)))

      {:error, _reason} = error ->
        error
    end
  end

  def start_link(_opts), do: {:error, :invalid_query_cursor_key_context}

  @spec server_name(map()) :: atom()
  def server_name(%{name: name}) when is_atom(name), do: :"#{name}.Flow.Query.CursorKeyStore"

  @spec default_path(map()) :: binary()
  def default_path(%{data_dir: data_dir}) when is_binary(data_dir) and data_dir != "" do
    Path.join([data_dir, "registry", @key_filename])
  end

  @spec key(map() | GenServer.server()) :: {:ok, <<_::256>>} | {:error, atom()}
  def key(%{name: _name} = ctx), do: key(server_name(ctx))

  def key(server) when is_atom(server) or is_pid(server) do
    try do
      GenServer.call(server, :key, @call_timeout)
    catch
      :exit, _reason -> {:error, :query_storage_unavailable}
    end
  end

  def key(_server), do: {:error, :query_storage_unavailable}

  @impl true
  def init(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)

    with :ok <- validate_context(ctx),
         {:ok, key, source} <- load_key(ctx, opts) do
      {:ok, %{key: key, source: source}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:key, _from, state), do: {:reply, {:ok, state.key}, state}

  defp load_key(ctx, opts) do
    case configured_key(ctx, opts) do
      :none -> load_or_create_file(default_path(ctx))
      {:ok, key} -> {:ok, key, :configured}
      {:error, _reason} = error -> error
    end
  end

  defp configured_key(ctx, opts) do
    configured =
      case Keyword.get(opts, :key) do
        nil -> configured_application_key(ctx.name)
        value -> value
      end

    decode_configured_key(configured)
  end

  defp configured_application_key(instance_name) do
    case Application.get_env(:ferricstore, :flow_query_cursor_key) do
      keys when is_map(keys) ->
        Map.get(keys, instance_name) || Map.get(keys, Atom.to_string(instance_name))

      key ->
        key
    end
  end

  defp decode_configured_key(nil), do: :none

  defp decode_configured_key({:raw, key}) when is_binary(key) and byte_size(key) == @key_bytes,
    do: {:ok, key}

  defp decode_configured_key(encoded) when is_binary(encoded) do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, key} when byte_size(key) == @key_bytes -> {:ok, key}
      _invalid -> {:error, :invalid_query_cursor_key}
    end
  end

  defp decode_configured_key(_configured), do: {:error, :invalid_query_cursor_key}

  defp load_or_create_file(path) do
    case load_existing_file(path) do
      {:error, :query_cursor_key_missing} -> create_key_file(path)
      result -> result
    end
  end

  defp load_existing_file(path) do
    case FS.read_private_nofollow(path, @key_bytes) do
      {:ok, key} when byte_size(key) == @key_bytes -> {:ok, key, :persisted}
      {:ok, _invalid_size} -> {:error, :invalid_query_cursor_key_file}
      {:error, {:not_found, _message}} -> {:error, :query_cursor_key_missing}
      {:error, {:insecure_permissions, _message}} -> {:error, :insecure_query_cursor_key_file}
      {:error, {:permission_denied, _message}} -> {:error, :query_cursor_key_unavailable}
      {:error, _reason} -> {:error, :invalid_query_cursor_key_file}
    end
  end

  defp create_key_file(path) do
    directory = Path.dirname(path)
    tmp_path = path <> ".tmp-#{System.unique_integer([:positive, :monotonic])}"
    key = :crypto.strong_rand_bytes(@key_bytes)

    with :ok <- ensure_directory(directory),
         :ok <- write_private_synced(tmp_path, key),
         {:ok, published_key, source} <- publish_key(tmp_path, path, directory, key) do
      {:ok, published_key, source}
    else
      {:error, _reason} ->
        FS.rm(tmp_path)
        {:error, :query_cursor_key_unavailable}
    end
  end

  defp publish_key(tmp_path, path, directory, key) do
    case File.ln(tmp_path, path) do
      :ok ->
        with :ok <- FS.rm(tmp_path),
             :ok <- fsync_dir(directory) do
          {:ok, key, :generated}
        end

      {:error, :eexist} ->
        with :ok <- FS.rm(tmp_path),
             :ok <- fsync_dir(directory) do
          load_existing_file(path)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_directory(directory) do
    existed? = FS.dir?(directory)

    with :ok <- FS.mkdir_p(directory) do
      maybe_fsync_parent(directory, existed?)
    end
  end

  defp maybe_fsync_parent(_directory, true), do: :ok
  defp maybe_fsync_parent(directory, false), do: fsync_dir(Path.dirname(directory))

  defp write_private_synced(path, key) do
    case File.open(path, [:write, :binary, :exclusive], fn io ->
           with :ok <- File.chmod(path, 0o600),
                :ok <- IO.binwrite(io, key) do
             :file.sync(io)
           end
         end) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fsync_dir(path) do
    case NIF.v2_fsync_dir(path) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      _invalid -> {:error, :fsync_failed}
    end
  end

  defp validate_context(%{name: name, data_dir: data_dir})
       when is_atom(name) and is_binary(data_dir) and data_dir != "",
       do: :ok

  defp validate_context(_ctx), do: {:error, :invalid_query_cursor_key_context}
end
