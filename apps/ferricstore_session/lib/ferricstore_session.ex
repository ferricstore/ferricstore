defmodule FerricStore.Session.Store do
  @moduledoc """
  A `Plug.Session.Store` adapter backed by FerricStore.

  Sessions are stored as serialized Erlang terms under keys with a configurable
  prefix (default `"session"`). Each session key has the form `"<prefix>:<sid>"`.

  Unlike the default cookie store, sessions are stored server-side with automatic
  TTL expiry. Unlike ETS-based stores, sessions survive application restarts
  because FerricStore persists all data to disk via Bitcask and WARaft segments.

  ## Options

    * `:prefix` - Key prefix for session entries. Defaults to `"session"`.
    * `:ttl` - Time-to-live in seconds. Defaults to `86_400` (24 hours).

  ## Usage

      # In your Phoenix endpoint or router:
      plug Plug.Session,
        store: FerricStore.Session.Store,
        key: "_my_app_key",
        ttl: 3600,
        prefix: "sess"

  ## Why Use This?

    * **Survives restarts** -- sessions persist on disk, so deploys don't log out users
    * **Shared across nodes** -- in a cluster, any node can read any session (Raft-replicated)
    * **Automatic expiry** -- TTL-based cleanup, no manual garbage collection
    * **No cookie size limit** -- store arbitrarily large session data (cookies are capped at 4KB)

  ## Installation

      # mix.exs
      {:ferricstore_session, "~> 0.1.0"}

  """

  @behaviour Plug.Session.Store

  @default_prefix "session"
  @default_ttl 86_400

  @type opts :: %{prefix: String.t(), ttl: pos_integer()}

  @impl true
  @spec init(Keyword.t()) :: opts()
  def init(opts) do
    %{
      prefix: Keyword.get(opts, :prefix, @default_prefix),
      ttl: Keyword.get(opts, :ttl, @default_ttl)
    }
  end

  @impl true
  @spec get(Plug.Conn.t(), binary() | nil, opts()) :: {binary() | nil, map()}
  def get(_conn, sid, opts) when is_binary(sid) and sid != "" do
    key = session_key(opts.prefix, sid)

    case FerricStore.get(key) do
      {:ok, nil} ->
        {nil, %{}}

      {:ok, data} ->
        case safe_deserialize(data) do
          {:ok, map} -> {sid, map}
          :error -> {nil, %{}}
        end
    end
  end

  def get(_conn, _sid, _opts), do: {nil, %{}}

  @impl true
  @spec put(Plug.Conn.t(), binary() | nil, map(), opts()) :: binary()
  def put(_conn, nil, data, opts) do
    sid = generate_sid()
    store_session(sid, data, opts)
    sid
  end

  def put(_conn, sid, data, opts) do
    store_session(sid, data, opts)
    sid
  end

  @impl true
  @spec delete(Plug.Conn.t(), binary(), opts()) :: :ok
  def delete(_conn, sid, opts) do
    key = session_key(opts.prefix, sid)
    FerricStore.del(key)
    :ok
  end

  defp store_session(sid, data, opts) do
    key = session_key(opts.prefix, sid)
    serialized = :erlang.term_to_binary(data)
    ttl_ms = opts.ttl * 1_000
    FerricStore.psetex(key, ttl_ms, serialized)
  end

  defp session_key(prefix, sid), do: "#{prefix}:#{sid}"

  defp generate_sid do
    :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)
  end

  defp safe_deserialize(data) do
    {:ok, :erlang.binary_to_term(data, [:safe])}
  rescue
    ArgumentError -> :error
  end
end
