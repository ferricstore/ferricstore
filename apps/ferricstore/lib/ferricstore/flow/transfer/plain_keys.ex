defmodule Ferricstore.Flow.Transfer.PlainKeys do
  @moduledoc false

  # The plain (non-Flow) keys of an export: strings, including the types stored
  # as strings (bitmaps, HyperLogLogs, t-digests), and hashes, lists, sets,
  # sorted sets and streams with every stored row (stream consumer groups,
  # pending entries and consumers included), each with its expiry. Collections
  # are read and written the way COPY does (Commands.CompoundSnapshot and
  # compound batch puts), so the target maintains its own indexes. Keys hold no
  # data-format digests, so they travel unchanged.
  #
  # Probabilistic structures (Bloom and cuckoo filters, count-min sketches,
  # top-k) live in per-shard sidecar files: their metadata and sidecar bytes
  # travel together and the target recreates the sidecar (a :prob_restore Raft
  # command).
  #
  # The invocation catalogs are hashes and sets under `f:{f}:invocation:`; the
  # governance catalogs are rebuilt by the Flow restore and are skipped. Any
  # other key in the Flow keyspace is refused.

  alias Ferricstore.Commands.CompoundSnapshot
  alias Ferricstore.{DataDir, ProbFile}
  alias Ferricstore.Store.{Ops, Router, TypeRegistry}

  @page 1_000
  @unsupported_samples 5
  @collections ~w(hash list set zset stream)
  @probabilistic %{"bloom" => :bloom, "cms" => :cms, "cuckoo" => :cuckoo, "topk" => :topk}
  # A sidecar travels in one Raft command; this is the hard value-size ceiling.
  @max_sidecar_bytes 512 * 1_024 * 1_024
  @invocation_prefix "f:{f}:invocation:"
  @governance_catalog_prefix "f:{flow-governance}:gov:catalog:"

  @spec scan(FerricStore.Instance.t()) ::
          {:ok, %{keys: [{binary(), binary()}], unsupported: [binary()]}} | {:error, term()}
  def scan(ctx), do: scan_pages(ctx, "0", %{keys: [], unsupported: []}, true)

  defp scan_pages(_ctx, "0", acc, false),
    do: {:ok, %{keys: Enum.reverse(acc.keys), unsupported: Enum.reverse(acc.unsupported)}}

  defp scan_pages(ctx, cursor, acc, _first?) do
    case Router.scan_keys_page(ctx, cursor, @page, nil, nil) do
      {:ok, {next, keys}} ->
        case Enum.reduce_while(keys, {:ok, acc}, &classify(ctx, &1, &2)) do
          {:ok, acc} -> scan_pages(ctx, next, acc, false)
          error -> error
        end

      {:error, reason} ->
        {:error, {:key_scan_failed, reason}}
    end
  end

  defp classify(_ctx, @governance_catalog_prefix <> _rest, {:ok, acc}), do: {:cont, {:ok, acc}}

  defp classify(ctx, key, {:ok, acc}) do
    flow_keyspace? =
      String.starts_with?(key, "f:{") and not String.starts_with?(key, @invocation_prefix)

    case TypeRegistry.get_type(key, ctx) do
      {:error, reason} ->
        {:halt, {:error, {:key_read_failed, key, reason}}}

      "none" ->
        {:cont, {:ok, acc}}

      type
      when not flow_keyspace? and
             (type == "string" or type in @collections or is_map_key(@probabilistic, type)) ->
        {:cont, {:ok, %{acc | keys: [{key, type} | acc.keys]}}}

      _unsupported ->
        {:cont, {:ok, unsupported(acc, key)}}
    end
  end

  defp unsupported(%{unsupported: keys} = acc, key) when length(keys) < @unsupported_samples,
    do: %{acc | unsupported: [key | keys]}

  defp unsupported(acc, _key), do: acc

  @spec refuse_unsupported(map()) :: :ok | {:error, {:unsupported_keys, [binary()]}}
  def refuse_unsupported(%{unsupported: []}), do: :ok
  def refuse_unsupported(%{unsupported: keys}), do: {:error, {:unsupported_keys, keys}}

  # Writes each key as frames through `write_frame` ((hash, term) -> {:ok, hash}):
  # a string as {:key, key, "string", value, expire_at_ms}, a collection as
  # {:key, key, type, entries} followed by {:key_more, key, type, entries} for the
  # rest of its rows, in the chunks `chunk` splits, and a probabilistic structure
  # as {:prob, key, type, metadata, sidecar_bytes, expire_at_ms}. Keys gone since
  # the scan are skipped.
  def write(ctx, keys, hash, write_frame, chunk) do
    Enum.reduce_while(keys, {:ok, hash, 0}, fn {key, type}, {:ok, hash, count} ->
      case write_key(ctx, key, type, hash, write_frame, chunk) do
        {:ok, hash, written} -> {:cont, {:ok, hash, count + written}}
        {:error, reason} -> {:halt, {:error, {:key_export_failed, key, reason}}}
      end
    end)
  end

  defp write_key(ctx, key, "string", hash, write_frame, _chunk) do
    case Ops.get_meta(ctx, key) do
      nil ->
        {:ok, hash, 0}

      {value, expire_at_ms} when is_binary(value) ->
        with {:ok, hash} <- write_frame.(hash, {:key, key, "string", value, expire_at_ms}),
             do: {:ok, hash, 1}

      {:error, _reason} = error ->
        error
    end
  end

  defp write_key(ctx, key, type, hash, write_frame, _chunk)
       when is_map_key(@probabilistic, type) do
    prob_type = Map.fetch!(@probabilistic, type)

    case Ops.get_meta(ctx, key) do
      nil ->
        {:ok, hash, 0}

      {metadata, expire_at_ms} when is_binary(metadata) ->
        with {:ok, bytes} <- read_sidecar(ctx, key, type),
             {:ok, hash} <-
               write_frame.(hash, {:prob, key, prob_type, metadata, bytes, expire_at_ms}),
             do: {:ok, hash, 1}

      {:error, _reason} = error ->
        error
    end
  end

  defp write_key(ctx, key, type, hash, write_frame, chunk) do
    case CompoundSnapshot.snapshot(key, type, ctx) do
      {:ok, []} ->
        {:ok, hash, 0}

      {:ok, entries} ->
        [first | rest] = chunk.(entries)

        with {:ok, hash} <- write_frame.(hash, {:key, key, type, first}),
             {:ok, hash} <- write_rest(hash, key, type, rest, write_frame),
             do: {:ok, hash, 1}

      {:error, _reason} = error ->
        error
    end
  end

  defp write_rest(hash, key, type, chunks, write_frame) do
    Enum.reduce_while(chunks, {:ok, hash}, fn entries, {:ok, hash} ->
      case write_frame.(hash, {:key_more, key, type, entries}) do
        {:ok, hash} -> {:cont, {:ok, hash}}
        error -> {:halt, error}
      end
    end)
  end

  @spec restore(FerricStore.Instance.t(), tuple()) :: :ok | {:error, term()}
  def restore(ctx, {:key, key, "string", value, expire_at_ms}) do
    case Ops.put(ctx, key, value, expire_at_ms) do
      :ok -> :ok
      error -> {:error, {:key_restore_failed, key, error}}
    end
  end

  def restore(ctx, {kind, key, type, entries})
      when kind in [:key, :key_more] and type in @collections and is_list(entries) do
    case Ops.compound_batch_put(ctx, key, entries) do
      :ok -> :ok
      error -> {:error, {:key_restore_failed, key, error}}
    end
  end

  def restore(ctx, {:prob, key, type, metadata, bytes, expire_at_ms}) do
    case Router.prob_write(ctx, {:prob_restore, key, type, metadata, bytes, expire_at_ms}) do
      :ok -> :ok
      error -> {:error, {:key_restore_failed, key, error}}
    end
  end

  def restore(_ctx, frame), do: {:error, {:invalid_key_frame, frame}}

  # The sidecar of a quiesced store is complete on disk: mutations publish
  # through a rename at the apply boundary.
  defp read_sidecar(ctx, key, type) do
    ctx.data_dir
    |> DataDir.shard_data_path(Router.shard_for(ctx, key))
    |> Path.join("prob")
    |> ProbFile.path(key, type)
    |> Ferricstore.FS.read_nofollow(@max_sidecar_bytes)
    |> case do
      {:ok, bytes} -> {:ok, bytes}
      {:error, reason} -> {:error, {:prob_sidecar_read_failed, reason}}
    end
  end

  # A key the store holds, if any (pages of a scan may be empty).
  @spec first_key(FerricStore.Instance.t(), binary()) :: {:ok, binary() | nil} | {:error, term()}
  def first_key(ctx, cursor \\ "0") do
    case Router.scan_keys_page(ctx, cursor, 1, nil, nil) do
      {:ok, {_next, [key | _rest]}} -> {:ok, key}
      {:ok, {"0", []}} -> {:ok, nil}
      {:ok, {next, []}} -> first_key(ctx, next)
      {:error, reason} -> {:error, {:key_scan_failed, reason}}
    end
  end
end
