defmodule Ferricstore.Flow.Transfer.Retag do
  @moduledoc false

  # Rewrites the Flow keys a run stores inside its own data (value refs in the
  # record and history, link rows, signal rows) from one data format to another.
  # Those keys carry a format-dependent digest only in their partition tag, so
  # format 1 -> 2 replaces `{f:<43 chars>}` with `{f:<22 chars>}`
  # (Ferricstore.Flow.Digest.convert_legacy/1); `{f}` and `{fa:N}` keys read the
  # same in both formats. A 16-byte digest cannot regrow, so 2 -> 1 is refused.

  alias Ferricstore.Flow.Digest

  @ref_fields [:payload_ref, :result_ref, :error_ref]

  @spec key(binary(), 1 | 2, 1 | 2) :: {:ok, binary()} | {:error, term()}
  def key(key, format, format) when is_binary(key), do: {:ok, key}

  def key("f:{f:" <> rest = key, 1, 2) do
    with [digest, tail] <- :binary.split(rest, "}"),
         {:ok, short} <- Digest.convert_legacy(digest) do
      {:ok, "f:{f:" <> short <> "}" <> tail}
    else
      _invalid -> {:error, {:not_a_format_1_key, key}}
    end
  end

  def key(key, 1, 2) when is_binary(key), do: {:ok, key}
  def key(key, 2, 1) when is_binary(key), do: {:error, {:cannot_restore_into_format_1, key}}

  @doc false
  # Governance keys carry the format's digest in their partition tag, and limit
  # storage keys also in the scope digest after `limit-storage:` and, for a
  # reservation, in the trailing digest of its id.
  @spec governance_key(binary(), 1 | 2, 1 | 2) :: {:ok, binary()} | {:error, term()}
  def governance_key(key, format, format) when is_binary(key), do: {:ok, key}

  def governance_key(key, from, to) when is_binary(key) do
    with {:ok, tagged} <- key(key, from, to) do
      case :binary.split(tagged, "}:gov:limit-storage:") do
        [tag, storage] -> limit_storage_key(tag <> "}:gov:limit-storage:", storage, key)
        [_other] -> {:ok, tagged}
      end
    end
  end

  defp limit_storage_key(prefix, storage, source_key) do
    with [scope_digest, rest] <- :binary.split(storage, ":"),
         {:ok, scope_digest} <- Digest.convert_legacy(scope_digest),
         {:ok, rest} <- limit_storage_rest(rest) do
      {:ok, prefix <> scope_digest <> ":" <> rest}
    else
      _invalid -> {:error, {:not_a_format_1_key, source_key}}
    end
  end

  defp limit_storage_rest("reservation:" <> rest) do
    with [shard_id, epoch, id_digest] <- :binary.split(rest, ":", [:global]),
         {:ok, id_digest} <- Digest.convert_legacy(id_digest) do
      {:ok, "reservation:" <> shard_id <> ":" <> epoch <> ":" <> id_digest}
    end
  end

  defp limit_storage_rest(rest), do: {:ok, rest}

  @spec record(map(), 1 | 2, 1 | 2) :: {:ok, map()} | {:error, term()}
  def record(record, format, format) when is_map(record), do: {:ok, record}

  def record(record, from, to) when is_map(record) do
    with {:ok, record} <- retag_fields(record, @ref_fields, from, to),
         {:ok, refs} <- value_refs(Map.get(record, :value_refs), from, to) do
      {:ok, if(is_nil(refs), do: record, else: Map.put(record, :value_refs, refs))}
    end
  end

  @spec history_snapshot(tuple(), 1 | 2, 1 | 2) :: {:ok, tuple()} | {:error, term()}
  def history_snapshot(snapshot, format, format) when is_tuple(snapshot), do: {:ok, snapshot}

  # Positions in Codec.encode_history_snapshot/1 order.
  @payload_ref 14
  @result_ref 18
  @error_ref 19
  @meta 21

  def history_snapshot(snapshot, from, to) when tuple_size(snapshot) == 22 do
    Enum.reduce_while([@payload_ref, @result_ref, @error_ref], {:ok, snapshot}, fn index,
                                                                                   {:ok, acc} ->
      case optional_key(elem(acc, index), from, to) do
        {:ok, value} -> {:cont, {:ok, put_elem(acc, index, value)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc} ->
        with {:ok, meta} <- history_meta(elem(acc, @meta), from, to),
             do: {:ok, put_elem(acc, @meta, meta)}

      error ->
        error
    end
  end

  def history_snapshot(snapshot, _from, _to), do: {:error, {:invalid_history_snapshot, snapshot}}

  defp history_meta(pairs, from, to) when is_list(pairs) do
    Enum.reduce_while(pairs, {:ok, []}, fn {name, value}, {:ok, acc} ->
      case history_meta_value(name, value, from, to) do
        {:ok, value} -> {:cont, {:ok, [{name, value} | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, pairs} -> {:ok, Enum.reverse(pairs)}
      error -> error
    end
  end

  # Named-value refs are stored as the JSON Codec.Support.encode_value_refs/1
  # writes; any other meta value that is exactly a run-scoped key is a ref too.
  defp history_meta_value("value_refs", json, from, to) do
    with {:ok, refs} when is_map(refs) <- Jason.decode(json),
         {:ok, refs} <- json_value_refs(refs, from, to) do
      {:ok, Jason.encode!(refs)}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, {:invalid_history_value_refs, json}}
    end
  end

  defp history_meta_value(_name, "f:{f:" <> _rest = value, from, to), do: key(value, from, to)
  defp history_meta_value(_name, value, _from, _to), do: {:ok, value}

  defp json_value_refs(refs, from, to) do
    Enum.reduce_while(refs, {:ok, %{}}, fn
      {name, %{"ref" => ref} = entry}, {:ok, acc} when is_binary(ref) ->
        case key(ref, from, to) do
          {:ok, ref} -> {:cont, {:ok, Map.put(acc, name, %{entry | "ref" => ref})}}
          {:error, _reason} = error -> {:halt, error}
        end

      {name, entry}, {:ok, acc} ->
        {:cont, {:ok, Map.put(acc, name, entry)}}
    end)
  end

  defp value_refs(nil, _from, _to), do: {:ok, nil}

  defp value_refs(refs, from, to) when is_map(refs) do
    Enum.reduce_while(refs, {:ok, %{}}, fn
      {name, %{ref: ref} = entry}, {:ok, acc} when is_binary(ref) ->
        case key(ref, from, to) do
          {:ok, ref} -> {:cont, {:ok, Map.put(acc, name, %{entry | ref: ref})}}
          {:error, _reason} = error -> {:halt, error}
        end

      {name, entry}, {:ok, acc} ->
        {:cont, {:ok, Map.put(acc, name, entry)}}
    end)
  end

  defp value_refs(refs, _from, _to), do: {:error, {:invalid_value_refs, refs}}

  defp retag_fields(record, fields, from, to) do
    Enum.reduce_while(fields, {:ok, record}, fn field, {:ok, acc} ->
      case optional_key(Map.get(acc, field), from, to) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, value} -> {:cont, {:ok, Map.put(acc, field, value)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp optional_key(nil, _from, _to), do: {:ok, nil}
  defp optional_key(value, from, to) when is_binary(value), do: key(value, from, to)
  defp optional_key(value, _from, _to), do: {:error, {:invalid_ref, value}}
end
