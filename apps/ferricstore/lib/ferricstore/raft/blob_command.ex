defmodule Ferricstore.Raft.BlobCommand do
  @moduledoc """
  Prepares large-value Raft commands for blob side-channel replication.

  The ref-only command shapes are only safe for a single-member Raft group: the
  local apply side can validate that the blob record already exists before it
  stores the ref in Bitcask. Multi-member groups still need a blob transfer
  protocol before followers can apply refs without the original payload.
  """

  alias Ferricstore.Flow
  alias Ferricstore.Store.{BlobRef, BlobStore, BlobValue, CompoundKey}

  @flow_blob_value_ref_tag :ferricstore_flow_blob_value_ref
  @flow_blob_value_external :ferricstore_flow_blob_value_external
  @flow_value_fields [:payload, :result, :error]
  @flow_value_commands [
    :flow_create,
    :flow_create_many,
    :flow_create_pipeline_batch,
    :flow_complete,
    :flow_complete_many,
    :flow_transition,
    :flow_transition_many,
    :flow_retry,
    :flow_retry_many,
    :flow_fail,
    :flow_fail_many,
    :flow_cancel,
    :flow_cancel_many
  ]

  @type command ::
          {:put, binary(), binary(), non_neg_integer()}
          | {:set, binary(), binary(), non_neg_integer(), map()}
          | {:append, binary(), binary()}
          | {:getset, binary(), binary()}
          | {:setrange, binary(), non_neg_integer(), binary()}
          | {:cas, binary(), binary(), binary(), non_neg_integer() | nil}
          | {:locked_put, binary(), binary(), non_neg_integer(), term()}
          | {:compound_put, binary(), binary(), non_neg_integer()}
          | {:compound_batch_put, binary(), [{binary(), binary(), non_neg_integer()}]}
          | {:put_batch, [{binary(), binary(), non_neg_integer()}]}
          | term()

  @doc """
  Returns a command that can be submitted to Raft.

  When side-channel storage is disabled, or the Raft group has more than one
  member, commands are returned unchanged. In one-node mode, large values are
  written to the blob store first and Raft receives only the small encoded ref.
  """
  @spec prepare(map(), non_neg_integer(), command(), keyword()) ::
          {:ok, command()} | {:error, term()}
  def prepare(ctx, shard_index, command, opts \\ []) do
    threshold = BlobValue.threshold(ctx)

    cond do
      threshold <= 0 ->
        {:ok, command}

      not Keyword.get(opts, :single_member?, false) ->
        {:ok, command}

      true ->
        prepare_enabled(ctx, shard_index, threshold, command)
    end
  end

  @doc """
  Extracts the encoded blob ref from a Flow value marker.

  Flow payload/result/error values are encoded before they enter the blob store,
  so apply can store a ref to the encoded Flow value without re-encoding bytes.
  """
  @spec flow_blob_value_ref(term()) :: {:ok, binary()} | :error
  def flow_blob_value_ref({@flow_blob_value_ref_tag, encoded_ref}) when is_binary(encoded_ref),
    do: {:ok, encoded_ref}

  def flow_blob_value_ref(_value), do: :error

  @doc """
  Returns true when `command` contains a value that would use the blob
  side-channel if the Raft group is eligible.

  Batcher uses this as a cheap hot-path guard so enabling a large-value
  threshold does not force a Ra membership lookup for every tiny SET.
  """
  @spec side_channel_candidate?(map(), command()) :: boolean()
  def side_channel_candidate?(ctx, command) do
    threshold = BlobValue.threshold(ctx)
    threshold > 0 and command_candidate?(command, threshold)
  end

  defp prepare_enabled(%{data_dir: data_dir}, shard_index, threshold, {:put, key, value, exp})
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_binary(value) do
    case prepare_value(data_dir, shard_index, threshold, value) do
      {:ok, {^value, :value}} -> {:ok, {:put, key, value, exp}}
      {:ok, {encoded_ref, :blob_ref}} -> {:ok, {:put_blob_ref, key, encoded_ref, exp}}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_enabled(
         %{data_dir: data_dir},
         shard_index,
         threshold,
         {:set, key, value, exp, opts}
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_binary(value) and is_map(opts) do
    case prepare_value(data_dir, shard_index, threshold, value) do
      {:ok, {^value, :value}} -> {:ok, {:set, key, value, exp, opts}}
      {:ok, {encoded_ref, :blob_ref}} -> {:ok, {:set_blob_ref, key, encoded_ref, exp, opts}}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_enabled(%{data_dir: data_dir}, shard_index, threshold, {:getset, key, value})
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_binary(value) do
    case prepare_value(data_dir, shard_index, threshold, value) do
      {:ok, {^value, :value}} -> {:ok, {:getset, key, value}}
      {:ok, {encoded_ref, :blob_ref}} -> {:ok, {:getset_blob_ref, key, encoded_ref}}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_enabled(%{data_dir: data_dir}, shard_index, threshold, {:append, key, suffix})
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_binary(suffix) do
    case prepare_value(data_dir, shard_index, threshold, suffix) do
      {:ok, {^suffix, :value}} -> {:ok, {:append, key, suffix}}
      {:ok, {encoded_ref, :blob_ref}} -> {:ok, {:append_blob_ref, key, encoded_ref}}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_enabled(
         %{data_dir: data_dir},
         shard_index,
         threshold,
         {:setrange, key, offset, value}
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_integer(offset) and offset >= 0 and is_binary(value) do
    case prepare_value(data_dir, shard_index, threshold, value) do
      {:ok, {^value, :value}} -> {:ok, {:setrange, key, offset, value}}
      {:ok, {encoded_ref, :blob_ref}} -> {:ok, {:setrange_blob_ref, key, offset, encoded_ref}}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_enabled(
         %{data_dir: data_dir},
         shard_index,
         threshold,
         {:cas, key, expected, new_value, ttl_ms}
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_binary(expected) and is_binary(new_value) do
    case prepare_value(data_dir, shard_index, threshold, new_value) do
      {:ok, {^new_value, :value}} ->
        {:ok, {:cas, key, expected, new_value, ttl_ms}}

      {:ok, {encoded_ref, :blob_ref}} ->
        {:ok, {:cas_blob_ref, key, expected, encoded_ref, ttl_ms}}

      {:error, _reason} = error ->
        error
    end
  end

  defp prepare_enabled(
         %{data_dir: data_dir},
         shard_index,
         threshold,
         {:locked_put, key, value, expire_at_ms, owner_ref}
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_binary(value) do
    if locked_put_blob_side_channel_key?(key) do
      case prepare_value(data_dir, shard_index, threshold, value) do
        {:ok, {^value, :value}} ->
          {:ok, {:locked_put, key, value, expire_at_ms, owner_ref}}

        {:ok, {encoded_ref, :blob_ref}} ->
          {:ok, {:locked_put_blob_ref, key, encoded_ref, expire_at_ms, owner_ref}}

        {:error, _reason} = error ->
          error
      end
    else
      {:ok, {:locked_put, key, value, expire_at_ms, owner_ref}}
    end
  end

  defp prepare_enabled(
         %{data_dir: data_dir},
         shard_index,
         threshold,
         {:compound_put, compound_key, value, expire_at_ms}
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(compound_key) and is_binary(value) do
    if compound_blob_side_channel_key?(compound_key) do
      case prepare_value(data_dir, shard_index, threshold, value) do
        {:ok, {^value, :value}} ->
          {:ok, {:compound_put, compound_key, value, expire_at_ms}}

        {:ok, {encoded_ref, :blob_ref}} ->
          {:ok, {:compound_put_blob_ref, compound_key, encoded_ref, expire_at_ms}}

        {:error, _reason} = error ->
          error
      end
    else
      {:ok, {:compound_put, compound_key, value, expire_at_ms}}
    end
  end

  defp prepare_enabled(
         %{data_dir: data_dir},
         shard_index,
         threshold,
         {:compound_batch_put, redis_key, entries}
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(redis_key) and is_list(entries) do
    with {:ok, prepared, externalized?} <-
           prepare_compound_batch_entries(data_dir, shard_index, threshold, entries) do
      if externalized? do
        {:ok, {:compound_blob_batch_put, redis_key, prepared}}
      else
        {:ok, {:compound_batch_put, redis_key, entries}}
      end
    end
  end

  defp prepare_enabled(%{data_dir: data_dir}, shard_index, threshold, {:put_batch, entries})
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_list(entries) do
    with {:ok, prepared, externalized?} <-
           prepare_batch_entries(data_dir, shard_index, threshold, entries) do
      if externalized? do
        {:ok, {:put_blob_batch, prepared}}
      else
        {:ok, {:put_batch, entries}}
      end
    end
  end

  defp prepare_enabled(%{data_dir: data_dir}, shard_index, threshold, {:batch, commands})
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_list(commands) do
    with {:ok, prepared_commands} <-
           prepare_generic_batch_commands(data_dir, shard_index, threshold, commands) do
      {:ok, {:batch, prepared_commands}}
    end
  end

  defp prepare_enabled(
         %{data_dir: data_dir},
         shard_index,
         threshold,
         {:flow_terminal_pipeline_batch, op, key, attrs} = command
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_atom(op) and is_map(attrs) do
    case prepare_flow_attrs(data_dir, shard_index, threshold, attrs) do
      {:ok, prepared_attrs, true} ->
        {:ok, {:flow_terminal_pipeline_batch, op, key, prepared_attrs}}

      {:ok, _prepared_attrs, false} ->
        {:ok, command}

      {:error, _reason} = error ->
        error
    end
  end

  defp prepare_enabled(%{data_dir: data_dir}, shard_index, threshold, {command, key, attrs} = raw)
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_atom(command) and is_map(attrs) do
    if flow_value_command?(command) do
      case prepare_flow_attrs(data_dir, shard_index, threshold, attrs) do
        {:ok, prepared_attrs, true} ->
          {:ok, {command, key, prepared_attrs}}

        {:ok, _prepared_attrs, false} ->
          {:ok, raw}

        {:error, _reason} = error ->
          error
      end
    else
      {:ok, raw}
    end
  end

  defp prepare_enabled(_ctx, _shard_index, _threshold, command), do: {:ok, command}

  defp command_candidate?({:put, _key, value, _expire_at_ms}, threshold)
       when is_binary(value) do
    externalize?(value, threshold)
  end

  defp command_candidate?({:set, _key, value, _expire_at_ms, opts}, threshold)
       when is_binary(value) and is_map(opts) do
    externalize?(value, threshold)
  end

  defp command_candidate?({:getset, _key, value}, threshold) when is_binary(value) do
    externalize?(value, threshold)
  end

  defp command_candidate?({:append, _key, suffix}, threshold) when is_binary(suffix) do
    externalize?(suffix, threshold)
  end

  defp command_candidate?({:setrange, _key, offset, value}, threshold)
       when is_integer(offset) and offset >= 0 and is_binary(value) do
    externalize?(value, threshold)
  end

  defp command_candidate?({:cas, _key, expected, new_value, _ttl_ms}, threshold)
       when is_binary(expected) and is_binary(new_value) do
    externalize?(new_value, threshold)
  end

  defp command_candidate?({:locked_put, key, value, _expire_at_ms, _owner_ref}, threshold)
       when is_binary(key) and is_binary(value) do
    locked_put_blob_side_channel_key?(key) and externalize?(value, threshold)
  end

  defp command_candidate?({:compound_put, compound_key, value, _expire_at_ms}, threshold)
       when is_binary(compound_key) and is_binary(value) do
    compound_blob_side_channel_key?(compound_key) and externalize?(value, threshold)
  end

  defp command_candidate?({:compound_batch_put, _redis_key, entries}, threshold)
       when is_list(entries) do
    Enum.any?(entries, fn
      {compound_key, value, _expire_at_ms} when is_binary(compound_key) and is_binary(value) ->
        compound_blob_side_channel_key?(compound_key) and externalize?(value, threshold)

      _other ->
        false
    end)
  end

  defp command_candidate?({:put_batch, entries}, threshold) when is_list(entries) do
    Enum.any?(entries, fn
      {_key, value, _expire_at_ms} when is_binary(value) -> externalize?(value, threshold)
      _other -> false
    end)
  end

  defp command_candidate?({:batch, commands}, threshold) when is_list(commands) do
    Enum.any?(commands, &command_candidate?(&1, threshold))
  end

  defp command_candidate?({:flow_terminal_pipeline_batch, op, _key, attrs}, threshold)
       when is_atom(op) and is_map(attrs) do
    flow_attrs_candidate?(attrs, threshold)
  end

  defp command_candidate?({command, _key, attrs}, threshold)
       when is_atom(command) and is_map(attrs) do
    flow_value_command?(command) and flow_attrs_candidate?(attrs, threshold)
  end

  defp command_candidate?(_command, _threshold), do: false

  defp prepare_batch_entries(data_dir, shard_index, threshold, entries) do
    entries
    |> Enum.reduce_while({:ok, [], []}, fn
      {key, value, expire_at_ms}, {:ok, acc, external_payloads}
      when is_binary(key) and is_binary(value) ->
        if externalize?(value, threshold) do
          marker = {key, :external, expire_at_ms}
          {:cont, {:ok, [marker | acc], [value | external_payloads]}}
        else
          {:cont, {:ok, [{key, value, expire_at_ms, :value} | acc], external_payloads}}
        end

      _invalid, {:ok, _acc, _external_payloads} ->
        {:halt, {:error, :invalid_put_batch_entry}}
    end)
    |> case do
      {:ok, prepared, []} ->
        {:ok, Enum.reverse(prepared), false}

      {:ok, prepared, external_payloads} ->
        with {:ok, refs} <-
               BlobStore.put_many(data_dir, shard_index, Enum.reverse(external_payloads)) do
          {:ok, inflate_batch_blob_refs(Enum.reverse(prepared), refs), true}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp inflate_batch_blob_refs(prepared, refs) do
    {result, []} =
      Enum.map_reduce(prepared, refs, fn
        {key, :external, expire_at_ms}, [ref | rest] ->
          {{key, BlobRef.encode!(ref), expire_at_ms, :blob_ref}, rest}

        entry, refs ->
          {entry, refs}
      end)

    result
  end

  defp prepare_generic_batch_commands(data_dir, shard_index, threshold, commands) do
    Enum.reduce_while(commands, {:ok, [], []}, fn
      {:put, key, value, expire_at_ms}, {:ok, acc, external_payloads}
      when is_binary(key) and is_binary(value) ->
        if externalize?(value, threshold) do
          {:cont, {:ok, [{:put_external, key, expire_at_ms} | acc], [value | external_payloads]}}
        else
          {:cont, {:ok, [{:put, key, value, expire_at_ms} | acc], external_payloads}}
        end

      {:set, key, value, expire_at_ms, opts}, {:ok, acc, external_payloads}
      when is_binary(key) and is_binary(value) and is_map(opts) ->
        if externalize?(value, threshold) do
          marker = {:set_external, key, expire_at_ms, opts}
          {:cont, {:ok, [marker | acc], [value | external_payloads]}}
        else
          {:cont, {:ok, [{:set, key, value, expire_at_ms, opts} | acc], external_payloads}}
        end

      {:getset, key, value}, {:ok, acc, external_payloads}
      when is_binary(key) and is_binary(value) ->
        if externalize?(value, threshold) do
          {:cont, {:ok, [{:getset_external, key} | acc], [value | external_payloads]}}
        else
          {:cont, {:ok, [{:getset, key, value} | acc], external_payloads}}
        end

      {:append, key, suffix}, {:ok, acc, external_payloads}
      when is_binary(key) and is_binary(suffix) ->
        if externalize?(suffix, threshold) do
          {:cont, {:ok, [{:append_external, key} | acc], [suffix | external_payloads]}}
        else
          {:cont, {:ok, [{:append, key, suffix} | acc], external_payloads}}
        end

      {:setrange, key, offset, value}, {:ok, acc, external_payloads}
      when is_binary(key) and is_integer(offset) and offset >= 0 and is_binary(value) ->
        if externalize?(value, threshold) do
          marker = {:setrange_external, key, offset}
          {:cont, {:ok, [marker | acc], [value | external_payloads]}}
        else
          {:cont, {:ok, [{:setrange, key, offset, value} | acc], external_payloads}}
        end

      {:cas, key, expected, new_value, ttl_ms}, {:ok, acc, external_payloads}
      when is_binary(key) and is_binary(expected) and is_binary(new_value) ->
        if externalize?(new_value, threshold) do
          marker = {:cas_external, key, expected, ttl_ms}
          {:cont, {:ok, [marker | acc], [new_value | external_payloads]}}
        else
          {:cont, {:ok, [{:cas, key, expected, new_value, ttl_ms} | acc], external_payloads}}
        end

      {:locked_put, key, value, expire_at_ms, owner_ref}, {:ok, acc, external_payloads}
      when is_binary(key) and is_binary(value) ->
        if locked_put_blob_side_channel_key?(key) and externalize?(value, threshold) do
          marker = {:locked_put_external, key, expire_at_ms, owner_ref}
          {:cont, {:ok, [marker | acc], [value | external_payloads]}}
        else
          {:cont,
           {:ok, [{:locked_put, key, value, expire_at_ms, owner_ref} | acc], external_payloads}}
        end

      {:compound_put, compound_key, value, expire_at_ms}, {:ok, acc, external_payloads}
      when is_binary(compound_key) and is_binary(value) ->
        if compound_blob_side_channel_key?(compound_key) and externalize?(value, threshold) do
          marker = {:compound_put_external, compound_key, expire_at_ms}
          {:cont, {:ok, [marker | acc], [value | external_payloads]}}
        else
          {:cont,
           {:ok, [{:compound_put, compound_key, value, expire_at_ms} | acc], external_payloads}}
        end

      {:put_batch, entries}, {:ok, acc, external_payloads} when is_list(entries) ->
        case prepare_generic_put_batch_entries(entries, threshold, acc, external_payloads) do
          {:ok, acc, external_payloads} -> {:cont, {:ok, acc, external_payloads}}
          {:error, _reason} = error -> {:halt, error}
        end

      {:compound_batch_put, redis_key, entries}, {:ok, acc, external_payloads}
      when is_binary(redis_key) and is_list(entries) ->
        case prepare_generic_compound_batch_entries(
               redis_key,
               entries,
               threshold,
               acc,
               external_payloads
             ) do
          {:ok, acc, external_payloads} -> {:cont, {:ok, acc, external_payloads}}
          {:error, _reason} = error -> {:halt, error}
        end

      {:flow_terminal_pipeline_batch, op, key, attrs}, {:ok, acc, external_payloads}
      when is_atom(op) and is_map(attrs) ->
        case prepare_generic_flow_attrs(attrs, threshold, external_payloads) do
          {:ok, prepared_attrs, external_payloads} ->
            command = {:flow_terminal_pipeline_batch, op, key, prepared_attrs}
            {:cont, {:ok, [command | acc], external_payloads}}

          {:error, _reason} = error ->
            {:halt, error}
        end

      {command, key, attrs}, {:ok, acc, external_payloads}
      when is_atom(command) and is_map(attrs) ->
        if flow_value_command?(command) do
          case prepare_generic_flow_attrs(attrs, threshold, external_payloads) do
            {:ok, prepared_attrs, external_payloads} ->
              {:cont, {:ok, [{command, key, prepared_attrs} | acc], external_payloads}}

            {:error, _reason} = error ->
              {:halt, error}
          end
        else
          {:cont, {:ok, [{command, key, attrs} | acc], external_payloads}}
        end

      command, {:ok, acc, external_payloads} ->
        {:cont, {:ok, [command | acc], external_payloads}}
    end)
    |> case do
      {:ok, prepared, []} ->
        {:ok, Enum.reverse(prepared)}

      {:ok, prepared, external_payloads} ->
        with {:ok, refs} <-
               BlobStore.put_many(data_dir, shard_index, Enum.reverse(external_payloads)) do
          {:ok, inflate_generic_batch_blob_refs(Enum.reverse(prepared), refs)}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp prepare_generic_put_batch_entries(entries, threshold, acc, external_payloads) do
    Enum.reduce_while(entries, {:ok, acc, external_payloads}, fn
      {key, value, expire_at_ms}, {:ok, acc, external_payloads}
      when is_binary(key) and is_binary(value) ->
        if externalize?(value, threshold) do
          {:cont, {:ok, [{:put_external, key, expire_at_ms} | acc], [value | external_payloads]}}
        else
          {:cont, {:ok, [{:put, key, value, expire_at_ms} | acc], external_payloads}}
        end

      _invalid, {:ok, _acc, _external_payloads} ->
        {:halt, {:error, :invalid_put_batch_entry}}
    end)
  end

  defp prepare_compound_batch_entries(data_dir, shard_index, threshold, entries) do
    entries
    |> Enum.reduce_while({:ok, [], []}, fn
      {compound_key, value, expire_at_ms}, {:ok, acc, external_payloads}
      when is_binary(compound_key) and is_binary(value) ->
        if compound_blob_side_channel_key?(compound_key) and externalize?(value, threshold) do
          marker = {compound_key, :external, expire_at_ms}
          {:cont, {:ok, [marker | acc], [value | external_payloads]}}
        else
          {:cont, {:ok, [{compound_key, value, expire_at_ms, :value} | acc], external_payloads}}
        end

      _invalid, {:ok, _acc, _external_payloads} ->
        {:halt, {:error, :invalid_compound_batch_entry}}
    end)
    |> case do
      {:ok, prepared, []} ->
        {:ok, Enum.reverse(prepared), false}

      {:ok, prepared, external_payloads} ->
        with {:ok, refs} <-
               BlobStore.put_many(data_dir, shard_index, Enum.reverse(external_payloads)) do
          {:ok, inflate_compound_batch_blob_refs(Enum.reverse(prepared), refs), true}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp prepare_generic_compound_batch_entries(
         redis_key,
         entries,
         threshold,
         acc,
         external_payloads
       ) do
    entries
    |> Enum.reduce_while({:ok, [], external_payloads, false}, fn
      {compound_key, value, expire_at_ms}, {:ok, prepared, external_payloads, externalized?}
      when is_binary(compound_key) and is_binary(value) ->
        if compound_blob_side_channel_key?(compound_key) and externalize?(value, threshold) do
          marker = {compound_key, :external, expire_at_ms}
          {:cont, {:ok, [marker | prepared], [value | external_payloads], true}}
        else
          {:cont,
           {:ok, [{compound_key, value, expire_at_ms, :value} | prepared], external_payloads,
            externalized?}}
        end

      _invalid, {:ok, _prepared, _external_payloads, _externalized?} ->
        {:halt, {:error, :invalid_compound_batch_entry}}
    end)
    |> case do
      {:ok, prepared, external_payloads, true} ->
        {:ok, [{:compound_blob_batch_external, redis_key, Enum.reverse(prepared)} | acc],
         external_payloads}

      {:ok, _prepared, external_payloads, false} ->
        {:ok, [{:compound_batch_put, redis_key, entries} | acc], external_payloads}

      {:error, _reason} = error ->
        error
    end
  end

  defp inflate_generic_batch_blob_refs(prepared, refs) do
    {result, []} =
      Enum.map_reduce(prepared, refs, fn
        {:put_external, key, expire_at_ms}, [ref | rest] ->
          {{:put_blob_ref, key, BlobRef.encode!(ref), expire_at_ms}, rest}

        {:set_external, key, expire_at_ms, opts}, [ref | rest] ->
          {{:set_blob_ref, key, BlobRef.encode!(ref), expire_at_ms, opts}, rest}

        {:getset_external, key}, [ref | rest] ->
          {{:getset_blob_ref, key, BlobRef.encode!(ref)}, rest}

        {:append_external, key}, [ref | rest] ->
          {{:append_blob_ref, key, BlobRef.encode!(ref)}, rest}

        {:setrange_external, key, offset}, [ref | rest] ->
          {{:setrange_blob_ref, key, offset, BlobRef.encode!(ref)}, rest}

        {:cas_external, key, expected, ttl_ms}, [ref | rest] ->
          {{:cas_blob_ref, key, expected, BlobRef.encode!(ref), ttl_ms}, rest}

        {:locked_put_external, key, expire_at_ms, owner_ref}, [ref | rest] ->
          {{:locked_put_blob_ref, key, BlobRef.encode!(ref), expire_at_ms, owner_ref}, rest}

        {:compound_put_external, compound_key, expire_at_ms}, [ref | rest] ->
          {{:compound_put_blob_ref, compound_key, BlobRef.encode!(ref), expire_at_ms}, rest}

        {:compound_blob_batch_external, redis_key, entries}, refs ->
          {inflated_entries, refs} = inflate_compound_batch_blob_refs_with_rest(entries, refs)
          {{:compound_blob_batch_put, redis_key, inflated_entries}, refs}

        {:flow_terminal_pipeline_batch, op, key, attrs}, refs ->
          {inflated_attrs, refs} = inflate_flow_attrs_with_rest(attrs, refs)
          {{:flow_terminal_pipeline_batch, op, key, inflated_attrs}, refs}

        {command, key, attrs}, refs when is_atom(command) and is_map(attrs) ->
          if flow_value_command?(command) do
            {inflated_attrs, refs} = inflate_flow_attrs_with_rest(attrs, refs)
            {{command, key, inflated_attrs}, refs}
          else
            {{command, key, attrs}, refs}
          end

        command, refs ->
          {command, refs}
      end)

    result
  end

  defp inflate_compound_batch_blob_refs(prepared, refs) do
    {result, []} = inflate_compound_batch_blob_refs_with_rest(prepared, refs)
    result
  end

  defp inflate_compound_batch_blob_refs_with_rest(prepared, refs) do
    Enum.map_reduce(prepared, refs, fn
      {compound_key, :external, expire_at_ms}, [ref | rest] ->
        {{compound_key, BlobRef.encode!(ref), expire_at_ms, :blob_ref}, rest}

      entry, refs ->
        {entry, refs}
    end)
  end

  defp prepare_value(data_dir, shard_index, threshold, value) do
    if externalize?(value, threshold) do
      with {:ok, ref} <- BlobStore.put(data_dir, shard_index, value) do
        {:ok, {BlobRef.encode!(ref), :blob_ref}}
      end
    else
      {:ok, {value, :value}}
    end
  end

  defp prepare_flow_attrs(data_dir, shard_index, threshold, attrs) do
    with {:ok, prepared_attrs, external_payloads} <-
           prepare_flow_attrs_placeholders(attrs, threshold) do
      case external_payloads do
        [] ->
          {:ok, prepared_attrs, false}

        [_ | _] ->
          with {:ok, refs} <-
                 BlobStore.put_many(data_dir, shard_index, Enum.reverse(external_payloads)) do
            {inflated_attrs, []} = inflate_flow_attrs_with_rest(prepared_attrs, refs)
            {:ok, inflated_attrs, true}
          end
      end
    end
  end

  defp prepare_generic_flow_attrs(attrs, threshold, external_payloads) do
    with {:ok, prepared_attrs, flow_external_payloads} <-
           prepare_flow_attrs_placeholders(attrs, threshold) do
      {:ok, prepared_attrs, flow_external_payloads ++ external_payloads}
    end
  end

  defp prepare_flow_attrs_placeholders(%{records: records} = attrs, threshold)
       when is_list(records) do
    with {:ok, prepared_shared, shared_external_payloads} <-
           prepare_flow_shared_attrs_placeholders(Map.get(attrs, :shared), threshold),
         {:ok, prepared_records, record_external_payloads} <-
           prepare_flow_records_attrs_placeholders(records, threshold) do
      prepared_attrs =
        attrs
        |> Map.put(:records, prepared_records)
        |> put_prepared_flow_shared_attrs(prepared_shared)

      {:ok, prepared_attrs, record_external_payloads ++ shared_external_payloads}
    end
  end

  defp prepare_flow_attrs_placeholders(attrs, threshold) when is_map(attrs) do
    prepare_flow_record_attrs_placeholders(attrs, threshold)
  end

  defp prepare_flow_shared_attrs_placeholders(nil, _threshold), do: {:ok, nil, []}

  defp prepare_flow_shared_attrs_placeholders(shared, threshold) when is_map(shared) do
    prepare_flow_record_attrs_placeholders(shared, threshold)
  end

  defp prepare_flow_shared_attrs_placeholders(_shared, _threshold),
    do: {:error, :invalid_flow_shared_attrs}

  defp prepare_flow_records_attrs_placeholders(records, threshold) do
    records
    |> Enum.reduce_while({:ok, [], []}, fn
      record_attrs, {:ok, prepared_records, external_payloads} when is_map(record_attrs) ->
        case prepare_flow_record_attrs_placeholders(record_attrs, threshold) do
          {:ok, prepared_record, record_external_payloads} ->
            {:cont,
             {:ok, [prepared_record | prepared_records],
              record_external_payloads ++ external_payloads}}

          {:error, _reason} = error ->
            {:halt, error}
        end

      _record_attrs, {:ok, _prepared_records, _external_payloads} ->
        {:halt, {:error, :invalid_flow_record_attrs}}
    end)
    |> case do
      {:ok, prepared_records, external_payloads} ->
        {:ok, Enum.reverse(prepared_records), external_payloads}

      {:error, _reason} = error ->
        error
    end
  end

  defp put_prepared_flow_shared_attrs(attrs, nil), do: attrs
  defp put_prepared_flow_shared_attrs(attrs, shared), do: Map.put(attrs, :shared, shared)

  defp prepare_flow_record_attrs_placeholders(%{idempotent: true} = attrs, _threshold),
    do: {:ok, attrs, []}

  defp prepare_flow_record_attrs_placeholders(attrs, threshold) when is_map(attrs) do
    Enum.reduce_while(@flow_value_fields, {:ok, attrs, []}, fn kind,
                                                               {:ok, prepared_attrs,
                                                                external_payloads} ->
      case Map.fetch(prepared_attrs, kind) do
        {:ok, value} ->
          encoded_value = Flow.encode_value(value)

          if externalize?(encoded_value, threshold) do
            prepared_attrs = Map.put(prepared_attrs, kind, @flow_blob_value_external)
            {:cont, {:ok, prepared_attrs, [encoded_value | external_payloads]}}
          else
            {:cont, {:ok, prepared_attrs, external_payloads}}
          end

        :error ->
          {:cont, {:ok, prepared_attrs, external_payloads}}
      end
    end)
  end

  defp inflate_flow_attrs_with_rest(%{records: records} = attrs, refs) when is_list(records) do
    {attrs, refs} = inflate_flow_shared_attrs_with_rest(attrs, refs)

    {records, refs} =
      Enum.map_reduce(records, refs, fn record_attrs, refs ->
        inflate_flow_record_attrs_with_rest(record_attrs, refs)
      end)

    {%{attrs | records: records}, refs}
  end

  defp inflate_flow_attrs_with_rest(attrs, refs) when is_map(attrs) do
    inflate_flow_record_attrs_with_rest(attrs, refs)
  end

  defp inflate_flow_shared_attrs_with_rest(%{shared: shared} = attrs, refs) when is_map(shared) do
    {shared, refs} = inflate_flow_record_attrs_with_rest(shared, refs)
    {%{attrs | shared: shared}, refs}
  end

  defp inflate_flow_shared_attrs_with_rest(attrs, refs), do: {attrs, refs}

  defp inflate_flow_record_attrs_with_rest(attrs, refs) when is_map(attrs) do
    Enum.reduce(@flow_value_fields, {attrs, refs}, fn kind, {attrs, refs} ->
      case {Map.get(attrs, kind), refs} do
        {@flow_blob_value_external, [ref | rest]} ->
          marker = {@flow_blob_value_ref_tag, BlobRef.encode!(ref)}
          {Map.put(attrs, kind, marker), rest}

        _other ->
          {attrs, refs}
      end
    end)
  end

  defp flow_attrs_candidate?(%{records: records} = attrs, threshold) when is_list(records) do
    flow_attrs_candidate?(Map.get(attrs, :shared), threshold) or
      Enum.any?(records, &flow_attrs_candidate?(&1, threshold))
  end

  defp flow_attrs_candidate?(%{idempotent: true}, _threshold), do: false

  defp flow_attrs_candidate?(attrs, threshold) when is_map(attrs) do
    Enum.any?(@flow_value_fields, fn kind ->
      case Map.fetch(attrs, kind) do
        {:ok, value} -> externalize?(Flow.encode_value(value), threshold)
        :error -> false
      end
    end)
  end

  defp flow_attrs_candidate?(_attrs, _threshold), do: false

  defp flow_value_command?(command), do: command in @flow_value_commands

  defp externalize?(value, threshold) do
    byte_size(value) >= threshold or BlobRef.ref?(value)
  end

  defp compound_blob_side_channel_key?(<<"H:", _rest::binary>>), do: true
  defp compound_blob_side_channel_key?(<<"L:", _rest::binary>>), do: true
  defp compound_blob_side_channel_key?(<<"X:", _rest::binary>>), do: true
  defp compound_blob_side_channel_key?(_key), do: false

  defp locked_put_blob_side_channel_key?(key) do
    compound_blob_side_channel_key?(key) or not CompoundKey.internal_key?(key)
  end
end
