defmodule Ferricstore.Raft.BlobCommand do
  @moduledoc """
  Prepares large-value Raft commands for blob side-channel replication.

  The ref-only command shapes are only safe for a single-member Raft group: the
  local apply side can validate that the blob record already exists before it
  stores the ref in Bitcask. Multi-member groups still need a blob transfer
  protocol before followers can apply refs without the original payload.
  """

  alias Ferricstore.Raft.BlobCommand.{FlowAttrs, PayloadWriter}
  alias Ferricstore.Store.{BlobRef, BlobStore, BlobValue}

  @flow_blob_value_ref_tag :ferricstore_flow_blob_value_ref
  @protection_key :ferricstore_blob_command_protection
  @type command ::
          {:put, binary(), binary(), non_neg_integer()}
          | {:set, binary(), binary(), non_neg_integer(), map()}
          | {:append, binary(), binary()}
          | {:getset, binary(), binary()}
          | {:setrange, binary(), non_neg_integer(), binary()}
          | {:cas, binary(), binary(), binary(), non_neg_integer() | nil}
          | {:fetch_or_compute_publish, binary(), binary(), non_neg_integer(), binary()}
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
    prepare_result(ctx, shard_index, command, opts)
  end

  @doc false
  @spec prepare_protected(map(), non_neg_integer(), command(), keyword()) ::
          {:ok, command(), BlobStore.protection_token()} | {:error, term()}
  def prepare_protected(ctx, shard_index, command, opts \\ []) do
    with {:ok, data_dir} <- context_data_dir(ctx) do
      begin_protection_collection(data_dir, shard_index)

      try do
        case prepare_result(ctx, shard_index, command, opts) do
          {:ok, prepared} ->
            {:ok, prepared, pop_protection_collection()}

          {:error, _reason} = error ->
            BlobStore.unprotect(pop_protection_collection())
            error
        end
      after
        clear_protection_collection()
      end
    else
      :error -> {:ok, command, nil}
    end
  end

  defp prepare_result(ctx, shard_index, command, opts) do
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
         {:fetch_or_compute_publish, key, value, expire_at_ms, owner_ref}
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_binary(value) and is_binary(owner_ref) do
    case prepare_value(data_dir, shard_index, threshold, value) do
      {:ok, {^value, :value}} ->
        {:ok, {:fetch_or_compute_publish, key, value, expire_at_ms, owner_ref}}

      {:ok, {encoded_ref, :blob_ref}} ->
        {:ok, {:fetch_or_compute_publish_blob_ref, key, encoded_ref, expire_at_ms, owner_ref}}

      {:error, _reason} = error ->
        error
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

  defp prepare_enabled(ctx, shard_index, threshold, {operation, entries})
       when operation in [:mset, :msetnx] and is_list(entries) do
    case prepare_enabled(ctx, shard_index, threshold, {:put_batch, entries}) do
      {:ok, {:put_batch, prepared}} -> {:ok, {operation, prepared}}
      {:ok, {:put_blob_batch, prepared}} -> {:ok, {atomic_blob_operation(operation), prepared}}
      {:error, _reason} = error -> error
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

  defp prepare_enabled(ctx, shard_index, threshold, {:flow_policy_fence, installs, command})
       when is_list(installs) and is_tuple(command) do
    with {:ok, prepared} <- prepare_enabled(ctx, shard_index, threshold, command) do
      {:ok, {:flow_policy_fence, installs, prepared}}
    end
  end

  defp prepare_enabled(
         %{data_dir: data_dir},
         shard_index,
         threshold,
         {:flow_create_with_catalog, key, catalog, attrs} = command
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_map(catalog) and is_map(attrs) do
    case FlowAttrs.prepare_flow_attrs(data_dir, shard_index, threshold, attrs) do
      {:ok, prepared_attrs, true} ->
        {:ok, {:flow_create_with_catalog, key, catalog, prepared_attrs}}

      {:ok, _prepared_attrs, false} ->
        {:ok, command}

      {:error, _reason} = error ->
        error
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
    case FlowAttrs.prepare_flow_attrs(data_dir, shard_index, threshold, attrs) do
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
    cond do
      FlowAttrs.flow_named_value_command?(command) ->
        case FlowAttrs.prepare_flow_named_value_attrs(data_dir, shard_index, threshold, attrs) do
          {:ok, prepared_attrs, true} ->
            {:ok, {command, key, prepared_attrs}}

          {:ok, _prepared_attrs, false} ->
            {:ok, raw}

          {:error, _reason} = error ->
            error
        end

      FlowAttrs.flow_value_command?(command) ->
        case FlowAttrs.prepare_flow_attrs(data_dir, shard_index, threshold, attrs) do
          {:ok, prepared_attrs, true} ->
            {:ok, {command, key, prepared_attrs}}

          {:ok, _prepared_attrs, false} ->
            {:ok, raw}

          {:error, _reason} = error ->
            error
        end

      true ->
        {:ok, raw}
    end
  end

  defp prepare_enabled(_ctx, _shard_index, _threshold, command), do: {:ok, command}

  defp atomic_blob_operation(:mset), do: :mset_blob_batch
  defp atomic_blob_operation(:msetnx), do: :msetnx_blob_batch

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

  defp command_candidate?(
         {:fetch_or_compute_publish, key, value, _expire_at_ms, owner_ref},
         threshold
       )
       when is_binary(key) and is_binary(value) and is_binary(owner_ref) do
    externalize?(value, threshold)
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

  defp command_candidate?({operation, entries}, threshold)
       when operation in [:mset, :msetnx] and is_list(entries) do
    command_candidate?({:put_batch, entries}, threshold)
  end

  defp command_candidate?({:batch, commands}, threshold) when is_list(commands) do
    Enum.any?(commands, &command_candidate?(&1, threshold))
  end

  defp command_candidate?({:flow_policy_fence, _installs, command}, threshold),
    do: command_candidate?(command, threshold)

  defp command_candidate?({:flow_create_with_catalog, _key, catalog, attrs}, threshold)
       when is_map(catalog) and is_map(attrs),
       do: FlowAttrs.flow_attrs_candidate?(attrs, threshold)

  defp command_candidate?({:flow_terminal_pipeline_batch, op, _key, attrs}, threshold)
       when is_atom(op) and is_map(attrs) do
    FlowAttrs.flow_attrs_candidate?(attrs, threshold)
  end

  defp command_candidate?({command, _key, attrs}, threshold)
       when is_atom(command) and is_map(attrs) do
    cond do
      FlowAttrs.flow_named_value_command?(command) ->
        FlowAttrs.flow_named_value_attrs_candidate?(attrs, threshold)

      FlowAttrs.flow_value_command?(command) ->
        FlowAttrs.flow_attrs_candidate?(attrs, threshold)

      true ->
        false
    end
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
               put_blob_payloads(data_dir, shard_index, Enum.reverse(external_payloads)) do
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
        case FlowAttrs.prepare_generic_flow_attrs(attrs, threshold, external_payloads) do
          {:ok, prepared_attrs, external_payloads} ->
            command = {:flow_terminal_pipeline_batch, op, key, prepared_attrs}
            {:cont, {:ok, [command | acc], external_payloads}}

          {:error, _reason} = error ->
            {:halt, error}
        end

      {command, key, attrs}, {:ok, acc, external_payloads}
      when is_atom(command) and is_map(attrs) ->
        cond do
          FlowAttrs.flow_named_value_command?(command) ->
            case FlowAttrs.prepare_generic_flow_named_value_attrs(
                   attrs,
                   threshold,
                   external_payloads
                 ) do
              {:ok, prepared_attrs, external_payloads} ->
                {:cont, {:ok, [{command, key, prepared_attrs} | acc], external_payloads}}

              {:error, _reason} = error ->
                {:halt, error}
            end

          FlowAttrs.flow_value_command?(command) ->
            case FlowAttrs.prepare_generic_flow_attrs(attrs, threshold, external_payloads) do
              {:ok, prepared_attrs, external_payloads} ->
                {:cont, {:ok, [{command, key, prepared_attrs} | acc], external_payloads}}

              {:error, _reason} = error ->
                {:halt, error}
            end

          true ->
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
               put_blob_payloads(data_dir, shard_index, Enum.reverse(external_payloads)) do
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
      when is_binary(compound_key) and is_binary(value) and is_integer(expire_at_ms) and
             expire_at_ms >= 0 ->
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
               put_blob_payloads(data_dir, shard_index, Enum.reverse(external_payloads)) do
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

        {:compound_put_external, compound_key, expire_at_ms}, [ref | rest] ->
          {{:compound_put_blob_ref, compound_key, BlobRef.encode!(ref), expire_at_ms}, rest}

        {:compound_blob_batch_external, redis_key, entries}, refs ->
          {inflated_entries, refs} = inflate_compound_batch_blob_refs_with_rest(entries, refs)
          {{:compound_blob_batch_put, redis_key, inflated_entries}, refs}

        {:flow_terminal_pipeline_batch, op, key, attrs}, refs ->
          {inflated_attrs, refs} = FlowAttrs.inflate_flow_attrs_with_rest(attrs, refs)
          {{:flow_terminal_pipeline_batch, op, key, inflated_attrs}, refs}

        {command, key, attrs}, refs when is_atom(command) and is_map(attrs) ->
          cond do
            FlowAttrs.flow_named_value_command?(command) ->
              {inflated_attrs, refs} =
                FlowAttrs.inflate_flow_named_value_attrs_with_rest(attrs, refs)

              {{command, key, inflated_attrs}, refs}

            FlowAttrs.flow_value_command?(command) ->
              {inflated_attrs, refs} = FlowAttrs.inflate_flow_attrs_with_rest(attrs, refs)
              {{command, key, inflated_attrs}, refs}

            true ->
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
      with {:ok, ref} <- PayloadWriter.put_blob_payload(data_dir, shard_index, value) do
        {:ok, {BlobRef.encode!(ref), :blob_ref}}
      end
    else
      {:ok, {value, :value}}
    end
  end

  defp externalize?(value, threshold) do
    byte_size(value) >= threshold or BlobRef.ref?(value)
  end

  defp context_data_dir(%{data_dir: data_dir}) when is_binary(data_dir), do: {:ok, data_dir}
  defp context_data_dir(_ctx), do: :error

  defp begin_protection_collection(data_dir, shard_index) do
    Process.put(@protection_key, {data_dir, shard_index, []})
    :ok
  end

  defp pop_protection_collection do
    case Process.get(@protection_key) do
      {_data_dir, _shard_index, tokens} when is_list(tokens) -> Enum.reverse(tokens)
      _other -> nil
    end
  end

  defp clear_protection_collection do
    Process.delete(@protection_key)
    :ok
  end

  def put_blob_payloads(data_dir, shard_index, payloads) do
    PayloadWriter.put_blob_payloads(data_dir, shard_index, payloads)
  end

  defp compound_blob_side_channel_key?(<<"H:", _rest::binary>>), do: true
  defp compound_blob_side_channel_key?(<<"L:", _rest::binary>>), do: true
  defp compound_blob_side_channel_key?(<<"X:", _rest::binary>>), do: true
  defp compound_blob_side_channel_key?(_key), do: false
end
