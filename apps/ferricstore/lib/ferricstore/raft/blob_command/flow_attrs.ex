defmodule Ferricstore.Raft.BlobCommand.FlowAttrs do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Raft.BlobCommand.PayloadWriter
  alias Ferricstore.Store.BlobRef

  @flow_blob_value_ref_tag :ferricstore_flow_blob_value_ref
  @flow_blob_value_external :ferricstore_flow_blob_value_external
  @flow_value_fields [:payload, :result, :error]
  @flow_named_value_fields [:value]
  @flow_named_value_commands [:flow_named_value_put]
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

  def prepare_flow_attrs(data_dir, shard_index, threshold, attrs) do
    with {:ok, prepared_attrs, external_payloads} <-
           prepare_flow_attrs_placeholders(attrs, threshold) do
      case external_payloads do
        [] ->
          {:ok, prepared_attrs, false}

        [_ | _] ->
          with {:ok, refs} <-
                 PayloadWriter.put_blob_payloads(
                   data_dir,
                   shard_index,
                   Enum.reverse(external_payloads)
                 ) do
            {inflated_attrs, []} = inflate_flow_attrs_with_rest(prepared_attrs, refs)
            {:ok, inflated_attrs, true}
          end
      end
    end
  end

  def prepare_flow_named_value_attrs(data_dir, shard_index, threshold, attrs) do
    with {:ok, prepared_attrs, external_payloads} <-
           prepare_flow_named_value_attrs_placeholders(attrs, threshold) do
      case external_payloads do
        [] ->
          {:ok, prepared_attrs, false}

        [_ | _] ->
          with {:ok, refs} <-
                 PayloadWriter.put_blob_payloads(
                   data_dir,
                   shard_index,
                   Enum.reverse(external_payloads)
                 ) do
            {inflated_attrs, []} = inflate_flow_named_value_attrs_with_rest(prepared_attrs, refs)
            {:ok, inflated_attrs, true}
          end
      end
    end
  end

  def prepare_generic_flow_attrs(attrs, threshold, external_payloads) do
    with {:ok, prepared_attrs, flow_external_payloads} <-
           prepare_flow_attrs_placeholders(attrs, threshold) do
      {:ok, prepared_attrs, flow_external_payloads ++ external_payloads}
    end
  end

  def prepare_generic_flow_named_value_attrs(attrs, threshold, external_payloads) do
    with {:ok, prepared_attrs, flow_external_payloads} <-
           prepare_flow_named_value_attrs_placeholders(attrs, threshold) do
      {:ok, prepared_attrs, flow_external_payloads ++ external_payloads}
    end
  end

  def prepare_flow_attrs_placeholders(%{records: records} = attrs, threshold)
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

  def prepare_flow_attrs_placeholders(attrs, threshold) when is_map(attrs) do
    prepare_flow_record_attrs_placeholders(attrs, threshold)
  end

  def prepare_flow_shared_attrs_placeholders(nil, _threshold), do: {:ok, nil, []}

  def prepare_flow_shared_attrs_placeholders(shared, threshold) when is_map(shared) do
    prepare_flow_record_attrs_placeholders(shared, threshold)
  end

  def prepare_flow_shared_attrs_placeholders(_shared, _threshold),
    do: {:error, :invalid_flow_shared_attrs}

  def prepare_flow_records_attrs_placeholders(records, threshold) do
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

  def put_prepared_flow_shared_attrs(attrs, nil), do: attrs
  def put_prepared_flow_shared_attrs(attrs, shared), do: Map.put(attrs, :shared, shared)

  def prepare_flow_record_attrs_placeholders(%{idempotent: true} = attrs, threshold) do
    # Idempotent Flow creates compare named-value digests from the command value.
    # Direct payload/result/error refs are safe to externalize because duplicate
    # checks resolve blob markers back to the encoded original value.
    prepare_flow_direct_value_fields(attrs, @flow_value_fields, threshold)
  end

  def prepare_flow_record_attrs_placeholders(attrs, threshold) when is_map(attrs) do
    with {:ok, attrs, external_payloads} <-
           prepare_flow_direct_value_fields(attrs, @flow_value_fields, threshold),
         {:ok, attrs, named_external_payloads} <-
           prepare_flow_values_map_placeholders(attrs, threshold) do
      {:ok, attrs, named_external_payloads ++ external_payloads}
    end
  end

  def prepare_flow_named_value_attrs_placeholders(%{idempotent: true} = attrs, _threshold),
    do: {:ok, attrs, []}

  def prepare_flow_named_value_attrs_placeholders(attrs, threshold) when is_map(attrs) do
    prepare_flow_direct_value_fields(attrs, @flow_named_value_fields, threshold)
  end

  def prepare_flow_direct_value_fields(attrs, fields, threshold) do
    Enum.reduce_while(fields, {:ok, attrs, []}, fn kind,
                                                   {:ok, prepared_attrs, external_payloads} ->
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

  def prepare_flow_values_map_placeholders(attrs, threshold) do
    case Map.fetch(attrs, :values) do
      {:ok, values} when is_map(values) ->
        values
        |> Enum.reduce_while({:ok, %{}, []}, fn
          {name, value}, {:ok, prepared_values, external_payloads} when is_binary(name) ->
            encoded_value = Flow.encode_value(value)

            if externalize?(encoded_value, threshold) do
              prepared_values = Map.put(prepared_values, name, @flow_blob_value_external)
              {:cont, {:ok, prepared_values, [encoded_value | external_payloads]}}
            else
              {:cont, {:ok, Map.put(prepared_values, name, value), external_payloads}}
            end

          _invalid, {:ok, _prepared_values, _external_payloads} ->
            {:halt, {:error, :invalid_flow_values_map}}
        end)
        |> case do
          {:ok, prepared_values, external_payloads} ->
            {:ok, Map.put(attrs, :values, prepared_values), external_payloads}

          {:error, _reason} = error ->
            error
        end

      {:ok, _invalid} ->
        {:error, :invalid_flow_values_map}

      :error ->
        {:ok, attrs, []}
    end
  end

  def inflate_flow_attrs_with_rest(%{records: records} = attrs, refs) when is_list(records) do
    {attrs, refs} = inflate_flow_shared_attrs_with_rest(attrs, refs)

    {records, refs} =
      Enum.map_reduce(records, refs, fn record_attrs, refs ->
        inflate_flow_record_attrs_with_rest(record_attrs, refs)
      end)

    {%{attrs | records: records}, refs}
  end

  def inflate_flow_attrs_with_rest(attrs, refs) when is_map(attrs) do
    inflate_flow_record_attrs_with_rest(attrs, refs)
  end

  def inflate_flow_shared_attrs_with_rest(%{shared: shared} = attrs, refs) when is_map(shared) do
    {shared, refs} = inflate_flow_record_attrs_with_rest(shared, refs)
    {%{attrs | shared: shared}, refs}
  end

  def inflate_flow_shared_attrs_with_rest(attrs, refs), do: {attrs, refs}

  def inflate_flow_record_attrs_with_rest(attrs, refs) when is_map(attrs) do
    {attrs, refs} = inflate_flow_direct_value_fields_with_rest(attrs, @flow_value_fields, refs)
    inflate_flow_values_map_with_rest(attrs, refs)
  end

  def inflate_flow_named_value_attrs_with_rest(attrs, refs) when is_map(attrs) do
    inflate_flow_direct_value_fields_with_rest(attrs, @flow_named_value_fields, refs)
  end

  def inflate_flow_direct_value_fields_with_rest(attrs, fields, refs) do
    Enum.reduce(fields, {attrs, refs}, fn kind, {attrs, refs} ->
      case {Map.get(attrs, kind), refs} do
        {@flow_blob_value_external, [ref | rest]} ->
          marker = {@flow_blob_value_ref_tag, BlobRef.encode!(ref)}
          {Map.put(attrs, kind, marker), rest}

        _other ->
          {attrs, refs}
      end
    end)
  end

  def inflate_flow_values_map_with_rest(%{values: values} = attrs, refs) when is_map(values) do
    {values, refs} =
      Enum.reduce(values, {%{}, refs}, fn
        {name, @flow_blob_value_external}, {values, [ref | rest]} ->
          marker = {@flow_blob_value_ref_tag, BlobRef.encode!(ref)}
          {Map.put(values, name, marker), rest}

        {name, value}, {values, refs} ->
          {Map.put(values, name, value), refs}
      end)

    {%{attrs | values: values}, refs}
  end

  def inflate_flow_values_map_with_rest(attrs, refs), do: {attrs, refs}

  def flow_attrs_candidate?(%{records: records} = attrs, threshold) when is_list(records) do
    flow_attrs_candidate?(Map.get(attrs, :shared), threshold) or
      Enum.any?(records, &flow_attrs_candidate?(&1, threshold))
  end

  def flow_attrs_candidate?(%{idempotent: true} = attrs, threshold) do
    flow_direct_value_fields_candidate?(attrs, @flow_value_fields, threshold)
  end

  def flow_attrs_candidate?(attrs, threshold) when is_map(attrs) do
    flow_direct_value_fields_candidate?(attrs, @flow_value_fields, threshold) or
      flow_values_map_candidate?(attrs, threshold)
  end

  def flow_attrs_candidate?(_attrs, _threshold), do: false

  def flow_named_value_attrs_candidate?(%{idempotent: true}, _threshold), do: false

  def flow_named_value_attrs_candidate?(attrs, threshold) when is_map(attrs) do
    flow_direct_value_fields_candidate?(attrs, @flow_named_value_fields, threshold)
  end

  def flow_named_value_attrs_candidate?(_attrs, _threshold), do: false

  def flow_direct_value_fields_candidate?(attrs, fields, threshold) do
    Enum.any?(fields, fn kind ->
      case Map.fetch(attrs, kind) do
        {:ok, value} -> externalize?(Flow.encode_value(value), threshold)
        :error -> false
      end
    end)
  end

  def flow_values_map_candidate?(%{values: values}, threshold) when is_map(values) do
    Enum.any?(values, fn
      {name, value} when is_binary(name) -> externalize?(Flow.encode_value(value), threshold)
      _other -> false
    end)
  end

  def flow_values_map_candidate?(_attrs, _threshold), do: false

  def flow_named_value_command?(command), do: command in @flow_named_value_commands
  def flow_value_command?(command), do: command in @flow_value_commands

  def externalize?(value, threshold) do
    byte_size(value) >= threshold or BlobRef.ref?(value)
  end
end
