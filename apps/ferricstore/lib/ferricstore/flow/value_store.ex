defmodule Ferricstore.Flow.ValueStore do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.Telemetry, as: FlowTelemetry
  alias Ferricstore.Stats
  alias Ferricstore.Store.{BlobValue, ColdRead, Router}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @max_ref_size 4_096
  def value_put(ctx, value, opts \\ [])

  def value_put(ctx, value, opts) when is_list(opts) do
    started = FlowTelemetry.start_time()

    result =
      with :ok <- validate_opts(opts),
           {:ok, partition_key} <- optional_partition_key(opts),
           {:ok, owner_flow_id} <- optional_binary_or_nil(opts, :owner_flow_id, nil),
           :ok <- validate_ref_size(:owner_flow_id, owner_flow_id),
           {:ok, name} <- optional_binary_or_nil(opts, :name, nil),
           :ok <- validate_ref_size(:name, name),
           {:ok, override?} <- optional_boolean(opts, :override, false),
           {:ok, now} <- optional_now_ms(opts),
           {:ok, ttl_ms} <- optional_pos_integer_or_nil(opts, :ttl_ms) do
        if is_binary(owner_flow_id) and is_binary(name) do
          with {:ok, return_mode} <- optional_value_put_return_mode(opts) do
            attrs =
              named_value_attrs_from_parts(
                value,
                owner_flow_id,
                name,
                partition_key,
                override?,
                now,
                return_mode
              )

            Router.flow_named_value_put(ctx, attrs)
          end
        else
          ref_id = shared_value_ref_id()
          ref = Keys.value_key(ref_id, :shared, 1, partition_key)

          with {:ok, return_mode} <- optional_value_put_return_mode(opts),
               :ok <- validate_key_size(ref),
               expire_at = flow_value_expire_at(now, ttl_ms),
               :ok <- Router.put(ctx, ref, Codec.encode_value(value), expire_at) do
            response = %{
              ref: ref,
              partition_key: partition_key,
              owner_flow_id: owner_flow_id,
              return: return_mode
            }

            {:ok, shared_value_put_response(response)}
          end
        end
      end

    FlowTelemetry.observe(:value_put, started, result, %{
      flow_id: Keyword.get(opts, :owner_flow_id)
    })
  end

  def value_put(_ctx, _value, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc false
  def named_value_attrs(value, opts) when is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, owner_flow_id} <- optional_binary_or_nil(opts, :owner_flow_id, nil),
         :ok <- validate_ref_size(:owner_flow_id, owner_flow_id),
         {:ok, name} <- optional_binary_or_nil(opts, :name, nil),
         :ok <- validate_ref_size(:name, name),
         true <- is_binary(owner_flow_id) and is_binary(name),
         {:ok, override?} <- optional_boolean(opts, :override, false),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, return_mode} <- optional_value_put_return_mode(opts) do
      {:ok,
       named_value_attrs_from_parts(
         value,
         owner_flow_id,
         name,
         partition_key,
         override?,
         now,
         return_mode
       )}
    else
      false -> {:error, "ERR flow named value put requires owner_flow_id and name"}
      {:error, _reason} = error -> error
    end
  end

  def named_value_attrs(_value, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc false
  def shared_value_put_batch(_ctx, []), do: []

  def shared_value_put_batch(ctx, items) when is_list(items) do
    prepared =
      Enum.map(items, fn
        {value, opts} when is_list(opts) -> shared_value_put_attrs(value, opts)
        _other -> {:error, "ERR flow opts must be a keyword list"}
      end)

    commands =
      prepared
      |> Enum.flat_map(fn
        {:ok, %{ref: ref, encoded: encoded, expire_at: expire_at}} ->
          [{ref, {:put, ref, encoded, expire_at}}]

        {:error, _reason} ->
          []
      end)

    write_results =
      case commands do
        [] -> []
        _ -> Router.pipeline_write_batch(ctx, commands)
      end

    {results, _remaining_writes} =
      Enum.map_reduce(prepared, write_results, fn
        {:ok, attrs}, [:ok | rest] ->
          {{:ok, shared_value_put_response(attrs)}, rest}

        {:ok, attrs}, [{:ok, :ok} | rest] ->
          {{:ok, shared_value_put_response(attrs)}, rest}

        {:ok, _attrs}, [{:error, _reason} = error | rest] ->
          {error, rest}

        {:ok, _attrs}, [other | rest] ->
          {{:error, "ERR flow value put failed: #{inspect(other)}"}, rest}

        {:ok, _attrs}, [] ->
          {{:error, "ERR flow value put failed"}, []}

        {:error, _reason} = error, writes ->
          {error, writes}
      end)

    results
  end

  def shared_value_put_batch(_ctx, _items), do: [{:error, "ERR flow opts must be a keyword list"}]
  def value_mget(ctx, refs, opts \\ [])

  def value_mget(ctx, refs, opts) when is_list(refs) and is_list(opts) do
    with {:ok, max_bytes} <- optional_value_mget_max_bytes(opts) do
      case raw_mget(ctx, refs) do
        values when is_list(values) ->
          {:ok,
           refs
           |> Enum.zip(values)
           |> Enum.map(fn {ref, value} -> decode_or_omit_value(ref, value, max_bytes) end)}

        {:error, _reason} = error ->
          error

        other ->
          {:error, "ERR flow value mget failed: #{inspect(other)}"}
      end
    end
  end

  def value_mget(_ctx, _refs, opts) when not is_list(opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def value_mget(_ctx, _refs, _opts), do: {:error, "ERR flow refs must be a list"}

  defp optional_value_mget_max_bytes(opts) do
    value =
      Keyword.get(
        opts,
        :max_bytes,
        Keyword.get(opts, :value_max_bytes, Keyword.get(opts, :payload_max_bytes))
      )

    case value do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, "ERR flow max_bytes must be a non-negative integer"}
    end
  end

  defp decode_or_omit_value(_ref, nil, _max_bytes), do: nil

  defp decode_or_omit_value(ref, value, max_bytes)
       when is_binary(ref) and is_binary(value) and is_integer(max_bytes) and
              byte_size(value) > max_bytes do
    %{ref: ref, value_omitted: true, value_size: byte_size(value)}
  end

  defp decode_or_omit_value(_ref, value, _max_bytes), do: Codec.decode_value(value)

  def raw_mget(_ctx, []), do: []

  def raw_mget(ctx, refs) do
    values =
      Stats.with_cache_tracking_disabled(fn ->
        Router.batch_get(ctx, refs)
      end)

    flow_value_fill_lmdb_missing(values, ctx, refs)
  end

  def raw_mget_with_file_refs(_ctx, [], _min_file_ref_size), do: []

  def raw_mget_with_file_refs(ctx, refs, min_file_ref_size) do
    values =
      Stats.with_cache_tracking_disabled(fn ->
        Router.batch_get_with_file_refs(ctx, refs, min_file_ref_size)
      end)

    flow_value_fill_lmdb_missing(values, ctx, refs)
  end

  defp flow_value_fill_lmdb_missing(values, ctx, refs)
       when is_list(values) and is_list(refs) and length(values) == length(refs) do
    missing =
      refs
      |> Enum.zip(values)
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {{ref, nil}, idx} when is_binary(ref) ->
          if flow_generated_payload_value_ref?(ref), do: [{idx, ref}], else: []

        _entry ->
          []
      end)

    if missing == [] do
      values
    else
      lmdb_values =
        ctx
        |> flow_value_lmdb_mget(Enum.map(missing, fn {_idx, ref} -> ref end))
        |> List.to_tuple()

      replacements =
        missing
        |> Enum.with_index()
        |> Map.new(fn {{idx, _ref}, lmdb_idx} -> {idx, elem(lmdb_values, lmdb_idx)} end)

      values
      |> Enum.with_index()
      |> Enum.map(fn
        {nil, idx} -> Map.get(replacements, idx)
        {value, _idx} -> value
      end)
    end
  end

  defp flow_value_fill_lmdb_missing(values, _ctx, _refs), do: values

  defp flow_value_lmdb_mget(_ctx, []), do: []

  defp flow_value_lmdb_mget(ctx, refs) do
    now = now_ms()

    results =
      refs
      |> Enum.with_index()
      |> Enum.group_by(fn {ref, _idx} -> flow_value_lmdb_path(ctx, ref) end)
      |> Enum.reduce(%{}, fn {path, group}, acc ->
        group_refs = Enum.map(group, fn {ref, _idx} -> ref end)

        lmdb_values =
          case Ferricstore.Flow.LMDB.get_many(path, group_refs) do
            {:ok, values} -> values
            {:error, _reason} -> Enum.map(group_refs, fn _ref -> :not_found end)
          end

        flow_value_lmdb_decode_group(ctx, group, lmdb_values, now, acc)
      end)

    for idx <- 0..(length(refs) - 1)//1, do: Map.get(results, idx)
  end

  defp flow_value_lmdb_path(ctx, ref) do
    shard_index = Router.shard_for(ctx, ref)

    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> Ferricstore.Flow.LMDB.path()
  end

  defp flow_value_lmdb_decode_group(ctx, group, lmdb_values, now, acc) do
    {acc, locators} =
      group
      |> Enum.zip(lmdb_values)
      |> Enum.reduce({acc, []}, fn {{ref, idx}, lmdb_value}, {inner_acc, locators} ->
        case flow_value_lmdb_classify(ctx, ref, lmdb_value, now) do
          {:value, value} ->
            {Map.put(inner_acc, idx, value), locators}

          {:locator, locator} ->
            {inner_acc, [{idx, ref, locator} | locators]}

          :missing ->
            {inner_acc, locators}
        end
      end)

    flow_value_lmdb_read_locators(ctx, locators, acc)
  end

  defp flow_value_lmdb_classify(ctx, ref, {:ok, blob}, now) when is_binary(blob) do
    case Ferricstore.Flow.LMDB.decode_value_locator(blob, now) do
      {:ok, locator} ->
        {:locator, locator}

      :not_locator ->
        case Ferricstore.Flow.LMDB.decode_value(blob, now) do
          {:ok, value} -> {:value, flow_value_maybe_materialize_lmdb_value(ctx, ref, value)}
          _other -> :missing
        end

      _other ->
        :missing
    end
  end

  defp flow_value_lmdb_classify(_ctx, _ref, _result, _now), do: :missing

  defp flow_value_lmdb_read_locators(_ctx, [], acc), do: acc

  defp flow_value_lmdb_read_locators(ctx, locators, acc) do
    {waraft_locators, other_locators} =
      Enum.split_with(locators, fn {_idx, _ref, {file_id, _offset, _value_size}} ->
        waraft_segment_file_id?(file_id)
      end)

    acc =
      Enum.reduce(other_locators, acc, fn {idx, ref, locator}, inner_acc ->
        case flow_value_read_lmdb_locator(ctx, ref, locator) do
          nil -> inner_acc
          value -> Map.put(inner_acc, idx, value)
        end
      end)

    waraft_locators
    |> Enum.group_by(fn {_idx, ref, {file_id, _offset, _value_size}} ->
      {Router.shard_for(ctx, ref), file_id}
    end)
    |> Enum.reduce(acc, fn {{shard_index, file_id}, entries}, inner_acc ->
      refs = Enum.map(entries, fn {_idx, ref, _locator} -> ref end)

      case Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
             ctx,
             shard_index,
             file_id,
             refs
           ) do
        {:ok, values} ->
          flow_value_lmdb_put_waraft_locator_values(ctx, shard_index, entries, values, inner_acc)

        _missing_or_error ->
          inner_acc
      end
    end)
  end

  defp flow_value_lmdb_put_waraft_locator_values(ctx, shard_index, entries, values, acc) do
    found =
      Enum.flat_map(entries, fn {idx, ref, _locator} ->
        case Map.fetch(values, ref) do
          {:ok, value} -> [{idx, value}]
          :error -> []
        end
      end)

    materialized =
      BlobValue.maybe_materialize_many(
        ctx.data_dir,
        shard_index,
        BlobValue.threshold(ctx),
        Enum.map(found, fn {_idx, value} -> value end)
      )

    found
    |> Enum.zip(materialized)
    |> Enum.reduce(acc, fn
      {{idx, _value}, {:ok, materialized_value}}, inner_acc ->
        Map.put(inner_acc, idx, materialized_value)

      {_entry, {:error, _reason}}, inner_acc ->
        inner_acc
    end)
  end

  defp waraft_segment_file_id?({tag, index})
       when tag in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
              is_integer(index) and index > 0,
       do: true

  defp waraft_segment_file_id?(_file_id), do: false

  defp flow_value_maybe_materialize_lmdb_value(ctx, ref, value) when is_binary(value) do
    shard_index = Router.shard_for(ctx, ref)

    case BlobValue.maybe_materialize(
           ctx.data_dir,
           shard_index,
           BlobValue.threshold(ctx),
           value
         ) do
      {:ok, materialized} -> materialized
      {:error, _reason} -> nil
    end
  end

  defp flow_value_maybe_materialize_lmdb_value(_ctx, _ref, value), do: value

  defp flow_value_read_lmdb_locator(ctx, key, {file_id, offset, _value_size}) do
    shard_index = Router.shard_for(ctx, key)

    with {:ok, value} <- flow_value_read_locator_bytes(ctx, shard_index, key, file_id, offset),
         {:ok, materialized} <-
           BlobValue.maybe_materialize(
             ctx.data_dir,
             shard_index,
             BlobValue.threshold(ctx),
             value
           ) do
      materialized
    else
      _error -> nil
    end
  end

  defp flow_value_read_locator_bytes(ctx, shard_index, key, file_id, offset)
       when is_integer(file_id) and file_id >= 0 do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> ShardETS.file_path(file_id)
    |> ColdRead.pread_keyed(offset, key, 10_000)
  end

  defp flow_value_read_locator_bytes(
         ctx,
         shard_index,
         _key,
         {:flow_history, _file_id} = file_id,
         offset
       ) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> Ferricstore.Flow.HistoryProjector.read_value(file_id, offset)
  end

  defp flow_value_read_locator_bytes(ctx, shard_index, key, file_id, _offset)
       when is_tuple(file_id) do
    Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(ctx, shard_index, file_id, key)
  end

  defp flow_value_read_locator_bytes(_ctx, _shard_index, _key, _file_id, _offset),
    do: {:error, :bad_flow_value_locator}

  defp flow_generated_payload_value_ref?("f:" <> _rest = ref) do
    case :binary.split(ref, ":v:") do
      ["f:" <> tag, <<kind, ?:, rest::binary>>]
      when byte_size(tag) > 0 and kind in [?p, ?r, ?e, ?s] and byte_size(rest) > 0 ->
        true

      _other ->
        false
    end
  end

  defp flow_generated_payload_value_ref?(_ref), do: false

  defp validate_opts(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, "ERR flow opts must be a keyword list"}
  end

  defp optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      nil -> {:ok, nil}
      :global -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow partition_key must be a non-empty string or :global"}
    end
  end

  defp optional_binary_or_nil(opts, key, default) do
    case Keyword.get(opts, key, default) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a string"}
    end
  end

  defp validate_ref_size(_key, nil), do: :ok

  defp validate_ref_size(key, value) when is_binary(value) do
    if byte_size(value) <= @max_ref_size do
      :ok
    else
      {:error, "ERR flow #{key} too large (max #{@max_ref_size} bytes)"}
    end
  end

  defp optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a boolean"}
    end
  end

  defp optional_now_ms(opts) do
    case Keyword.fetch(opts, :now_ms) do
      {:ok, value} when is_integer(value) and value >= 0 ->
        {:ok, value}

      {:ok, _value} ->
        {:error, "ERR flow now_ms must be a non-negative integer"}

      :error ->
        {:ok, nil}
    end
  end

  defp optional_pos_integer_or_nil(opts, key) do
    case Keyword.get(opts, key, nil) do
      nil -> {:ok, nil}
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a positive integer"}
    end
  end

  defp optional_value_put_return_mode(opts) do
    case Keyword.get(opts, :return) do
      nil -> {:ok, nil}
      :ok_on_success -> {:ok, :ok_on_success}
      "ok_on_success" -> {:ok, :ok_on_success}
      "OK_ON_SUCCESS" -> {:ok, :ok_on_success}
      _ -> {:error, "ERR flow value put return must be ok_on_success"}
    end
  end

  defp shared_value_ref_id do
    :crypto.strong_rand_bytes(18)
    |> Base.url_encode64(padding: false)
  end

  defp flow_value_expire_at(_now, nil), do: 0
  defp flow_value_expire_at(now, ttl_ms), do: now + ttl_ms

  defp shared_value_put_attrs(value, opts) do
    with :ok <- validate_opts(opts),
         {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, owner_flow_id} <- optional_binary_or_nil(opts, :owner_flow_id, nil),
         :ok <- validate_ref_size(:owner_flow_id, owner_flow_id),
         {:ok, name} <- optional_binary_or_nil(opts, :name, nil),
         :ok <- validate_ref_size(:name, name),
         true <- is_nil(name),
         {:ok, now} <- optional_now_ms(opts),
         {:ok, ttl_ms} <- optional_pos_integer_or_nil(opts, :ttl_ms),
         {:ok, return_mode} <- optional_value_put_return_mode(opts) do
      ref_id = shared_value_ref_id()
      ref = Keys.value_key(ref_id, :shared, 1, partition_key)
      now = now || now_ms()

      with :ok <- validate_key_size(ref) do
        {:ok,
         %{
           ref: ref,
           partition_key: partition_key,
           owner_flow_id: owner_flow_id,
           return: return_mode,
           encoded: Codec.encode_value(value),
           expire_at: flow_value_expire_at(now, ttl_ms)
         }}
      end
    else
      false -> {:error, "ERR flow named value put requires owner_flow_id and name"}
      {:error, _reason} = error -> error
    end
  end

  defp shared_value_put_response(%{return: :ok_on_success}), do: :ok

  defp shared_value_put_response(attrs) do
    %{ref: Map.fetch!(attrs, :ref), partition_key: Map.get(attrs, :partition_key)}
    |> maybe_put_attr(:owner_flow_id, Map.get(attrs, :owner_flow_id))
  end

  defp named_value_attrs_from_parts(
         value,
         owner_flow_id,
         name,
         partition_key,
         override?,
         now,
         return_mode
       ) do
    %{
      id: owner_flow_id,
      name: name,
      value: value,
      partition_key: partition_key,
      override: override?
    }
    |> maybe_put_attr(:now_ms, now)
    |> maybe_put_attr(:return, return_mode)
  end

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  defp maybe_put_attr(attrs, _key, nil), do: attrs
  defp maybe_put_attr(attrs, key, value), do: Map.put(attrs, key, value)

  defp now_ms, do: CommandTime.now_ms()
end
