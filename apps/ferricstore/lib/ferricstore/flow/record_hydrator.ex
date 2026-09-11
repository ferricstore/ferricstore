defmodule Ferricstore.Flow.RecordHydrator do
  @moduledoc false

  alias Ferricstore.Flow.{Codec, Keys, Locator, RecordIdentity}
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Store.{BlobRef, BlobValue, ColdRead}

  @default_timeout_ms 10_000
  @default_max_bytes 16 * 1_024 * 1_024
  @maximum_records 4_096
  @maximum_bytes 1 * 1_024 * 1_024 * 1_024
  @maximum_time_ms 0xFFFF_FFFF_FFFF_FFFF

  @type request :: {binary(), Locator.t()}

  @spec read_many(map(), non_neg_integer(), [request()], keyword()) ::
          {:ok, [map() | nil]} | {:error, term()}
  def read_many(ctx, shard_index, requests, opts \\ [])

  def read_many(ctx, shard_index, requests, opts),
    do: do_read_many(ctx, shard_index, requests, opts, :decoded)

  @spec read_encoded_many(map(), non_neg_integer(), [request()], keyword()) ::
          {:ok, [binary() | nil]} | {:error, term()}
  def read_encoded_many(ctx, shard_index, requests, opts \\ [])

  def read_encoded_many(ctx, shard_index, requests, opts),
    do: do_read_many(ctx, shard_index, requests, opts, :encoded)

  @doc false
  @spec read_stored_many(map(), non_neg_integer(), [request()], keyword()) ::
          {:ok, [binary() | nil]} | {:error, term()}
  def read_stored_many(ctx, shard_index, requests, opts \\ [])

  def read_stored_many(ctx, shard_index, requests, opts),
    do: do_read_many(ctx, shard_index, requests, opts, :stored)

  @doc false
  @spec read_storage_refs_many(map(), non_neg_integer(), [request()], keyword()) ::
          {:ok, [binary() | nil]} | {:error, term()}
  def read_storage_refs_many(ctx, shard_index, requests, opts \\ [])

  def read_storage_refs_many(ctx, shard_index, requests, opts),
    do: do_read_many(ctx, shard_index, requests, opts, :storage_ref)

  defp do_read_many(%{data_dir: data_dir} = ctx, shard_index, requests, opts, mode)
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_list(requests) and is_list(opts) and
              mode in [:decoded, :encoded, :stored, :storage_ref] do
    max_bytes = Keyword.get(opts, :max_bytes, @default_max_bytes)
    timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
    include_expired? = Keyword.get(opts, :include_expired, false)
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))
    clock_ms = Keyword.get(opts, :clock_ms, fn -> System.monotonic_time(:millisecond) end)

    with :ok <- validate_limits(requests, max_bytes, timeout_ms),
         :ok <- validate_include_expired(include_expired?),
         :ok <- validate_now_ms(now_ms),
         :ok <- validate_clock(clock_ms),
         {:ok, started_ms} <- call_clock(clock_ms),
         deadline_ms = started_ms + timeout_ms,
         {:ok, prepared} <-
           prepare_requests(ctx, shard_index, requests, include_expired?, now_ms),
         :ok <- admit_locator_bytes(prepared, max_bytes),
         {:ok, raw_values} <-
           read_grouped(ctx, shard_index, prepared, deadline_ms, clock_ms, include_expired?),
         :ok <- check_deadline(deadline_ms, clock_ms),
         :ok <- validate_physical_value_sizes(raw_values, prepared),
         {:ok, records} <-
           finalize_values(
             mode,
             raw_values,
             prepared,
             ctx,
             shard_index,
             max_bytes,
             deadline_ms,
             clock_ms
           ),
         :ok <- check_deadline(deadline_ms, clock_ms) do
      if mode == :stored, do: {:ok, raw_values}, else: {:ok, records}
    end
  rescue
    _error -> {:error, :hydration_failed}
  catch
    _kind, _reason -> {:error, :hydration_failed}
  end

  defp do_read_many(_ctx, _shard_index, _requests, _opts, _mode),
    do: {:error, :invalid_hydration_request}

  defp validate_limits(requests, max_bytes, timeout_ms) do
    cond do
      length(requests) > @maximum_records ->
        {:error, :hydration_record_limit_exceeded}

      not is_integer(max_bytes) or max_bytes <= 0 or max_bytes > @maximum_bytes ->
        {:error, :invalid_hydration_byte_budget}

      not is_integer(timeout_ms) or timeout_ms <= 0 ->
        {:error, :invalid_hydration_timeout}

      true ->
        :ok
    end
  end

  defp validate_include_expired(value) when is_boolean(value), do: :ok
  defp validate_include_expired(_value), do: {:error, :invalid_hydration_expiry_mode}

  defp validate_now_ms(now_ms)
       when is_integer(now_ms) and now_ms >= 0 and now_ms <= @maximum_time_ms,
       do: :ok

  defp validate_now_ms(_now_ms), do: {:error, :invalid_hydration_time}

  defp validate_clock(clock_ms) when is_function(clock_ms, 0), do: :ok
  defp validate_clock(_clock_ms), do: {:error, :invalid_hydration_request}

  defp call_clock(clock_ms) do
    case clock_ms.() do
      now_ms when is_integer(now_ms) -> {:ok, now_ms}
      _invalid -> {:error, :invalid_hydration_clock}
    end
  rescue
    _error -> {:error, :invalid_hydration_clock}
  catch
    _kind, _reason -> {:error, :invalid_hydration_clock}
  end

  defp remaining_timeout_ms(deadline_ms, clock_ms) do
    with {:ok, now_ms} <- call_clock(clock_ms) do
      remaining_ms = deadline_ms - now_ms

      if remaining_ms > 0,
        do: {:ok, remaining_ms},
        else: {:error, :hydration_timeout}
    end
  end

  defp check_deadline(deadline_ms, clock_ms) do
    case remaining_timeout_ms(deadline_ms, clock_ms) do
      {:ok, _remaining_ms} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp prepare_requests(ctx, shard_index, requests, include_expired?, now_ms) do
    requests
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn
      {{state_key, %Locator{kind: :state} = locator}, index}, {:ok, acc}
      when is_binary(state_key) ->
        with true <- Locator.hydration_ready?(locator),
             {:ok, id} <- Keys.run_id_from_state_key(state_key),
             true <- id == locator.flow_id,
             {:ok, source} <-
               hydration_source(
                 ctx,
                 shard_index,
                 state_key,
                 locator,
                 include_expired?,
                 now_ms
               ) do
          prepared = %{
            index: index,
            state_key: state_key,
            locator: locator,
            source: source
          }

          {:cont, {:ok, [prepared | acc]}}
        else
          _invalid -> {:halt, {:error, :invalid_hydration_request}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_hydration_request}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp hydration_source(
         _ctx,
         _shard_index,
         _state_key,
         %Locator{expire_at_ms: expire_at_ms},
         false,
         now_ms
       )
       when is_integer(expire_at_ms) and expire_at_ms > 0 and expire_at_ms <= now_ms,
       do: {:ok, :expired}

  defp hydration_source(ctx, shard_index, state_key, locator, _include_expired?, _now_ms),
    do: source(ctx, shard_index, state_key, locator)

  defp source(%{data_dir: data_dir}, shard_index, state_key, %Locator{
         file_id: file_id,
         offset: offset,
         value_size: value_size
       })
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0 do
    path =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Path.join("#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")

    {:ok, {:bitcask, path, offset, state_key}}
  end

  defp source(_ctx, _shard_index, state_key, %Locator{
         file_id: {kind, index} = file_id,
         offset: offset,
         value_size: value_size,
         segment_generation: ordinal,
         frame_size: frame_size
       })
       when kind in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
              is_integer(index) and index > 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0 and is_integer(ordinal) and ordinal >= 0 and
              is_integer(frame_size) and frame_size >= 8,
       do: {:ok, {:waraft, file_id, ordinal, offset, frame_size, state_key}}

  defp source(_ctx, _shard_index, _state_key, _locator),
    do: {:error, :unsupported_hydration_location}

  defp admit_locator_bytes(prepared, max_bytes) do
    prepared
    |> Enum.reduce_while(0, fn
      %{source: :expired}, total -> {:cont, total}
      %{locator: %{value_size: bytes}}, total -> bounded_sum(total, bytes, max_bytes)
    end)
    |> admission_result()
  end

  defp read_grouped(ctx, shard_index, prepared, deadline_ms, clock_ms, include_expired?) do
    bitcask = Enum.filter(prepared, &match?(%{source: {:bitcask, _, _, _}}, &1))
    waraft = Enum.filter(prepared, &match?(%{source: {:waraft, _, _, _, _, _}}, &1))

    with {:ok, values} <- read_bitcask(bitcask, deadline_ms, clock_ms),
         {:ok, values} <-
           read_waraft(
             waraft,
             ctx,
             shard_index,
             deadline_ms,
             clock_ms,
             include_expired?,
             values
           ) do
      {:ok, Enum.map(prepared, &Map.get(values, &1.index))}
    end
  end

  defp read_bitcask([], _deadline_ms, _clock_ms), do: {:ok, %{}}

  defp read_bitcask(prepared, deadline_ms, clock_ms) do
    locations =
      Enum.map(prepared, fn %{source: {:bitcask, path, offset, key}} ->
        {path, offset, key}
      end)

    with {:ok, timeout_ms} <- remaining_timeout_ms(deadline_ms, clock_ms) do
      case ColdRead.pread_batch_keyed(locations, timeout_ms) do
        {:ok, values} when length(values) == length(prepared) ->
          put_raw_values(prepared, values, %{})

        {:ok, _values} ->
          {:error, :hydration_result_count_mismatch}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp read_waraft(
         [],
         _ctx,
         _shard_index,
         _deadline_ms,
         _clock_ms,
         _include_expired?,
         values
       ),
       do: {:ok, values}

  defp read_waraft(
         prepared,
         ctx,
         shard_index,
         deadline_ms,
         clock_ms,
         include_expired?,
         values
       ) do
    requests =
      Enum.map(prepared, fn %{
                              source: {:waraft, file_id, ordinal, offset, frame_size, state_key}
                            } ->
        %{
          file_id: file_id,
          ordinal: ordinal,
          offset: offset,
          frame_size: frame_size,
          key: state_key
        }
      end)

    with {:ok, timeout_ms} <- remaining_timeout_ms(deadline_ms, clock_ms) do
      read_mode = if include_expired?, do: :include_expired, else: :live

      result =
        WARaftSegmentReader.read_physical_values(
          ctx,
          shard_index,
          requests,
          timeout_ms,
          read_mode
        )

      case result do
        {:ok, values_by_key} when is_map(values_by_key) ->
          next =
            Enum.reduce(prepared, values, fn request, values_acc ->
              Map.put(values_acc, request.index, Map.get(values_by_key, request.state_key))
            end)

          {:ok, next}

        {:error, :deadline_exceeded} ->
          {:error, :hydration_timeout}

        {:error, _reason} = error ->
          error

        _invalid ->
          {:error, :invalid_hydration_read}
      end
    end
  end

  defp put_raw_values([], [], values), do: {:ok, values}

  defp put_raw_values([request | requests], [value | rest], values)
       when is_binary(value) or is_nil(value) do
    put_raw_values(requests, rest, Map.put(values, request.index, value))
  end

  defp put_raw_values(_prepared, _values, _acc), do: {:error, :invalid_hydration_read}

  defp validate_physical_value_sizes([], []), do: :ok

  defp validate_physical_value_sizes(
         [value | values],
         [%{locator: %Locator{value_size: expected_size}} | prepared]
       )
       when is_binary(value) and byte_size(value) == expected_size,
       do: validate_physical_value_sizes(values, prepared)

  defp validate_physical_value_sizes([nil | values], [_request | prepared]),
    do: validate_physical_value_sizes(values, prepared)

  defp validate_physical_value_sizes([value | _values], [%{locator: %Locator{}} | _prepared])
       when is_binary(value),
       do: {:error, :hydrated_record_size_mismatch}

  defp validate_physical_value_sizes(_values, _prepared),
    do: {:error, :hydration_result_count_mismatch}

  defp finalize_values(
         :storage_ref,
         raw_values,
         prepared,
         _ctx,
         _shard_index,
         _max_bytes,
         _deadline_ms,
         _clock_ms
       ) do
    validate_storage_refs(raw_values, prepared, [])
  end

  defp finalize_values(
         mode,
         raw_values,
         prepared,
         ctx,
         shard_index,
         max_bytes,
         deadline_ms,
         clock_ms
       )
       when mode in [:decoded, :encoded, :stored] do
    with :ok <- admit_materialized_bytes(raw_values, ctx, max_bytes),
         {:ok, values} <- materialize_values(raw_values, ctx, shard_index),
         :ok <- check_deadline(deadline_ms, clock_ms),
         :ok <- admit_actual_bytes(values, max_bytes),
         validation_mode = if(mode == :stored, do: :encoded, else: mode) do
      decode_and_validate(values, prepared, validation_mode)
    end
  end

  defp validate_storage_refs([], [], acc), do: {:ok, Enum.reverse(acc)}

  defp validate_storage_refs([nil | values], [_request | prepared], acc),
    do: validate_storage_refs(values, prepared, [nil | acc])

  defp validate_storage_refs(
         [stored | values],
         [%{locator: %Locator{checksum: checksum}} | prepared],
         acc
       )
       when is_binary(stored) and is_binary(checksum) and byte_size(checksum) == 32 do
    if storage_checksum_matches?(stored, checksum) do
      validate_storage_refs(values, prepared, [stored | acc])
    else
      {:error, :hydrated_record_identity_mismatch}
    end
  end

  defp validate_storage_refs(_values, _prepared, _acc),
    do: {:error, :hydration_result_count_mismatch}

  defp storage_checksum_matches?(stored, checksum) do
    case BlobRef.decode(stored) do
      {:ok, %BlobRef{checksum: blob_checksum}} ->
        :crypto.hash_equals(blob_checksum, checksum)

      :error ->
        checksum_matches?(checksum, stored)
    end
  end

  defp admit_materialized_bytes(values, ctx, max_bytes) do
    threshold = BlobValue.threshold(ctx)

    values
    |> Enum.reduce_while(0, fn value, total ->
      bounded_sum(total, estimated_materialized_bytes(value, threshold), max_bytes)
    end)
    |> admission_result()
  end

  defp estimated_materialized_bytes(value, threshold)
       when is_binary(value) and threshold > 0 do
    case BlobRef.decode(value) do
      {:ok, %{size: size}} when is_integer(size) and size >= 0 -> size
      :error -> byte_size(value)
    end
  end

  defp estimated_materialized_bytes(value, _threshold) when is_binary(value),
    do: byte_size(value)

  defp estimated_materialized_bytes(nil, _threshold), do: 0

  defp materialize_values(values, %{data_dir: data_dir} = ctx, shard_index) do
    results =
      BlobValue.maybe_materialize_many(
        data_dir,
        shard_index,
        BlobValue.threshold(ctx),
        values
      )

    Enum.reduce_while(results, {:ok, []}, fn
      {:ok, value}, {:ok, acc} when is_binary(value) or is_nil(value) ->
        {:cont, {:ok, [value | acc]}}

      {:error, reason}, _acc ->
        {:halt, {:error, {:blob_materialize_failed, reason}}}

      _invalid, _acc ->
        {:halt, {:error, :invalid_blob_materialize_result}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp admit_actual_bytes(values, max_bytes) do
    values
    |> Enum.reduce_while(0, fn
      value, total when is_binary(value) -> bounded_sum(total, byte_size(value), max_bytes)
      nil, total -> {:cont, total}
    end)
    |> admission_result()
  end

  defp bounded_sum(total, bytes, max_bytes)
       when is_integer(bytes) and bytes >= 0 and total + bytes <= max_bytes,
       do: {:cont, total + bytes}

  defp bounded_sum(_total, _bytes, _max_bytes), do: {:halt, :too_large}

  defp admission_result(:too_large), do: {:error, :hydration_byte_budget_exceeded}
  defp admission_result(_total), do: :ok

  defp decode_and_validate(values, prepared, mode)
       when length(values) == length(prepared) and mode in [:decoded, :encoded] do
    with {:ok, encoded_values} <- collect_checked_values(values, prepared, []),
         decoded when is_list(decoded) <- Codec.decode_records(encoded_values) do
      restore_validated_values(values, prepared, decoded, mode, [])
    end
  rescue
    _error -> {:error, :invalid_hydrated_record}
  end

  defp decode_and_validate(_values, _prepared, _mode),
    do: {:error, :hydration_result_count_mismatch}

  defp collect_checked_values([], [], acc), do: {:ok, Enum.reverse(acc)}

  defp collect_checked_values([nil | values], [_request | prepared], acc),
    do: collect_checked_values(values, prepared, acc)

  defp collect_checked_values([encoded | values], [request | prepared], acc)
       when is_binary(encoded) do
    if checksum_matches?(request.locator.checksum, encoded),
      do: collect_checked_values(values, prepared, [encoded | acc]),
      else: {:error, :hydrated_record_identity_mismatch}
  end

  defp collect_checked_values(_values, _prepared, _acc),
    do: {:error, :hydration_result_count_mismatch}

  defp restore_validated_values([], [], [], _mode, acc), do: {:ok, Enum.reverse(acc)}

  defp restore_validated_values(
         [nil | values],
         [_request | prepared],
         decoded,
         mode,
         acc
       ),
       do: restore_validated_values(values, prepared, decoded, mode, [nil | acc])

  defp restore_validated_values(
         [encoded | values],
         [request | prepared],
         [record | decoded],
         mode,
         acc
       )
       when is_binary(encoded) and is_map(record) do
    if valid_record?(record, request) do
      result = if mode == :decoded, do: record, else: encoded
      restore_validated_values(values, prepared, decoded, mode, [result | acc])
    else
      {:error, :hydrated_record_identity_mismatch}
    end
  end

  defp restore_validated_values(_values, _prepared, _decoded, _mode, _acc),
    do: {:error, :invalid_hydrated_record}

  defp valid_record?(record, %{state_key: state_key, locator: locator}) do
    Map.get(record, :id) == locator.flow_id and Map.get(record, :version) == locator.version and
      RecordIdentity.owns_state_key?(record, state_key)
  end

  defp checksum_matches?(checksum, encoded) when byte_size(checksum) == 32,
    do: :crypto.hash_equals(:crypto.hash(:sha256, encoded), checksum)

  defp checksum_matches?(_checksum, _encoded), do: false
end
