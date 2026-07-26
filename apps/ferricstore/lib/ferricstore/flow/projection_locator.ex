defmodule Ferricstore.Flow.ProjectionLocator do
  @moduledoc false

  alias Ferricstore.Flow.{Codec, Keys, Locator, RecordIdentity}

  @waraft_location_kinds [:waraft_segment, :waraft_projection, :waraft_apply_projection]

  @spec decode_source(binary(), binary(), non_neg_integer(), {term(), term(), term()}) ::
          {:ok, map(), Locator.t()}
          | {:error,
             :invalid_source_flow_record
             | :source_flow_identity_mismatch
             | {:source_location_unavailable, binary()}}
  def decode_source(state_key, encoded_record, expire_at_ms, {file_id, offset, value_size})
      when is_binary(state_key) and is_binary(encoded_record) do
    with {:ok, record} <- decode_owned_record(state_key, encoded_record),
         {:ok, locator} <-
           build_locator(
             state_key,
             record,
             encoded_record,
             expire_at_ms,
             file_id,
             offset,
             value_size
           ) do
      {:ok, record, locator}
    end
  rescue
    _error -> {:error, :invalid_source_flow_record}
  end

  def decode_source(_state_key, _encoded_record, _expire_at_ms, _source_location),
    do: {:error, :invalid_source_flow_record}

  @spec decode_source_at_least(
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          {term(), term(), term()}
        ) ::
          {:ok, map(), Locator.t()}
          | {:stale, non_neg_integer()}
          | {:error,
             :invalid_source_flow_record
             | :source_flow_identity_mismatch
             | {:source_location_unavailable, binary()}}
  def decode_source_at_least(
        state_key,
        encoded_record,
        expected_version,
        expire_at_ms,
        {file_id, offset, value_size}
      )
      when is_binary(state_key) and is_binary(encoded_record) and is_integer(expected_version) and
             expected_version >= 0 do
    with {:ok, record} <- decode_owned_record(state_key, encoded_record) do
      if record.version < expected_version do
        {:stale, record.version}
      else
        with {:ok, locator} <-
               build_locator(
                 state_key,
                 record,
                 encoded_record,
                 expire_at_ms,
                 file_id,
                 offset,
                 value_size
               ) do
          {:ok, record, locator}
        end
      end
    end
  rescue
    _error -> {:error, :invalid_source_flow_record}
  end

  def decode_source_at_least(
        _state_key,
        _encoded_record,
        _expected_version,
        _expire_at_ms,
        _source_location
      ),
      do: {:error, :invalid_source_flow_record}

  defp decode_owned_record(state_key, encoded_record) do
    with record when is_map(record) <- Codec.decode_record(encoded_record),
         id when is_binary(id) and id != "" <- Map.get(record, :id),
         version when is_integer(version) and version >= 0 <- Map.get(record, :version),
         true <- RecordIdentity.owns_state_key?(record, state_key) do
      {:ok, record}
    else
      false -> {:error, :source_flow_identity_mismatch}
      _invalid -> {:error, :invalid_source_flow_record}
    end
  end

  defp build_locator(
         state_key,
         record,
         encoded_record,
         expire_at_ms,
         file_id,
         offset,
         value_size
       ) do
    with id when is_binary(id) and id != "" <- Map.get(record, :id),
         version when is_integer(version) and version >= 0 <- Map.get(record, :version),
         {:ok, raft_index} <- source_raft_index(file_id, version),
         :ok <- validate_source_location(offset, value_size),
         checksum = :crypto.hash(:sha256, encoded_record),
         {:ok, locator} <-
           Locator.new(
             flow_id: id,
             kind: :state,
             version: version,
             raft_index: raft_index,
             file_id: file_id,
             offset: offset,
             value_size: value_size,
             checksum: checksum,
             expire_at_ms: expire_at_ms
           ) do
      {:ok, locator}
    else
      {:error, :source_location_unavailable} -> unavailable(state_key)
      {:error, :bad_locator} -> unavailable(state_key)
      _invalid -> {:error, :invalid_source_flow_record}
    end
  end

  defp validate_source_location(offset, value_size)
       when is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size > 0,
       do: :ok

  defp validate_source_location(_offset, _value_size),
    do: {:error, :source_location_unavailable}

  defp source_raft_index({kind, index}, _version)
       when kind in @waraft_location_kinds and is_integer(index) and index > 0,
       do: {:ok, index}

  defp source_raft_index(file_id, version)
       when is_integer(file_id) and file_id >= 0 and is_integer(version) and version >= 0,
       do: {:ok, version}

  defp source_raft_index(_file_id, _version), do: {:error, :source_location_unavailable}

  defp unavailable(state_key) do
    case Keys.run_id_from_state_key(state_key) do
      {:ok, id} -> {:error, {:source_location_unavailable, id}}
      _invalid -> {:error, {:source_location_unavailable, state_key}}
    end
  end
end
