defmodule Ferricstore.Bench.QueryStorageFixture do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, LMDB, Locator, ProjectionLocator}
  alias Ferricstore.Flow.Query.{QueryRowCodec, SourceCatalog}
  alias Ferricstore.Raft.WARaftBackend.RuntimeSupervisor
  alias Ferricstore.Raft.WARaftSegmentReader

  @default_page_records 256
  @maximum_page_records 256

  @spec write!(map(), [map()], keyword()) :: %{
          source_bytes: non_neg_integer(),
          query_row_bytes: non_neg_integer(),
          source_catalog_bytes: non_neg_integer(),
          source_catalog_ops: [tuple()],
          encoded_by_key: %{binary() => binary()}
        }
  def write!(ctx, records, opts \\ [])

  def write!(ctx, records, opts)
      when is_map(ctx) and is_list(records) and records != [] and is_list(opts) do
    shard_index = Keyword.get(opts, :shard_index, 0)
    page_records = Keyword.get(opts, :page_records, @default_page_records)

    validate!(ctx, records, shard_index, page_records)
    :ok = RuntimeSupervisor.ensure_started()
    Ferricstore.DataDir.ensure_layout!(ctx.data_dir, ctx.shard_count)
    path = lmdb_path(ctx, shard_index)

    records
    |> Enum.chunk_every(page_records)
    |> Enum.reduce(empty_result(), fn page, total ->
      merge(total, write_page!(ctx, shard_index, path, page))
    end)
  end

  def write!(_ctx, _records, _opts),
    do: raise(ArgumentError, "query storage fixture requires a nonempty bounded record list")

  @spec cleanup(map(), non_neg_integer()) :: :ok
  def cleanup(%{data_dir: data_dir}, shard_index \\ 0)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    WARaftSegmentReader.clear_apply_projection_cache(data_dir, shard_index)
    :ok
  end

  defp write_page!(ctx, shard_index, path, records) do
    index = System.unique_integer([:positive, :monotonic])
    file_id = {:waraft_apply_projection, index}

    sources =
      Enum.map(records, fn record ->
        state_key = Keys.state_key(Map.fetch!(record, :id), Map.fetch!(record, :partition_key))
        encoded = Ferricstore.Flow.encode_record(record)
        %{state_key: state_key, record: record, encoded: encoded, expire_at_ms: 0}
      end)

    :ok =
      WARaftSegmentReader.put_apply_projection(
        ctx.data_dir,
        shard_index,
        index,
        Enum.map(sources, &{&1.state_key, &1.encoded, &1.expire_at_ms})
      )

    {:ok, durable_count} =
      WARaftSegmentReader.ensure_apply_projection_entries_durable(
        ctx.data_dir,
        shard_index,
        Enum.map(sources, &{index, &1.state_key})
      )

    true = durable_count == length(sources)

    {:ok, {segment_generation, frame_offset, frame_size}} =
      WARaftSegmentReader.physical_location(ctx, shard_index, file_id)

    {ops, result} =
      Enum.flat_map_reduce(sources, empty_result(), fn source, result ->
        {:ok, record, source_locator} =
          ProjectionLocator.decode_source(
            source.state_key,
            source.encoded,
            source.expire_at_ms,
            {file_id, frame_offset, byte_size(source.encoded)}
          )

        {:ok, locator} =
          Locator.relocate(source_locator,
            segment_generation: segment_generation,
            offset: frame_offset,
            frame_size: frame_size
          )

        true = Locator.hydration_ready?(locator)

        {:ok, query_row} =
          QueryRowCodec.encode(source.state_key, record, locator, source.expire_at_ms)

        type_catalog_key = Keys.type_catalog_member_key(source.record.type, source.state_key)

        {:ok, {:put, catalog_key, catalog_value}} =
          SourceCatalog.put_op(type_catalog_key, source.state_key)

        catalog_op = {:put_new, catalog_key, catalog_value}

        next = %{
          source_bytes:
            result.source_bytes + byte_size(source.state_key) + byte_size(source.encoded),
          query_row_bytes:
            result.query_row_bytes + byte_size(source.state_key) + byte_size(query_row),
          source_catalog_bytes:
            result.source_catalog_bytes + byte_size(catalog_key) + byte_size(catalog_value),
          source_catalog_ops: [catalog_op | result.source_catalog_ops],
          encoded_by_key: Map.put(result.encoded_by_key, source.state_key, source.encoded)
        }

        {[{:put, source.state_key, query_row}, catalog_op], next}
      end)

    :ok = LMDB.write_batch(path, ops)
    result
  end

  defp validate!(ctx, records, shard_index, page_records) do
    state_keys =
      Enum.map(records, fn record ->
        Keys.state_key(Map.fetch!(record, :id), Map.fetch!(record, :partition_key))
      end)

    valid =
      is_binary(Map.get(ctx, :data_dir)) and Map.get(ctx, :data_dir) != "" and
        is_integer(Map.get(ctx, :shard_count)) and Map.get(ctx, :shard_count) > 0 and
        is_integer(shard_index) and shard_index >= 0 and shard_index < ctx.shard_count and
        is_integer(page_records) and page_records > 0 and
        page_records <= @maximum_page_records and
        length(state_keys) == MapSet.size(MapSet.new(state_keys))

    if valid, do: :ok, else: raise(ArgumentError, "invalid query storage fixture")
  end

  defp empty_result,
    do: %{
      source_bytes: 0,
      query_row_bytes: 0,
      source_catalog_bytes: 0,
      source_catalog_ops: [],
      encoded_by_key: %{}
    }

  defp merge(left, right) do
    %{
      source_bytes: left.source_bytes + right.source_bytes,
      query_row_bytes: left.query_row_bytes + right.query_row_bytes,
      source_catalog_bytes: left.source_catalog_bytes + right.source_catalog_bytes,
      source_catalog_ops: right.source_catalog_ops ++ left.source_catalog_ops,
      encoded_by_key: Map.merge(left.encoded_by_key, right.encoded_by_key)
    }
  end

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end
end
