defmodule Ferricstore.Bench.QuerySoakFixture do
  @moduledoc false

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.{Codec, Keys, LMDB}

  alias Ferricstore.Flow.Query.{
    CompositeBackfill,
    MandatoryScope,
    RegisteredIndex
  }

  alias Ferricstore.Bench.QueryDataset
  alias Ferricstore.Flow.Query.IndexCatalog

  @projection_page_records 16
  @source_page_records 256

  @spec prepare!(pos_integer(), keyword()) :: map()
  def prepare!(record_count, opts \\ [])
      when is_integer(record_count) and record_count > 0 and is_list(opts) do
    suffix = System.unique_integer([:positive, :monotonic])

    data_dir =
      Keyword.get_lazy(opts, :data_dir, fn ->
        Path.join(System.tmp_dir!(), "ferricstore_query_shape_soak_#{suffix}")
      end)

    ctx = context(data_dir)

    try do
      {:ok, catalog} = IndexCatalog.load()
      records = QueryDataset.records(record_count)
      source_bytes = write_source_records(ctx, records)
      encoded_by_key = encoded_records(records)
      read_entries = read_entries_fun(encoded_by_key)

      {projection_us, projection} =
        timed(fn -> project_all(ctx, records, catalog.definitions, read_entries) end)

      %{
        ctx: ctx,
        data_dir: data_dir,
        path: lmdb_path(ctx),
        catalog: catalog,
        definitions: catalog.definitions,
        indexes: Enum.map(catalog.definitions, &active_index/1),
        records: records,
        source_bytes: source_bytes,
        projection: Map.put(projection, :elapsed_us, projection_us)
      }
    rescue
      error ->
        File.rm_rf!(data_dir)
        reraise error, __STACKTRACE__
    end
  end

  @spec cleanup(map()) :: :ok
  def cleanup(%{data_dir: data_dir}) when is_binary(data_dir) do
    File.rm_rf!(data_dir)
    :ok
  end

  defp project_all(ctx, records, definitions, read_entries) do
    records
    |> Enum.chunk_every(@projection_page_records)
    |> Enum.reduce(empty_metrics(), fn page, total ->
      projected =
        Enum.map(page, fn record ->
          %{
            state_key: Keys.state_key(record.id, record.partition_key),
            record: record,
            expire_at_ms: 0
          }
        end)

      {:ok, metrics} =
        CompositeBackfill.project_page(ctx, 0, projected, definitions,
          projection_definitions: definitions,
          read_entries_fun: read_entries
        )

      merge_metrics(total, metrics)
    end)
  end

  defp write_source_records(ctx, records) do
    records
    |> Enum.chunk_every(@source_page_records)
    |> Enum.reduce(0, fn page, total_bytes ->
      ops =
        Enum.map(page, fn record ->
          key = Keys.state_key(record.id, record.partition_key)
          value = LMDB.encode_value(Codec.encode_record(record), 0)
          {:put, key, value}
        end)

      :ok = LMDB.write_batch(lmdb_path(ctx), ops)

      total_bytes +
        Enum.reduce(ops, 0, fn {:put, key, value}, bytes ->
          bytes + byte_size(key) + byte_size(value)
        end)
    end)
  end

  defp encoded_records(records) do
    Map.new(records, fn record ->
      {Keys.state_key(record.id, record.partition_key), Codec.encode_record(record)}
    end)
  end

  defp read_entries_fun(encoded_by_key) do
    fn _ctx, 0, keys ->
      {:ok,
       Enum.map(keys, fn key ->
         case Map.fetch(encoded_by_key, key) do
           {:ok, encoded} -> {encoded, 0}
           :error -> nil
         end
       end)}
    end
  end

  defp active_index(definition) do
    RegisteredIndex.new!(definition, :active,
      coverage: %{complete_shards: 1, total_shards: 1, validation: :passed}
    )
  end

  defp empty_metrics do
    %{projected_records: 0, written_entries: 0, write_ops: 0, written_bytes: 0}
  end

  defp merge_metrics(left, right) do
    Map.new(left, fn {field, value} -> {field, value + Map.fetch!(right, field)} end)
  end

  defp timed(fun) do
    started = System.monotonic_time(:microsecond)
    result = fun.()
    {System.monotonic_time(:microsecond) - started, result}
  end

  defp lmdb_path(ctx) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end

  defp context(data_dir) do
    {:ok, metadata_snapshot} =
      MetadataExtension.configure(FerricStore.Flow.MetadataExtension.Disabled, [])

    %{
      name: :flow_query_shape_soak,
      data_dir: data_dir,
      shard_count: 1,
      slot_map: List.to_tuple(List.duplicate(0, 1_024)),
      flow_metadata_snapshot: metadata_snapshot,
      query_mandatory_scope: MandatoryScope.dedicated()
    }
  end
end
