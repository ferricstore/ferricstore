# Measures the production covering-row projection cost across the launch catalog.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryCoveringWriteCost do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Flow.{Keys, LMDB}

  alias Ferricstore.Flow.Query.{
    CompositeIndex,
    CompositeProjection,
    CoveringCodec,
    Field,
    IndexCatalog,
    IndexDefinition
  }

  alias Ferricstore.TermCodec

  def run do
    {:ok, catalog} = IndexCatalog.load()
    covering = catalog.definitions

    plain =
      Enum.map(covering, fn definition ->
        definition
        |> Map.from_struct()
        |> Map.put(:version, 1)
        |> Map.put(:covering_fields, [])
        |> IndexDefinition.new!()
      end)

    migration = plain ++ covering
    records = Enum.map(1..64, &record/1)

    current = project(records, plain)
    candidate = project(records, covering)
    covers = covering_records(records, covering)
    current_covers = Enum.map(covers, &TermCodec.encode({:flow_composite_covering, 1, &1}))
    compact_covers = Enum.map(covers, &production_encode/1)

    true = length(current) == length(candidate)
    true = Enum.map(current_covers, &current_decode/1) == covers
    true = Enum.zip_with(compact_covers, covers, &production_decode/2) == covers

    root = temp_root()

    paths = %{
      plain: Path.join(root, "plain"),
      covering: Path.join(root, "covering"),
      migration: Path.join(root, "migration")
    }

    Enum.each(paths, fn {_name, path} -> File.mkdir_p!(path) end)

    try do
      plain_commit = reconcile_and_commit(paths.plain, records, plain)
      covering_commit = reconcile_and_commit(paths.covering, records, covering)
      migration_commit = reconcile_and_commit(paths.migration, records, migration)

      true =
        migration_commit.written_entries ==
          plain_commit.written_entries + covering_commit.written_entries

      IO.puts(
        "covering_write_cost records=#{length(records)} indexes=#{length(covering)} " <>
          "plain_bytes=#{entry_bytes(current)} covering_bytes=#{entry_bytes(candidate)} " <>
          "etf_cover_bytes=#{binary_bytes(current_covers)} " <>
          "compact_cover_bytes=#{binary_bytes(compact_covers)} " <>
          "plain_commit_bytes=#{plain_commit.written_bytes} " <>
          "covering_commit_bytes=#{covering_commit.written_bytes} " <>
          "migration_commit_bytes=#{migration_commit.written_bytes}"
      )

      jobs = %{
        "baseline plain projection/#{length(records)} records" => fn ->
          project(records, plain)
        end,
        "production covering projection/#{length(records)} records" => fn ->
          project(records, covering)
        end,
        "plain v1 reconcile + LMDB commit/#{length(records)} records" => fn ->
          reconcile_and_commit(paths.plain, records, plain)
        end,
        "covering v2 reconcile + LMDB commit/#{length(records)} records" => fn ->
          reconcile_and_commit(paths.covering, records, covering)
        end,
        "upgrade dual-generation reconcile + LMDB commit/#{length(records)} records" => fn ->
          reconcile_and_commit(paths.migration, records, migration)
        end,
        "legacy ETF cover encode/#{length(covers)} rows" => fn ->
          Enum.map(covers, &TermCodec.encode({:flow_composite_covering, 1, &1}))
        end,
        "production compact cover encode/#{length(covers)} rows" => fn ->
          Enum.map(covers, &production_encode/1)
        end,
        "legacy ETF cover decode/#{length(covers)} rows" => fn ->
          Enum.map(current_covers, &current_decode/1)
        end,
        "production compact cover decode/#{length(covers)} rows" => fn ->
          Enum.zip_with(compact_covers, covers, &production_decode/2)
        end
      }

      options =
        "query-covering-write-cost"
        |> QueryPerformance.benchee_options()
        |> Keyword.put(:parallel, 1)

      Benchee.run(jobs, options)
    after
      Enum.each(paths, fn {_name, path} -> Ferricstore.Bitcask.NIF.lmdb_release(path) end)
      File.rm_rf!(root)
    end
  end

  defp reconcile_and_commit(path, records, definitions) do
    state_keys = Enum.map(records, &Keys.state_key(&1.id, &1.partition_key))

    {:ok, cache} =
      CompositeProjection.prefetch_reverse_values(
        path,
        state_keys,
        CompositeProjection.new_cache()
      )

    {reversed, _cache} =
      Enum.reduce(Enum.zip(records, state_keys), {[], cache}, fn {record, state_key},
                                                                 {acc, cache} ->
        {:ok, ops, cache} =
          CompositeProjection.reconcile(path, state_key, record, 0, definitions, cache)

        {:lists.reverse(ops, acc), cache}
      end)

    ops = Enum.reverse(reversed)
    :ok = LMDB.write_batch(path, ops)

    %{
      write_ops: length(ops),
      written_entries:
        Enum.count(ops, fn
          {:put, key, _value} -> String.starts_with?(key, IndexDefinition.global_storage_prefix())
          _operation -> false
        end),
      written_bytes:
        Enum.reduce(ops, 0, fn
          {:put, key, value}, bytes -> bytes + byte_size(key) + byte_size(value)
          _operation, bytes -> bytes
        end)
    }
  end

  defp project(records, definitions) do
    Enum.flat_map(records, fn record ->
      state_key = Keys.state_key(record.id, record.partition_key)

      Enum.flat_map(definitions, fn definition ->
        {:ok, entries} = CompositeIndex.entries_validated(definition, record, state_key, 0)
        entries
      end)
    end)
  end

  defp covering_records(records, definitions) do
    for record <- records, definition <- definitions do
      Enum.reduce(definition.covering_fields, %{id: record.id, version: record.version}, fn
        field, cover when field in [:run_id, :version] ->
          cover

        field, cover ->
          case Field.fetch(record, field) do
            {:ok, value} -> Map.put(cover, field, value)
            :missing -> cover
          end
      end)
    end
  end

  defp production_encode(record) do
    {:ok, encoded} = CoveringCodec.encode(record)
    encoded
  end

  defp production_decode(encoded, record) do
    {:ok, decoded} = CoveringCodec.decode(encoded, record.id, record.version)
    decoded
  end

  defp current_decode(encoded) do
    {:ok, {:flow_composite_covering, 1, record}} = TermCodec.decode(encoded)
    record
  end

  defp entry_bytes(entries) do
    Enum.reduce(entries, 0, fn entry, bytes ->
      bytes + byte_size(entry.key) + byte_size(entry.value)
    end)
  end

  defp binary_bytes(values), do: Enum.reduce(values, 0, &(byte_size(&1) + &2))

  defp temp_root do
    Path.join(
      System.tmp_dir!(),
      "ferricstore-covering-write-cost-#{System.unique_integer([:positive, :monotonic])}"
    )
  end

  defp record(number) do
    id = "run-#{String.pad_leading(Integer.to_string(number), 8, "0")}"

    %{
      id: id,
      partition_key: "tenant-a",
      type: "invoice",
      state: "queued",
      version: number,
      priority: rem(number, 10),
      created_at_ms: number,
      updated_at_ms: number,
      next_run_at_ms: number,
      lease_deadline_ms: number + 1_000,
      attempts: 0,
      run_state: "ready",
      max_active_ms: nil,
      parent_flow_id: nil,
      root_flow_id: nil,
      correlation_id: nil,
      attributes: %{},
      state_meta: %{}
    }
  end
end

Ferricstore.Bench.QueryCoveringWriteCost.run()
