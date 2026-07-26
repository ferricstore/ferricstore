Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryRowCodec do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Flow.{Keys, Locator}

  alias Ferricstore.Flow.Query.{
    QueryRowCodec,
    QueryRowDecodePlan,
    QueryRowStore,
    Field,
    RecordProjection,
    ResultCodec
  }

  @page_sizes [1, 25, 100]
  @shapes [:sparse, :typical, :metadata_heavy]

  def run do
    page_sizes = QueryPerformance.integer_list_env("BENCH_CODEC_PAGE_SIZES", @page_sizes)
    shapes = selected_shapes()
    legacy_codec = load_legacy_codec()

    inputs =
      Map.new(shapes, fn shape ->
        {Atom.to_string(shape), dataset(shape, Enum.max(page_sizes))}
      end)

    Enum.each(inputs, fn {_shape, dataset} -> preflight!(dataset, page_sizes, legacy_codec) end)

    jobs =
      Enum.reduce(page_sizes, %{}, fn page_size, jobs ->
        jobs
        |> Map.put("query-row encode/page-#{page_size}", fn dataset ->
          encode_page(dataset, page_size)
        end)
        |> Map.put("query-row decode/page-#{page_size}", fn dataset ->
          decode_page(dataset, page_size)
        end)
        |> Map.put("query-row selective decode/page-#{page_size}", fn dataset ->
          decode_query_page(dataset, page_size, %QueryRowDecodePlan{
            attributes: :none,
            state_meta: :none
          })
        end)
        |> Map.put("query-row attributes decode/page-#{page_size}", fn dataset ->
          decode_query_page(dataset, page_size, %QueryRowDecodePlan{
            attributes: :all,
            state_meta: :none
          })
        end)
        |> Map.put("query-row dynamic sections decode/page-#{page_size}", fn dataset ->
          decode_query_page(dataset, page_size, %QueryRowDecodePlan{
            attributes: :all,
            state_meta: :all
          })
        end)
        |> Map.put("query-row exact fields decode/page-#{page_size}", fn dataset ->
          decode_query_page(dataset, page_size, dataset.exact_decode_plan)
        end)
        |> Map.put("query-row reference decode/page-#{page_size}", fn dataset ->
          decode_reference_page(dataset, page_size)
        end)
        |> Map.put("query-row batch decode/page-#{page_size}", fn dataset ->
          decode_batch(dataset, page_size)
        end)
        |> Map.put("result encode/page-#{page_size}", fn dataset ->
          dataset
          |> response(page_size)
          |> ResultCodec.encode()
        end)
        |> Map.put("result encode with size/page-#{page_size}", fn dataset ->
          dataset
          |> response(page_size)
          |> ResultCodec.encode_with_size()
        end)
        |> Map.put("result measure + encode/page-#{page_size}", fn dataset ->
          response = response(dataset, page_size)
          size = ResultCodec.encoded_size(response)
          payload = ResultCodec.encode(response)
          ^size = byte_size(payload)
          payload
        end)
        |> maybe_add_legacy_jobs(legacy_codec, page_size)
      end)

    Benchee.run(
      jobs,
      [inputs: inputs] ++ QueryPerformance.benchee_options("query-row-codec")
    )
  end

  defp preflight!(dataset, page_sizes, legacy_codec) do
    Enum.each(page_sizes, fn page_size ->
      expected = Enum.take(dataset.records, page_size)

      ^expected =
        dataset
        |> decode_page(page_size)
        |> Enum.map(& &1.record)

      lean =
        decode_query_page(dataset, page_size, %QueryRowDecodePlan{
          attributes: :none,
          state_meta: :none
        })

      true =
        Enum.all?(lean, fn row ->
          not Map.has_key?(row.record, :attributes) and
            not Map.has_key?(row.record, :state_meta)
        end)

      exact = decode_query_page(dataset, page_size, dataset.exact_decode_plan)

      true =
        exact
        |> Enum.zip(expected)
        |> Enum.all?(fn {row, record} ->
          not Map.has_key?(row.record, :attributes) and
            not Map.has_key?(row.record, :state_meta) and
            Enum.all?(exact_fields(dataset.exact_decode_plan), fn field ->
              Field.fetch(row.record, field) == Field.fetch(record, field)
            end)
        end)

      references = decode_reference_page(dataset, page_size)
      true = length(references) == page_size

      {:ok, decoded} = decode_batch(dataset, page_size)
      ^expected = Enum.map(decoded, & &1.record)

      response = response(dataset, page_size)
      payload = ResultCodec.encode(response)
      true = is_binary(payload)
      true = ResultCodec.encoded_size(response) == byte_size(payload)

      preflight_legacy!(dataset, page_size, legacy_codec)
    end)
  end

  defp preflight_legacy!(_dataset, _page_size, nil), do: :ok

  defp preflight_legacy!(dataset, page_size, legacy_codec) do
    expected_encoded = encode_page(dataset, page_size)
    ^expected_encoded = encode_page(dataset, page_size, legacy_codec)

    expected_decoded = decode_page(dataset, page_size)
    ^expected_decoded = decode_page(dataset, page_size, legacy_codec)
    :ok
  end

  defp encode_page(dataset, page_size, codec \\ QueryRowCodec) do
    dataset.rows
    |> Enum.take(page_size)
    |> Enum.map(fn {state_key, record, locator, expire_at_ms, _encoded} ->
      {:ok, encoded} = codec.encode(state_key, record, locator, expire_at_ms)
      encoded
    end)
  end

  defp decode_page(dataset, page_size, codec \\ QueryRowCodec) do
    dataset.rows
    |> Enum.take(page_size)
    |> Enum.map(fn {state_key, _record, _locator, _expire_at_ms, encoded} ->
      {:ok, decoded} = codec.decode(encoded, state_key, 0)
      decoded
    end)
  end

  defp decode_reference_page(dataset, page_size) do
    dataset.rows
    |> Enum.take(page_size)
    |> Enum.map(fn {state_key, _record, _locator, _expire_at_ms, encoded} ->
      {:ok, decoded} = QueryRowCodec.decode_reference(encoded, state_key, 0)
      decoded
    end)
  end

  defp decode_query_page(dataset, page_size, decode_plan) do
    dataset.rows
    |> Enum.take(page_size)
    |> Enum.map(fn {state_key, _record, _locator, _expire_at_ms, encoded} ->
      {:ok, decoded} = QueryRowCodec.decode_for_query(encoded, state_key, 0, decode_plan)
      decoded
    end)
  end

  defp decode_batch(dataset, page_size) do
    rows = Enum.take(dataset.rows, page_size)
    state_keys = Enum.map(rows, &elem(&1, 0))
    values = Enum.map(rows, &elem(&1, 4))
    QueryRowStore.decode_many(state_keys, values, 0)
  end

  defp response(dataset, page_size) do
    records = Enum.take(dataset.result_records, page_size)

    %{
      version: ResultCodec.contract(),
      records: records,
      page: %{has_more: false, cursor: nil},
      quality: %{
        exactness: "projected_exact",
        freshness: "projection_watermark",
        coverage: "complete",
        pagination: "none"
      },
      usage: %{
        range_seeks: 1,
        range_pages: 1,
        scanned_entries: page_size,
        scanned_bytes: 0,
        hydrated_records: 0,
        residual_checks: 0,
        duplicate_entries: 0,
        result_records: page_size,
        response_bytes: 0,
        memory_high_water_bytes: 0,
        wall_time_us: 1
      }
    }
  end

  defp dataset(shape, count) do
    rows =
      Enum.map(1..count, fn ordinal ->
        record = record(shape, ordinal)
        state_key = Keys.state_key(record.id, record.partition_key)
        locator = locator(record.id, record.version, ordinal)
        {:ok, encoded} = QueryRowCodec.encode(state_key, record, locator, 0)
        {state_key, record, locator, 0, encoded}
      end)

    records = Enum.map(rows, &elem(&1, 1))
    {:ok, result_records} = RecordProjection.project_records(records, :runs, :all)

    %{
      rows: rows,
      records: records,
      result_records: result_records,
      exact_decode_plan: exact_decode_plan(shape)
    }
  end

  defp exact_decode_plan(:metadata_heavy) do
    %QueryRowDecodePlan{
      attributes: MapSet.new(["attribute-1"]),
      state_meta: %{"state-1" => MapSet.new(["metadata-1"])}
    }
  end

  defp exact_decode_plan(_shape) do
    %QueryRowDecodePlan{
      attributes: MapSet.new(["customer"]),
      state_meta: %{"queued" => MapSet.new(["worker"])}
    }
  end

  defp exact_fields(%QueryRowDecodePlan{attributes: attributes, state_meta: state_meta}) do
    attribute_fields = Enum.map(attributes, &{:attribute, &1})

    state_meta_fields =
      Enum.flat_map(state_meta, fn {state, names} ->
        Enum.map(names, &{:state_meta, state, &1})
      end)

    attribute_fields ++ state_meta_fields
  end

  defp record(shape, ordinal) do
    id = "run-#{ordinal}"

    base = %{
      id: id,
      type: "invoice",
      state: "queued",
      version: ordinal,
      partition_key: "tenant-a",
      root_flow_id: id
    }

    enrich(base, shape, ordinal)
  end

  defp enrich(record, :sparse, _ordinal), do: record

  defp enrich(record, :typical, ordinal) do
    Map.merge(record, %{
      priority: rem(ordinal, 10),
      created_at_ms: ordinal * 10,
      updated_at_ms: ordinal * 10 + 1,
      next_run_at_ms: ordinal * 10 + 2,
      attempts: rem(ordinal, 4),
      run_state: "ready",
      correlation_id: "correlation-#{ordinal}",
      attributes: %{
        "customer" => "customer-#{ordinal}",
        "region" => "eu-west",
        "labels" => ["urgent", "finance"]
      },
      indexed_attributes: ["customer", "region"],
      state_meta: %{
        "queued" => %{"worker" => "worker-#{ordinal}", "attempt" => ordinal}
      },
      indexed_state_meta: "worker"
    })
  end

  defp enrich(record, :metadata_heavy, ordinal) do
    attributes =
      Map.new(1..16, fn index ->
        {"attribute-#{index}", String.duplicate(<<65 + rem(index, 26)>>, 64)}
      end)

    state_meta =
      Map.new(1..4, fn state ->
        {"state-#{state}",
         Map.new(1..8, fn index ->
           {"metadata-#{index}", "#{ordinal}-" <> String.duplicate("m", 48)}
         end)}
      end)

    record
    |> enrich(:typical, ordinal)
    |> Map.merge(%{
      attributes: attributes,
      indexed_attributes: ["attribute-1", "attribute-2"],
      state_meta: state_meta,
      indexed_state_meta: "metadata-1"
    })
  end

  defp locator(id, version, ordinal) do
    Locator.new!(
      flow_id: id,
      kind: :state,
      version: version,
      raft_index: ordinal,
      file_id: 1,
      offset: ordinal * 4_096,
      value_size: 1_024,
      frame_size: 2_048,
      checksum: :crypto.hash(:sha256, id),
      expire_at_ms: nil,
      segment_generation: 1
    )
  end

  defp maybe_add_legacy_jobs(jobs, nil, _page_size), do: jobs

  defp maybe_add_legacy_jobs(jobs, legacy_codec, page_size) do
    jobs
    |> Map.put("legacy query-row encode/page-#{page_size}", fn dataset ->
      encode_page(dataset, page_size, legacy_codec)
    end)
    |> Map.put("legacy query-row decode/page-#{page_size}", fn dataset ->
      decode_page(dataset, page_size, legacy_codec)
    end)
  end

  defp load_legacy_codec do
    case System.get_env("BENCH_LEGACY_QUERY_ROW_CODEC") do
      nil ->
        nil

      path ->
        Code.require_file(path)
        Ferricstore.Bench.LegacyQueryRowCodec
    end
  end

  defp selected_shapes do
    case System.get_env("BENCH_CODEC_SHAPES") do
      nil ->
        @shapes

      raw ->
        shapes =
          raw
          |> String.split(",", trim: true)
          |> Enum.map(&(&1 |> String.trim() |> String.to_existing_atom()))

        if shapes != [] and Enum.all?(shapes, &(&1 in @shapes)),
          do: shapes,
          else: raise(ArgumentError, "BENCH_CODEC_SHAPES contains an unsupported row shape")
    end
  end
end

Ferricstore.Bench.QueryRowCodec.run()
