# Benchmark-only prototypes for LMDB query-planner changes. None of the
# candidate codecs or cleanup paths are used by production code.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerLMDBCandidates.CompactCodec do
  @moduledoc false

  alias Ferricstore.Flow.LMDB

  @version 1
  @nil_state 0x01
  @has_discovery 0x02
  @digest_component_tag <<0xFF, 0x01>>
  @max_raw_identity_component_bytes 256
  @max_discovery_component_bytes 1_024
  @max_u32 4_294_967_295

  def entry(index_key, id, updated_at_ms, expire_at_ms, state_key, discovery_component \\ nil)
      when is_binary(index_key) and is_binary(id) and is_integer(updated_at_ms) and
             updated_at_ms >= 0 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             (is_binary(state_key) or is_nil(state_key)) and
             (is_binary(discovery_component) or is_nil(discovery_component)) do
    prefix = LMDB.query_index_prefix(index_key)
    id_component = bounded_component(id)
    encoded_state = state_key || ""
    encoded_discovery = discovery_component || ""
    flags = if(is_nil(state_key), do: @nil_state, else: 0)
    flags = if(is_nil(discovery_component), do: flags, else: Bitwise.bor(flags, @has_discovery))

    if byte_size(id) > @max_u32 or byte_size(encoded_discovery) > @max_discovery_component_bytes do
      raise ArgumentError, "benchmark compact row exceeds its length fields"
    end

    key = prefix <> <<updated_at_ms::unsigned-big-64, id_component::binary>>

    value =
      <<@version, flags, expire_at_ms::unsigned-big-64, byte_size(id)::unsigned-big-32,
        byte_size(encoded_discovery)::unsigned-big-16, id::binary, encoded_state::binary,
        encoded_discovery::binary>>

    {key, value}
  end

  def decode_entries(entries, prefix, now_ms) do
    entries
    |> Enum.reduce_while({:ok, []}, fn
      {key, value}, {:ok, acc} ->
        case decode_entry(key, value, prefix) do
          {:ok, id, updated_at_ms, expire_at_ms, state_key, _discovery_component}
          when expire_at_ms <= 0 or expire_at_ms > now_ms ->
            {:cont, {:ok, [{id, updated_at_ms, state_key} | acc]}}

          {:ok, _id, _updated_at_ms, _expire_at_ms, _state_key, _discovery_component} ->
            {:cont, {:ok, acc}}

          :error ->
            {:halt, {:error, :invalid_compact_query_index_entry}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_compact_query_index_entry}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  def decode_discovery_entries(entries, raw_prefix, index_key_prefix, now_ms)
      when is_list(entries) and is_binary(raw_prefix) and is_binary(index_key_prefix) and
             is_integer(now_ms) and now_ms >= 0 do
    if raw_prefix == LMDB.query_index_raw_prefix(index_key_prefix) do
      entries
      |> Enum.reduce_while({:ok, []}, fn
        {key, value}, {:ok, acc} when is_binary(key) and is_binary(value) ->
          case decode_discovery_entry(key, value, raw_prefix, index_key_prefix) do
            {:ok, id, updated_at_ms, expire_at_ms, state_key, discovery_component}
            when expire_at_ms <= 0 or expire_at_ms > now_ms ->
              {:cont, {:ok, [{discovery_component, id, updated_at_ms, state_key} | acc]}}

            {:ok, _id, _updated_at_ms, _expire_at_ms, _state_key, _discovery_component} ->
              {:cont, {:ok, acc}}

            :error ->
              {:halt, {:error, :invalid_compact_query_index_entry}}
          end

        _invalid, _acc ->
          {:halt, {:error, :invalid_compact_query_index_entry}}
      end)
      |> case do
        {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
        {:error, _reason} = error -> error
      end
    else
      {:error, :invalid_compact_query_index_prefix}
    end
  end

  def decode_discovery_entries(_entries, _raw_prefix, _index_key_prefix, _now_ms),
    do: {:error, :invalid_compact_query_index_entries}

  defp decode_entry(key, value, prefix) do
    prefix_bytes = byte_size(prefix)

    with <<candidate_prefix::binary-size(prefix_bytes), updated_at_ms::unsigned-big-64,
           key_id::binary>> <- key,
         true <- candidate_prefix == prefix,
         {:ok, id, expire_at_ms, state_key, discovery_component} <- decode_value(value),
         true <- bounded_component(id) == key_id do
      {:ok, id, updated_at_ms, expire_at_ms, state_key, discovery_component}
    else
      _invalid -> :error
    end
  end

  defp decode_discovery_entry(key, value, raw_prefix, index_key_prefix) do
    raw_prefix_bytes = byte_size(raw_prefix)

    with <<candidate_prefix::binary-size(raw_prefix_bytes), index_digest::binary-size(32), 0,
           updated_at_ms::unsigned-big-64, key_id::binary>> <- key,
         true <- candidate_prefix == raw_prefix,
         {:ok, id, expire_at_ms, state_key, discovery_component} <- decode_value(value),
         true <- is_binary(discovery_component),
         true <- :crypto.hash(:sha256, index_key_prefix <> discovery_component) == index_digest,
         true <- bounded_component(id) == key_id do
      {:ok, id, updated_at_ms, expire_at_ms, state_key, discovery_component}
    else
      _invalid -> :error
    end
  end

  defp decode_value(value) do
    allowed_flags = Bitwise.bor(@nil_state, @has_discovery)

    with <<@version, flags, expire_at_ms::unsigned-big-64, id_bytes::unsigned-big-32,
           discovery_bytes::unsigned-big-16, payload::binary>> <- value,
         true <- Bitwise.band(flags, Bitwise.bnot(allowed_flags)) == 0,
         true <- discovery_bytes <= @max_discovery_component_bytes,
         state_bytes when state_bytes >= 0 <- byte_size(payload) - id_bytes - discovery_bytes,
         <<id::binary-size(id_bytes), encoded_state::binary-size(state_bytes),
           encoded_discovery::binary-size(discovery_bytes)>> <- payload,
         true <- Bitwise.band(flags, @nil_state) == 0 or encoded_state == "",
         true <- Bitwise.band(flags, @has_discovery) != 0 or encoded_discovery == "" do
      state_key = if Bitwise.band(flags, @nil_state) == 0, do: encoded_state, else: nil

      discovery_component =
        if Bitwise.band(flags, @has_discovery) == 0, do: nil, else: encoded_discovery

      {:ok, id, expire_at_ms, state_key, discovery_component}
    else
      _invalid -> :error
    end
  end

  defp bounded_component(value) do
    if byte_size(value) <= @max_raw_identity_component_bytes and
         not String.starts_with?(value, @digest_component_tag) do
      value
    else
      @digest_component_tag <> :crypto.hash(:sha256, value)
    end
  end
end

defmodule Ferricstore.Bench.QueryPlannerLMDBCandidates do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Bench.QueryPlannerLMDBCandidates.CompactCodec
  alias Ferricstore.Flow.{Keys, LMDB, LMDBIndexDecode}

  @index_key "parent:benchmark"
  @page_sizes [25, 100, 4_096]
  @dataset_entries 100_000
  @batch_size 5_000

  def run do
    case System.get_env("BENCH_CANDIDATE_SECTION", "all") do
      "all" ->
        benchmark_codec_section()
        benchmark_expiry_cleanup()

      "codec" ->
        benchmark_codec_section()

      "codec-shapes" ->
        benchmark_codec_shapes()

      "expiry" ->
        benchmark_expiry_cleanup()

      invalid ->
        raise ArgumentError,
              "BENCH_CANDIDATE_SECTION must be all, codec, codec-shapes, or expiry; " <>
                "got #{inspect(invalid)}"
    end
  end

  defp benchmark_codec_section do
    dataset = build_codec_dataset()

    try do
      print_storage(dataset)
      benchmark_codec(dataset)
      benchmark_codec_shapes()
    after
      cleanup_dataset(dataset)
    end
  end

  defp build_codec_dataset do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-query-candidates-#{System.unique_integer([:positive])}"
      )

    current_path = Path.join(root, "current")
    compact_path = Path.join(root, "compact")
    File.mkdir_p!(current_path)
    File.mkdir_p!(compact_path)
    prefix = LMDB.query_index_prefix(@index_key)

    1..@dataset_entries
    |> Stream.chunk_every(@batch_size)
    |> Enum.each(fn indexes ->
      current_ops =
        Enum.map(indexes, fn index ->
          {key, value} = current_entry(index)
          {:put, key, value}
        end)

      compact_ops =
        Enum.map(indexes, fn index ->
          {key, value} = compact_entry(index)
          {:put, key, value}
        end)

      :ok = LMDB.write_batch(current_path, current_ops)
      :ok = LMDB.write_batch(compact_path, compact_ops)
    end)

    current_entries = Map.new(@page_sizes, &{&1, read_page!(current_path, prefix, &1)})
    compact_entries = Map.new(@page_sizes, &{&1, read_page!(compact_path, prefix, &1)})

    Enum.each(@page_sizes, fn page_size ->
      {:ok, current_decoded} =
        LMDBIndexDecode.query_entries_readonly(Map.fetch!(current_entries, page_size), 1)

      {:ok, compact_decoded} =
        CompactCodec.decode_entries(Map.fetch!(compact_entries, page_size), prefix, 1)

      true = current_decoded == compact_decoded
    end)

    %{
      root: root,
      current_path: current_path,
      compact_path: compact_path,
      prefix: prefix,
      current_entries: current_entries,
      compact_entries: compact_entries
    }
  end

  defp print_storage(dataset) do
    {current_key, current_value} = current_entry(1)
    {compact_key, compact_value} = compact_entry(1)
    current_bytes = QueryPerformance.directory_bytes(dataset.current_path)
    compact_bytes = QueryPerformance.directory_bytes(dataset.compact_path)

    IO.puts(
      "codec_size current_row_bytes=#{byte_size(current_key) + byte_size(current_value)} " <>
        "compact_row_bytes=#{byte_size(compact_key) + byte_size(compact_value)} " <>
        "current_physical_bytes=#{current_bytes} compact_physical_bytes=#{compact_bytes}"
    )
  end

  defp benchmark_codec(dataset) do
    decode_jobs =
      Enum.reduce(@page_sizes, %{}, fn page_size, jobs ->
        current_entries = Map.fetch!(dataset.current_entries, page_size)
        compact_entries = Map.fetch!(dataset.compact_entries, page_size)

        jobs
        |> Map.put("current decode/page-#{page_size}", fn ->
          LMDBIndexDecode.query_entries_readonly(current_entries, 1)
        end)
        |> Map.put("compact decode/page-#{page_size}", fn ->
          CompactCodec.decode_entries(compact_entries, dataset.prefix, 1)
        end)
        |> Map.put("current scan+decode/page-#{page_size}", fn ->
          with {:ok, entries} <-
                 LMDB.prefix_entries(dataset.current_path, dataset.prefix, page_size) do
            LMDBIndexDecode.query_entries_readonly(entries, 1)
          end
        end)
        |> Map.put("compact scan+decode/page-#{page_size}", fn ->
          with {:ok, entries} <-
                 LMDB.prefix_entries(dataset.compact_path, dataset.prefix, page_size) do
            CompactCodec.decode_entries(entries, dataset.prefix, 1)
          end
        end)
      end)

    Benchee.run(
      decode_jobs,
      QueryPerformance.benchee_options("query-planner-lmdb-codec-candidates")
    )
  end

  defp benchmark_codec_shapes do
    count = 4_096

    long_id_pairs =
      build_shape_pairs(count, fn index ->
        id = String.duplicate("long-id-", 40) <> Integer.to_string(index)

        {
          LMDB.query_index_entry(@index_key, id, index, 0, "state-#{index}"),
          CompactCodec.entry(@index_key, id, index, 0, "state-#{index}")
        }
      end)

    nil_state_pairs =
      build_shape_pairs(count, fn index ->
        id = id(index)

        {
          LMDB.query_index_entry(@index_key, id, index, 0, nil),
          CompactCodec.entry(@index_key, id, index, 0, nil)
        }
      end)

    index_key_prefix = Keys.attribute_index_prefix("benchmark", "queued", "region")
    index_key = Keys.attribute_index_key("benchmark", "queued", "region", "emea")
    raw_prefix = LMDB.query_index_raw_prefix(index_key_prefix)

    {_sample_key, sample_value} = LMDB.query_index_entry(index_key, "sample", 1, 0, nil)

    {:ok,
     {_family_digest, _index_digest, discovery_component, _id, _updated_at_ms, _expire_at_ms,
      _state_key}} = LMDB.decode_query_index_value(sample_value)

    true = is_binary(discovery_component)

    discovery_pairs =
      build_shape_pairs(count, fn index ->
        id = id(index)

        {
          LMDB.query_index_entry(index_key, id, index, 0, "state-#{index}"),
          CompactCodec.entry(
            index_key,
            id,
            index,
            0,
            "state-#{index}",
            discovery_component
          )
        }
      end)

    prefix = LMDB.query_index_prefix(@index_key)
    discovery_prefix = LMDB.query_index_prefix(index_key)

    assert_exact_shape!(long_id_pairs, prefix)
    assert_exact_shape!(nil_state_pairs, prefix)
    assert_exact_shape!(discovery_pairs, discovery_prefix)

    {:ok, current_discovery} =
      current_discovery_entries(discovery_pairs.current, raw_prefix, 1)

    {:ok, compact_discovery} =
      CompactCodec.decode_discovery_entries(
        discovery_pairs.compact,
        raw_prefix,
        index_key_prefix,
        1
      )

    true = current_discovery == compact_discovery

    {corrupt_key, _valid_value} =
      CompactCodec.entry(index_key, "corrupt", 1, 0, nil, discovery_component)

    {^corrupt_key, corrupt_value} =
      CompactCodec.entry(index_key, "corrupt", 1, 0, nil, "wrong-discovery")

    {:error, :invalid_compact_query_index_entry} =
      CompactCodec.decode_discovery_entries(
        [{corrupt_key, corrupt_value}],
        raw_prefix,
        index_key_prefix,
        1
      )

    print_shape_size("long-id", long_id_pairs)
    print_shape_size("nil-state", nil_state_pairs)
    print_shape_size("discovery", discovery_pairs)

    jobs = %{
      "current exact decode/long-id-page-4096" => fn ->
        LMDBIndexDecode.query_entries_readonly(long_id_pairs.current, 1)
      end,
      "compact exact decode/long-id-page-4096" => fn ->
        CompactCodec.decode_entries(long_id_pairs.compact, prefix, 1)
      end,
      "current exact decode/nil-state-page-4096" => fn ->
        LMDBIndexDecode.query_entries_readonly(nil_state_pairs.current, 1)
      end,
      "compact exact decode/nil-state-page-4096" => fn ->
        CompactCodec.decode_entries(nil_state_pairs.compact, prefix, 1)
      end,
      "current exact decode/discovery-page-4096" => fn ->
        LMDBIndexDecode.query_entries_readonly(discovery_pairs.current, 1)
      end,
      "compact exact decode/discovery-page-4096" => fn ->
        CompactCodec.decode_entries(discovery_pairs.compact, discovery_prefix, 1)
      end,
      "current broad decode/discovery-page-4096" => fn ->
        current_discovery_entries(discovery_pairs.current, raw_prefix, 1)
      end,
      "compact broad validated decode/discovery-page-4096" => fn ->
        CompactCodec.decode_discovery_entries(
          discovery_pairs.compact,
          raw_prefix,
          index_key_prefix,
          1
        )
      end
    }

    Benchee.run(jobs, QueryPerformance.benchee_options("query-planner-lmdb-codec-shapes"))
  end

  defp build_shape_pairs(count, entry_fun) do
    pairs = Enum.map(1..count, entry_fun)

    %{
      current: pairs |> Enum.map(&elem(&1, 0)) |> Enum.sort_by(&elem(&1, 0)),
      compact: pairs |> Enum.map(&elem(&1, 1)) |> Enum.sort_by(&elem(&1, 0))
    }
  end

  defp assert_exact_shape!(pairs, prefix) do
    {:ok, current} = LMDBIndexDecode.query_entries_readonly(pairs.current, 1)
    {:ok, compact} = CompactCodec.decode_entries(pairs.compact, prefix, 1)
    true = current == compact
  end

  defp print_shape_size(name, pairs) do
    [{current_key, current_value} | _rest] = pairs.current
    [{compact_key, compact_value} | _rest] = pairs.compact

    IO.puts(
      "codec_shape_size shape=#{name} " <>
        "current_row_bytes=#{byte_size(current_key) + byte_size(current_value)} " <>
        "compact_row_bytes=#{byte_size(compact_key) + byte_size(compact_value)}"
    )
  end

  defp current_discovery_entries(entries, raw_prefix, now_ms) do
    entries
    |> Enum.reduce_while({:ok, []}, fn
      {key, value}, {:ok, acc} when is_binary(key) and is_binary(value) ->
        case LMDB.decode_query_index_value(value) do
          {:ok,
           {family_digest, index_digest, discovery_component, id, updated_at_ms, expire_at_ms,
            state_key}} ->
            cond do
              not String.starts_with?(key, raw_prefix) ->
                {:halt, {:error, :invalid_current_query_index_prefix}}

              not LMDB.query_index_entry_key?(
                key,
                family_digest,
                index_digest,
                id,
                updated_at_ms
              ) ->
                {:halt, {:error, :invalid_current_query_index_entry}}

              expire_at_ms <= 0 or expire_at_ms > now_ms ->
                {:cont, {:ok, [{discovery_component, id, updated_at_ms, state_key} | acc]}}

              true ->
                {:cont, {:ok, acc}}
            end

          :error ->
            {:halt, {:error, :invalid_current_query_index_entry}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_current_query_index_entry}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp benchmark_expiry_cleanup do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-query-expiry-candidate-#{System.unique_integer([:positive])}"
      )

    path = Path.join(root, "env")
    File.mkdir_p!(path)
    queue = spawn_link(fn -> cleanup_queue_start(10_000) end)

    try do
      Enum.each([1, 25, 100], fn count ->
        entries =
          Enum.map(1..count, fn index ->
            LMDB.query_index_entry(
              "expiry:benchmark",
              "expired-#{index}",
              index,
              1,
              "state-#{index}"
            )
          end)

        synchronous =
          for _iteration <- 1..10 do
            put_entries!(path, entries)

            {elapsed, {:ok, []}} =
              QueryPerformance.timed_ns(fn ->
                LMDBIndexDecode.query_entries(entries, path, 2)
              end)

            elapsed
          end

        {deferred, background} =
          Enum.reduce(1..10, {[], []}, fn _iteration, {latencies, cleanup_latencies} ->
            put_entries!(path, entries)
            reference = make_ref()

            {elapsed, {:ok, []}} =
              QueryPerformance.timed_ns(fn ->
                with {:ok, []} <- LMDBIndexDecode.query_entries_readonly(entries, 2) do
                  operations = Enum.map(entries, fn {key, _value} -> {:delete, key} end)
                  send(queue, {:enqueue, self(), reference, path, operations})

                  receive do
                    {:cleanup_enqueued, ^reference} -> {:ok, []}
                    {:cleanup_queue_full, ^reference} -> {:error, :cleanup_queue_full}
                  after
                    5_000 -> raise "deferred cleanup admission timed out"
                  end
                end
              end)

            cleanup_elapsed =
              receive do
                {:cleanup_complete, ^reference, :ok, cleanup_elapsed} -> cleanup_elapsed
              after
                5_000 -> raise "deferred cleanup benchmark timed out"
              end

            {[elapsed | latencies], [cleanup_elapsed | cleanup_latencies]}
          end)

        QueryPerformance.print_summary(
          "synchronous expiry cleanup/count-#{count}",
          QueryPerformance.latency_summary(synchronous)
        )

        QueryPerformance.print_summary(
          "deferred expiry enqueue/count-#{count}",
          QueryPerformance.latency_summary(deferred)
        )

        QueryPerformance.print_summary(
          "deferred expiry background commit/count-#{count}",
          QueryPerformance.latency_summary(background)
        )
      end)
    after
      send(queue, :stop)
      _ = Ferricstore.Bitcask.NIF.lmdb_release(path)
      File.rm_rf!(root)
    end
  end

  defp cleanup_queue_start(max_entries) do
    queue = self()
    worker = spawn_link(fn -> cleanup_worker_loop(queue) end)
    cleanup_queue_loop(worker, 0, max_entries)
  end

  defp cleanup_queue_loop(worker, queued_entries, max_entries) do
    receive do
      {:enqueue, caller, reference, path, operations} ->
        operation_count = length(operations)

        if queued_entries + operation_count <= max_entries do
          send(worker, {:cleanup, caller, reference, path, operations})
          send(caller, {:cleanup_enqueued, reference})
          cleanup_queue_loop(worker, queued_entries + operation_count, max_entries)
        else
          send(caller, {:cleanup_queue_full, reference})
          cleanup_queue_loop(worker, queued_entries, max_entries)
        end

      {:cleanup_finished, operation_count} ->
        cleanup_queue_loop(worker, queued_entries - operation_count, max_entries)

      :stop ->
        send(worker, :stop)
    end
  end

  defp cleanup_worker_loop(queue) do
    receive do
      {:cleanup, caller, reference, path, operations} ->
        {elapsed, result} =
          QueryPerformance.timed_ns(fn -> LMDB.write_batch(path, operations) end)

        send(caller, {:cleanup_complete, reference, result, elapsed})
        send(queue, {:cleanup_finished, length(operations)})
        cleanup_worker_loop(queue)

      :stop ->
        :ok
    end
  end

  defp put_entries!(path, entries) do
    :ok = LMDB.write_batch(path, Enum.map(entries, fn {key, value} -> {:put, key, value} end))
  end

  defp current_entry(index) do
    LMDB.query_index_entry(
      @index_key,
      id(index),
      index,
      0,
      "state-#{index}"
    )
  end

  defp compact_entry(index) do
    CompactCodec.entry(
      @index_key,
      id(index),
      index,
      0,
      "state-#{index}"
    )
  end

  defp id(index), do: "flow-#{String.pad_leading(Integer.to_string(index), 8, "0")}"

  defp read_page!(path, prefix, count) do
    {:ok, entries} = LMDB.prefix_entries(path, prefix, count)
    ^count = length(entries)
    entries
  end

  defp cleanup_dataset(dataset) do
    _ = Ferricstore.Bitcask.NIF.lmdb_release(dataset.current_path)
    _ = Ferricstore.Bitcask.NIF.lmdb_release(dataset.compact_path)
    File.rm_rf!(dataset.root)
  end
end

Ferricstore.Bench.QueryPlannerLMDBCandidates.run()
