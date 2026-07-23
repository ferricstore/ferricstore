# Benchmark-only compact codecs for composite index entry and reverse values.
# Production codecs and persisted formats are unchanged.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerCompositeCodecCandidates.CompactCodec do
  @moduledoc false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.Query.CompositeIndex

  @entry_version 1
  @reverse_version 1
  @max_u32 4_294_967_295
  @max_reverse_entries 128
  @max_key_bytes 511

  def encode_entry(id, state_key, record_version, expire_at_ms)
      when is_binary(id) and id != "" and is_binary(state_key) and state_key != "" and
             is_integer(record_version) and record_version >= 0 and
             is_integer(expire_at_ms) and expire_at_ms >= 0 do
    if byte_size(id) > @max_u32 do
      raise ArgumentError, "compact composite id exceeds its length field"
    end

    <<@entry_version, byte_size(id)::unsigned-big-32, record_version::unsigned-big-64,
      expire_at_ms::unsigned-big-64, id::binary, state_key::binary>>
  end

  def decode_entry(key, value) when is_binary(key) and is_binary(value) do
    with <<@entry_version, id_bytes::unsigned-big-32, record_version::unsigned-big-64,
           expire_at_ms::unsigned-big-64, payload::binary>> <- value,
         true <- id_bytes > 0 and id_bytes < byte_size(payload),
         <<id::binary-size(id_bytes), state_key::binary>> <- payload,
         {:ok, ^id} <- Keys.run_id_from_state_key(state_key),
         true <- CompositeIndex.entry_key_matches_id?(key, id) do
      {:ok,
       %{
         id: id,
         state_key: state_key,
         record_version: record_version,
         expire_at_ms: expire_at_ms
       }}
    else
      _invalid -> :error
    end
  end

  def decode_entry(_key, _value), do: :error

  def encode_reverse(state_key, keys, expire_at_ms)
      when is_binary(state_key) and state_key != "" and is_list(keys) and keys != [] and
             length(keys) <= @max_reverse_entries and is_integer(expire_at_ms) and
             expire_at_ms >= 0 do
    keys = Enum.sort(keys)

    if length(keys) == length(Enum.uniq(keys)) and
         Enum.all?(keys, &(is_binary(&1) and byte_size(&1) <= @max_key_bytes)) and
         byte_size(state_key) <= @max_u32 do
      encoded_keys = encode_front_coded_keys(keys, <<>>, [])

      IO.iodata_to_binary([
        <<@reverse_version, expire_at_ms::unsigned-big-64, byte_size(state_key)::unsigned-big-32,
          length(keys)::unsigned-big-16>>,
        state_key,
        encoded_keys
      ])
    else
      raise ArgumentError, "compact composite reverse value is invalid"
    end
  end

  def decode_reverse(value, expected_state_key)
      when is_binary(value) and is_binary(expected_state_key) and expected_state_key != "" do
    with <<@reverse_version, expire_at_ms::unsigned-big-64, state_bytes::unsigned-big-32,
           count::unsigned-big-16, payload::binary>> <- value,
         true <- count > 0 and count <= @max_reverse_entries,
         true <- state_bytes < byte_size(payload),
         <<state_key::binary-size(state_bytes), encoded_keys::binary>> <- payload,
         true <- state_key == expected_state_key,
         {:ok, id} <- Keys.run_id_from_state_key(state_key),
         {:ok, keys, <<>>} <- decode_front_coded_keys(encoded_keys, count, <<>>, []),
         true <- keys == Enum.sort(keys) and length(keys) == length(Enum.uniq(keys)),
         true <- valid_reverse_keys?(keys, id) do
      {:ok, %{keys: keys, expire_at_ms: expire_at_ms}}
    else
      _invalid -> :error
    end
  end

  def decode_reverse(_value, _expected_state_key), do: :error

  defp encode_front_coded_keys([], _previous, acc), do: Enum.reverse(acc)

  defp encode_front_coded_keys([key | keys], previous, acc) do
    common_bytes = common_prefix_bytes(previous, key, 0)
    suffix_bytes = byte_size(key) - common_bytes
    suffix = binary_part(key, common_bytes, suffix_bytes)

    encode_front_coded_keys(
      keys,
      key,
      [[<<common_bytes::unsigned-big-16, suffix_bytes::unsigned-big-16>>, suffix] | acc]
    )
  end

  defp common_prefix_bytes(left, right, offset)
       when offset < byte_size(left) and offset < byte_size(right) do
    if :binary.at(left, offset) == :binary.at(right, offset),
      do: common_prefix_bytes(left, right, offset + 1),
      else: offset
  end

  defp common_prefix_bytes(_left, _right, offset), do: offset

  defp decode_front_coded_keys(rest, 0, _previous, acc),
    do: {:ok, Enum.reverse(acc), rest}

  defp decode_front_coded_keys(
         <<common_bytes::unsigned-big-16, suffix_bytes::unsigned-big-16, rest::binary>>,
         count,
         previous,
         acc
       )
       when common_bytes <= byte_size(previous) and suffix_bytes <= byte_size(rest) do
    <<suffix::binary-size(suffix_bytes), tail::binary>> = rest
    key = binary_part(previous, 0, common_bytes) <> suffix

    if byte_size(key) <= @max_key_bytes do
      decode_front_coded_keys(tail, count - 1, key, [key | acc])
    else
      :error
    end
  end

  defp decode_front_coded_keys(_encoded, _count, _previous, _acc), do: :error

  defp valid_reverse_keys?(keys, id) do
    suffix = <<0x60, :crypto.hash(:sha256, id)::binary-size(32)>>
    suffix_bytes = byte_size(suffix)

    Enum.all?(keys, fn key ->
      byte_size(key) >= suffix_bytes and
        binary_part(key, byte_size(key) - suffix_bytes, suffix_bytes) == suffix
    end)
  end
end

defmodule Ferricstore.Bench.QueryPlannerCompositeCodecCandidates do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Bench.QueryPlannerCompositeCodecCandidates.CompactCodec
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.Query.{CompositeIndex, IndexDefinition}
  alias Ferricstore.TermCodec

  @entry_rows 100_000
  @decode_page 4_096
  @batch_size 5_000

  def run do
    case System.get_env("BENCH_CANDIDATE_SECTION", "all") do
      "all" ->
        benchmark_entries(definition())
        benchmark_reverse_values()

      "entries" ->
        benchmark_entries(definition())

      "reverse" ->
        benchmark_reverse_values()

      invalid ->
        raise ArgumentError,
              "BENCH_CANDIDATE_SECTION must be all, entries, or reverse; " <>
                "got #{inspect(invalid)}"
    end
  end

  defp benchmark_entries(definition) do
    root = temp_root()
    current_path = Path.join(root, "current")
    compact_path = Path.join(root, "compact")
    File.mkdir_p!(current_path)
    File.mkdir_p!(compact_path)

    try do
      1..@entry_rows
      |> Stream.chunk_every(@batch_size)
      |> Enum.each(fn indexes ->
        {current_ops, compact_ops} =
          Enum.map_reduce(indexes, [], fn index, compact_acc ->
            {key, current_value, compact_value} = entry_values(definition, index)
            {{:put, key, current_value}, [{:put, key, compact_value} | compact_acc]}
          end)

        :ok = LMDB.write_batch(current_path, current_ops)
        :ok = LMDB.write_batch(compact_path, Enum.reverse(compact_ops))
      end)

      prefix = IndexDefinition.storage_prefix(definition)
      current_entries = read_page(current_path, prefix, @decode_page)
      compact_entries = read_page(compact_path, prefix, @decode_page)

      {:ok, current_decoded} = decode_current_entries(current_entries)
      {:ok, compact_decoded} = decode_compact_entries(compact_entries)
      true = current_decoded == compact_decoded

      [{key, value} | _rest] = current_entries
      [{^key, compact_value} | _rest] = compact_entries

      :error =
        CompactCodec.decode_entry(
          key,
          binary_part(compact_value, 0, byte_size(compact_value) - 1)
        )

      IO.puts(
        "composite_entry_size current_row_bytes=#{byte_size(key) + byte_size(value)} " <>
          "compact_row_bytes=#{byte_size(key) + byte_size(compact_value)} " <>
          "current_physical_bytes=#{QueryPerformance.directory_bytes(current_path)} " <>
          "compact_physical_bytes=#{QueryPerformance.directory_bytes(compact_path)}"
      )

      encode_id = "run-0000000001"
      encode_state_key = Keys.state_key(encode_id, "tenant-benchmark")

      Benchee.run(
        %{
          "current ETF composite encode" => fn ->
            TermCodec.encode({:flow_composite_entry, 1, encode_id, encode_state_key, 1, 0})
          end,
          "candidate compact composite encode" => fn ->
            CompactCodec.encode_entry(encode_id, encode_state_key, 1, 0)
          end,
          "current ETF composite decode/page-4096" => fn ->
            decode_current_entries(current_entries)
          end,
          "candidate compact composite decode/page-4096" => fn ->
            decode_compact_entries(compact_entries)
          end,
          "current ETF composite scan+decode/page-4096" => fn ->
            with {:ok, entries} <- LMDB.prefix_entries(current_path, prefix, @decode_page) do
              decode_current_entries(entries)
            end
          end,
          "candidate compact composite scan+decode/page-4096" => fn ->
            with {:ok, entries} <- LMDB.prefix_entries(compact_path, prefix, @decode_page) do
              decode_compact_entries(entries)
            end
          end
        },
        QueryPerformance.benchee_options("query-planner-composite-entry-codec-candidates")
      )
    after
      release(current_path)
      release(compact_path)
      File.rm_rf!(root)
    end
  end

  defp entry_values(definition, index) do
    id = "run-#{String.pad_leading(Integer.to_string(index), 10, "0")}"
    partition_key = "tenant-benchmark"
    state_key = Keys.state_key(id, partition_key)

    record = %{
      id: id,
      type: "invoice",
      state: "queued",
      partition_key: partition_key,
      updated_at_ms: index,
      version: rem(index, 1_000)
    }

    {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)

    compact = CompactCodec.encode_entry(id, state_key, record.version, 0)
    {entry.key, entry.value, compact}
  end

  defp decode_current_entries(entries) do
    decode_entries(entries, fn key, value ->
      with {:ok, decoded} <- CompositeIndex.decode_entry_value(value),
           true <- CompositeIndex.entry_key_matches_id?(key, decoded.id) do
        {:ok, decoded}
      else
        _invalid -> :error
      end
    end)
  end

  defp decode_compact_entries(entries),
    do: decode_entries(entries, &CompactCodec.decode_entry/2)

  defp decode_entries(entries, decoder) do
    entries
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, acc} ->
      case decoder.(key, value) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        :error -> {:halt, {:error, :invalid_composite_entry}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp benchmark_reverse_values do
    jobs =
      Enum.reduce([1, 8, 32, 128], %{}, fn fanout, jobs ->
        {state_key, keys} = reverse_keys(fanout)
        current = CompositeIndex.encode_reverse_value(state_key, keys, 5_000)
        compact = CompactCodec.encode_reverse(state_key, keys, 5_000)

        {:ok, current_state} = CompositeIndex.decode_reverse_state(current, state_key)
        {:ok, compact_state} = CompactCodec.decode_reverse(compact, state_key)
        true = current_state.keys == compact_state.keys
        true = current_state.expire_at_ms == compact_state.expire_at_ms
        :error = CompactCodec.decode_reverse(compact, Keys.state_key("wrong", "tenant-benchmark"))

        IO.puts(
          "composite_reverse_size fanout=#{fanout} current_bytes=#{byte_size(current)} " <>
            "compact_bytes=#{byte_size(compact)}"
        )

        Map.merge(jobs, %{
          "current ETF reverse encode/fanout-#{fanout}" => fn ->
            CompositeIndex.encode_reverse_value(state_key, keys, 5_000)
          end,
          "candidate front-coded reverse encode/fanout-#{fanout}" => fn ->
            CompactCodec.encode_reverse(state_key, keys, 5_000)
          end,
          "current ETF reverse decode/fanout-#{fanout}" => fn ->
            CompositeIndex.decode_reverse_state(current, state_key)
          end,
          "candidate front-coded reverse decode/fanout-#{fanout}" => fn ->
            CompactCodec.decode_reverse(compact, state_key)
          end
        })
      end)

    Benchee.run(
      jobs,
      QueryPerformance.benchee_options("query-planner-composite-reverse-candidates")
    )
  end

  defp reverse_keys(fanout) do
    definition =
      IndexDefinition.new!(%{
        id: "runs_by_tag_updated",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {{:attribute, "tags"}, :asc, :hashed},
          {:updated_at_ms, :desc}
        ]
      })

    id = "reverse-run"
    partition_key = "tenant-benchmark"
    state_key = Keys.state_key(id, partition_key)

    record = %{
      id: id,
      type: "invoice",
      state: "queued",
      partition_key: partition_key,
      updated_at_ms: 100,
      version: 3,
      attributes: %{
        "tags" => Enum.map(1..fanout, &"tag-#{String.pad_leading(Integer.to_string(&1), 4, "0")}")
      }
    }

    {:ok, entries} = CompositeIndex.entries(definition, record, state_key, 5_000)
    ^fanout = length(entries)
    {state_key, entries |> Enum.map(& &1.key) |> Enum.sort()}
  end

  defp definition do
    IndexDefinition.new!(%{
      id: "runs_by_type_state_updated",
      version: 1,
      fields: [
        {:partition_key, :asc},
        {:type, :asc},
        {:state, :asc},
        {:updated_at_ms, :asc}
      ]
    })
  end

  defp read_page(path, prefix, count) do
    {:ok, entries} = LMDB.prefix_entries(path, prefix, count)
    ^count = length(entries)
    entries
  end

  defp temp_root do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-composite-codec-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    root
  end

  defp release(path), do: Ferricstore.Bitcask.NIF.lmdb_release(path)
end

Ferricstore.Bench.QueryPlannerCompositeCodecCandidates.run()
