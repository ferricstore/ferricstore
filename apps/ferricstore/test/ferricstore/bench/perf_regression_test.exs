defmodule Ferricstore.Bench.PerfRegressionTest do
  @moduledoc """
  Micro-benchmarks that prove specific hot-path performance issues
  and verify that fixes deliver measurable improvement.

  Run with: mix test test/ferricstore/bench/perf_regression_test.exs --include bench --timeout 120000
  """
  use ExUnit.Case, async: false

  @moduletag :bench

  # ---------------------------------------------------------------------------
  # 1. RESP Parser: Application.get_env vs persistent_term
  # ---------------------------------------------------------------------------

  if Code.ensure_loaded?(FerricstoreServer.Resp.Parser) do
    test "benchmark: RESP Parser.parse/1 Application.get_env overhead" do
      # Parser.parse/1 calls Application.get_env on every invocation (line 130).
      # Parser.parse/2 accepts the max_value_size directly, bypassing the lookup.
      # This benchmark quantifies the overhead.

      data = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
      max = 1_048_576
      n = 200_000

      # Warm up
      for _ <- 1..1000 do
        FerricstoreServer.Resp.Parser.parse(data)
        FerricstoreServer.Resp.Parser.parse(data, max)
      end

      # Measure parse/1 (calls Application.get_env)
      {time_with_env, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Parser.parse(data)
        end)

      # Measure parse/2 (no Application.get_env)
      {time_without_env, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Parser.parse(data, max)
        end)

      ops_with = n / (time_with_env / 1_000_000)
      ops_without = n / (time_without_env / 1_000_000)
      overhead_pct = (time_with_env - time_without_env) / time_without_env * 100

      IO.puts(
        "  Parser.parse/1 (Application.get_env): #{round(ops_with)} ops/sec (#{time_with_env}us)"
      )

      IO.puts(
        "  Parser.parse/2 (direct):              #{round(ops_without)} ops/sec (#{time_without_env}us)"
      )

      IO.puts("  Overhead: #{Float.round(overhead_pct, 1)}%")

      # The test passes regardless -- it's measuring, not gating.
      # But log a finding if overhead > 5%.
      if overhead_pct > 5.0 do
        IO.puts(
          "  ** FINDING: Application.get_env adds #{Float.round(overhead_pct, 1)}% overhead to RESP parsing"
        )
      end

      assert true
    end
  end

  # ---------------------------------------------------------------------------
  # 2. Compound key building overhead
  # ---------------------------------------------------------------------------

  test "benchmark: compound key string concatenation" do
    # hash_field/2 does: "H:" <> key <> <<0>> <> field
    # This is 3 binary concatenations, producing 2 intermediate binaries.
    # Measure total cost and compare against an iolist approach.

    key = "user:12345"
    field = "name"
    n = 500_000

    # Warm up
    for _ <- 1..1000 do
      Ferricstore.Store.CompoundKey.hash_field(key, field)
    end

    # Measure current implementation (string concat)
    {time_concat, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: Ferricstore.Store.CompoundKey.hash_field(key, field)
      end)

    # Measure iolist-to-binary approach (potential fix)
    {time_iolist, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: IO.iodata_to_binary(["H:", key, <<0>>, field])
      end)

    ops_concat = n / (time_concat / 1_000_000)
    ops_iolist = n / (time_iolist / 1_000_000)

    IO.puts("  CompoundKey concat:  #{round(ops_concat)} ops/sec (#{time_concat}us)")
    IO.puts("  IOList alternative:  #{round(ops_iolist)} ops/sec (#{time_iolist}us)")

    # Also measure the type_key which is simpler: "T:" <> key
    {time_type_key, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: Ferricstore.Store.CompoundKey.type_key(key)
      end)

    IO.puts("  CompoundKey.type_key: #{round(n / (time_type_key / 1_000_000))} ops/sec")
    assert true
  end

  # ---------------------------------------------------------------------------
  # 3. RESP Encoder micro-benchmark
  # ---------------------------------------------------------------------------

  if Code.ensure_loaded?(FerricstoreServer.Resp.Encoder) do
    test "benchmark: RESP encoding common responses" do
      n = 500_000

      # Warm up
      for _ <- 1..1000 do
        FerricstoreServer.Resp.Encoder.encode(:ok)
        FerricstoreServer.Resp.Encoder.encode("hello world")
        FerricstoreServer.Resp.Encoder.encode(["one", "two", "three", "four", "five"])
      end

      # Measure :ok encoding
      {time_ok, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Encoder.encode(:ok)
        end)

      # Measure bulk string encoding
      {time_bulk, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Encoder.encode("hello world")
        end)

      # Measure array encoding (5 elements)
      {time_array, _} =
        :timer.tc(fn ->
          for _ <- 1..n,
              do: FerricstoreServer.Resp.Encoder.encode(["one", "two", "three", "four", "five"])
        end)

      # Measure nil encoding
      {time_nil, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Encoder.encode(nil)
        end)

      IO.puts("  Encode :ok:        #{round(n / (time_ok / 1_000_000))} ops/sec")
      IO.puts("  Encode bulk str:   #{round(n / (time_bulk / 1_000_000))} ops/sec")
      IO.puts("  Encode 5-array:    #{round(n / (time_array / 1_000_000))} ops/sec")
      IO.puts("  Encode nil:        #{round(n / (time_nil / 1_000_000))} ops/sec")
      assert true
    end
  end

  # ---------------------------------------------------------------------------
  # 4. Shard routing: crc32 + slot map lookup
  # ---------------------------------------------------------------------------

  test "benchmark: shard routing cost (shard_for + shard_name)" do
    n = 500_000
    key = "user:42:session"

    # Ensure slot map is initialized
    try do
      :persistent_term.get(:ferricstore_slot_map)
    rescue
      ArgumentError ->
        Ferricstore.Store.SlotMap.init(4)
    end

    # Warm up
    for _ <- 1..1000 do
      Ferricstore.Store.Router.shard_for(FerricStore.Instance.get(:default), key)
      Ferricstore.Store.Router.shard_name(FerricStore.Instance.get(:default), 0)
    end

    # Measure shard_for (crc32 + slot map lookup)
    {time_shard_for, _} =
      :timer.tc(fn ->
        for _ <- 1..n,
            do: Ferricstore.Store.Router.shard_for(FerricStore.Instance.get(:default), key)
      end)

    # Measure shard_name (atom interpolation)
    {time_shard_name, _} =
      :timer.tc(fn ->
        for _ <- 1..n,
            do: Ferricstore.Store.Router.shard_name(FerricStore.Instance.get(:default), 0)
      end)

    # Measure the combined routing path
    {time_combined, _} =
      :timer.tc(fn ->
        for _ <- 1..n do
          idx = Ferricstore.Store.Router.shard_for(FerricStore.Instance.get(:default), key)
          Ferricstore.Store.Router.shard_name(FerricStore.Instance.get(:default), idx)
        end
      end)

    # Measure raw crc32 for baseline
    {time_crc32, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: :erlang.crc32(key) |> Bitwise.band(1023)
      end)

    IO.puts("  Raw crc32:      #{round(n / (time_crc32 / 1_000_000))} ops/sec (#{time_crc32}us)")

    IO.puts(
      "  shard_for:      #{round(n / (time_shard_for / 1_000_000))} ops/sec (#{time_shard_for}us)"
    )

    IO.puts(
      "  shard_name:     #{round(n / (time_shard_name / 1_000_000))} ops/sec (#{time_shard_name}us)"
    )

    IO.puts(
      "  Combined:       #{round(n / (time_combined / 1_000_000))} ops/sec (#{time_combined}us)"
    )

    # shard_name creates an atom every call via string interpolation.
    # If a cached tuple approach is faster, we'll see it here.
    assert true
  end

  # ---------------------------------------------------------------------------
  # 5. Stats.counter_ref persistent_term overhead
  # ---------------------------------------------------------------------------

  test "benchmark: Stats.counter_ref indirection cost" do
    # Every Stats function calls counter_ref() which does persistent_term.get.
    # incr_commands alone means 1 persistent_term.get per request.
    # incr_keyspace_hits adds another. That's 2+ persistent_term.gets per request
    # just for stats.

    n = 1_000_000

    # Warm up
    for _ <- 1..1000 do
      Ferricstore.Stats.incr_commands()
    end

    # Measure Stats.incr_commands (persistent_term.get + counters.add)
    {time_incr, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: Ferricstore.Stats.incr_commands()
      end)

    # Measure raw counters.add with pre-fetched ref (no persistent_term indirection)
    ref = :persistent_term.get(:ferricstore_stats_counter_ref)

    {time_raw, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: :counters.add(ref, 2, 1)
      end)

    # Measure persistent_term.get alone (atom key)
    {time_pt, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: :persistent_term.get(:ferricstore_stats_counter_ref)
      end)

    ops_incr = n / (time_incr / 1_000_000)
    ops_raw = n / (time_raw / 1_000_000)
    ops_pt = n / (time_pt / 1_000_000)
    overhead_pct = (time_incr - time_raw) / time_raw * 100

    IO.puts("  Stats.incr_commands:     #{round(ops_incr)} ops/sec (#{time_incr}us)")
    IO.puts("  Raw counters.add:        #{round(ops_raw)} ops/sec (#{time_raw}us)")
    IO.puts("  persistent_term.get:     #{round(ops_pt)} ops/sec (#{time_pt}us)")
    IO.puts("  Overhead of indirection: #{Float.round(overhead_pct, 1)}%")

    assert true
  end

  # ---------------------------------------------------------------------------
  # 6. shard_name atom creation overhead
  # ---------------------------------------------------------------------------

  test "benchmark: shard_name string interpolation vs pre-computed atom" do
    # Router.shard_name/1 does: :"Ferricstore.Store.Shard.#{index}"
    # This creates an atom via string interpolation on every call.
    # Pre-computing atoms in a tuple would eliminate the string formatting.

    n = 1_000_000

    # Build a pre-computed tuple for comparison
    shard_names = List.to_tuple(for i <- 0..15, do: :"Ferricstore.Store.Shard.#{i}")

    # Warm up
    for _ <- 1..1000 do
      Ferricstore.Store.Router.shard_name(FerricStore.Instance.get(:default), 0)
      _ = elem(shard_names, 0)
    end

    # Measure current implementation (string interpolation -> atom)
    {time_interp, _} =
      :timer.tc(fn ->
        for _ <- 1..n do
          Ferricstore.Store.Router.shard_name(FerricStore.Instance.get(:default), 0)
          Ferricstore.Store.Router.shard_name(FerricStore.Instance.get(:default), 1)
          Ferricstore.Store.Router.shard_name(FerricStore.Instance.get(:default), 2)
          Ferricstore.Store.Router.shard_name(FerricStore.Instance.get(:default), 3)
        end
      end)

    # Measure tuple lookup (pre-computed atoms)
    {time_tuple, _} =
      :timer.tc(fn ->
        for _ <- 1..n do
          _ = elem(shard_names, 0)
          _ = elem(shard_names, 1)
          _ = elem(shard_names, 2)
          _ = elem(shard_names, 3)
        end
      end)

    ops_interp = n * 4 / (time_interp / 1_000_000)
    ops_tuple = n * 4 / (time_tuple / 1_000_000)
    speedup = time_interp / max(time_tuple, 1)

    IO.puts("  shard_name (interpolation): #{round(ops_interp)} ops/sec (#{time_interp}us)")
    IO.puts("  elem(tuple, i) (cached):    #{round(ops_tuple)} ops/sec (#{time_tuple}us)")
    IO.puts("  Speedup: #{Float.round(speedup, 1)}x")

    assert true
  end

  # ---------------------------------------------------------------------------
  # 8. exists? GenServer.call vs exists_fast? ETS direct
  # ---------------------------------------------------------------------------

  test "benchmark: exists_fast? vs exists? (GenServer.call overhead)" do
    # Router.exists?/1 does GenServer.call, while exists_fast?/1 reads ETS directly.
    # This measures the cost difference.

    # We need the ETS table and a key. Since shard GenServers may not be
    # running in test, we'll create a standalone ETS table and benchmark
    # the ETS lookup pattern directly.

    table = :ets.new(:bench_keydir, [:set, :public, {:read_concurrency, true}])
    key = "test:key"
    :ets.insert(table, {key, "value", 0, 5, 1, 0, 5})
    n = 1_000_000

    # Warm up
    for _ <- 1..1000 do
      case :ets.lookup(table, key) do
        [{^key, _val, 0, _lfu, _fid, _off, _vsize}] -> true
        _ -> false
      end
    end

    # Measure direct ETS lookup (what exists_fast? does)
    {time_ets, _} =
      :timer.tc(fn ->
        for _ <- 1..n do
          case :ets.lookup(table, key) do
            [{^key, _val, 0, _lfu, _fid, _off, _vsize}] -> true
            _ -> false
          end
        end
      end)

    ops_ets = n / (time_ets / 1_000_000)
    IO.puts("  ETS direct lookup: #{round(ops_ets)} ops/sec (#{time_ets}us)")
    IO.puts("  (GenServer.call adds ~1-5us per call on top of this)")

    :ets.delete(table)
    assert true
  end

  # ---------------------------------------------------------------------------
  # 9. RESP Parser: full parse pipeline
  # ---------------------------------------------------------------------------

  if Code.ensure_loaded?(FerricstoreServer.Resp.Parser) do
    test "benchmark: RESP parse throughput for common commands" do
      n = 200_000

      set_cmd = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
      get_cmd = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
      pipeline = String.duplicate(get_cmd, 10)
      inline_cmd = "PING\r\n"

      max = 1_048_576

      # Warm up
      for _ <- 1..1000 do
        FerricstoreServer.Resp.Parser.parse(set_cmd, max)
        FerricstoreServer.Resp.Parser.parse(get_cmd, max)
        FerricstoreServer.Resp.Parser.parse(pipeline, max)
      end

      {time_set, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Parser.parse(set_cmd, max)
        end)

      {time_get, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Parser.parse(get_cmd, max)
        end)

      {time_pipeline, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Parser.parse(pipeline, max)
        end)

      {time_inline, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Parser.parse(inline_cmd, max)
        end)

      IO.puts("  Parse SET cmd:     #{round(n / (time_set / 1_000_000))} ops/sec")
      IO.puts("  Parse GET cmd:     #{round(n / (time_get / 1_000_000))} ops/sec")
      IO.puts("  Parse 10-cmd pipe: #{round(n / (time_pipeline / 1_000_000))} ops/sec")
      IO.puts("  Parse PING inline: #{round(n / (time_inline / 1_000_000))} ops/sec")
      assert true
    end
  end

  # ---------------------------------------------------------------------------
  # 10. Encoder encode_list_counted: Enum.reverse overhead
  # ---------------------------------------------------------------------------

  if Code.ensure_loaded?(FerricstoreServer.Resp.Encoder) do
    test "benchmark: Encoder list encoding with varying sizes" do
      n = 200_000

      list_1 = ["hello"]
      list_5 = Enum.map(1..5, &"item_#{&1}")
      list_20 = Enum.map(1..20, &"item_#{&1}")

      # Warm up
      for _ <- 1..1000 do
        FerricstoreServer.Resp.Encoder.encode(list_1)
        FerricstoreServer.Resp.Encoder.encode(list_5)
        FerricstoreServer.Resp.Encoder.encode(list_20)
      end

      {time_1, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Encoder.encode(list_1)
        end)

      {time_5, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Encoder.encode(list_5)
        end)

      {time_20, _} =
        :timer.tc(fn ->
          for _ <- 1..n, do: FerricstoreServer.Resp.Encoder.encode(list_20)
        end)

      IO.puts("  Encode 1-element list:  #{round(n / (time_1 / 1_000_000))} ops/sec")
      IO.puts("  Encode 5-element list:  #{round(n / (time_5 / 1_000_000))} ops/sec")
      IO.puts("  Encode 20-element list: #{round(n / (time_20 / 1_000_000))} ops/sec")
      assert true
    end
  end
end
