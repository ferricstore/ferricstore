# bench/resp_bench.exs
#
# Benchmarks for the RESP3 parser and encoder.
#
# Run:
#   MIX_ENV=bench mix run bench/resp_bench.exs
#
# The RESP layer is pure-functional (no process state, no NIF), so no
# application startup is needed.

alias Ferricstore.Resp.{Parser, Encoder}

# ---------------------------------------------------------------------------
# Data fixtures
# ---------------------------------------------------------------------------

# 1KB payload (used for bulk string benchmarks)
payload_1kb = :crypto.strong_rand_bytes(1024) |> Base.encode64() |> binary_part(0, 1024)

# Single simple string
simple_string_bin = "+OK\r\n"

# Single bulk string with 1KB payload
bulk_1kb_bin = "$#{byte_size(payload_1kb)}\r\n#{payload_1kb}\r\n"

# Array of 10 bulk strings (simulates a typical multi-key response)
ten_bulk_strings =
  Enum.map(1..10, fn i ->
    val = "value_#{String.pad_leading(Integer.to_string(i), 4, "0")}"
    "$#{byte_size(val)}\r\n#{val}\r\n"
  end)
  |> IO.iodata_to_binary()

nested_array_bin = "*10\r\n" <> ten_bulk_strings

# Map with 20 entries (40 RESP elements: key, value, key, value, ...)
map_pairs =
  Enum.map(1..20, fn i ->
    k = "key_#{i}"
    v = "value_#{i}"
    "$#{byte_size(k)}\r\n#{k}\r\n$#{byte_size(v)}\r\n#{v}\r\n"
  end)
  |> IO.iodata_to_binary()

map_20_bin = "%20\r\n" <> map_pairs

# 1000 pipelined SET commands: *3\r\n$3\r\nSET\r\n$keyN\r\nkeyN\r\n$valN\r\nvalN\r\n
pipelined_1000 =
  Enum.map(1..1000, fn i ->
    k = "key:#{i}"
    v = "val:#{i}"
    "*3\r\n$3\r\nSET\r\n$#{byte_size(k)}\r\n#{k}\r\n$#{byte_size(v)}\r\n#{v}\r\n"
  end)
  |> IO.iodata_to_binary()

# Encoder input fixtures
simple_ok = {:simple, "OK"}

array_10 =
  Enum.map(1..10, fn i -> "value_#{String.pad_leading(Integer.to_string(i), 4, "0")}" end)

map_10 =
  Enum.into(1..10, %{}, fn i -> {"key_#{i}", "value_#{i}"} end)

bulk_1kb_term = payload_1kb

# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

IO.puts("=== RESP3 Parser & Encoder Benchmarks ===\n")

Benchee.run(
  %{
    # -- Parser benchmarks --------------------------------------------------

    "Parser: 1000 pipelined SET commands" => fn ->
      {:ok, _values, ""} = Parser.parse(pipelined_1000)
    end,
    "Parser: simple string (+OK)" => fn ->
      {:ok, [{:simple, "OK"}], ""} = Parser.parse(simple_string_bin)
    end,
    "Parser: bulk string (1KB payload)" => fn ->
      {:ok, [_value], ""} = Parser.parse(bulk_1kb_bin)
    end,
    "Parser: array of 10 bulk strings" => fn ->
      {:ok, [_array], ""} = Parser.parse(nested_array_bin)
    end,
    "Parser: map with 20 entries" => fn ->
      {:ok, [_map], ""} = Parser.parse(map_20_bin)
    end,

    # -- Encoder benchmarks -------------------------------------------------

    "Encoder: {:simple, \"OK\"}" => fn ->
      Encoder.encode(simple_ok)
    end,
    "Encoder: 10-element array" => fn ->
      Encoder.encode(array_10)
    end,
    "Encoder: map with 10 keys" => fn ->
      Encoder.encode(map_10)
    end,
    "Encoder: bulk string (1KB)" => fn ->
      Encoder.encode(bulk_1kb_term)
    end,

    # -- Roundtrip benchmarks -----------------------------------------------

    "Roundtrip: simple string" => fn ->
      bin = Encoder.encode(simple_ok) |> IO.iodata_to_binary()
      {:ok, [{:simple, "OK"}], ""} = Parser.parse(bin)
    end,
    "Roundtrip: 10-element array" => fn ->
      bin = Encoder.encode(array_10) |> IO.iodata_to_binary()
      {:ok, [_array], ""} = Parser.parse(bin)
    end,
    "Roundtrip: map with 10 keys" => fn ->
      bin = Encoder.encode(map_10) |> IO.iodata_to_binary()
      {:ok, [_map], ""} = Parser.parse(bin)
    end
  },
  time: 5,
  warmup: 2,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: "bench/output/resp.html", auto_open: false}
  ]
)
