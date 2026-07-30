alias FerricstoreServer.Native.Codec
alias FerricstoreServer.Native.NIF

rows = fn count ->
  Enum.map(1..count, fn id ->
    ["#{id}-0", "field", "value"]
  end)
end

rows_10 = rows.(10)
rows_100 = rows.(100)
rows_128 = rows.(128)
rows_1_000 = rows.(1_000)

generic = fn range ->
  Codec.encode_response_frames(0x0100, 1, 1, :ok, range, max_response_bytes: 1_048_576)
end

command = fn range ->
  Codec.encode_command_response_frames(0x0100, 1, 1, :ok, range, max_response_bytes: 1_048_576)
end

true = command.(rows_10) == generic.(rows_10)
true = command.(rows_100) == generic.(rows_100)
true = command.(rows_128) == generic.(rows_128)
true = command.(rows_1_000) == generic.(rows_1_000)

Benchee.run(
  %{
    "command range x10" => fn -> command.(rows_10) end,
    "generic range x10" => fn -> generic.(rows_10) end,
    "command range x100" => fn -> command.(rows_100) end,
    "generic range x100" => fn -> generic.(rows_100) end,
    "command Rust range x128" => fn -> command.(rows_128) end,
    "generic range x128" => fn -> generic.(rows_128) end,
    "command Rust range x1000" => fn -> command.(rows_1_000) end,
    "generic range x1000" => fn -> generic.(rows_1_000) end,
    "direct NIF range x1000" => fn ->
      NIF.encode_stream_rows_response_frame(0x0100, 1, 1, rows_1_000, 1_048_576)
    end
  },
  warmup: 2,
  time: 4,
  memory_time: 2,
  reduction_time: 2,
  print: [benchmarking: false, configuration: false]
)
