alias Ferricstore.NativeValueCodec
alias FerricstoreServer.Native.Codec

payload = %{
  "command" => "XRANGE",
  "args" => ["bench:stream", "123-0", "+", "COUNT", "10"]
}

body = NativeValueCodec.encode(payload)
{:ok, ^payload} = Codec.decode_body(body)

mixed_payload = %{
  "command" => "MIXED",
  "args" => ["binary", 123, nil, %{"nested" => "value"}]
}

mixed_body = NativeValueCodec.encode(mixed_payload)
{:ok, ^mixed_payload} = Codec.decode_body(mixed_body)

Benchee.run(
  %{
    "decode mixed native request" => fn -> Codec.decode_body(mixed_body) end,
    "decode native XRANGE request" => fn -> Codec.decode_body(body) end
  },
  warmup: 2,
  time: 5,
  memory_time: 0,
  reduction_time: 0,
  print: [benchmarking: false, configuration: false]
)
