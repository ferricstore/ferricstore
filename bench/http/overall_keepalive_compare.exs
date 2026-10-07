# BENCH_VARIANT=baseline BENCH_TRIAL=1 ERL_FLAGS='+S 8:8' \
#   mise exec -- mix run --no-start bench/http/overall_keepalive_compare.exs \
#   --clients 64 --requests-per-client 500 --commands-per-request 1 --warmup 20
# Keep each connection below Cowboy's default maximum keep-alive request count.

variant = System.get_env("BENCH_VARIANT", "current")
trial = System.get_env("BENCH_TRIAL", "1")
source_path = "apps/ferricstore_http/lib/ferricstore_http/auth/cache.ex"

source =
  case variant do
    "baseline" ->
      Jason.decode!(File.read!("bench/results/auth-cache-maintenance-perf.json"))[
        "baseline_source"
      ]

    "current" ->
      File.read!(source_path)
  end

Code.compiler_options(ignore_module_conflict: true)
Code.compile_string(source, source_path)

{:ok, capture} = StringIO.open("")
original_leader = Process.group_leader()
Process.group_leader(self(), capture)

try do
  Code.require_file("keepalive_benchmark.exs", __DIR__)
after
  Process.group_leader(self(), original_leader)
end

{_input, output} = StringIO.contents(capture)
StringIO.close(capture)
IO.write(output)
[_, rate] = Regex.run(~r/throughput: (\d+) requests\/s/, output)

result = %{
  variant: variant,
  trial: trial,
  requests_per_second: String.to_integer(rate),
  source_sha256: Base.encode16(:crypto.hash(:sha256, source), case: :lower),
  output: output
}

path = "bench/output/overall-http/#{variant}-#{trial}.json"
File.mkdir_p!(Path.dirname(path))
File.write!(path, Jason.encode!(result, pretty: true))
