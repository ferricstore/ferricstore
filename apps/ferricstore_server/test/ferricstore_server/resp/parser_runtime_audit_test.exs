defmodule FerricstoreServer.Resp.ParserRuntimeAuditTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../../../", __DIR__)

  defp read!(path), do: File.read!(Path.join(@repo_root, path))

  test "RESP parser NIF uses the precompiled release artifact path" do
    nif_source = read!("apps/ferricstore/lib/ferricstore/resp/parser_nif.ex")
    mix_source = read!("apps/ferricstore/mix.exs")
    release_workflow = read!(".github/workflows/release.yml")
    hex_publish_workflow = read!(".github/workflows/hex-publish.yml")

    assert nif_source =~ "use RustlerPrecompiled",
           "RESP parser is release-critical; it must load the same precompiled artifact shape as the Bitcask and WAL NIFs instead of requiring a manually copied local .so"

    assert nif_source =~ ~s(crate: "resp_parser_nif")
    refute nif_source =~ "force_build: System.get_env"
    assert mix_source =~ ":rustler_precompiled"

    assert release_workflow =~
             ~s(crate: ["ferricstore_bitcask", "ferricstore_wal_nif", "resp_parser_nif"])

    assert hex_publish_workflow =~ "RESP_COUNT"
    assert hex_publish_workflow =~ "checksum-Elixir.Ferricstore.Resp.ParserNif.exs"
  end

  test "server transaction and blocking runtime do not call legacy Dispatcher.dispatch" do
    paths = [
      "apps/ferricstore_server/lib/ferricstore_server/connection/transaction.ex",
      "apps/ferricstore_server/lib/ferricstore_server/connection/blocking.ex",
      "apps/ferricstore/lib/ferricstore/store/shard/transaction.ex",
      "apps/ferricstore/lib/ferricstore/raft/state_machine.ex"
    ]

    offenders =
      for path <- paths,
          line = read!(path),
          String.contains?(line, "Dispatcher.dispatch(") do
        path
      end

    assert offenders == []
  end

  test "connection parsed dispatch does not route queued or pubsub commands through legacy dispatch" do
    source = read!("apps/ferricstore_server/lib/ferricstore_server/connection.ex")
    [_, dispatch_parsed | _] = String.split(source, "  defp dispatch_parsed", parts: 3)

    [dispatch_parsed, _] =
      String.split(
        dispatch_parsed,
        "  # ---------------------------------------------------------------------------\n  # Dispatch helpers",
        parts: 2
      )

    refute String.contains?(dispatch_parsed, "dispatch(cmd, args, state)")
  end
end
