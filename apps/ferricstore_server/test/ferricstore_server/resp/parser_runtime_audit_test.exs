defmodule FerricstoreServer.Resp.ParserRuntimeAuditTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../../../", __DIR__)

  defp read!(path), do: File.read!(Path.join(@repo_root, path))

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
