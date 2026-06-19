defmodule Ferricstore.Commands.AstPurityAuditTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../../../", __DIR__)

  defp read!(path), do: File.read!(Path.join(@repo_root, path))

  test "migrated command modules do not bridge AST handlers through raw handle/3" do
    paths = [
      "apps/ferricstore/lib/ferricstore/commands/hash.ex",
      "apps/ferricstore/lib/ferricstore/commands/set.ex",
      "apps/ferricstore/lib/ferricstore/commands/list.ex",
      "apps/ferricstore/lib/ferricstore/commands/generic.ex",
      "apps/ferricstore/lib/ferricstore/commands/stream.ex",
      "apps/ferricstore/lib/ferricstore/commands/pubsub.ex",
      "apps/ferricstore/lib/ferricstore/commands/hyperloglog.ex",
      "apps/ferricstore/lib/ferricstore/commands/bloom.ex",
      "apps/ferricstore/lib/ferricstore/commands/cuckoo.ex",
      "apps/ferricstore/lib/ferricstore/commands/cms.ex",
      "apps/ferricstore/lib/ferricstore/commands/tdigest.ex",
      "apps/ferricstore/lib/ferricstore/commands/topk.ex",
      "apps/ferricstore/lib/ferricstore/commands/geo.ex",
      "apps/ferricstore/lib/ferricstore/commands/sorted_set.ex",
      "apps/ferricstore/lib/ferricstore/commands/flow.ex"
    ]

    offenders =
      for path <- paths,
          source = read!(path),
          Regex.match?(~r/def\s+handle_ast[^\n]*do:\s*handle\(/, source) or
            Regex.match?(~r/Ferricstore\.Commands\.[A-Za-z0-9_]+\.handle\(/, source) do
        path
      end

    assert offenders == []
  end

  test "dispatcher string and pubsub AST hot path does not bridge through raw handlers" do
    source = read!("apps/ferricstore/lib/ferricstore/commands/dispatcher.ex")

    [ast_source, _raw_dispatch_source] =
      String.split(source, "  @doc \"\"\"\n  Dispatches", parts: 2)

    refute ast_source =~ "Strings.handle(\""
    refute ast_source =~ "PubSub.handle(\"PUBLISH\""
  end

  test "embedded public APIs do not call raw semantic command handlers" do
    paths = [
      "apps/ferricstore/lib/ferricstore.ex",
      "apps/ferricstore/lib/ferricstore/impl.ex"
    ]

    offenders =
      for path <- paths,
          source = read!(path),
          Regex.match?(~r/Ferricstore\.Commands\.[A-Za-z0-9_]+\.handle\(/, source) or
            Regex.match?(~r/Router\.(hll_op|bitmap_op|geo_op|tdigest_op)\(/, source) do
        path
      end

    assert offenders == []
  end

  test "transaction AST helper does not contain an Elixir semantic parser" do
    source = read!("apps/ferricstore/lib/ferricstore/transaction/ast.ex")

    refute source =~ "legacy_ast"
    refute source =~ "parse_set_options"
    refute source =~ "parse_getex_options"
    refute source =~ "defp parse_integer"
    assert source =~ "parse_commands"
  end
end
