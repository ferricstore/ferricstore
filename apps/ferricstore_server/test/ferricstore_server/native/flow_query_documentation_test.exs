defmodule FerricstoreServer.Native.FlowQueryDocumentationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{
    Binder,
    Budget,
    Field,
    IndexCatalog,
    ReferenceParser,
    ResultCodec,
    Surface
  }

  alias FerricstoreServer.Native.FQLParser

  @documentation_path Path.expand("../../../../../docs/flow-query.md", __DIR__)
  @native_protocol_path Path.expand("../../../../../docs/native-protocol.md", __DIR__)
  @error_source_path Path.expand(
                       "../../../../ferricstore/lib/ferricstore/flow/query/error.ex",
                       __DIR__
                     )

  setup_all do
    {:ok,
     documentation: File.read!(@documentation_path),
     native_protocol: File.read!(@native_protocol_path)}
  end

  test "every runnable FQL example parses identically and accepts typed bindings", %{
    documentation: documentation
  } do
    queries =
      ~r/```fql\s*\n(.*?)\n```/s
      |> Regex.scan(documentation, capture: :all_but_first)
      |> List.flatten()
      |> Enum.map(&String.trim/1)

    assert length(queries) >= 16

    for query <- queries do
      assert {:ok, request} = FQLParser.parse_diagnostic(query),
             "documented FQL example did not parse:\n#{query}"

      assert {:ok, ^request} = ReferenceParser.parse_diagnostic(query),
             "documented FQL example differs between production and reference parsers:\n#{query}"

      params =
        request
        |> parameter_types(%{})
        |> Map.new(fn {name, type} -> {name, example_value(type)} end)

      assert {:ok, _bound_request} = Binder.bind(request, params),
             "documented FQL example did not accept typed bindings:\n#{query}"
    end
  end

  test "the field reference tracks every supported built-in field", %{
    documentation: documentation
  } do
    built_in_fields =
      Field.supported_external_names()
      |> Enum.reject(&String.contains?(&1, "<"))

    for field <- built_in_fields do
      assert documentation =~ "| `#{field}` |",
             "missing built-in field #{field} from the FQL field reference"
    end
  end

  test "the index reference tracks the bundled catalog", %{documentation: documentation} do
    assert {:ok, catalog} = IndexCatalog.load()

    for definition <- catalog.definitions do
      assert documentation =~ "| `#{definition.id}` |",
             "missing bundled index #{definition.id} from the query guide"
    end
  end

  test "the execution-limit reference tracks the immutable default budget", %{
    documentation: documentation
  } do
    for {field, value} <- Map.from_struct(Budget.default()) do
      assert documentation =~ "| `#{field}` | `#{value}` |",
             "missing default query budget #{field}=#{value} from the query guide"
    end
  end

  test "the guide contains the complete user and operator reference", %{
    documentation: documentation
  } do
    required_sections = [
      "## Quick Start",
      "## FQL1 Grammar",
      "## Query Forms",
      "## Fields And Types",
      "## Field Projection",
      "## Predicates And Values",
      "## Ordering And Pagination",
      "## Result Contract",
      "## Reading EXPLAIN",
      "## Index Design",
      "## Index Operations",
      "## Errors And Troubleshooting",
      "## Security",
      "## Performance Tuning",
      "## Architecture And Correctness",
      "## SQL Comparison",
      "## Further Reading"
    ]

    for section <- required_sections do
      assert documentation =~ section, "missing query documentation section: #{section}"
    end

    assert documentation =~ "https://www.postgresql.org/docs/current/using-explain.html"
    assert documentation =~ "https://www.sqlite.org/eqp.html"
    assert documentation =~ "https://www.sqlite.org/queryplanner.html"
  end

  test "the guide covers every public entry point and versioned response contract", %{
    documentation: documentation
  } do
    assert documentation =~ "FerricStore.flow_query"
    assert documentation =~ "FLOW.QUERY FQL1"
    assert documentation =~ "0x0231"

    for contract <- [
          Surface.request_contract(),
          Surface.default_result_contract(),
          Surface.default_explain_contract(),
          Surface.index_status_contract()
        ] do
      assert documentation =~ contract, "missing query contract #{contract} from the guide"
    end

    for shape <- Surface.default_capability_manifest().shapes do
      assert documentation =~ "`#{shape}`", "missing advertised query shape #{shape}"
    end

    for capability <- Surface.default_capability_manifest().capabilities do
      assert documentation =~ "`#{capability}`",
             "missing advertised query capability #{capability}"
    end
  end

  test "the native protocol documents the complete compact query result schema", %{
    native_protocol: native_protocol
  } do
    assert native_protocol =~ "flow_query_result_v1"
    assert native_protocol =~ "tag `0xA0`"
    assert native_protocol =~ "compact_flow_responses: boolean"
    assert native_protocol =~ "compact_response_codecs: list of advertised codec names"
    assert native_protocol =~ "response_codecs.selected_compact"
    assert native_protocol =~ "`0x0100`"
    assert native_protocol =~ "`0x0231`"

    for {field, index} <- Enum.with_index(ResultCodec.record_fields()) do
      assert Regex.match?(~r/\|\s*#{index}\s*\|\s*`#{field}`\s*\|/, native_protocol),
             "missing compact query-result bit #{index}=#{field} from the native protocol"
    end

    for field <- ResultCodec.usage_fields() do
      assert native_protocol =~ "   #{field}\n",
             "missing compact query-result usage field #{field} from the native protocol"
    end
  end

  test "the troubleshooting table covers every structured query error", %{
    documentation: documentation
  } do
    error_codes =
      ~r/\{"([a-z_]+)",\s*"(?:ERR|NOPERM) /m
      |> Regex.scan(File.read!(@error_source_path), capture: :all_but_first)
      |> List.flatten()
      |> Enum.uniq()

    assert length(error_codes) >= 29

    for code <- error_codes do
      assert documentation =~ "| `#{code}` |", "missing query error #{code}"
    end
  end

  defp parameter_types({:parameter, type, name}, acc), do: Map.put(acc, name, type)

  defp parameter_types(tuple, acc) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.reduce(acc, &parameter_types/2)
  end

  defp parameter_types(list, acc) when is_list(list),
    do: Enum.reduce(list, acc, &parameter_types/2)

  defp parameter_types(map, acc) when is_map(map),
    do: map |> Map.values() |> Enum.reduce(acc, &parameter_types/2)

  defp parameter_types(_term, acc), do: acc

  defp example_value(:integer), do: 1
  defp example_value(type) when type in [:keyword, :dynamic], do: String.duplicate("v", 16)
end
