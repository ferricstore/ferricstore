%{
  configs: [
    %{
      name: "default",
      strict: true,
      files: %{
        included: ["apps/ferricstore_http/lib/", "apps/ferricstore_http/test/"],
        excluded: [~r"/_build/", ~r"/deps/"]
      },
      checks: [
        {Credo.Check.Readability.MaxLineLength, max_length: 120},
        {Credo.Check.Refactor.CyclomaticComplexity, max_complexity: 10},
        {Credo.Check.Refactor.FunctionArity, max_arity: 6},
        {Credo.Check.Refactor.Nesting, max_nesting: 2}
      ]
    }
  ]
}
