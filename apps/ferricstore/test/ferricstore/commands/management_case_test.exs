defmodule Ferricstore.Commands.ManagementCaseTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.NativeAstParser

  test "scoped management subcommands are case-insensitive before ACL discovery" do
    for {command, args, expected_ast, expected_keys} <- [
          {"FERRICSTORE.NAMESPACE", ["ensure", "tenant:a"],
           {:ferricstore_namespace, ["ENSURE", "tenant:a"]}, ["tenant:a:*"]},
          {"FERRICSTORE.QUOTA", ["get", "tenant:b"],
           {:ferricstore_quota, ["GET", "tenant:b"]}, ["tenant:b:*"]},
          {"FERRICSTORE.TELEMETRY", ["namespace_usage", "tenant:c"],
           {:ferricstore_telemetry, ["NAMESPACE_USAGE", "tenant:c"]}, ["tenant:c:*"]}
        ] do
      assert {:ok, ^command, _normalized_args, ^expected_ast, ^expected_keys} =
               NativeAstParser.parse(command, args)
    end
  end
end
