defmodule FerricstoreServer.Health.Dashboard.QueryParamsTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.QueryParams

  test "keyword lookup does not intern an unknown string key" do
    key = "unknown_dashboard_key_#{System.unique_integer([:positive])}"

    assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
    assert QueryParams.dashboard_param([known: "value"], key) == ""
    assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
  end

  test "keyword lookup still matches existing atom keys" do
    assert QueryParams.dashboard_param([range: "hour"], "range") == "hour"
  end

  test "list lookup supports string keys without atom conversion" do
    assert QueryParams.dashboard_param([{"range", "hour"}], "range") == "hour"
  end
end
