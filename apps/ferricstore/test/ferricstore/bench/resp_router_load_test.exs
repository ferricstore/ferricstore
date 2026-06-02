defmodule Ferricstore.Bench.RespRouterLoadTest do
  use ExUnit.Case, async: true

  @support_path Path.expand("../../../../../bench/support/resp_router_load.exs", __DIR__)

  Code.require_file(@support_path)

  alias Ferricstore.Bench.RespRouterLoad

  test "SET pipeline builder accounts for one OK reply per command" do
    payload = RespRouterLoad.payload(8)
    {wire, expected_bytes, count} = RespRouterLoad.pipeline(:set, 41, 3, payload, 128)

    assert count == 3
    assert expected_bytes == 3 * byte_size("+OK\r\n")
    assert wire =~ "SET"
    assert wire =~ "bench:42"
    assert wire =~ payload
  end

  test "GET pipeline builder accounts for fixed-size bulk replies" do
    payload = RespRouterLoad.payload(10)
    {wire, expected_bytes, count} = RespRouterLoad.pipeline(:get, 0, 4, payload, 2)

    assert count == 4
    assert expected_bytes == 4 * RespRouterLoad.response_bytes(:get, payload)
    assert wire =~ "GET"
    assert wire =~ "bench:1"
    assert wire =~ "bench:2"
  end

  test "mixed pipeline builder preserves command order and expected reply sizes" do
    payload = RespRouterLoad.payload(6)
    {wire, expected_bytes, count} = RespRouterLoad.pipeline(:mixed, 0, 4, payload, 2)

    assert count == 4

    assert expected_bytes ==
             2 * RespRouterLoad.response_bytes(:get, payload) +
               2 * RespRouterLoad.response_bytes(:set, payload)

    assert wire =~ "GET"
    assert wire =~ "SET"
  end

  test "work ranges cover the requested total without dropping tail commands" do
    assert RespRouterLoad.work_ranges(10, 3) == [{0, 4}, {4, 3}, {7, 3}]
    assert RespRouterLoad.work_ranges(2, 4) == [{0, 1}, {1, 1}]
  end
end
