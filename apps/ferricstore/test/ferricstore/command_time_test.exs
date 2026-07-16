defmodule Ferricstore.CommandTimeTest do
  use ExUnit.Case, async: true

  alias Ferricstore.CommandTime
  alias Ferricstore.HLC

  test "falls back to cluster HLC time outside raft apply" do
    before_ms = HLC.now_ms()
    now_ms = CommandTime.now_ms()
    after_ms = HLC.now_ms()

    assert now_ms >= before_ms
    assert now_ms <= after_ms
  end

  test "uses stamped raft apply time inside scoped command execution" do
    local_now = HLC.now_ms()
    stamped_now = local_now - 30_000

    assert CommandTime.with_now_ms(stamped_now, fn -> CommandTime.now_ms() end) == stamped_now
  end

  test "preserves a zero timestamp inside raft apply" do
    assert CommandTime.with_now_ms(0, fn -> CommandTime.now_ms() end) == 0
  end

  test "rejects negative raft apply timestamps" do
    assert_raise FunctionClauseError, fn ->
      CommandTime.with_now_ms(-1, fn -> :unreachable end)
    end
  end

  test "restores previous apply time after nested scopes" do
    outer = HLC.now_ms() - 20_000
    inner = outer + 5_000

    assert CommandTime.with_now_ms(outer, fn ->
             nested = CommandTime.with_now_ms(inner, fn -> CommandTime.now_ms() end)
             {CommandTime.now_ms(), nested}
           end) == {outer, inner}
  end
end
