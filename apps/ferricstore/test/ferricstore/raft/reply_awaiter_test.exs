defmodule Ferricstore.Raft.ReplyAwaiterTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.ReplyAwaiter

  test "await returns replies sent through GenServer.reply" do
    {from, token} = ReplyAwaiter.new()

    GenServer.reply(from, :ok)

    assert :ok == ReplyAwaiter.await(token, 100, {:error, :timeout})
  end

  test "timeout drops late replies instead of leaking them into caller mailbox" do
    {from, token} = ReplyAwaiter.new()

    Process.send_after(self(), {:reply_late, from}, 20)

    assert {:error, :timeout} == ReplyAwaiter.await(token, 1, {:error, :timeout})

    receive do
      {:reply_late, late_from} -> GenServer.reply(late_from, :late)
    after
      100 -> flunk("late reply trigger did not arrive")
    end

    refute_receive {_tag, :late}, 50
  end

  test "batch collection ignores unrelated reference tuple messages" do
    unrelated_ref = make_ref()
    send(self(), {unrelated_ref, :do_not_consume})

    {from, token} = ReplyAwaiter.new()
    GenServer.reply(from, :ok)

    assert {:ok, [{token, :ok}], []} == ReplyAwaiter.collect([token], 100)
    assert_received {^unrelated_ref, :do_not_consume}
  end

  test "tagged batch collection returns metadata without consuming unrelated messages" do
    unrelated_ref = make_ref()
    send(self(), {unrelated_ref, :do_not_consume})

    {from, token} = ReplyAwaiter.new()
    GenServer.reply(from, :ok)

    assert {:ok, [{{:shard, 1}, :ok}], []} ==
             ReplyAwaiter.collect_tagged([{token, {:shard, 1}}], 100)

    assert_received {^unrelated_ref, :do_not_consume}
  end

  test "batch collection cancels unresolved tokens on timeout" do
    {from, token} = ReplyAwaiter.new()

    assert {:timeout, [], [token]} == ReplyAwaiter.collect([token], 1)

    GenServer.reply(from, :late)
    refute_receive {_tag, :late}, 50
  end

  test "forced quorum shard-native calls use alias-backed waiters" do
    source =
      Path.expand("../../..", __DIR__)
      |> Path.join("lib/ferricstore/store/shard/native_ops.ex")
      |> File.read!()

    assert source =~ "ReplyAwaiter.new()"
    refute source =~ "{self(), ref}"
  end
end
