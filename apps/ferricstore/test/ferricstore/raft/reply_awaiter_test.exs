defmodule Ferricstore.Raft.ReplyAwaiterTest do
  use ExUnit.Case, async: true
  @moduletag :raft

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

  test "batch collection preserves replies for another waiter batch on timeout" do
    {other_from, {_other_alias_ref, other_tag} = other_token} = ReplyAwaiter.new()
    {from, {_alias_ref, tag} = token} = ReplyAwaiter.new()
    GenServer.reply(other_from, :other_batch)

    assert {:timeout, [], [token]} == ReplyAwaiter.collect([token], 10)
    assert_received {^other_tag, :other_batch}

    GenServer.reply(from, :late)
    refute_receive {^tag, :late}, 50

    :erlang.unalias(elem(other_token, 0))
  end

  test "tagged batch collection preserves replies for another waiter batch on timeout" do
    {other_from, {_other_alias_ref, other_tag} = other_token} = ReplyAwaiter.new()
    {from, {_alias_ref, tag} = token} = ReplyAwaiter.new()
    GenServer.reply(other_from, :other_batch)

    assert {:timeout, [], [{^token, :wanted}]} =
             ReplyAwaiter.collect_tagged([{token, :wanted}], 10)

    assert_received {^other_tag, :other_batch}

    GenServer.reply(from, :late)
    refute_receive {^tag, :late}, 50

    :erlang.unalias(elem(other_token, 0))
  end

  test "batch collection accepts an infinite timeout" do
    {from, token} = ReplyAwaiter.new()
    GenServer.reply(from, :ok)

    assert {:ok, [{token, :ok}], []} == ReplyAwaiter.collect([token], :infinity)

    {tagged_from, tagged_token} = ReplyAwaiter.new()
    GenServer.reply(tagged_from, :ok)

    assert {:ok, [{:shard, :ok}], []} ==
             ReplyAwaiter.collect_tagged([{tagged_token, :shard}], :infinity)
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
