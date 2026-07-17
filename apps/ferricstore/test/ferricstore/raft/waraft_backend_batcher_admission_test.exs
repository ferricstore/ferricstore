defmodule Ferricstore.Raft.WARaftBackendBatcherAdmissionTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.WARaftBackend.Batcher
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Raft.ApplyContext
  alias Ferricstore.Raft.ReplyAwaiter
  alias Ferricstore.Raft.Batcher, as: RaftBatcher

  test "namespace queue rejects writes after the next batch reaches its cap" do
    pid = start_batcher(65_001, namespace_batch_max: 2)

    prefix = "rate"
    timer_token = make_ref()

    slot = %{
      commands: [{:put, "rate:2", "v", 0}, {:put, "rate:1", "v", 0}],
      froms: [{self(), make_ref()}, {self(), make_ref()}],
      count: 2,
      timer_ref: nil,
      timer_token: timer_token,
      window_ms: 10,
      created_mono: System.monotonic_time()
    }

    :sys.replace_state(pid, fn state ->
      %{
        state
        | slots: %{prefix => slot},
          in_flight: %{{:prefix, prefix} => make_ref()}
      }
    end)

    assert {:error, :batcher_overloaded} =
             GenServer.call(pid, {:write, prefix, {:put, "rate:3", "v", 0}, 10}, 100)

    assert %{slots: %{^prefix => %{count: 2}}} = :sys.get_state(pid)
    assert Process.alive?(pid)
  end

  test "namespace queue shares replicated work behind an in-flight flush" do
    context = ApplyContext.new(batch_command_apply_budget: 3)
    pid = start_batcher(65_015, namespace_batch_max: 10, apply_context: context)
    prefix = "mset"

    slot = %{
      commands: [{:mset, [{"first", "1", nil}, {"second", "2", nil}]}],
      froms: [{self(), make_ref()}],
      count: 1,
      command_work: 2,
      compound_work: 0,
      visit_work: 1,
      timer_ref: nil,
      timer_token: make_ref(),
      window_ms: 10,
      created_mono: System.monotonic_time()
    }

    :sys.replace_state(pid, fn state ->
      %{
        state
        | slots: %{prefix => slot},
          in_flight: %{{:prefix, prefix} => make_ref()}
      }
    end)

    assert {:error, :batch_command_apply_budget_exceeded} =
             GenServer.call(
               pid,
               {:write, prefix, {:mset, [{"third", "3", nil}, {"fourth", "4", nil}]}, 10},
               100
             )

    assert %{slots: %{^prefix => %{count: 1, command_work: 2}}} = :sys.get_state(pid)
  end

  test "generic queue rejects a group that would exceed the next-batch cap" do
    pid = start_batcher(65_002, hot_batch_max: 2)
    install_hot_queue(pid, :batch, :batch_slot, 1)

    assert {:error, :batcher_overloaded} =
             GenServer.call(
               pid,
               {:write_batch, [{:delete, "k:2"}, {:delete, "k:3"}], 0},
               100
             )

    assert %{batch_slot: %{count: 1}} = :sys.get_state(pid)
  end

  test "generic queue rejects an oversized first group" do
    pid = start_batcher(65_007, hot_batch_max: 2)

    assert {:error, :batcher_overloaded} =
             GenServer.call(
               pid,
               {:write_batch, [{:delete, "k:1"}, {:delete, "k:2"}, {:delete, "k:3"}], 1},
               100
             )

    assert %{batch_slot: nil} = :sys.get_state(pid)
  end

  test "generic queue rolls over an idle partial slot for an individually valid group" do
    previous_hook = Application.get_env(:ferricstore, :waraft_backend_batcher_call_hook)
    Application.put_env(:ferricstore, :waraft_backend_batcher_call_hook, {:block, self()})

    on_exit(fn ->
      restore_env(:waraft_backend_batcher_call_hook, previous_hook)
    end)

    pid = start_batcher(65_014, hot_batch_max: 2)
    first_reply_tag = make_ref()
    second_reply_tag = make_ref()

    GenServer.cast(
      pid,
      {:write_batch, [{:delete, "k:1"}], 5_000, {self(), first_reply_tag}, nil}
    )

    assert %{batch_slot: %{count: 1}, in_flight: in_flight} = :sys.get_state(pid)
    assert in_flight == %{}

    GenServer.cast(
      pid,
      {:write_batch, [{:delete, "k:2"}, {:delete, "k:3"}], 5_000, {self(), second_reply_tag}, nil}
    )

    assert_receive {:waraft_backend_batcher_call, :__commit_single_batch_direct__, ref, worker},
                   100

    assert %{batch_slot: %{count: 2}, in_flight: %{batch: _ref}} = :sys.get_state(pid)
    refute_receive {^second_reply_tag, {:error, :batcher_overloaded}}, 0

    send(worker, {ref, :continue})
  end

  test "put queue rejects a group that would exceed the next-batch cap" do
    pid = start_batcher(65_003, hot_batch_max: 2)
    install_hot_queue(pid, :put_batch, :put_slot, 1)

    assert {:error, :batcher_overloaded} =
             GenServer.call(
               pid,
               {:write_put_batch, [{"k:2", "v", 0}, {"k:3", "v", 0}], 1},
               100
             )

    assert %{put_slot: %{count: 1}} = :sys.get_state(pid)
  end

  test "put queue rejects an oversized first group" do
    pid = start_batcher(65_008, hot_batch_max: 2)

    assert {:error, :batcher_overloaded} =
             GenServer.call(
               pid,
               {:write_put_batch, [{"k:1", "v", 0}, {"k:2", "v", 0}, {"k:3", "v", 0}], 1},
               100
             )

    assert %{put_slot: nil} = :sys.get_state(pid)
  end

  test "delete queue rejects a group that would exceed the next-batch cap" do
    pid = start_batcher(65_004, hot_batch_max: 2)
    install_hot_queue(pid, :delete_batch, :delete_slot, 1)

    assert {:error, :batcher_overloaded} =
             GenServer.call(pid, {:write_delete_batch, ["k:2", "k:3"], 1}, 100)

    assert %{delete_slot: %{count: 1}} = :sys.get_state(pid)
  end

  test "delete queue rejects an oversized first group" do
    pid = start_batcher(65_009, hot_batch_max: 2)

    assert {:error, :batcher_overloaded} =
             GenServer.call(pid, {:write_delete_batch, ["k:1", "k:2", "k:3"], 1}, 100)

    assert %{delete_slot: nil} = :sys.get_state(pid)
  end

  test "async hot queue overload replies instead of stranding the caller" do
    pid = start_batcher(65_005, hot_batch_max: 1)
    install_hot_queue(pid, :put_batch, :put_slot, 1)
    reply_tag = make_ref()

    GenServer.cast(pid, {:write_put_batch, [{"k:2", "v", 0}], 1, {self(), reply_tag}, nil})

    assert_receive {^reply_tag, {:error, :batcher_overloaded}}, 100
    assert %{put_slot: %{count: 1}} = :sys.get_state(pid)
  end

  test "generic queue rejects inner apply work above the replicated budget" do
    context = ApplyContext.new(batch_command_apply_budget: 2)
    pid = start_batcher(65_010, hot_batch_max: 10, apply_context: context)
    reply_tag = make_ref()

    GenServer.cast(
      pid,
      {:write_batch,
       [
         {:mset,
          [
            {"k:1", "v", 0},
            {"k:2", "v", 0},
            {"k:3", "v", 0}
          ]}
       ], 5_000, {self(), reply_tag}, nil}
    )

    assert_receive {^reply_tag, {:error, :batch_command_apply_budget_exceeded}}, 100
    assert %{batch_slot: nil} = :sys.get_state(pid)
  end

  test "generic queue applies the compound budget across separately submitted groups" do
    context =
      ApplyContext.new(
        batch_command_apply_budget: 10,
        compound_member_apply_budget: 1
      )

    pid = start_batcher(65_011, hot_batch_max: 10, apply_context: context)
    first_reply_tag = make_ref()
    second_reply_tag = make_ref()

    GenServer.cast(
      pid,
      {:write_batch, [{:zadd_single, "zset", 1.0, "first"}], 5_000, {self(), first_reply_tag},
       nil}
    )

    assert %{batch_slot: %{count: 1}} = :sys.get_state(pid)

    :sys.replace_state(pid, fn state ->
      %{state | in_flight: %{batch: make_ref()}}
    end)

    GenServer.cast(
      pid,
      {:write_batch, [{:zadd_single, "zset", 2.0, "second"}], 5_000, {self(), second_reply_tag},
       nil}
    )

    assert_receive {^second_reply_tag, {:error, :compound_member_apply_budget_exceeded}}, 100
    refute_receive {^first_reply_tag, _reply}, 0
    assert %{batch_slot: %{count: 1}} = :sys.get_state(pid)
  end

  test "put queue rejects work above the replicated batch budget" do
    context = ApplyContext.new(batch_command_apply_budget: 2)
    pid = start_batcher(65_012, hot_batch_max: 10, apply_context: context)
    reply_tag = make_ref()

    GenServer.cast(
      pid,
      {:write_put_batch, [{"k:1", "v", 0}, {"k:2", "v", 0}, {"k:3", "v", 0}], 5_000,
       {self(), reply_tag}, nil}
    )

    assert_receive {^reply_tag, {:error, :batch_command_apply_budget_exceeded}}, 100
    assert %{put_slot: nil} = :sys.get_state(pid)
  end

  test "delete queue rejects work above the replicated batch budget" do
    context = ApplyContext.new(batch_command_apply_budget: 2)
    pid = start_batcher(65_013, hot_batch_max: 10, apply_context: context)
    reply_tag = make_ref()

    GenServer.cast(
      pid,
      {:write_delete_batch, ["k:1", "k:2", "k:3"], 5_000, {self(), reply_tag}, nil}
    )

    assert_receive {^reply_tag, {:error, :batch_command_apply_budget_exceeded}}, 100
    assert %{delete_slot: nil} = :sys.get_state(pid)
  end

  test "generic async submission uses the bounded batcher queue" do
    shard_index = 65_006
    pid = start_batcher(shard_index, hot_batch_max: 1)
    install_hot_queue(pid, :batch, :batch_slot, 1)
    reply_tag = make_ref()

    assert :ok =
             Batcher.write_batch_async(
               shard_index,
               [{:incr, "counter", 1}],
               {self(), reply_tag},
               nil
             )

    assert_receive {^reply_tag, {:error, :batcher_overloaded}}, 100
    assert %{batch_slot: %{count: 1}} = :sys.get_state(pid)
  end

  test "public generic async submission replies on admission errors" do
    reply_tag = make_ref()

    assert :ok =
             WARaftBackend.write_batch_async(
               -1,
               [{:incr, "counter", 1}],
               {self(), reply_tag}
             )

    assert_receive {^reply_tag, {:error, {:invalid_shard_index, -1}}}, 100
  end

  test "Raft async facade does not spawn one blocking task per submitted write" do
    source =
      __DIR__
      |> Path.join("../../../lib/ferricstore/raft/batcher.ex")
      |> Path.expand()
      |> File.read!()

    assert source =~ "WARaftBackend.write_batch_async"
    refute source =~ "defp reply_later"
    refute source =~ "Task.start"
  end

  test "generic async single submission preserves the scalar reply contract" do
    key = "batcher:single:#{System.unique_integer([:positive])}"
    {from, token} = ReplyAwaiter.new()

    try do
      assert :ok = RaftBatcher.write_async(0, {:incr, key, 1}, from)

      assert {:ok, 1} == ReplyAwaiter.await(token, 5_000, {:error, :timeout})
    after
      _ = WARaftBackend.write(0, {:delete, key})
    end
  end

  defp start_batcher(shard_index, opts) do
    {:ok, pid} = Batcher.start_link(shard_index, opts)
    Process.unlink(pid)

    on_exit(fn ->
      if Process.alive?(pid), do: Process.exit(pid, :kill)
    end)

    pid
  end

  defp install_hot_queue(pid, kind, field, count) do
    slot = %{
      groups: [],
      count: count,
      timer_ref: nil,
      timer_token: make_ref(),
      window_ms: 1,
      created_mono: System.monotonic_time()
    }

    :sys.replace_state(pid, fn state ->
      state
      |> Map.put(field, slot)
      |> Map.put(:in_flight, %{kind => make_ref()})
    end)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
