defmodule FerricstoreServer.Native.SessionRetainedBudgetTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.PreparedCommand
  alias FerricstoreServer.Native.{ResourceBudget, Session}

  @tag :native_session_retained_budget
  test "MULTI queues enforce one global retained-byte ceiling across sessions" do
    payload = %{
      "command" => "SET",
      "args" => ["global-session-budget", :binary.copy("v", 256)]
    }

    assert {:ok, prepared} = Session.prepare_command(payload)

    command_bytes =
      prepared
      |> PreparedCommand.detach_retained_binaries()
      |> :erlang.external_size()

    budget = start_budget(command_bytes)
    first = begin_multi(session_state(budget))
    second = begin_multi(session_state(budget))

    assert {:ok, "QUEUED", first_queued} = Session.execute(payload, first)
    assert ResourceBudget.usage(budget).session_bytes == command_bytes

    assert {:error, message, rejected} = Session.execute(payload, second)
    assert message =~ "global retained session byte limit"
    assert rejected.multi_error
    assert rejected.multi_queue == []
    assert ResourceBudget.usage(budget).session_bytes == command_bytes

    Session.clear(first_queued)
    assert ResourceBudget.usage(budget).session_bytes == 0

    assert {:ok, "QUEUED", second_queued} = Session.execute(payload, second)
    Session.clear(second_queued)
    assert ResourceBudget.usage(budget).session_bytes == 0
  end

  @tag :native_session_retained_budget
  test "WATCH and MULTI share a lease that shrinks and releases with session state" do
    budget = start_budget(1_000_000)
    state = session_state(budget)
    key = "session-retained-watch:{#{System.unique_integer([:positive, :monotonic])}}"

    assert {:ok, "OK", watched} =
             Session.execute(%{"command" => "WATCH", "args" => [key]}, state)

    assert watched.watched_key_bytes > 0
    assert ResourceBudget.usage(budget).session_bytes == watched.watched_key_bytes

    multi = begin_multi(watched)

    assert {:ok, "QUEUED", queued} =
             Session.execute(
               %{"command" => "SET", "args" => [key, :binary.copy("v", 128)]},
               multi
             )

    assert ResourceBudget.usage(budget).session_bytes ==
             queued.watched_key_bytes + queued.multi_queue_bytes

    assert {:ok, "OK", unwatched} =
             Session.execute(%{"command" => "UNWATCH", "args" => []}, queued)

    assert unwatched.watched_key_bytes == 0
    assert ResourceBudget.usage(budget).session_bytes == unwatched.multi_queue_bytes

    assert {:ok, "OK", discarded} =
             Session.execute(%{"command" => "DISCARD", "args" => []}, unwatched)

    assert discarded.multi_queue_bytes == 0
    assert ResourceBudget.usage(budget).session_bytes == 0
  end

  @tag :native_session_retained_budget
  test "WATCH rejects global exhaustion before submitting a Raft command" do
    budget = start_budget(1_000)
    assert {:ok, holder} = ResourceBudget.acquire(budget, :session_bytes, self(), 1_000)

    state = session_state(budget)
    key = "session-retained-watch-reject:{#{System.unique_integer([:positive, :monotonic])}}"
    shard_index = Ferricstore.Store.Router.shard_for(state.instance_ctx, key)
    before_position = raft_position(shard_index)

    assert {:error, message, unchanged} =
             Session.execute(%{"command" => "WATCH", "args" => [key]}, state)

    assert message =~ "global retained session byte limit"
    assert unchanged.watched_keys == %{}
    assert unchanged.watched_key_bytes == 0
    assert raft_position(shard_index) == before_position

    assert :ok = ResourceBudget.release(budget, holder)
  end

  defp start_budget(session_bytes) do
    name = :"native_session_retained_budget_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       limits: %{
         executions: 1,
         lanes: 1,
         blocking_requests: 1,
         chunk_streams: 1,
         chunk_bytes: 1,
         inbound_bytes: 1,
         subscription_bytes: 1,
         session_bytes: session_bytes
       }}
    )

    name
  end

  defp begin_multi(state) do
    assert {:ok, "OK", multi} =
             Session.execute(%{"command" => "MULTI", "args" => []}, state)

    multi
  end

  defp session_state(budget) do
    %{
      require_auth: false,
      authenticated: true,
      acl_cache: :full_access,
      username: "default",
      client_id: System.unique_integer([:positive, :monotonic]),
      peer: {{127, 0, 0, 1}, 12_345},
      instance_ctx: FerricStore.Instance.get(:default),
      resource_budget: budget,
      session_byte_token: nil,
      multi_state: :none,
      multi_queue: [],
      multi_queue_count: 0,
      multi_queue_bytes: 0,
      multi_queue_byte_limit: 1_024 * 1_024,
      multi_error: false,
      watched_keys: %{},
      watched_key_bytes: 0,
      watch_key_limit: 10,
      watch_key_byte_limit: 1_024 * 1_024,
      sandbox_namespace: nil,
      pubsub_channels: nil,
      pubsub_patterns: nil,
      pubsub_subscription_bytes: 0,
      pubsub_subscription_token: nil
    }
  end

  defp raft_position(shard_index) do
    {:ok, {:raft_log_pos, index, _term}} =
      Ferricstore.Raft.WARaftBackend.storage_position(shard_index)

    index
  end
end
