defmodule FerricstoreServer.Native.SessionSubscriptionBudgetTest do
  use ExUnit.Case, async: false

  alias Ferricstore.PubSub
  alias FerricstoreServer.Native.{OutboundBudget, ResourceBudget}
  alias FerricstoreServer.Native.Session

  @tag :native_subscription_budget
  test "subscriptions enforce retained-byte budgets and release global capacity" do
    budget = :"native_subscription_budget_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: budget,
       limits: %{
         executions: 1,
         lanes: 1,
         blocking_requests: 1,
         chunk_streams: 1,
         chunk_bytes: 1,
         inbound_bytes: 1,
         subscription_bytes: 150
       }}
    )

    state = session_state(budget, 150)
    first_channel = String.duplicate("c", 80)

    assert {:ok, _ack, state} =
             Session.execute(%{"command" => "SUBSCRIBE", "args" => [first_channel]}, state)

    assert state.pubsub_subscription_bytes == 144
    assert ResourceBudget.usage(budget).subscription_bytes == 144

    assert {:error, error, unchanged} =
             Session.execute(%{"command" => "SUBSCRIBE", "args" => ["second"]}, state)

    assert error =~ "subscription byte limit"
    assert unchanged.pubsub_channels == state.pubsub_channels
    assert unchanged.pubsub_subscription_bytes == 144
    assert ResourceBudget.usage(budget).subscription_bytes == 144

    Session.clear(state)
    assert ResourceBudget.usage(budget).subscription_bytes == 0
  end

  @tag :native_subscription_budget
  test "subscriptions reserve outbound bytes before pubsub mailbox delivery" do
    budget = :"native_subscription_delivery_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: budget,
       limits: %{
         executions: 1,
         lanes: 1,
         blocking_requests: 1,
         chunk_streams: 1,
         chunk_bytes: 1,
         inbound_bytes: 1,
         subscription_bytes: 1_000,
         session_bytes: 1,
         outbound_bytes: 1_000
       }}
    )

    counter = OutboundBudget.new_counter()

    state =
      budget
      |> session_state(1_000)
      |> Map.merge(%{outbound_counter: counter, max_outbound_bytes: 1_000})

    assert {:ok, _ack, subscribed} =
             Session.execute(%{"command" => "SUBSCRIBE", "args" => ["guarded-channel"]}, state)

    assert PubSub.publish("guarded-channel", "payload") == 1

    assert_receive {:pubsub_message, "guarded-channel", "payload", %OutboundBudget{} = lease}
    assert OutboundBudget.usage(counter) > 0
    assert ResourceBudget.usage(budget).outbound_bytes > 0

    assert :ok = OutboundBudget.release(lease)
    Session.clear(subscribed)
    assert OutboundBudget.usage(counter) == 0
    assert ResourceBudget.usage(budget).outbound_bytes == 0
  end

  defp session_state(budget, max_subscription_bytes) do
    %{
      require_auth: false,
      authenticated: true,
      acl_cache: :full_access,
      username: "default",
      client_id: System.unique_integer([:positive, :monotonic]),
      peer: {{127, 0, 0, 1}, 12_345},
      resource_budget: budget,
      max_pubsub_subscription_bytes: max_subscription_bytes,
      pubsub_subscription_bytes: 0,
      pubsub_subscription_token: nil,
      pubsub_channels: nil,
      pubsub_patterns: nil,
      multi_state: :none,
      multi_queue: [],
      multi_queue_count: 0,
      multi_queue_bytes: 0,
      multi_queue_byte_limit: 1_024,
      multi_error: false,
      watched_keys: %{},
      watched_key_bytes: 0,
      watch_key_limit: 10,
      watch_key_byte_limit: 1_024,
      sandbox_namespace: nil
    }
  end
end
