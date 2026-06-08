defmodule Ferricstore.Commands.FlowSignalCommandTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.{MockStore, ShardHelpers}

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp uid(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"

  test "dispatches Flow signal through Rust AST" do
    id = uid("signal-dispatch")

    assert "OK" =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [
                 id,
                 "TYPE",
                 "signal-dispatch",
                 "STATE",
                 "waiting_payment",
                 "PARTITION",
                 "tenant-a",
                 "RUN_AT",
                 "1000",
                 "NOW",
                 "1000"
               ],
               MockStore.make()
             )

    assert "OK" =
             Dispatcher.dispatch(
               "FLOW.SIGNAL",
               [
                 id,
                 "PARTITION",
                 "tenant-a",
                 "SIGNAL",
                 "payment_received",
                 "IDEMPOTENCY",
                 "stripe_evt_1",
                 "IF_STATE",
                 "manual_review",
                 "IF_STATE",
                 "waiting_payment",
                 "TRANSITION_TO",
                 "verify_payment",
                 "VALUE",
                 "payment_event",
                 "payment-bytes",
                 "RUN_AT",
                 "1250",
                 "NOW",
                 "1100"
               ],
               MockStore.make()
             )

    assert %{"state" => "verify_payment", "values" => %{"payment_event" => "payment-bytes"}} =
             Dispatcher.dispatch(
               "FLOW.GET",
               [id, "PARTITION", "tenant-a", "VALUE", "payment_event"],
               MockStore.make()
             )
  end
end
