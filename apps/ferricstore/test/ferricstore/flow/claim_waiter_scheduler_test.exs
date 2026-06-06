defmodule Ferricstore.Flow.ClaimWaiterSchedulerTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.ClaimWaiterScheduler

  test "schedule_next_due fails closed for unsupported inputs" do
    assert ClaimWaiterScheduler.schedule_next_due(%{}, :bad_type, :any, nil, :any, nil) == :ok
  end

  test "schedule_next_due tolerates missing router/index context" do
    assert ClaimWaiterScheduler.schedule_next_due(%{}, "email", :any, nil, :any, nil) == :ok
  end
end
