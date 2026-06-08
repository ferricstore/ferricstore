defmodule Ferricstore.Flow.ClaimDueAPITest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.ClaimDueAPI

  test "wait_keys supports explicit state" do
    assert {:ok, [_ | _]} =
             ClaimDueAPI.wait_keys("type", state: "queued", partition_key: "p")
  end

  test "return_records supports compact jobs" do
    assert [["id", "p", "lease", 7]] =
             ClaimDueAPI.return_records(
               :ctx,
               [%{id: "id", partition_key: "p", lease_token: "lease", fencing_token: 7}],
               %{enabled?: false, max_bytes: 0},
               :jobs_compact,
               nil
             )
  end
end
