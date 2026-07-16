defmodule Ferricstore.CrossShardLockRenewalTest do
  use ExUnit.Case, async: true

  alias Ferricstore.CrossShardOp

  test "a live cross-shard execute window renews its locks repeatedly" do
    renewals = :atomics.new(1, signed: false)

    assert {:ok, :done} =
             CrossShardOp.__with_lock_renewal_for_test__(
               fn _lease ->
                 Process.sleep(75)
                 :done
               end,
               fn ->
                 :atomics.add(renewals, 1, 1)
                 :ok
               end,
               10
             )

    assert :atomics.get(renewals, 1) >= 3
  end

  test "a failed renewal fails closed and prevents later locked writes" do
    parent = self()

    assert {:error, :lock_lease_lost} =
             CrossShardOp.__with_lock_renewal_for_test__(
               fn lease ->
                 assert_receive :renewal_failed, 250

                 assert {:error, :lock_lease_lost} =
                          CrossShardOp.__locked_write_for_test__(lease, fn ->
                            send(parent, :write_ran)
                            :ok
                          end)

                 :ignored_success
               end,
               fn ->
                 send(parent, :renewal_failed)
                 {:error, :not_lock_owner}
               end,
               10
             )

    refute_received :write_ran
  end
end
