defmodule Ferricstore.Store.ColdReadTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.ColdRead

  test "await_tokio returns successful completion" do
    assert {:ok, "value"} =
             ColdRead.await_tokio(
               fn proxy, corr_id ->
                 send(proxy, {:tokio_complete, corr_id, :ok, "value"})
                 :ok
               end,
               100
             )
  end

  test "await_tokio returns submit errors" do
    assert {:error, :closed} =
             ColdRead.await_tokio(
               fn _proxy, _corr_id ->
                 {:error, :closed}
               end,
               100
             )
  end

  test "await_tokio does not leak late completion into caller mailbox after timeout" do
    parent = self()

    assert {:error, :timeout} =
             ColdRead.await_tokio(
               fn proxy, corr_id ->
                 send(parent, {:proxy_started, proxy, corr_id})
                 :ok
               end,
               5
             )

    assert_receive {:proxy_started, proxy, corr_id}
    send(proxy, {:tokio_complete, corr_id, :ok, "late"})
    Process.sleep(20)

    refute_received _
  end

  test "await_tokio does not leak delayed submit errors after timeout" do
    parent = self()

    assert {:error, :timeout} =
             ColdRead.await_tokio(
               fn _proxy, _corr_id ->
                 send(parent, :submit_started)
                 Process.sleep(25)
                 {:error, :closed}
               end,
               5
             )

    assert_receive :submit_started
    Process.sleep(50)

    refute_received _
  end
end
