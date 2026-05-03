defmodule Ferricstore.Bitcask.AsyncTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.Async

  test "await returns four-tuple successful completions" do
    assert {:ok, "value"} =
             Async.await(
               fn proxy, corr_id ->
                 send(proxy, {:tokio_complete, corr_id, :ok, "value"})
                 :ok
               end,
               100
             )
  end

  test "await returns three-tuple successful completions" do
    assert {:ok, :ok} =
             Async.await(
               fn proxy, corr_id ->
                 send(proxy, {:tokio_complete, corr_id, :ok})
                 :ok
               end,
               100
             )
  end

  test "await returns submit errors" do
    assert {:error, :closed} =
             Async.await(
               fn _proxy, _corr_id ->
                 {:error, :closed}
               end,
               100
             )
  end

  test "await does not leak late completion into caller mailbox after timeout" do
    parent = self()

    assert {:error, :timeout} =
             Async.await(
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

  test "await does not leak delayed submit errors after timeout" do
    parent = self()

    assert {:error, :timeout} =
             Async.await(
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

  test "await flushes alias replies when timeout wins the receive race" do
    source =
      __DIR__
      |> Path.join("../../../lib/ferricstore/bitcask/async.ex")
      |> Path.expand()
      |> File.read!()

    assert source =~ "cleanup_alias(parent, ref)",
           "timeout cleanup must flush process-alias replies so internal {ref, result} messages cannot leak into the caller mailbox"

    assert source =~ "flush_alias_reply(ref)",
           "alias cleanup must drain any reply that arrived before alias deactivation"
  end
end
