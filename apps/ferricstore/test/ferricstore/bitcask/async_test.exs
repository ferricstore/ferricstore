defmodule Ferricstore.Bitcask.AsyncTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.Async

  test "monitor cleanup does not scan unrelated caller messages" do
    Enum.each(1..10_000, fn _ -> send(self(), :unrelated) end)
    {:reductions, before_count} = Process.info(self(), :reductions)

    for _ <- 1..100 do
      assert {:ok, :ok} =
               Async.await(
                 fn proxy, corr_id ->
                   send(proxy, {:tokio_complete, corr_id, :ok})
                   :ok
                 end,
                 :infinity
               )
    end

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 100_000
    assert {:message_queue_len, 10_000} = Process.info(self(), :message_queue_len)
  end

  test "infinite wait reports proxy death without leaking monitor messages" do
    test_pid = self()

    caller =
      spawn(fn ->
        result =
          Async.await(
            fn proxy, _corr_id ->
              send(test_pid, {:proxy_started, proxy})
              :ok
            end,
            :infinity
          )

        send(test_pid, {:result, result})

        receive do
          :inspect_mailbox -> send(test_pid, {:mailbox, Process.info(self(), :messages)})
        end
      end)

    on_exit(fn -> Process.exit(caller, :kill) end)
    assert_receive {:proxy_started, proxy}
    on_exit(fn -> Process.exit(proxy, :kill) end)
    Process.exit(proxy, :kill)
    assert_receive {:result, {:error, {:proxy_exit, :killed}}}, 500
    send(caller, :inspect_mailbox)
    assert_receive {:mailbox, {:messages, []}}
  end

  test "infinite proxy wait terminates when its caller dies" do
    test_pid = self()

    caller =
      spawn(fn ->
        Async.await(
          fn proxy, _corr_id ->
            send(test_pid, {:proxy_started, proxy})
            :ok
          end,
          :infinity
        )
      end)

    on_exit(fn -> Process.exit(caller, :kill) end)
    assert_receive {:proxy_started, proxy}
    on_exit(fn -> Process.exit(proxy, :kill) end)
    monitor = Process.monitor(proxy)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^proxy, _reason}, 500
  end

  test "successful completion wins over the proxy normal exit and cleans monitors" do
    {:monitors, before_monitors} = Process.info(self(), :monitors)

    for _ <- 1..100 do
      assert {:ok, "value"} =
               Async.await(
                 fn proxy, corr_id ->
                   send(proxy, {:tokio_complete, corr_id, :ok, "value"})
                   :ok
                 end,
                 :infinity
               )
    end

    assert {:monitors, ^before_monitors} = Process.info(self(), :monitors)
    refute_received {:DOWN, _, _, _, _}
    refute_received {{:proxy_down, _}, _, _, _, _}
  end

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

  test "await supports completion-bound waits" do
    assert {:ok, :ok} =
             Async.await(
               fn proxy, corr_id ->
                 send(proxy, {:tokio_complete, corr_id, :ok})
                 :ok
               end,
               :infinity
             )
  end

  test "a completion-bound waiter reports proxy death instead of hanging forever" do
    parent = self()

    caller =
      Task.async(fn ->
        Async.await(
          fn proxy, _corr_id ->
            send(parent, {:review_proxy, proxy})
            :ok
          end,
          :infinity
        )
      end)

    try do
      assert_receive {:review_proxy, proxy}, 1_000
      Process.exit(proxy, :kill)
      assert {:ok, {:error, {:proxy_exit, :killed}}} = Task.yield(caller, 1_000)
    after
      Task.shutdown(caller, :brutal_kill)
    end
  end

  test "a submitted completion-bound proxy exits when its caller is killed" do
    parent = self()

    caller =
      spawn(fn ->
        Async.await(
          fn proxy, _corr_id ->
            send(parent, {:review_orphan_proxy, proxy})
            :ok
          end,
          :infinity
        )
      end)

    assert_receive {:review_orphan_proxy, proxy}, 1_000

    on_exit(fn ->
      Process.exit(caller, :kill)
      Process.exit(proxy, :kill)
    end)

    monitor = Process.monitor(proxy)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^proxy, _}, 1_000
  end

  test "recursive removal waits for a definitive native completion" do
    source =
      __DIR__
      |> Path.join("../../../lib/ferricstore/fs.ex")
      |> Path.expand()
      |> File.read!()

    refute source =~ "@rm_rf_timeout_ms",
           "rm_rf must not return an indeterminate timeout while native deletion continues"

    assert source =~ ~r/Async\.await\([\s\S]*?:infinity[\s\S]*?\)/,
           "rm_rf must wait until the destructive operation has definitively completed"
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

  test "await reports submit exceptions without waiting for the operation timeout" do
    started_at = System.monotonic_time(:millisecond)

    assert {:error, {:submit_failed, :error, %RuntimeError{message: "submit exploded"}}} =
             Async.await(fn _proxy, _corr_id -> raise "submit exploded" end, 1_000)

    assert System.monotonic_time(:millisecond) - started_at < 200
  end

  test "await rejects invalid submit results without timing out" do
    assert {:error, {:invalid_submit_result, :queued}} =
             Async.await(fn _proxy, _corr_id -> :queued end, 100)
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

  test "timed-out await shuts down its proxy promptly" do
    test_pid = self()

    result =
      Async.await(
        fn proxy, corr_id ->
          send(test_pid, {:proxy_started, proxy, corr_id})
          Process.sleep(40)
          :ok
        end,
        50
      )

    assert {:error, :timeout} = result
    assert_receive {:proxy_started, proxy, _corr_id}

    Process.sleep(5)

    refute Process.alive?(proxy),
           "timed-out async waits must not keep one proxy process alive for another full timeout"
  end

  test "timed-out await shuts down proxy promptly when completion never arrives" do
    test_pid = self()

    assert {:error, :timeout} =
             Async.await(
               fn proxy, corr_id ->
                 send(test_pid, {:proxy_started, proxy, corr_id})
                 :ok
               end,
               10
             )

    assert_receive {:proxy_started, proxy, _corr_id}
    Process.sleep(5)

    refute Process.alive?(proxy),
           "a submitted async wait with no completion must not keep its proxy alive for a second full timeout"
  end
end
