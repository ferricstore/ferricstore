Code.require_file(
  "blocking_bug_hunt_test/sections/blocking_list_dispatch_converts_storage_raises_into_error_replies.exs",
  __DIR__
)

Code.require_file("blocking_bug_hunt_test/sections/blpop_float_timeout_0_5.exs", __DIR__)

defmodule FerricstoreServer.Commands.BlockingBugHuntTest do
  @moduledoc """
  Targeted tests that probe blocking list commands for correctness bugs.

  These tests cover edge cases and race conditions in:

    * `Ferricstore.Commands.Blocking` -- argument parsing and non-blocking dispatch
    * `Ferricstore.Waiters` -- ETS waiter registry accuracy
    * `FerricstoreServer.Connection` -- blocking dispatch over TCP

  TCP-level tests use a real Ranch listener (the one started by the application
  supervisor in test) and raw `:gen_tcp` connections with RESP3 encoding.

  ## Bug areas probed

  1. **BLPOP with float timeout (0.5)** -- `parse_timeout/1` uses `trunc/1`
     which truncates `0.5 * 1000` to 500ms. Verify the conversion is correct
     for various fractional second values.
  2. **BLPOP on multiple keys** -- first non-empty key should win immediately.
  3. **BLPOP timeout then normal command** -- connection must remain usable
     after a blocking timeout expires.
  4. **BRPOP wakes on RPUSH** -- BRPOP should pop from the right, not the left.
  5. **BLMOVE with empty source** -- should block (not crash) and return nil on
     timeout.
  6. **BLMPOP with COUNT > list length** -- should return all available elements,
     not error.
  7. **Two clients BLPOP same key** -- only one should wake per push (FIFO).
  8. **BLPOP then client disconnect** -- waiter entry must be cleaned up from
     the ETS table.
  9. **Waiters.count accuracy** -- after register/unregister cycles, the count
     must be exact.
  10. **BLPOP with negative timeout** -- must return an error, never block.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.{Blocking, List, Stream}
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Blocking, as: ConnBlocking
  alias FerricstoreServer.Connection.Store, as: ConnStore
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.MockStore
  alias Ferricstore.Waiters

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup_all do
    # Discover the listener port. If the listener isn't running, skip TCP tests.
    {port, tcp_healthy} =
      try do
        port = Listener.port()

        # Probe whether TCP commands actually work (the working tree may have
        # broken shard code that causes connection crashes). If we cannot
        # complete a PING, mark TCP as unhealthy.
        healthy =
          try do
            {:ok, sock} =
              :gen_tcp.connect(
                {127, 0, 0, 1},
                port,
                [:binary, active: false, packet: :raw],
                2_000
              )

            send_cmd_raw(sock, ["HELLO", "3"])

            case recv_raw(sock, 2_000) do
              {:ok, _} ->
                send_cmd_raw(sock, ["PING"])

                case recv_raw(sock, 2_000) do
                  {:ok, _} ->
                    :gen_tcp.close(sock)
                    true

                  _ ->
                    :gen_tcp.close(sock)
                    false
                end

              _ ->
                :gen_tcp.close(sock)
                false
            end
          rescue
            _ -> false
          catch
            _, _ -> false
          end

        {port, healthy}
      rescue
        _ -> {0, false}
      catch
        _, _ -> {0, false}
      end

    %{port: port, tcp_healthy: tcp_healthy}
  end

  setup do
    reset_server_auth_state()
    on_exit(fn -> reset_server_auth_state() end)

    # Ensure ETS table exists
    if :ets.whereis(:ferricstore_waiters) == :undefined do
      Waiters.init()
    end

    # Clean up stale waiters from prior tests
    :ets.delete_all_objects(:ferricstore_waiters)

    :ok
  end

  defp reset_server_auth_state do
    Ferricstore.Config.set("requirepass", "")
    Acl.reset!()
    ConnAuth.broadcast_acl_invalidation(:all)
    :ok
  end

  # ===========================================================================
  # TCP helpers
  # ===========================================================================

  use FerricstoreServer.Commands.BlockingBugHuntTest.Sections.BlockingListDispatchConvertsStorageRaisesIntoErrorReplies

  defp send_cmd_raw(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :gen_tcp.send(sock, data)
  end

  defp recv_raw(sock, timeout) do
    recv_raw_buf(sock, "", timeout)
  end

  defp recv_raw_buf(sock, buf, timeout) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val], _rest} -> {:ok, val}
          {:ok, [], _} -> recv_raw_buf(sock, buf2, timeout)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock, timeout \\ 5_000) do
    recv_response_buf(sock, "", timeout)
  end

  defp recv_response_buf(sock, buf, timeout) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val], _rest} -> {:ok, val}
          {:ok, [], _} -> recv_response_buf(sock, buf2, timeout)
        end

      {:error, :timeout} ->
        {:error, :timeout}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    {:ok, _greeting} = recv_response(sock)
    sock
  end

  defp ukey(name), do: "bughunt_#{name}_#{:erlang.unique_integer([:positive])}"

  defp wait_until(fun, deadline_ms \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    do_wait_until(fun, deadline)
  end

  defp assert_stream_entry({:continue, response, _state}, stream, id) do
    assert {:ok, [[[^stream, [[^id, "f", _value]]]]], ""} =
             Parser.parse(IO.iodata_to_binary(response))
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition was not met before timeout")

      true ->
        Process.sleep(5)
        do_wait_until(fun, deadline)
    end
  end

  defp with_raw_store(ctx, update_fn) do
    raw_store_key = raw_store_key(ctx)
    old_raw_store = :persistent_term.get(raw_store_key, :missing)
    Process.put({__MODULE__, :old_raw_store, ctx.name}, old_raw_store)

    ctx
    |> FerricstoreServer.Connection.Store.build_raw_store()
    |> update_fn.()
    |> then(&:persistent_term.put(raw_store_key, &1))
  end

  defp restore_raw_store(ctx) do
    raw_store_key = raw_store_key(ctx)

    case Process.delete({__MODULE__, :old_raw_store, ctx.name}) do
      :missing -> :persistent_term.erase(raw_store_key)
      nil -> :persistent_term.erase(raw_store_key)
      store -> :persistent_term.put(raw_store_key, store)
    end
  end

  defp raw_store_key(ctx), do: {:ferricstore_raw_store, ctx.name}

  # Skips a test if TCP is not healthy (shard crash loop in working tree).
  defp require_tcp!(%{tcp_healthy: true}), do: :ok

  defp require_tcp!(%{tcp_healthy: false}) do
    ExUnit.Assertions.flunk(
      "TCP infrastructure unhealthy (shard crash loop in working tree) -- skipping TCP test"
    )
  end

  # ===========================================================================
  # 1. BLPOP with timeout 0.5 (float seconds)
  # ===========================================================================

  use FerricstoreServer.Commands.BlockingBugHuntTest.Sections.BlpopFloatTimeout05
end
