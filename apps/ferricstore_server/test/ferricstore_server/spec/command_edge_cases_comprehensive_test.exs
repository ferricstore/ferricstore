Code.require_file("command_edge_cases_comprehensive_test/sections/wrongtype_cross_type_access.exs", __DIR__)
Code.require_file("command_edge_cases_comprehensive_test/sections/wrong_arity_rejection_sweep.exs", __DIR__)
defmodule FerricstoreServer.Spec.CommandEdgeCasesComprehensiveTest do
  @moduledoc """
  Comprehensive edge-case tests for EVERY FerricStore command category.

  Tests the real TCP attack surface using RESP3 framing. Focuses on inputs
  that could crash, corrupt, or return wrong results:

    - WRONGTYPE cross-type access
    - Integer overflow / underflow at i64 boundaries
    - Empty keys, empty values, empty field names
    - Key size at u16 boundary (65535 bytes)
    - Negative indices beyond string/list length
    - Non-numeric values where numbers are expected
    - Odd argument counts where pairs are required
    - Operations on non-existent keys
    - SETRANGE zero-padding
    - GETRANGE boundary arithmetic
    - TTL edge cases (past timestamps, non-existent keys)
    - Transaction edge cases (EXEC without MULTI, errors inside MULTI)
    - Pipeline stress (1000 commands, mixed valid+invalid)
    - SET option combinations (NX+XX, EX+PX, KEEPTTL+EX)
    - Sorted set score parsing (+inf, -inf, NaN, non-numeric)
    - SRANDMEMBER / ZRANDMEMBER with negative count (duplicates allowed)
    - RENAME on non-existent source and COPY no-op cases
    - SCAN cursor-based iteration edge cases

  Each test is independent (flush_all_keys in setup).
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

  @moduletag timeout: 120_000

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    ShardHelpers.wait_shards_alive()
    %{port: Listener.port()}
  end

  setup %{port: port} do
    # Reset process dictionary recv buffers from any previous test
    Process.delete(:edge_parsed_queue)
    Process.delete(:edge_binary_buf)

    sock = connect_and_hello(port)
    ShardHelpers.flush_all_keys()
    on_exit(fn -> :gen_tcp.close(sock) end)
    %{sock: sock, port: port}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  use FerricstoreServer.Spec.CommandEdgeCasesComprehensiveTest.Sections.WrongtypeCrossTypeAccess

  use FerricstoreServer.Spec.CommandEdgeCasesComprehensiveTest.Sections.WrongArityRejectionSweep

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [
        :binary,
        active: false,
        packet: :raw,
        nodelay: true,
        recbuf: 4 * 1024 * 1024,
        sndbuf: 4 * 1024 * 1024
      ])

    send_cmd(sock, ["HELLO", "3"])
    # Use direct recv for HELLO to avoid polluting the process dictionary buffer
    _greeting = recv_direct(sock, "", 5_000)
    sock
  end

  # Direct recv that does not use process dictionary buffering.
  # Used for HELLO handshake and secondary sockets to avoid buffer cross-contamination.
  defp recv_direct(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | _], _rest} ->
        val

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_direct(sock, buf <> data, timeout)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  defp send_cmd(sock, cmd) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(cmd)))
  end

  # Process dictionary keys for buffering between recv_response calls
  @parsed_key :edge_parsed_queue
  @binary_key :edge_binary_buf

  defp recv_response(sock, timeout \\ 10_000) do
    case Process.get(@parsed_key, []) do
      [val | rest] ->
        Process.put(@parsed_key, rest)
        val

      [] ->
        buf = Process.get(@binary_key, "")
        recv_loop(sock, buf, timeout)
    end
  end

  defp recv_loop(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | rest_vals], rest_bin} ->
        Process.put(@parsed_key, rest_vals)
        Process.put(@binary_key, rest_bin)
        val

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_loop(sock, buf <> data, timeout)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  defp cmd(sock, args) do
    send_cmd(sock, args)
    recv_response(sock)
  end

  # cmd for secondary sockets that should not interfere with process dictionary buffer
  defp cmd_direct(sock, args) do
    send_cmd(sock, args)
    recv_direct(sock, "", 10_000)
  end

  # Hash tag ensures all keys co-locate on the same shard for multi-key ops.
  defp ukey(base), do: "{edgetest}:#{base}_#{:rand.uniform(9_999_999)}"

  defp assert_ok(result), do: assert(result == {:simple, "OK"} or result == :ok)

  defp assert_error_contains(result, substring) do
    assert {:error, msg} = result

    assert String.contains?(msg, substring),
           "Expected error containing #{inspect(substring)}, got: #{inspect(msg)}"
  end

  # ===========================================================================
  # WRONGTYPE CROSS-TYPE ERRORS
  # ===========================================================================


  # ===========================================================================
  # TTL / EXPIRY EDGE CASES
  # ===========================================================================


  # ===========================================================================
  # GENERIC COMMANDS EDGE CASES
  # ===========================================================================


  # ===========================================================================
  # SERVER COMMANDS EDGE CASES
  # ===========================================================================


  # ===========================================================================
  # TRANSACTION EDGE CASES
  # ===========================================================================


  # ===========================================================================
  # PIPELINE EDGE CASES
  # ===========================================================================


  # ===========================================================================
  # SCAN EDGE CASES
  # ===========================================================================


  # Helper: collects all keys from SCAN iterations
  defp scan_all(sock, opt_key, opt_value) do
    scan_loop(sock, "0", opt_key, opt_value, [])
  end

  defp scan_loop(sock, cursor, opt_key, opt_value, acc) do
    [next_cursor, keys] = cmd(sock, ["SCAN", cursor, opt_key, opt_value])
    new_acc = acc ++ keys

    if next_cursor == "0" do
      new_acc
    else
      scan_loop(sock, next_cursor, opt_key, opt_value, new_acc)
    end
  end

  # ===========================================================================
  # BITMAP EDGE CASES
  # ===========================================================================


  # ===========================================================================
  # HYPERLOGLOG EDGE CASES
  # ===========================================================================


  # ===========================================================================
  # CONNECTION / CLIENT EDGE CASES
  # ===========================================================================


  # ===========================================================================
  # BINARY SAFETY EDGE CASES
  # ===========================================================================


  # ===========================================================================
  # WRONG ARITY SWEEP: Ensure every command rejects wrong arg counts
  # ===========================================================================


  # ===========================================================================
  # STRESS: Rapid key creation and deletion
  # ===========================================================================

end
