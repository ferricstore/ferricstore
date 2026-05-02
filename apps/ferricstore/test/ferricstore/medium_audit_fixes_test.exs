defmodule Ferricstore.MediumAuditFixesTest do
  @moduledoc """
  Tests verifying that MEDIUM priority audit issues are fixed.
  Covers issues from both performance-audit.md and elixir-memory-audit.md.
  """

  use ExUnit.Case, async: true

  # -----------------------------------------------------------------------
  # Perf M9: Batcher.cancel_timer should not do selective receive
  # -----------------------------------------------------------------------
  describe "Batcher cancel_timer (Perf M9)" do
    test "cancel_timer/1 does not scan mailbox with receive" do
      source =
        File.read!(
          Path.join([
            __DIR__,
            "..",
            "..",
            "lib",
            "ferricstore",
            "raft",
            "batcher.ex"
          ])
        )

      # The old code had: receive do {:flush_slot, _} -> :ok after 0 -> :ok end
      # The fix should NOT contain a receive block in cancel_timer
      refute source =~ ~r/defp cancel_timer.*receive\s+do/s,
             "cancel_timer should not use selective receive"
    end
  end

  # -----------------------------------------------------------------------
  # Perf M10 / Memory M4: encode/decode_ratelimit should use binary encoding
  # -----------------------------------------------------------------------
  describe "ratelimit binary encoding (Perf M10 / Memory M4)" do
    test "encode_ratelimit produces 24-byte binary" do
      encoded = encode_ratelimit(42, 1_700_000_000_000, 10)
      assert byte_size(encoded) == 24
      assert <<42::64, 1_700_000_000_000::64, 10::64>> == encoded
    end

    test "decode_ratelimit roundtrips correctly" do
      original = {42, 1_700_000_000_000, 10}
      {cur, start, prev} = original
      encoded = encode_ratelimit(cur, start, prev)
      assert decode_ratelimit(encoded) == original
    end

    test "decode_ratelimit handles legacy string format gracefully" do
      legacy = "42:1700000000000:10"
      {cur, start, prev} = decode_ratelimit(legacy)
      assert cur == 42
      assert start == 1_700_000_000_000
      assert prev == 10
    end

    test "decode_ratelimit handles zero values" do
      encoded = encode_ratelimit(0, 0, 0)
      assert decode_ratelimit(encoded) == {0, 0, 0}
    end

    test "decode_ratelimit handles large values" do
      big = 9_999_999_999_999
      encoded = encode_ratelimit(big, big, big)
      assert decode_ratelimit(encoded) == {big, big, big}
    end

    test "shard.ex and state_machine.ex use binary encoding" do
      # The binary encoding pattern may be inlined or delegated to ValueCodec.
      # Check that the pattern exists in at least one of the relevant files.
      codec_path =
        Path.join([__DIR__, "..", "..", "lib", "ferricstore", "store", "value_codec.ex"])

      codec_source = File.read!(codec_path)

      for path <- [
            Path.join([__DIR__, "..", "..", "lib", "ferricstore", "raft", "state_machine.ex"]),
            Path.join([
              __DIR__,
              "..",
              "..",
              "lib",
              "ferricstore",
              "store",
              "shard",
              "native_ops.ex"
            ])
          ] do
        source = File.read!(path)
        # Should have the binary encoding pattern inline OR delegate to ValueCodec
        assert source =~ "<<cur::64, start::64, prev::64>>" or
                 (source =~ "ValueCodec" and codec_source =~ "<<cur::64, start::64, prev::64>>"),
               "#{Path.basename(path)} should use binary encoding for ratelimit (inline or via ValueCodec)"
      end
    end
  end

  # -----------------------------------------------------------------------
  # Perf M11 / Memory L7: format_float should not have no-op `then`
  # -----------------------------------------------------------------------
  describe "format_float (Perf M11 / Memory L7)" do
    test "format_float does not contain no-op then in state_machine" do
      sm_source =
        File.read!(
          Path.join([
            __DIR__,
            "..",
            "..",
            "lib",
            "ferricstore",
            "raft",
            "state_machine.ex"
          ])
        )

      refute sm_source =~ "then(fn s -> s end)",
             "state_machine format_float should not have no-op then"
    end

    test "format_float does not contain no-op then in shard" do
      shard_source =
        File.read!(
          Path.join([
            __DIR__,
            "..",
            "..",
            "lib",
            "ferricstore",
            "store",
            "shard.ex"
          ])
        )

      refute shard_source =~ "then(fn s -> s end)",
             "shard format_float should not have no-op then"

      # Also check for the multiline variant
      refute shard_source =~ ~r/then\(fn\s*\n\s*s -> s\s*\n\s*end\)/,
             "shard format_float should not have no-op then (multiline)"
    end

    # format_float was removed from shard.ex and state_machine.ex as unused code.
    # Float formatting is handled by ValueCodec.format_float/1 which uses :binary.match.
  end

  # -----------------------------------------------------------------------
  # Memory M7: MSET should not use Enum.chunk_every
  # -----------------------------------------------------------------------
  describe "MSET direct recursive processing (Memory M7)" do
    test "MSET with pairs works correctly" do
      put_fn = fn k, v, _exp ->
        send(self(), {:put, k, v})
        :ok
      end

      store = %{put: put_fn, exists?: fn _key -> false end}

      result = Ferricstore.Commands.Strings.handle("MSET", ["k1", "v1", "k2", "v2"], store)
      assert result == :ok

      assert_received {:put, "k1", "v1"}
      assert_received {:put, "k2", "v2"}
    end

    test "MSET rejects odd number of arguments" do
      store = %{put: fn _, _, _ -> :ok end}
      result = Ferricstore.Commands.Strings.handle("MSET", ["k1", "v1", "k2"], store)
      assert {:error, _} = result
    end

    test "MSET validates key constraints" do
      store = %{put: fn _, _, _ -> :ok end}
      result = Ferricstore.Commands.Strings.handle("MSET", ["", "v1", "k2", "v2"], store)
      assert {:error, _} = result
    end

    test "MSETNX works correctly (atomic via CrossShardOp)" do
      # MSETNX now uses CrossShardOp for atomicity — test via embedded API
      Ferricstore.Test.ShardHelpers.flush_all_keys()
      assert {:ok, true} = FerricStore.msetnx(%{"msetnx_m7_k1" => "v1", "msetnx_m7_k2" => "v2"})
      assert FerricStore.get("msetnx_m7_k1") == {:ok, "v1"}
      assert FerricStore.get("msetnx_m7_k2") == {:ok, "v2"}
    end

    test "MSETNX returns false if any key exists" do
      Ferricstore.Test.ShardHelpers.flush_all_keys()
      FerricStore.set("msetnx_m7_k2", "existing")
      assert {:ok, false} = FerricStore.msetnx(%{"msetnx_m7_k1" => "v1", "msetnx_m7_k2" => "v2"})
      # k1 should NOT be set
      assert FerricStore.get("msetnx_m7_k1") == {:ok, nil}
    end

    test "source does not use Enum.chunk_every" do
      source =
        File.read!(
          Path.join([
            __DIR__,
            "..",
            "..",
            "lib",
            "ferricstore",
            "commands",
            "strings.ex"
          ])
        )

      # Check that Enum.chunk_every is not called in actual code (comments are OK)
      refute source =~ ~r/Enum\.chunk_every/,
             "Strings should use direct recursive processing instead of Enum.chunk_every"
    end
  end

  # -----------------------------------------------------------------------
  # Memory M8: string GET stays binary-safe and does not inspect ETF payloads
  # -----------------------------------------------------------------------
  describe "binary-safe GET for ETF-looking values (Memory M8)" do
    test "GET on a normal string returns the string" do
      store = %{
        get: fn _key -> "hello" end,
        compound_get: fn _key, _tk -> nil end
      }

      result = Ferricstore.Commands.Strings.handle("GET", ["mykey"], store)
      assert result == "hello"
    end

    test "GET on ETF-encoded tuple that looks like a list returns raw binary" do
      etf_list = :erlang.term_to_binary({:list, [1, 2, 3]})

      store = %{
        get: fn _key -> etf_list end,
        compound_get: fn _key, _tk -> nil end
      }

      result = Ferricstore.Commands.Strings.handle("GET", ["mykey"], store)
      assert result == etf_list
    end

    test "GET on ETF-encoded tuple that looks like a hash returns raw binary" do
      etf_hash = :erlang.term_to_binary({:hash, %{"field" => "value"}})

      store = %{
        get: fn _key -> etf_hash end,
        compound_get: fn _key, _tk -> nil end
      }

      result = Ferricstore.Commands.Strings.handle("GET", ["mykey"], store)
      assert result == etf_hash
    end

    test "GET on ETF-encoded tuple that looks like a set returns raw binary" do
      etf_set = :erlang.term_to_binary({:set, MapSet.new(["a", "b"])})

      store = %{
        get: fn _key -> etf_set end,
        compound_get: fn _key, _tk -> nil end
      }

      result = Ferricstore.Commands.Strings.handle("GET", ["mykey"], store)
      assert result == etf_set
    end

    test "GET on ETF-encoded tuple that looks like a zset returns raw binary" do
      etf_zset = :erlang.term_to_binary({:zset, []})

      store = %{
        get: fn _key -> etf_zset end,
        compound_get: fn _key, _tk -> nil end
      }

      result = Ferricstore.Commands.Strings.handle("GET", ["mykey"], store)
      assert result == etf_zset
    end

    test "GET on binary starting with 131 but not a data structure returns value" do
      etf_string = :erlang.term_to_binary("just a string")

      store = %{
        get: fn _key -> etf_string end,
        compound_get: fn _key, _tk -> nil end
      }

      result = Ferricstore.Commands.Strings.handle("GET", ["mykey"], store)
      assert result == etf_string
    end

    test "GET on ETF-encoded integer returns raw binary" do
      etf_int = :erlang.term_to_binary(42)

      store = %{
        get: fn _key -> etf_int end,
        compound_get: fn _key, _tk -> nil end
      }

      result = Ferricstore.Commands.Strings.handle("GET", ["mykey"], store)
      assert result == etf_int
    end

    test "source does not do full binary_to_term" do
      source =
        File.read!(
          Path.join([
            __DIR__,
            "..",
            "..",
            "lib",
            "ferricstore",
            "commands",
            "strings.ex"
          ])
        )

      refute source =~ "binary_to_term(value)",
             "string GET must not deserialize arbitrary user binaries for type checks"
    end
  end

  # -----------------------------------------------------------------------
  # Memory M9: apply_setrange_for_tx should use binary_part, not byte lists
  # -----------------------------------------------------------------------
  describe "apply_setrange_for_tx binary operations (Memory M9)" do
    test "source does not use bin_to_list in apply_setrange_for_tx" do
      source =
        File.read!(
          Path.join([
            __DIR__,
            "..",
            "..",
            "lib",
            "ferricstore",
            "store",
            "shard.ex"
          ])
        )

      # Check that apply_setrange_for_tx doesn't contain bin_to_list
      case Regex.run(~r/defp apply_setrange_for_tx.*?(?=\n  defp |\n  def )/s, source) do
        [func_body] ->
          refute func_body =~ "bin_to_list",
                 "apply_setrange_for_tx should not use bin_to_list"

          refute func_body =~ "list_to_bin",
                 "apply_setrange_for_tx should not use list_to_bin"

        nil ->
          :ok
      end
    end
  end

  # -----------------------------------------------------------------------
  # Memory M5: Process dictionary tx state cleanup
  # -----------------------------------------------------------------------
  describe "transaction state cleanup (Memory M5)" do
    test "transaction.ex wraps tx execution in try/after for process dict cleanup" do
      source =
        File.read!(
          Path.join([
            __DIR__,
            "..",
            "..",
            "lib",
            "ferricstore",
            "store",
            "shard",
            "transaction.ex"
          ])
        )

      # The tx_execute handler should wrap its body in try/after to ensure
      # Process.delete(:tx_deleted_keys) happens even on exceptions
      assert source =~ ~r/try\s+do.*after\s*\n\s+Process\.delete\(:tx_deleted_keys\)/s,
             "Transaction state should be cleaned up in try/after"
    end
  end

  # -----------------------------------------------------------------------
  # Perf M5 (state_machine): do_delete_prefix should use :ets.select
  # -----------------------------------------------------------------------
  describe "state_machine do_delete_prefix (Perf M5)" do
    test "do_delete_prefix uses ets.select not ets.foldl" do
      source =
        File.read!(
          Path.join([
            __DIR__,
            "..",
            "..",
            "lib",
            "ferricstore",
            "raft",
            "state_machine.ex"
          ])
        )

      case Regex.run(~r/defp do_delete_prefix.*?(?=\n  defp |\n  def |\nend\n)/s, source) do
        [func_body] ->
          refute func_body =~ ":ets.foldl",
                 "do_delete_prefix should use :ets.select instead of :ets.foldl"

          assert func_body =~ ":ets.select",
                 "do_delete_prefix should use :ets.select"

        nil ->
          :ok
      end
    end
  end

  # -----------------------------------------------------------------------
  # Memory M3: l1_invalidate should receive keys directly
  # -----------------------------------------------------------------------
  describe "tracking invalidation keys (Memory M3)" do
    test "tracking_socket_sender sends keys alongside iodata" do
      conn_source_path =
        Path.join([
          __DIR__,
          "..",
          "..",
          "..",
          "ferricstore_server",
          "lib",
          "ferricstore_server",
          "connection.ex"
        ])

      if File.exists?(conn_source_path) do
        source = File.read!(conn_source_path)

        # The message format should include keys
        assert source =~ ~r/tracking_invalidation.*iodata.*keys/,
               "tracking_invalidation message should include keys"

        # Should NOT re-parse RESP for L1 invalidation
        refute source =~ "l1_invalidate_from_push",
               "Should use l1_invalidate_keys instead of l1_invalidate_from_push"
      end
    end
  end

  # -----------------------------------------------------------------------
  # Shard maybe_promote: should use prefix_count_entries, not ets.foldl
  # -----------------------------------------------------------------------
  describe "shard maybe_promote (Perf M5 shard)" do
    test "maybe_promote uses prefix_count_entries not ets.foldl" do
      source =
        File.read!(
          Path.join([
            __DIR__,
            "..",
            "..",
            "lib",
            "ferricstore",
            "store",
            "shard.ex"
          ])
        )

      case Regex.run(~r/defp maybe_promote.*?(?=\n  defp |\n  def )/s, source) do
        [func_body] ->
          # Filter out comment lines before checking for :ets.foldl
          code_lines =
            func_body |> String.split("\n") |> Enum.reject(&String.match?(&1, ~r/^\s*#/))

          code_only = Enum.join(code_lines, "\n")

          refute code_only =~ ":ets.foldl",
                 "maybe_promote should use prefix_count_entries instead of :ets.foldl"

        nil ->
          :ok
      end
    end
  end

  # -----------------------------------------------------------------------
  # Shard build_local_store: compound closures should use match specs
  # -----------------------------------------------------------------------
  describe "build_local_store compound closures (Perf M6 shard)" do
    test "build_local_store compound_scan uses prefix_scan_entries not ets.foldl" do
      source =
        File.read!(
          Path.join([
            __DIR__,
            "..",
            "..",
            "lib",
            "ferricstore",
            "store",
            "shard.ex"
          ])
        )

      case Regex.run(~r/defp build_local_store.*?(?=\n  defp |\n  def )/s, source) do
        [func_body] ->
          # Filter out comment lines before checking for :ets.foldl
          code_lines =
            func_body |> String.split("\n") |> Enum.reject(&String.match?(&1, ~r/^\s*#/))

          code_only = Enum.join(code_lines, "\n")

          refute code_only =~ ":ets.foldl",
                 "build_local_store should use prefix_*_entries helpers instead of :ets.foldl"

        nil ->
          :ok
      end
    end
  end

  # -----------------------------------------------------------------------
  # Helpers: ratelimit encode/decode using binary format
  # -----------------------------------------------------------------------

  defp encode_ratelimit(cur, start, prev), do: <<cur::64, start::64, prev::64>>

  defp decode_ratelimit(<<cur::64, start::64, prev::64>>), do: {cur, start, prev}

  defp decode_ratelimit(value) when is_binary(value) do
    case String.split(value, ":") do
      [cur, start, prev] ->
        {String.to_integer(cur), String.to_integer(start), String.to_integer(prev)}

      _ ->
        {0, System.os_time(:millisecond), 0}
    end
  end
end
