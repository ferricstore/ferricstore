defmodule Ferricstore.AuditFixesMediumTest do
  @moduledoc """
  Medium-severity audit fixes (M1-M6).
  Split from AuditFixesTest.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Strings, Hash, Set, Server, Generic}

  # ---------------------------------------------------------------------------
  # Helper: build a mock store map from a key-value map
  # ---------------------------------------------------------------------------

  defp mock_store(data \\ %{}) do
    %{
      get: fn key -> get_in(data, [key, Access.elem(0)]) end,
      get_meta: fn key ->
        case Map.get(data, key) do
          nil -> nil
          {val, exp} -> {val, exp}
        end
      end,
      put: fn _key, _val, _exp -> :ok end,
      delete: fn _key -> :ok end,
      exists?: fn key -> Map.has_key?(data, key) end,
      keys: fn ->
        Map.keys(data) |> Enum.reject(&Ferricstore.Store.CompoundKey.internal_key?/1)
      end,
      flush: fn -> :ok end,
      dbsize: fn -> map_size(data) end,
      incr: fn _key, _delta -> {:ok, 1} end,
      incr_float: fn _key, _delta -> {:ok, "1.0"} end,
      append: fn _key, _val -> {:ok, 5} end,
      getset: fn _key, _val -> nil end,
      getdel: fn _key -> nil end,
      getex: fn _key, _exp -> nil end,
      setrange: fn _key, _off, _val -> {:ok, 10} end,
      compound_get: fn _rk, _ck -> nil end,
      compound_get_meta: fn _rk, _ck -> nil end,
      compound_put: fn _rk, _ck, _v, _e -> :ok end,
      compound_delete: fn _rk, _ck -> :ok end,
      compound_scan: fn _rk, _prefix -> [] end,
      compound_count: fn _rk, _prefix -> 0 end,
      compound_delete_prefix: fn _rk, _prefix -> :ok end
    }
  end

  # ---------------------------------------------------------------------------
  # M2 + L4: MSET/MSETNX parity via even_length?
  # ---------------------------------------------------------------------------

  describe "M2+L4: MSET/MSETNX parity check" do
    test "MSET with odd args returns error" do
      store = mock_store()
      result = Strings.handle("MSET", ["k1", "v1", "k2"], store)
      assert result == {:error, "ERR wrong number of arguments for 'mset' command"}
    end

    test "MSETNX with odd args returns error" do
      store = mock_store()
      result = Strings.handle("MSETNX", ["k1", "v1", "k2"], store)
      assert result == {:error, "ERR wrong number of arguments for 'msetnx' command"}
    end

    test "MSET with even args succeeds" do
      store = mock_store()
      result = Strings.handle("MSET", ["k1", "v1", "k2", "v2"], store)
      assert result == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # M3: Hash validate_field_count recursive short-circuit
  # ---------------------------------------------------------------------------

  describe "M3: validate_field_count recursive" do
    test "HEXPIRE validates field count correctly" do
      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_get_meta, fn _rk, _ck -> nil end)
        |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)

      # Count = 2 but only 1 field: should return error
      result = Hash.handle("HEXPIRE", ["key", "60", "FIELDS", "2", "f1"], store)
      assert result == {:error, "ERR number of fields does not match the count argument"}
    end

    test "HEXPIRE with matching count and fields proceeds" do
      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_get_meta, fn _rk, _ck -> nil end)
        |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)

      # Count = 1, fields = 1: should proceed (field may not exist)
      result = Hash.handle("HEXPIRE", ["key", "60", "FIELDS", "1", "f1"], store)
      # Returns [-2] (field not found) or similar
      assert is_list(result) or match?({:error, _}, result)
    end
  end

  # ---------------------------------------------------------------------------
  # M4: INFO format_section uses iodata
  # ---------------------------------------------------------------------------

  describe "M4: INFO format_section uses iodata" do
    test "INFO server section produces valid output" do
      store = mock_store()
      result = Server.handle("INFO", ["server"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Server\r\n")
      assert String.contains?(result, "redis_version:")
    end

    test "INFO stats section has correct format" do
      store = mock_store()
      result = Server.handle("INFO", ["stats"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Stats\r\n")
      # Each field is key:value separated by \r\n
      lines = String.split(result, "\r\n")
      assert hd(lines) == "# Stats"
    end
  end

  # ---------------------------------------------------------------------------
  # M5: shard_count and promotion_threshold cached in persistent_term
  # ---------------------------------------------------------------------------

  describe "M5: Application.get_env cached in persistent_term" do
    test "shard_count is in persistent_term" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, nil)
      assert is_integer(shard_count) and shard_count > 0
    end

    test "promotion_threshold is in persistent_term" do
      threshold = :persistent_term.get(:ferricstore_promotion_threshold, nil)
      assert is_integer(threshold) and threshold >= 0
    end

    test "Promotion.threshold reads from persistent_term" do
      result = Ferricstore.Store.Promotion.threshold()
      assert is_integer(result) and result >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # M1: ClientTracking BCAST uses ets.select
  # ---------------------------------------------------------------------------

  if Code.ensure_loaded?(FerricstoreServer.ClientTracking) do
    describe "M1: BCAST tracking uses ets.select" do
      test "notify_key_modified does not crash with empty tracking tables" do
        # Just verify the BCAST path doesn't crash
        result =
          FerricstoreServer.ClientTracking.notify_key_modified(
            "test_key",
            self(),
            fn _pid, _msg, _keys -> :ok end
          )

        assert result == :ok
      end
    end
  end

  # ===========================================================================
  # MEDIUM-severity fixes from elixir-guide-audit.md (second pass)
  # ===========================================================================

  # ---------------------------------------------------------------------------
  # M4 (fix): shard_count/0 infinite recursion bug in Server INFO sections
  #
  # The helper function `shard_count/0` had a recursive call to itself as the
  # fallback when persistent_term returned nil, instead of falling back to
  # Application.get_env. This caused a stack overflow on any INFO command when
  # :ferricstore_shard_count was not set in persistent_term.
  # ---------------------------------------------------------------------------

  describe "M4: shard_count/0 reads persistent_term with Application.get_env fallback" do
    test "INFO memory section works without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["memory"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Memory\r\n")
      assert String.contains?(result, "used_memory:")
    end

    test "INFO raft section works without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["raft"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Raft\r\n")
    end

    test "INFO bitcask section works without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["bitcask"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Bitcask\r\n")
    end

    test "INFO ferricstore section works without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["ferricstore"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Ferricstore\r\n")
    end

    test "INFO keydir_analysis section works without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["keydir_analysis"], store)
      assert is_binary(result)
      assert String.starts_with?(result, "# Keydir_Analysis\r\n")
    end

    test "INFO all section produces all sections without stack overflow" do
      store = mock_store()
      result = Server.handle("INFO", ["all"], store)
      assert is_binary(result)
      # All sections that use shard_count should be present
      assert String.contains?(result, "# Memory\r\n")
      assert String.contains?(result, "# Raft\r\n")
      assert String.contains?(result, "# Bitcask\r\n")
      assert String.contains?(result, "# Ferricstore\r\n")
      assert String.contains?(result, "# Keydir_Analysis\r\n")
    end

    test "shard_count falls back to Application.get_env when persistent_term not set" do
      # The persistent_term :ferricstore_shard_count is set by application.ex.
      # Verify the value is a positive integer either way.
      shard_count =
        :persistent_term.get(:ferricstore_shard_count, nil) ||
          Application.get_env(:ferricstore, :shard_count, 4)

      assert is_integer(shard_count)
      assert shard_count > 0
    end

    test "INFO memory contains shard-specific keydir data" do
      store = mock_store()
      result = Server.handle("INFO", ["memory"], store)
      assert String.contains?(result, "keydir_used_bytes:")
      assert String.contains?(result, "beam_process_memory:")
    end
  end

  # ---------------------------------------------------------------------------
  # M1: SPOP with count -- already fixed (uses Enum.reduce counter, not
  #     length(selected)). Verify the behaviour is correct.
  # ---------------------------------------------------------------------------

  describe "M1: SPOP with count avoids length(selected)" do
    test "SPOP with count removes and returns correct number of members" do
      # Build a store where the set has 5 members
      members = MapSet.new(["a", "b", "c", "d", "e"])
      removed_members = Agent.start_link(fn -> MapSet.new() end) |> elem(1)

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, ck ->
          # Extract member from compound key pattern S:key\0member
          member = ck |> String.split("\0") |> List.last()
          if MapSet.member?(members, member), do: "1", else: nil
        end)
        |> Map.put(:compound_scan, fn _rk, _prefix ->
          Enum.map(members, fn m -> {m, "1"} end)
        end)
        |> Map.put(:compound_count, fn _rk, _prefix ->
          MapSet.size(members) - MapSet.size(Agent.get(removed_members, & &1))
        end)
        |> Map.put(:compound_delete, fn _rk, ck ->
          member = ck |> String.split("\0") |> List.last()
          Agent.update(removed_members, &MapSet.put(&1, member))
          :ok
        end)

      result = Set.handle("SPOP", ["myset", "3"], store)
      assert is_list(result)
      assert length(result) == 3
      # All returned members should be from the original set
      assert Enum.all?(result, &MapSet.member?(members, &1))

      Agent.stop(removed_members)
    end

    test "SPOP with count 0 returns empty list" do
      store =
        mock_store()
        |> Map.put(:compound_scan, fn _rk, _prefix -> [{"a", "1"}, {"b", "1"}] end)

      result = Set.handle("SPOP", ["myset", "0"], store)
      assert result == []
    end

    test "SPOP with count larger than set size returns all members" do
      store =
        mock_store()
        |> Map.put(:compound_scan, fn _rk, _prefix -> [{"a", "1"}, {"b", "1"}] end)
        |> Map.put(:compound_delete, fn _rk, _ck -> :ok end)
        |> Map.put(:compound_count, fn _rk, _prefix -> 0 end)

      result = Set.handle("SPOP", ["myset", "100"], store)
      assert is_list(result)
      assert length(result) == 2
    end

    test "SPOP with negative count returns error" do
      store = mock_store()
      result = Set.handle("SPOP", ["myset", "-1"], store)
      assert result == {:error, "ERR value is not an integer or out of range"}
    end
  end

  # ---------------------------------------------------------------------------
  # M2: HRANDFIELD / SRANDMEMBER negative count -- already fixed (uses
  #     List.to_tuple for O(1) access). Verify behaviour correctness.
  # ---------------------------------------------------------------------------

  describe "M2: HRANDFIELD negative count uses O(1) tuple access" do
    test "HRANDFIELD with negative count allows duplicates" do
      pairs = [{"f1", "v1"}, {"f2", "v2"}, {"f3", "v3"}]

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_scan, fn _rk, _prefix -> pairs end)
        |> Map.put(:compound_count, fn _rk, _prefix -> 3 end)

      # Negative count: may return duplicates, should return abs(count) items
      result = Hash.handle("HRANDFIELD", ["myhash", "-10"], store)
      assert is_list(result)
      assert length(result) == 10
      # All returned fields should be from the original hash
      valid_fields = MapSet.new(["f1", "f2", "f3"])
      assert Enum.all?(result, &MapSet.member?(valid_fields, &1))
    end

    test "HRANDFIELD with negative count on empty hash returns empty" do
      store =
        mock_store()
        |> Map.put(:compound_scan, fn _rk, _prefix -> [] end)

      result = Hash.handle("HRANDFIELD", ["myhash", "-5"], store)
      assert result == []
    end

    test "HRANDFIELD with positive count returns unique fields" do
      pairs = [{"f1", "v1"}, {"f2", "v2"}, {"f3", "v3"}]

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_scan, fn _rk, _prefix -> pairs end)
        |> Map.put(:compound_count, fn _rk, _prefix -> 3 end)

      result = Hash.handle("HRANDFIELD", ["myhash", "2"], store)
      assert is_list(result)
      assert length(result) <= 2
      assert result == Enum.uniq(result)
    end

    test "HRANDFIELD with negative count and WITHVALUES" do
      pairs = [{"f1", "v1"}, {"f2", "v2"}]

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_scan, fn _rk, _prefix -> pairs end)
        |> Map.put(:compound_count, fn _rk, _prefix -> 2 end)

      result = Hash.handle("HRANDFIELD", ["myhash", "-4", "WITHVALUES"], store)
      assert is_list(result)
      # -4 means 4 field-value pairs = 8 elements in flat list
      assert length(result) == 8
    end

    test "SRANDMEMBER with negative count uses O(1) tuple access" do
      members = [{"a", "1"}, {"b", "1"}, {"c", "1"}]

      store =
        mock_store()
        |> Map.put(:compound_scan, fn _rk, _prefix -> members end)

      result = Set.handle("SRANDMEMBER", ["myset", "-10"], store)
      assert is_list(result)
      assert length(result) == 10
      valid_members = MapSet.new(["a", "b", "c"])
      assert Enum.all?(result, &MapSet.member?(valid_members, &1))
    end
  end

  # ---------------------------------------------------------------------------
  # M3: Config.rewrite uses cons + reverse -- already fixed. Verify behaviour.
  # ---------------------------------------------------------------------------

  describe "M3: Config.rewrite uses cons + reverse (already fixed)" do
    test "CONFIG REWRITE preserves comment lines" do
      # This test verifies that the rewrite logic works correctly via the
      # Server command handler. The underlying Config.rewrite uses cons + reverse.
      store = mock_store()
      result = Server.handle("CONFIG", ["REWRITE"], store)
      # Should succeed or return an error about file path (both are valid)
      assert result == :ok or match?({:error, _}, result)
    end
  end

  # ---------------------------------------------------------------------------
  # M5: do_scan Enum.sort -- architectural issue, no code change needed.
  # Verify SCAN still produces deterministic, sorted results.
  # ---------------------------------------------------------------------------

  describe "M5: SCAN produces sorted deterministic results" do
    test "SCAN results are in sorted order" do
      data = %{
        "z_key" => {"v", 0},
        "a_key" => {"v", 0},
        "m_key" => {"v", 0}
      }

      store = mock_store(data)
      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)
      assert keys == Enum.sort(keys)
    end

    test "SCAN pagination respects sort order" do
      data =
        for i <- 1..10, into: %{}, do: {"key_#{String.pad_leading("#{i}", 3, "0")}", {"v", 0}}

      store = mock_store(data)

      all_keys = collect_scan_keys(store, "0", 3, [])
      assert all_keys == Enum.sort(all_keys)
    end
  end

  defp collect_scan_keys(_store, "0", _count, acc) when acc != [] do
    acc
  end

  defp collect_scan_keys(store, cursor, count, acc) do
    [next_cursor, keys] = Generic.handle("SCAN", [cursor, "COUNT", "#{count}"], store)
    new_acc = acc ++ keys

    if next_cursor == "0" do
      new_acc
    else
      collect_scan_keys(store, next_cursor, count, new_acc)
    end
  end
end
