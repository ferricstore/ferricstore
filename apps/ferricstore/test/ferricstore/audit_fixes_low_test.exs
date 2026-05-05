defmodule Ferricstore.AuditFixesLowTest do
  @moduledoc """
  Low-severity audit fixes (L1-L3).
  Split from AuditFixesTest.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.GlobMatcher
  alias Ferricstore.Commands.Generic

  # ---------------------------------------------------------------------------
  # Helper: build a mock store map from a key-value map
  # ---------------------------------------------------------------------------

  defp mock_store(data) do
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
  # L3: NamespaceConfig single ETS lookup
  # ---------------------------------------------------------------------------

  describe "L3: NamespaceConfig single ETS lookup" do
    test "set and get namespace config works" do
      prefix = "audit_test_#{:rand.uniform(100_000)}"

      # Set window_ms
      assert :ok = Ferricstore.NamespaceConfig.set(prefix, "window_ms", "42")

      # Get should reflect the change
      assert {:ok, entry} = Ferricstore.NamespaceConfig.get(prefix)
      assert entry.window_ms == 42

      # Cleanup
      Ferricstore.NamespaceConfig.reset(prefix)
    end

    test "removed durability field is rejected and preserves window" do
      prefix = "audit_dur_#{:rand.uniform(100_000)}"

      assert :ok = Ferricstore.NamespaceConfig.set(prefix, "window_ms", "10")
      assert {:error, msg} = Ferricstore.NamespaceConfig.set(prefix, "durability", "quorum")
      assert msg =~ "unknown namespace config field 'durability'"

      assert {:ok, entry} = Ferricstore.NamespaceConfig.get(prefix)
      assert entry.window_ms == 10

      # Cleanup
      Ferricstore.NamespaceConfig.reset(prefix)
    end
  end

  # ---------------------------------------------------------------------------
  # Verify GlobMatcher is used across all modules (L2 dedup)
  # ---------------------------------------------------------------------------

  describe "L2: glob matching unified in GlobMatcher" do
    test "SCAN uses GlobMatcher for non-prefix patterns" do
      # Use a pattern that is NOT a simple prefix:* so it goes through
      # the GlobMatcher code path (not the prefix index fast path).
      store =
        mock_store(%{
          "key_1" => {"v", 0},
          "key_2" => {"v", 0},
          "other_1" => {"v", 0}
        })

      [_cursor, keys] = Generic.handle("SCAN", ["0", "MATCH", "key_?"], store)
      assert Enum.sort(keys) == ["key_1", "key_2"]
    end
  end

  # ---------------------------------------------------------------------------
  # L1: GlobMatcher uses binary pattern match -- already fixed. Verify edge cases.
  # ---------------------------------------------------------------------------

  describe "L1: GlobMatcher binary pattern match (already fixed)" do
    test "star matches progressively longer subjects" do
      assert GlobMatcher.match?("", "*")
      assert GlobMatcher.match?("x", "*")
      assert GlobMatcher.match?(String.duplicate("x", 100), "*")
    end

    test "star at beginning and end" do
      assert GlobMatcher.match?("abc", "*abc")
      assert GlobMatcher.match?("abc", "abc*")
      assert GlobMatcher.match?("abc", "*abc*")
      assert GlobMatcher.match?("xabcy", "*abc*")
    end

    test "multiple stars" do
      assert GlobMatcher.match?("a:b:c", "*:*:*")
      assert GlobMatcher.match?("hello world foo", "hello*world*foo")
      refute GlobMatcher.match?("hello world", "hello*world*foo")
    end

    test "star with no remaining pattern chars" do
      assert GlobMatcher.match?("anything", "*")
      assert GlobMatcher.match?("", "*")
    end

    test "consecutive stars" do
      assert GlobMatcher.match?("abc", "**abc")
      assert GlobMatcher.match?("abc", "abc**")
      assert GlobMatcher.match?("abc", "**")
    end

    test "binary with null bytes" do
      assert GlobMatcher.match?(<<0, 1, 2>>, <<0, ?*, 2>>)
      refute GlobMatcher.match?(<<0, 1, 2>>, <<0, 3, 2>>)
    end

    test "very long subject with star pattern" do
      long = String.duplicate("a", 1000)
      assert GlobMatcher.match?(long, "a*a")
      assert GlobMatcher.match?(long, "*")
      refute GlobMatcher.match?(long, "b*")
    end

    test "unicode bytes" do
      assert GlobMatcher.match?("hello", "h?llo")
      # Multi-byte UTF-8: ? matches one byte, not one codepoint
      # This is consistent with Redis behaviour
    end
  end

  # ---------------------------------------------------------------------------
  # L2: encode_hash/decode_hash/is_encoded_hash? removed — compound keys now
  # ---------------------------------------------------------------------------
  # Hash encoding internals were removed. Compound-key storage is tested
  # through the HSET/HGET/HGETALL command-level tests.
end
