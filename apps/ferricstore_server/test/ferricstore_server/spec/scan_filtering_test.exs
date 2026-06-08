defmodule FerricstoreServer.Spec.ScanFilteringTest do
  @moduledoc """
  Spec section 8: SCAN Subkey Filtering tests.

  Verifies that SCAN correctly hides internal subkey entries and type byte
  prefixes from clients, and that DBSIZE excludes subkeys.

  Tests SC-001 through SC-008 from the test plan.

  SC-001 and SC-002 are tested through the live embedded store (which stores
  hashes as single serialized blobs, not compound keys). SC-003 and SC-004
  (SCAN TYPE filtering) require compound key type metadata (`T:key` entries)
  and are tested using MockStore against `Commands.Generic.handle/3` --
  the same approach as `Ferricstore.Commands.ScanTypeTest`, but focused on
  the exact spec scenarios. SC-005 through SC-008 are tested through the
  live embedded store.

  SC-001: SCAN does not return hash subkeys
  SC-002: SCAN strips type byte from metadata key
  SC-003: SCAN TYPE hash -- returns hash keys only
  SC-004: SCAN TYPE string -- returns string keys only
  SC-005: SCAN MATCH on user key namespace
  SC-006: SCAN count consistency -- multiple hashes, correct user key count
  SC-007: SCAN during concurrent writes (tagged :slow)
  SC-008: DBSIZE excludes subkeys
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  alias Ferricstore.Commands.Generic
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # Helpers -- live store SCAN iteration
  # ---------------------------------------------------------------------------

  # Performs a full SCAN iteration through the live embedded store using
  # `FerricStore.keys/1` (which already handles sandbox prefix stripping
  # and compound key filtering).
  defp live_scan_all(opts \\ []) do
    match_pattern = Keyword.get(opts, :match, "*")
    {:ok, keys} = FerricStore.keys(match_pattern)
    Enum.sort(keys)
  end

  # ---------------------------------------------------------------------------
  # Helpers -- MockStore SCAN iteration for TYPE tests
  # ---------------------------------------------------------------------------

  defp make_typed_store(entries) do
    initial =
      Enum.reduce(entries, %{}, fn {key, type}, acc ->
        case type do
          "string" ->
            Map.put(acc, key, {"value", 0})

          data_type when data_type in ~w(hash list set zset) ->
            type_key = CompoundKey.type_key(key)

            acc
            |> Map.put(key, {"", 0})
            |> Map.put(type_key, {data_type, 0})

          _other ->
            Map.put(acc, key, {"value", 0})
        end
      end)

    MockStore.make(initial)
  end

  defp mock_scan_all(store, opts) do
    mock_scan_all(store, "0", opts, [])
  end

  defp mock_scan_all(store, cursor, opts, acc) do
    type_filter = Keyword.get(opts, :type)
    match_pattern = Keyword.get(opts, :match)
    count = Keyword.get(opts, :count, 100)

    args = [cursor, "COUNT", Integer.to_string(count)]
    args = if match_pattern, do: args ++ ["MATCH", match_pattern], else: args
    args = if type_filter, do: args ++ ["TYPE", type_filter], else: args

    [next_cursor, keys] = Generic.handle("SCAN", args, store)

    all_keys = acc ++ keys

    if next_cursor == "0" do
      Enum.sort(all_keys)
    else
      mock_scan_all(store, next_cursor, opts, all_keys)
    end
  end

  # ---------------------------------------------------------------------------
  # SC-001: SCAN does not return hash subkeys
  # ---------------------------------------------------------------------------

  describe "SC-001: SCAN does not return hash subkeys" do
    test "HSET user:42 name alice age 30; SCAN returns 'user:42' only" do
      FerricStore.hset("user:42", %{"name" => "alice", "age" => "30"})

      keys = live_scan_all()

      assert "user:42" in keys
      refute Enum.any?(keys, &String.contains?(&1, "name"))
      refute Enum.any?(keys, &String.contains?(&1, "age"))
      refute Enum.any?(keys, &String.starts_with?(&1, "H:"))
    end

    test "hash subkeys with compound key format are invisible (MockStore)" do
      initial = %{
        "user:42" => {"", 0},
        "T:user:42" => {"hash", 0},
        CompoundKey.hash_field("user:42", "name") => {"alice", 0},
        CompoundKey.hash_field("user:42", "age") => {"30", 0}
      }

      store = MockStore.make(initial)

      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)

      assert keys == ["user:42"]
      refute Enum.any?(keys, &String.starts_with?(&1, "H:"))
      refute Enum.any?(keys, &String.starts_with?(&1, "T:"))
    end
  end

  # ---------------------------------------------------------------------------
  # SC-002: SCAN strips type byte from metadata key
  # ---------------------------------------------------------------------------

  describe "SC-002: SCAN strips type byte from metadata key" do
    test "HSET user:42 name alice; SCAN returns 'user:42' not 'T:user:42'" do
      FerricStore.hset("user:42", %{"name" => "alice"})

      keys = live_scan_all()

      assert "user:42" in keys
      refute Enum.any?(keys, &String.starts_with?(&1, "T:"))
    end

    test "type metadata key T:user:42 is stripped (MockStore)" do
      initial = %{
        "user:42" => {"", 0},
        "T:user:42" => {"hash", 0},
        CompoundKey.hash_field("user:42", "name") => {"alice", 0}
      }

      store = MockStore.make(initial)

      [_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)

      assert keys == ["user:42"]
    end

    test "SET plain str; no T: prefix visible" do
      FerricStore.set("plain", "str")

      keys = live_scan_all()

      assert "plain" in keys
      refute Enum.any?(keys, &String.starts_with?(&1, "T:"))
    end
  end

  # ---------------------------------------------------------------------------
  # SC-003: SCAN TYPE hash -- returns hash keys only
  # ---------------------------------------------------------------------------

  describe "SC-003: SCAN TYPE hash returns hash keys only" do
    test "HSET user:42 f v; SET plain str; SCAN 0 TYPE hash returns user:42 only" do
      store =
        make_typed_store([
          {"user:42", "hash"},
          {"plain", "string"}
        ])

      keys = mock_scan_all(store, type: "hash")

      assert keys == ["user:42"]
    end

    test "TYPE filter works on stripped user key" do
      store =
        make_typed_store([
          {"h1", "hash"},
          {"h2", "hash"},
          {"s1", "string"},
          {"s2", "set"}
        ])

      keys = mock_scan_all(store, type: "hash")

      assert keys == ["h1", "h2"]
    end
  end

  # ---------------------------------------------------------------------------
  # SC-004: SCAN TYPE string -- returns string keys only
  # ---------------------------------------------------------------------------

  describe "SC-004: SCAN TYPE string returns string keys only" do
    test "SET plain str; HSET h f v; SCAN 0 TYPE string returns 'plain' only" do
      store =
        make_typed_store([
          {"plain", "string"},
          {"h", "hash"}
        ])

      keys = mock_scan_all(store, type: "string")

      assert keys == ["plain"]
    end
  end

  # ---------------------------------------------------------------------------
  # SC-005: SCAN MATCH on user key namespace
  # ---------------------------------------------------------------------------

  describe "SC-005: SCAN MATCH on user key namespace" do
    test "MATCH session:* returns only session keys" do
      FerricStore.hset("session:abc", %{"f" => "v"})
      FerricStore.hset("other:abc", %{"f" => "v"})

      keys = live_scan_all(match: "session:*")

      assert "session:abc" in keys
      refute "other:abc" in keys
    end

    test "MATCH applied after type byte strip (MockStore)" do
      store =
        make_typed_store([
          {"session:abc", "hash"},
          {"other:abc", "hash"}
        ])

      keys = mock_scan_all(store, match: "session:*")

      assert keys == ["session:abc"]
    end

    test "MATCH pattern on string keys" do
      FerricStore.set("ns:key1", "v1")
      FerricStore.set("ns:key2", "v2")
      FerricStore.set("other:key", "v3")

      keys = live_scan_all(match: "ns:*")

      assert length(keys) == 2
      assert "ns:key1" in keys
      assert "ns:key2" in keys
    end
  end

  # ---------------------------------------------------------------------------
  # SC-006: SCAN count consistency
  # ---------------------------------------------------------------------------

  describe "SC-006: SCAN count consistency" do
    test "multiple hashes return exactly the correct number of user keys" do
      # h1 has 3 fields, h2 has 2 fields
      FerricStore.hset("h1", %{"a" => "1", "b" => "2", "c" => "3"})
      FerricStore.hset("h2", %{"x" => "1", "y" => "2"})

      keys = live_scan_all()

      # SCAN returns exactly 2 user keys, not 5 subkeys + 2 metadata
      assert length(keys) == 2
      assert Enum.sort(keys) == ["h1", "h2"]
    end

    test "full SCAN with small COUNT still finds all user keys (MockStore)" do
      entries =
        for i <- 1..10 do
          {"hash_#{String.pad_leading(Integer.to_string(i), 2, "0")}", "hash"}
        end

      store = make_typed_store(entries)

      # Use a small COUNT to force multiple iterations
      keys = mock_scan_all(store, count: 3)

      assert length(keys) == 10
    end

    test "SCAN returns correct count with many hashes via embedded API" do
      for i <- 1..10 do
        FerricStore.hset(
          "count_hash_#{String.pad_leading(Integer.to_string(i), 2, "0")}",
          %{"f" => "v"}
        )
      end

      {:ok, all_keys} = FerricStore.keys("count_hash_*")

      assert length(all_keys) == 10
    end
  end

  # ---------------------------------------------------------------------------
  # SC-007: SCAN during concurrent writes (tagged :slow)
  # ---------------------------------------------------------------------------

  describe "SC-007: SCAN during concurrent writes" do
    @tag :slow
    test "no subkeys visible in any SCAN batch during concurrent writes" do
      # Pre-populate some keys
      for i <- 1..20 do
        FerricStore.set("cw_key_#{i}", "initial_value_#{i}")
      end

      test_pid = self()

      # Start a writer that modifies keys concurrently
      _writer =
        spawn(fn ->
          for round <- 1..100 do
            idx = rem(round, 20) + 1
            FerricStore.set("cw_key_#{idx}", "updated_round_#{round}")
          end

          send(test_pid, :writer_done)
        end)

      # Concurrently perform SCAN iterations
      for _scan_round <- 1..20 do
        {:ok, keys} = FerricStore.keys()

        # No subkeys or internal keys should ever be visible
        Enum.each(keys, fn key ->
          refute String.starts_with?(key, "H:"),
                 "Subkey #{inspect(key)} leaked through SCAN"

          refute String.starts_with?(key, "T:"),
                 "Type metadata #{inspect(key)} leaked through SCAN"

          refute String.starts_with?(key, "S:"),
                 "Set subkey #{inspect(key)} leaked through SCAN"

          refute String.contains?(key, <<0>>),
                 "Key with null byte #{inspect(key)} leaked through SCAN"
        end)
      end

      assert_receive :writer_done, 10_000
    end
  end

  # ---------------------------------------------------------------------------
  # SC-008: DBSIZE excludes subkeys
  # ---------------------------------------------------------------------------

  describe "SC-008: DBSIZE excludes subkeys" do
    test "HSET with 3 subkeys + 1 metadata increments DBSIZE by 1" do
      {:ok, before_size} = FerricStore.dbsize()

      # 3 fields stored inside the hash -- in embedded mode this is a single
      # serialized blob at one key, but DBSIZE should only count user keys.
      FerricStore.hset("dbsize_hash", %{"a" => "1", "b" => "2", "c" => "3"})

      {:ok, after_size} = FerricStore.dbsize()

      assert after_size == before_size + 1
    end

    test "DBSIZE counts only user-visible keys with mixed types" do
      FerricStore.set("dbsize_str1", "v1")
      FerricStore.set("dbsize_str2", "v2")
      FerricStore.hset("dbsize_h1", %{"f1" => "v1", "f2" => "v2"})
      FerricStore.hset("dbsize_h2", %{"x" => "1"})

      {:ok, size} = FerricStore.dbsize()

      # 2 strings + 2 hashes = 4 user keys
      assert size == 4
    end

    test "DBSIZE stays correct after deleting a hash" do
      FerricStore.hset("dbsize_del", %{"a" => "1", "b" => "2"})

      # Under full-suite load, Raft apply may lag — retry until visible.
      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          {:ok, before_del} = FerricStore.dbsize()
          assert before_del == 1
        end,
        "dbsize should be 1 after hset",
        10,
        100
      )

      FerricStore.del("dbsize_del")

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          {:ok, after_del} = FerricStore.dbsize()
          assert after_del == 0
        end,
        "dbsize should be 0 after del",
        10,
        100
      )
    end

    test "DBSIZE via Commands.Server excludes compound keys (MockStore)" do
      # Verify the TCP command path (via Commands.Server.handle DBSIZE)
      # also excludes internal keys when compound keys are present.
      initial = %{
        "str1" => {"hello", 0},
        "myhash" => {"", 0},
        "T:myhash" => {"hash", 0},
        CompoundKey.hash_field("myhash", "f1") => {"v1", 0},
        CompoundKey.hash_field("myhash", "f2") => {"v2", 0},
        CompoundKey.hash_field("myhash", "f3") => {"v3", 0}
      }

      store = MockStore.make(initial)

      # DBSIZE should return 2 (str1 + myhash), not 6
      db_size = Ferricstore.Commands.Server.handle("DBSIZE", [], store)

      assert db_size == 2
    end
  end
end
