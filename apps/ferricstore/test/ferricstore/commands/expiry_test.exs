defmodule Ferricstore.Commands.ExpiryTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Expiry
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # EXPIRE
  # ---------------------------------------------------------------------------

  describe "EXPIRE" do
    test "EXPIRE existing key returns 1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "10"], store)
    end

    test "EXPIRE returns plain write errors" do
      store = %{
        expire_at_ms: fn "k" -> 0 end,
        get_meta: fn "k" -> {"v", 0} end,
        put: fn "k", "v", expire_at_ms when is_integer(expire_at_ms) and expire_at_ms > 0 ->
          {:error, :disk_full}
        end,
        compound_get_meta: fn _redis_key, _compound_key -> nil end
      }

      assert {:error, :disk_full} == Expiry.handle("EXPIRE", ["k", "10"], store)
    end

    test "EXPIRE batches compound member TTL writes and returns write errors" do
      type_key = CompoundKey.type_key("hash")
      field_a = CompoundKey.hash_field("hash", "a")
      field_b = CompoundKey.hash_field("hash", "b")

      store = %{
        expire_at_ms: fn "hash" -> nil end,
        get_meta: fn "hash" -> nil end,
        compound_get: fn "hash", ^type_key -> "hash" end,
        compound_get_meta: fn
          "hash", ^type_key -> {"hash", 0}
          "hash", _compound_key -> nil
        end,
        compound_put: fn "hash", compound_key, _value, _expire_at_ms ->
          flunk(
            "EXPIRE should batch the whole compound TTL rewrite, got #{inspect(compound_key)}"
          )
        end,
        compound_scan: fn "hash", _prefix -> [{"a", "1"}, {"b", "2"}] end,
        compound_batch_put: fn "hash", entries ->
          entries_by_key =
            Map.new(entries, fn {compound_key, value, expire_at_ms} ->
              {compound_key, {value, expire_at_ms}}
            end)

          assert %{
                   ^type_key => {"hash", exp_type},
                   ^field_a => {"1", exp_a},
                   ^field_b => {"2", exp_b}
                 } = entries_by_key

          assert map_size(entries_by_key) == 3
          assert exp_type == exp_a
          assert is_integer(exp_a) and exp_a > 0
          assert exp_a == exp_b
          {:error, :disk_full}
        end
      }

      assert {:error, :disk_full} == Expiry.handle("EXPIRE", ["hash", "10"], store)
    end

    test "EXPIRE missing key returns 0" do
      assert 0 == Expiry.handle("EXPIRE", ["missing", "10"], MockStore.make())
    end

    test "EXPIRE with negative seconds deletes the key (Redis 7+)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "-1"], store)
      assert store.get.("k") == nil
    end

    test "EXPIRE immediate delete does not load a cold plain value" do
      store = metadata_delete_store("cold_plain")

      assert 1 == Expiry.handle("EXPIRE", ["cold_plain", "0"], store)
      assert metadata_delete_seen?(store)
    end

    test "EXPIRE with non-integer returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Expiry.handle("EXPIRE", ["k", "abc"], store)
    end

    test "EXPIRE NX rejected by existing TTL does not load a cold plain value" do
      future = System.os_time(:millisecond) + 60_000
      store = metadata_only_expiry_store("cold_plain", future)

      assert 0 == Expiry.handle("EXPIRE", ["cold_plain", "10", "NX"], store)
    end

    test "EXPIRE XX rejected by persistent key does not load a cold plain value" do
      store = metadata_only_expiry_store("cold_plain", 0)

      assert 0 == Expiry.handle("EXPIRE", ["cold_plain", "10", "XX"], store)
    end

    test "EXPIRE no args returns error" do
      assert {:error, _} = Expiry.handle("EXPIRE", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # PEXPIRE
  # ---------------------------------------------------------------------------

  describe "PEXPIRE" do
    test "PEXPIRE existing key returns 1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("PEXPIRE", ["k", "5000"], store)
    end

    test "PEXPIRE with negative deletes the key (Redis 7+)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("PEXPIRE", ["k", "-1"], store)
      assert store.get.("k") == nil
    end

    test "PEXPIRE immediate delete does not load a cold plain value" do
      store = metadata_delete_store("cold_plain")

      assert 1 == Expiry.handle("PEXPIRE", ["cold_plain", "-1"], store)
      assert metadata_delete_seen?(store)
    end

    test "PEXPIRE missing key returns 0" do
      assert 0 == Expiry.handle("PEXPIRE", ["missing", "5000"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # TTL
  # ---------------------------------------------------------------------------

  describe "TTL" do
    test "TTL key with future expiry returns positive seconds" do
      future = System.os_time(:millisecond) + 10_000
      store = MockStore.make(%{"k" => {"v", future}})
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0
      assert ttl <= 10
    end

    test "TTL key with no expiry returns -1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Expiry.handle("TTL", ["k"], store)
    end

    test "TTL reads expiry without loading the value" do
      future = System.os_time(:millisecond) + 10_000

      store = %{
        expire_at_ms: fn "cold_ttl" -> future end,
        get_meta: fn _key -> flunk("TTL should not load the value") end,
        compound_get_meta: fn _redis_key, _compound_key -> nil end
      }

      ttl = Expiry.handle("TTL", ["cold_ttl"], store)
      assert ttl > 0
      assert ttl <= 10
    end

    test "TTL missing key returns -2" do
      assert -2 == Expiry.handle("TTL", ["missing"], MockStore.make())
    end

    test "TTL no args returns error" do
      assert {:error, _} = Expiry.handle("TTL", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # PTTL
  # ---------------------------------------------------------------------------

  describe "PTTL" do
    test "PTTL key with future expiry returns positive ms" do
      future = System.os_time(:millisecond) + 10_000
      store = MockStore.make(%{"k" => {"v", future}})
      pttl = Expiry.handle("PTTL", ["k"], store)
      assert pttl > 0
      assert pttl <= 10_000
    end

    test "PTTL key with no expiry returns -1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Expiry.handle("PTTL", ["k"], store)
    end

    test "PTTL reads expiry without loading the value" do
      future = System.os_time(:millisecond) + 10_000

      store = %{
        expire_at_ms: fn "cold_ttl" -> future end,
        get_meta: fn _key -> flunk("PTTL should not load the value") end,
        compound_get_meta: fn _redis_key, _compound_key -> nil end
      }

      pttl = Expiry.handle("PTTL", ["cold_ttl"], store)
      assert pttl > 0
      assert pttl <= 10_000
    end

    test "PTTL missing key returns -2" do
      assert -2 == Expiry.handle("PTTL", ["missing"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # PERSIST
  # ---------------------------------------------------------------------------

  describe "PERSIST" do
    test "PERSIST key with TTL returns 1 and removes expiry" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"v", future}})
      assert 1 == Expiry.handle("PERSIST", ["k"], store)
      assert -1 == Expiry.handle("TTL", ["k"], store)
    end

    test "PERSIST returns plain write errors" do
      future = System.os_time(:millisecond) + 60_000

      store = %{
        expire_at_ms: fn "k" -> future end,
        get_meta: fn "k" -> {"v", future} end,
        put: fn "k", "v", 0 -> {:error, :disk_full} end,
        compound_get_meta: fn _redis_key, _compound_key -> nil end
      }

      assert {:error, :disk_full} == Expiry.handle("PERSIST", ["k"], store)
    end

    test "PERSIST key without TTL returns 0" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 0 == Expiry.handle("PERSIST", ["k"], store)
    end

    test "PERSIST persistent cold plain key does not load the value" do
      store = metadata_only_expiry_store("cold_plain", 0)

      assert 0 == Expiry.handle("PERSIST", ["cold_plain"], store)
    end

    test "PERSIST missing key returns 0" do
      assert 0 == Expiry.handle("PERSIST", ["missing"], MockStore.make())
    end

    test "PERSIST no args returns error" do
      assert {:error, _} = Expiry.handle("PERSIST", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # EXPIREAT
  # ---------------------------------------------------------------------------

  describe "EXPIREAT" do
    test "EXPIREAT key with future unix timestamp returns 1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      future_unix = div(System.os_time(:millisecond), 1000) + 3600
      assert 1 == Expiry.handle("EXPIREAT", ["k", "#{future_unix}"], store)
    end

    test "EXPIREAT missing key returns 0" do
      future_unix = div(System.os_time(:millisecond), 1000) + 3600
      assert 0 == Expiry.handle("EXPIREAT", ["missing", "#{future_unix}"], MockStore.make())
    end

    test "EXPIREAT no args returns error" do
      assert {:error, _} = Expiry.handle("EXPIREAT", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # PEXPIREAT
  # ---------------------------------------------------------------------------

  describe "PEXPIREAT" do
    test "PEXPIREAT key with future ms timestamp returns 1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      future_ms = System.os_time(:millisecond) + 3_600_000
      assert 1 == Expiry.handle("PEXPIREAT", ["k", "#{future_ms}"], store)
    end

    test "PEXPIREAT missing key returns 0" do
      future_ms = System.os_time(:millisecond) + 3_600_000
      assert 0 == Expiry.handle("PEXPIREAT", ["missing", "#{future_ms}"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # EXPIRE — additional edge cases
  # ---------------------------------------------------------------------------

  describe "EXPIRE edge cases" do
    test "EXPIRE with 0 seconds expires key immediately" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "0"], store)
      # Key should be expired now — TTL returns -2
      assert -2 == Expiry.handle("TTL", ["k"], store)
    end

    test "EXPIRE updates existing TTL (last one wins)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "100"], store)
      assert 1 == Expiry.handle("EXPIRE", ["k", "200"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      # TTL should be close to 200, not 100
      assert ttl > 100
      assert ttl <= 200
    end

    test "EXPIRE preserves value" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "10"], store)
      assert "v" == store.get.("k")
    end

    test "EXPIRE GT rejected by older TTL does not load a cold plain value" do
      old_future = System.os_time(:millisecond) + 120_000
      store = metadata_only_expiry_store("cold_plain", old_future)

      assert 0 == Expiry.handle("EXPIRE", ["cold_plain", "10", "GT"], store)
    end

    test "EXPIRE LT rejected by newer TTL does not load a cold plain value" do
      old_future = System.os_time(:millisecond) + 10_000
      store = metadata_only_expiry_store("cold_plain", old_future)

      assert 0 == Expiry.handle("EXPIRE", ["cold_plain", "120", "LT"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # TTL — additional edge cases
  # ---------------------------------------------------------------------------

  describe "TTL edge cases" do
    test "TTL returns -2 for key expired via EXPIRE 0" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "0"], store)
      assert -2 == Expiry.handle("TTL", ["k"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # PTTL — additional edge cases
  # ---------------------------------------------------------------------------

  describe "PTTL edge cases" do
    test "PTTL returns millisecond precision" do
      future = System.os_time(:millisecond) + 5_000
      store = MockStore.make(%{"k" => {"v", future}})
      pttl = Expiry.handle("PTTL", ["k"], store)
      assert pttl > 0
      assert pttl <= 5_000
    end

    test "PTTL no args returns error" do
      assert {:error, _} = Expiry.handle("PTTL", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # PERSIST — additional edge cases
  # ---------------------------------------------------------------------------

  describe "PERSIST edge cases" do
    test "PERSIST after EXPIRE removes TTL" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIRE", ["k", "10"], store)
      # Verify TTL is set
      ttl_before = Expiry.handle("TTL", ["k"], store)
      assert ttl_before > 0

      assert 1 == Expiry.handle("PERSIST", ["k"], store)
      assert -1 == Expiry.handle("TTL", ["k"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # EXPIREAT — additional edge cases
  # ---------------------------------------------------------------------------

  describe "EXPIREAT edge cases" do
    test "EXPIREAT with past timestamp expires key immediately" do
      store = MockStore.make(%{"k" => {"v", 0}})
      # Unix epoch 1 = 1970-01-01 — definitely in the past
      assert 1 == Expiry.handle("EXPIREAT", ["k", "1"], store)
      assert -2 == Expiry.handle("TTL", ["k"], store)
    end

    test "EXPIREAT immediate delete does not load a cold plain value" do
      store = metadata_delete_store("cold_plain")

      assert 1 == Expiry.handle("EXPIREAT", ["cold_plain", "1"], store)
      assert metadata_delete_seen?(store)
    end

    test "EXPIREAT with non-integer returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Expiry.handle("EXPIREAT", ["k", "abc"], store)
    end
  end

  describe "PEXPIREAT edge cases" do
    test "PEXPIREAT immediate delete does not load a cold plain value" do
      store = metadata_delete_store("cold_plain")

      assert 1 == Expiry.handle("PEXPIREAT", ["cold_plain", "1"], store)
      assert metadata_delete_seen?(store)
    end
  end

  defp metadata_delete_store(key) do
    {:ok, pid} = Agent.start_link(fn -> false end)

    %{
      pid: pid,
      get: fn _key -> flunk("immediate expiry delete should not load the value") end,
      get_meta: fn _key -> flunk("immediate expiry delete should not load value metadata") end,
      exists?: fn ^key -> true end,
      delete: fn ^key ->
        Agent.update(pid, fn _ -> true end)
        :ok
      end,
      compound_get: fn _redis_key, _compound_key -> nil end,
      compound_scan: fn _redis_key, _prefix -> [] end,
      prob_write: fn _command -> :ok end
    }
  end

  defp metadata_delete_seen?(%{pid: pid}), do: Agent.get(pid, & &1)

  defp metadata_only_expiry_store(key, expire_at_ms) do
    %{
      expire_at_ms: fn ^key -> expire_at_ms end,
      get_meta: fn ^key -> flunk("no-op expiry command should not load the value") end,
      compound_get_meta: fn _redis_key, _compound_key -> nil end
    }
  end
end
