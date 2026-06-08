defmodule Ferricstore.Bugs.ExpireFlagsTest do
  @moduledoc """
  Bug #6: EXPIRE NX/XX/GT/LT Flags Not Implemented

  EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT don't support Redis 7.0+
  NX/XX/GT/LT modifiers. The EXPIRE handler only accepts [key, seconds]
  and returns "ERR wrong number of arguments" for [key, seconds, flag].

  File: commands/expiry.ex
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag timeout: 30_000

  alias Ferricstore.Commands.Expiry
  alias Ferricstore.Test.MockStore

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  describe "EXPIRE NX flag" do
    test "EXPIRE NX should set TTL when key has no TTL" do
      store = MockStore.make(%{"expire_nx" => {"value", 0}})

      # EXPIRE key 100 NX — NX = set only if key has no existing TTL
      result = Expiry.handle("EXPIRE", ["expire_nx", "100", "NX"], store)

      # BUG: Currently returns {:error, "ERR wrong number of arguments for 'expire' command"}
      # because the handler only matches [key, seconds], not [key, seconds, flag]
      assert result == 1,
             "EXPIRE NX on key without TTL should return 1 (set), got: #{inspect(result)}"
    end

    test "EXPIRE NX should NOT set TTL when key already has TTL" do
      # Key with existing expiry (1 hour from now)
      future = System.os_time(:millisecond) + 3_600_000
      store = MockStore.make(%{"expire_nx2" => {"value", future}})

      result = Expiry.handle("EXPIRE", ["expire_nx2", "200", "NX"], store)

      # NX = only set if no existing TTL; this key already has one
      assert result == 0,
             "EXPIRE NX on key with existing TTL should return 0 (not set), got: #{inspect(result)}"
    end
  end

  describe "EXPIRE XX flag" do
    test "EXPIRE XX should NOT set TTL when key has no TTL" do
      store = MockStore.make(%{"expire_xx" => {"value", 0}})

      # EXPIRE key 100 XX — XX = set only if key already has a TTL
      result = Expiry.handle("EXPIRE", ["expire_xx", "100", "XX"], store)

      # Should return 0 because the key has no TTL
      assert result == 0,
             "EXPIRE XX on key without TTL should return 0 (not set), got: #{inspect(result)}"
    end

    test "EXPIRE XX should set TTL when key already has TTL" do
      future = System.os_time(:millisecond) + 3_600_000
      store = MockStore.make(%{"expire_xx2" => {"value", future}})

      result = Expiry.handle("EXPIRE", ["expire_xx2", "200", "XX"], store)

      assert result == 1,
             "EXPIRE XX on key with existing TTL should return 1 (set), got: #{inspect(result)}"
    end
  end

  describe "EXPIRE GT flag" do
    test "EXPIRE GT should NOT replace TTL when new is shorter" do
      # Key with 1 hour TTL
      future = System.os_time(:millisecond) + 3_600_000
      store = MockStore.make(%{"expire_gt" => {"value", future}})

      # EXPIRE key 50 GT — GT = only set if new TTL is greater than current
      # 50 seconds < 3600 seconds, so should NOT replace
      result = Expiry.handle("EXPIRE", ["expire_gt", "50", "GT"], store)

      assert result == 0,
             "EXPIRE GT with shorter TTL should return 0 (not set), got: #{inspect(result)}"
    end

    test "EXPIRE GT should replace TTL when new is longer" do
      # Key with 30-second TTL
      future = System.os_time(:millisecond) + 30_000
      store = MockStore.make(%{"expire_gt2" => {"value", future}})

      # 200 seconds > 30 seconds, so should replace
      result = Expiry.handle("EXPIRE", ["expire_gt2", "200", "GT"], store)

      assert result == 1,
             "EXPIRE GT with longer TTL should return 1 (set), got: #{inspect(result)}"
    end
  end

  describe "EXPIRE LT flag" do
    test "EXPIRE LT should replace TTL when new is shorter" do
      # Key with 1 hour TTL
      future = System.os_time(:millisecond) + 3_600_000
      store = MockStore.make(%{"expire_lt" => {"value", future}})

      # EXPIRE key 50 LT — LT = only set if new TTL is less than current
      # 50 seconds < 3600 seconds, so should replace
      result = Expiry.handle("EXPIRE", ["expire_lt", "50", "LT"], store)

      assert result == 1,
             "EXPIRE LT with shorter TTL should return 1 (set), got: #{inspect(result)}"
    end

    test "EXPIRE LT should NOT replace TTL when new is longer" do
      # Key with 30-second TTL
      future = System.os_time(:millisecond) + 30_000
      store = MockStore.make(%{"expire_lt2" => {"value", future}})

      # 200 seconds > 30 seconds, so should NOT replace
      result = Expiry.handle("EXPIRE", ["expire_lt2", "200", "LT"], store)

      assert result == 0,
             "EXPIRE LT with longer TTL should return 0 (not set), got: #{inspect(result)}"
    end
  end
end
