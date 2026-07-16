defmodule Ferricstore.NamespaceConfigTest do
  @moduledoc false
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Commands.Namespace
  alias Ferricstore.Commands.Server
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Test.MockStore

  # Reset all namespace config overrides after each test to prevent
  # cross-test contamination.
  setup do
    NamespaceConfig.reset_all()

    on_exit(fn ->
      NamespaceConfig.reset_all()
    end)

    :ok
  end

  # ===========================================================================
  # NamespaceConfig GenServer API
  # ===========================================================================

  describe "NamespaceConfig.set/3" do
    test "sets window_ms for a prefix" do
      assert :ok = NamespaceConfig.set("rate", "window_ms", "10")
      {:ok, entry} = NamespaceConfig.get("rate")
      assert entry.window_ms == 10
    end

    test "tracks whether namespace overrides exist for hot-path routing" do
      refute NamespaceConfig.has_overrides?()

      assert :ok = NamespaceConfig.set("rate", "window_ms", "10")
      assert NamespaceConfig.has_overrides?()

      assert :ok = NamespaceConfig.reset("rate")
      refute NamespaceConfig.has_overrides?()
    end

    test "sets window_ms under WARaft native namespace windows" do
      assert :ok = NamespaceConfig.set("rate", "window_ms", "10")
      {:ok, entry} = NamespaceConfig.get("rate")
      assert entry.window_ms == 10
    end

    test "rejects removed durability field for a prefix" do
      assert {:error, msg} = NamespaceConfig.set("ts", "durability", "async")
      assert msg =~ "unknown namespace config field 'durability'"

      {:ok, entry} = NamespaceConfig.get("ts")
      assert entry.window_ms == 1
    end

    test "rejects old quorum durability setting" do
      assert {:error, msg} = NamespaceConfig.set("ts", "durability", "quorum")
      assert msg =~ "unknown namespace config field 'durability'"
    end

    test "updates window_ms without durability metadata" do
      NamespaceConfig.set("rate", "window_ms", "50")
      {:ok, entry} = NamespaceConfig.get("rate")
      assert entry.window_ms == 50
    end

    test "rejects removed durability field while preserving window_ms" do
      NamespaceConfig.set("rate", "window_ms", "42")
      assert {:error, _} = NamespaceConfig.set("rate", "durability", "quorum")
      {:ok, entry} = NamespaceConfig.get("rate")
      assert entry.window_ms == 42
    end

    test "sets changed_at to a recent timestamp" do
      before = System.os_time(:second)
      NamespaceConfig.set("rate", "window_ms", "10")
      {:ok, entry} = NamespaceConfig.get("rate")
      assert entry.changed_at >= before
      assert entry.changed_at <= System.os_time(:second) + 1
    end

    test "rejects zero window_ms" do
      assert {:error, msg} = NamespaceConfig.set("rate", "window_ms", "0")
      assert msg =~ "positive integer"
    end

    test "rejects negative window_ms" do
      assert {:error, msg} = NamespaceConfig.set("rate", "window_ms", "-5")
      assert msg =~ "positive integer"
    end

    test "rejects non-integer window_ms" do
      assert {:error, msg} = NamespaceConfig.set("rate", "window_ms", "abc")
      assert msg =~ "positive integer"
    end

    test "rejects float window_ms" do
      assert {:error, msg} = NamespaceConfig.set("rate", "window_ms", "1.5")
      assert msg =~ "positive integer"
    end

    test "rejects commit windows that can outlive the synchronous batch call" do
      assert {:error, msg} = NamespaceConfig.set("rate", "window_ms", "30000")
      assert msg =~ "at most 10000 milliseconds"
      assert NamespaceConfig.window_for("rate") == NamespaceConfig.default_window_ms()
    end

    test "rejects durability field regardless of value" do
      assert {:error, msg} = NamespaceConfig.set("rate", "durability", "sync")
      assert msg =~ "unknown namespace config field 'durability'"
    end

    test "rejects unknown field name" do
      assert {:error, msg} = NamespaceConfig.set("rate", "bogus_field", "10")
      assert msg =~ "unknown namespace config field"
    end

    test "enforces the configured entry limit under concurrent creators" do
      old_limit = Application.get_env(:ferricstore, :namespace_config_max_entries)
      Application.put_env(:ferricstore, :namespace_config_max_entries, 3)

      on_exit(fn ->
        if is_nil(old_limit) do
          Application.delete_env(:ferricstore, :namespace_config_max_entries)
        else
          Application.put_env(:ferricstore, :namespace_config_max_entries, old_limit)
        end
      end)

      results =
        1..20
        |> Task.async_stream(
          fn index -> NamespaceConfig.set("limited-#{index}", "window_ms", "10") end,
          max_concurrency: 20,
          ordered: false
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.count(results, &(&1 == :ok)) == 3
      assert length(NamespaceConfig.get_all()) == 3

      assert Enum.all?(Enum.reject(results, &(&1 == :ok)), fn {:error, message} ->
               message =~ "namespace config limit"
             end)
    end

    test "rejects prefixes and audit identities that exceed metadata bounds" do
      oversized = :binary.copy("x", 257)

      assert {:error, prefix_error} = NamespaceConfig.set(oversized, "window_ms", "10")
      assert prefix_error =~ "prefix exceeds"

      assert {:error, identity_error} =
               NamespaceConfig.set("safe", "window_ms", "10", oversized)

      assert identity_error =~ "changed_by exceeds"
      assert NamespaceConfig.get_all() == []
    end
  end

  describe "NamespaceConfig.get/1" do
    test "returns default entry for unconfigured prefix" do
      {:ok, entry} = NamespaceConfig.get("unknown_prefix")
      assert entry.prefix == "unknown_prefix"
      assert entry.window_ms == 1
      assert entry.changed_at == 0
      assert entry.changed_by == ""
    end

    test "returns configured entry for a set prefix" do
      NamespaceConfig.set("rate", "window_ms", "10")
      {:ok, entry} = NamespaceConfig.get("rate")
      assert entry.prefix == "rate"
      assert entry.window_ms == 10
    end

    test "normalizes trailing separators consistently for set, read, and reset" do
      assert :ok = NamespaceConfig.set("rate::", "window_ms", "10")
      assert {:ok, %{window_ms: 10}} = NamespaceConfig.get("rate:")
      assert NamespaceConfig.window_for("rate::") == 10
      assert :ok = NamespaceConfig.reset("rate:")
      assert NamespaceConfig.window_for("rate") == 1
    end
  end

  describe "NamespaceConfig.get_all/0" do
    test "returns empty list when no overrides exist" do
      assert NamespaceConfig.get_all() == []
    end

    test "returns all configured prefixes sorted alphabetically" do
      NamespaceConfig.set("zebra", "window_ms", "100")
      NamespaceConfig.set("alpha", "window_ms", "2")
      NamespaceConfig.set("middle", "window_ms", "50")

      entries = NamespaceConfig.get_all()
      prefixes = Enum.map(entries, & &1.prefix)
      assert prefixes == ["alpha", "middle", "zebra"]
    end

    test "returns correct values for each entry" do
      NamespaceConfig.set("rate", "window_ms", "10")
      NamespaceConfig.set("session", "window_ms", "1")

      entries = NamespaceConfig.get_all()
      rate = Enum.find(entries, &(&1.prefix == "rate"))
      session = Enum.find(entries, &(&1.prefix == "session"))

      assert rate.window_ms == 10
      assert session.window_ms == 1
    end
  end

  describe "NamespaceConfig.reset/1" do
    test "removes the override for a single prefix" do
      NamespaceConfig.set("rate", "window_ms", "10")
      assert :ok = NamespaceConfig.reset("rate")
      {:ok, entry} = NamespaceConfig.get("rate")
      assert entry.window_ms == 1
      assert entry.changed_at == 0
    end

    test "does not affect other prefixes" do
      NamespaceConfig.set("rate", "window_ms", "10")
      NamespaceConfig.set("session", "window_ms", "5")
      NamespaceConfig.reset("rate")

      {:ok, session} = NamespaceConfig.get("session")
      assert session.window_ms == 5
    end

    test "is a no-op for non-existent prefix" do
      assert :ok = NamespaceConfig.reset("nonexistent")
    end
  end

  describe "NamespaceConfig.reset_all/0" do
    test "removes all overrides" do
      NamespaceConfig.set("rate", "window_ms", "10")
      NamespaceConfig.set("session", "window_ms", "5")
      assert :ok = NamespaceConfig.reset_all()
      assert NamespaceConfig.get_all() == []
    end

    test "is a no-op when no overrides exist" do
      assert :ok = NamespaceConfig.reset_all()
    end
  end

  describe "NamespaceConfig.window_for/1" do
    test "returns configured window_ms" do
      NamespaceConfig.set("rate", "window_ms", "42")
      assert NamespaceConfig.window_for("rate") == 42
    end

    test "returns default for unconfigured prefix" do
      assert NamespaceConfig.window_for("unknown") == 1
    end
  end

  describe "NamespaceConfig.default_window_ms/0" do
    test "returns default window_ms" do
      assert NamespaceConfig.default_window_ms() == 1
    end
  end

  # ===========================================================================
  # FERRICSTORE.CONFIG command handler (via Namespace module)
  # ===========================================================================

  describe "FERRICSTORE.CONFIG SET" do
    test "SET prefix window_ms value via command handler" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["SET", "rate", "window_ms", "10"],
          MockStore.make()
        )

      assert result == :ok

      {:ok, entry} = NamespaceConfig.get("rate")
      assert entry.window_ms == 10
    end

    test "subcommand is case-insensitive in direct handler" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["set", "rate", "window_ms", "10"],
          MockStore.make()
        )

      assert result == :ok

      {:ok, entry} = NamespaceConfig.get("rate")
      assert entry.window_ms == 10
    end

    test "SET prefix durability via command handler is rejected as removed field" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["SET", "ts", "durability", "async"],
          MockStore.make()
        )

      assert {:error, msg} = result
      assert msg =~ "unknown namespace config field 'durability'"

      {:ok, entry} = NamespaceConfig.get("ts")
      assert entry.window_ms == 1
    end

    test "SET is case-insensitive on field name" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["SET", "rate", "WINDOW_MS", "10"],
          MockStore.make()
        )

      assert result == :ok

      {:ok, entry} = NamespaceConfig.get("rate")
      assert entry.window_ms == 10
    end

    test "SET with invalid field returns error" do
      result =
        Namespace.handle("FERRICSTORE.CONFIG", ["SET", "rate", "bogus", "10"], MockStore.make())

      assert {:error, msg} = result
      assert msg =~ "unknown namespace config field"
    end

    test "SET with invalid window_ms value returns error" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["SET", "rate", "window_ms", "abc"],
          MockStore.make()
        )

      assert {:error, _} = result
    end

    test "SET with wrong number of args returns error" do
      result =
        Namespace.handle("FERRICSTORE.CONFIG", ["SET", "rate", "window_ms"], MockStore.make())

      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end

    test "SET with too many args returns error" do
      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["SET", "rate", "window_ms", "10", "extra"],
          MockStore.make()
        )

      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end

    test "SET with no args after SET returns error" do
      result = Namespace.handle("FERRICSTORE.CONFIG", ["SET"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end
  end

  describe "FERRICSTORE.CONFIG GET" do
    test "GET single prefix returns flat key-value list" do
      NamespaceConfig.set("rate", "window_ms", "10")
      result = Namespace.handle("FERRICSTORE.CONFIG", ["GET", "rate"], MockStore.make())
      assert is_list(result)
      assert "prefix" in result
      assert "rate" in result
      assert "window_ms" in result
      assert "10" in result
      refute "durability" in result
    end

    test "GET unconfigured prefix returns defaults" do
      result = Namespace.handle("FERRICSTORE.CONFIG", ["GET", "unknown"], MockStore.make())

      assert result == [
               "prefix",
               "unknown",
               "window_ms",
               "1",
               "changed_at",
               "0",
               "changed_by",
               ""
             ]
    end

    test "GET with no prefix returns all configured prefixes" do
      NamespaceConfig.set("rate", "window_ms", "10")
      NamespaceConfig.set("session", "window_ms", "5")
      result = Namespace.handle("FERRICSTORE.CONFIG", ["GET"], MockStore.make())
      assert is_list(result)
      # Should contain entries for both rate and session
      assert "rate" in result
      assert "session" in result
    end

    test "GET with no prefix returns empty list when nothing configured" do
      result = Namespace.handle("FERRICSTORE.CONFIG", ["GET"], MockStore.make())
      assert result == []
    end
  end

  describe "FERRICSTORE.CONFIG RESET" do
    test "RESET single prefix removes the override" do
      NamespaceConfig.set("rate", "window_ms", "10")
      result = Namespace.handle("FERRICSTORE.CONFIG", ["RESET", "rate"], MockStore.make())
      assert result == :ok
      assert NamespaceConfig.get_all() == []
    end

    test "RESET with no prefix resets all namespaces" do
      NamespaceConfig.set("rate", "window_ms", "10")
      NamespaceConfig.set("session", "window_ms", "5")
      result = Namespace.handle("FERRICSTORE.CONFIG", ["RESET"], MockStore.make())
      assert result == :ok
      assert NamespaceConfig.get_all() == []
    end
  end

  describe "FERRICSTORE.CONFIG error handling" do
    test "unknown subcommand returns error" do
      result = Namespace.handle("FERRICSTORE.CONFIG", ["BADCMD"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "unknown subcommand"
    end

    test "no args returns error" do
      result = Namespace.handle("FERRICSTORE.CONFIG", [], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # Dispatcher integration
  # ===========================================================================

  describe "Dispatcher routes FERRICSTORE.CONFIG" do
    test "dispatches FERRICSTORE.CONFIG SET through the dispatcher" do
      store = MockStore.make()

      result =
        Dispatcher.dispatch("FERRICSTORE.CONFIG", ["SET", "rate", "window_ms", "10"], store)

      assert result == :ok

      {:ok, entry} = NamespaceConfig.get("rate")
      assert entry.window_ms == 10
    end

    test "dispatches FERRICSTORE.CONFIG GET through the dispatcher" do
      NamespaceConfig.set("rate", "window_ms", "10")
      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.CONFIG", ["get", "rate"], store)
      assert is_list(result)
      assert "rate" in result
    end

    test "dispatches case-insensitive subcommand" do
      store = MockStore.make()
      result = Dispatcher.dispatch("ferricstore.config", ["set", "rate", "window_ms", "5"], store)
      assert result == :ok
    end

    test "dispatches FERRICSTORE.CONFIG RESET" do
      NamespaceConfig.set("rate", "window_ms", "10")
      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.CONFIG", ["reset", "rate"], store)
      assert result == :ok
      assert NamespaceConfig.get_all() == []
    end
  end

  # ===========================================================================
  # INFO namespace_config section
  # ===========================================================================

  describe "INFO namespace_config" do
    test "INFO namespace_config returns section with default values" do
      store = MockStore.make()
      result = Server.handle("INFO", ["namespace_config"], store)
      assert is_binary(result)
      assert result =~ "# Namespace_Config"
      assert result =~ "namespace_config_count:0"
      assert result =~ "default_window_ms:1"
      refute result =~ "default_durability"
    end

    test "INFO namespace_config includes configured prefixes" do
      NamespaceConfig.set("rate", "window_ms", "10")
      NamespaceConfig.set("session", "window_ms", "5")

      store = MockStore.make()
      result = Server.handle("INFO", ["namespace_config"], store)
      assert result =~ "namespace_config_count:2"
      assert result =~ "ns_rate_window_ms:10"
      assert result =~ "ns_session_window_ms:5"
      refute result =~ "_durability"
    end

    test "INFO all includes namespace_config section" do
      store = MockStore.make()
      result = Server.handle("INFO", ["all"], store)
      assert result =~ "# Namespace_Config"
    end

    test "INFO with no args includes namespace_config section" do
      store = MockStore.make()
      result = Server.handle("INFO", [], store)
      assert result =~ "# Namespace_Config"
    end
  end

  # ===========================================================================
  # INFO namespace_config — namespace_config_all_default flag
  # ===========================================================================

  describe "INFO namespace_config_all_default flag" do
    test "reports namespace_config_all_default:1 when no namespaces have custom config" do
      store = MockStore.make()
      result = Server.handle("INFO", ["namespace_config"], store)
      assert result =~ "namespace_config_all_default:1"
    end

    test "reports namespace_config_all_default:0 when namespaces have custom config" do
      NamespaceConfig.set("rate", "window_ms", "10")

      store = MockStore.make()
      result = Server.handle("INFO", ["namespace_config"], store)
      assert result =~ "namespace_config_all_default:0"
    end

    test "reports namespace_config_all_default:1 after resetting all custom config" do
      NamespaceConfig.set("rate", "window_ms", "10")
      NamespaceConfig.reset_all()

      store = MockStore.make()
      result = Server.handle("INFO", ["namespace_config"], store)
      assert result =~ "namespace_config_all_default:1"
    end

    test "reports namespace_config_all_default:0 with multiple custom namespaces" do
      NamespaceConfig.set("rate", "window_ms", "10")
      NamespaceConfig.set("session", "window_ms", "5")

      store = MockStore.make()
      result = Server.handle("INFO", ["namespace_config"], store)
      assert result =~ "namespace_config_all_default:0"
    end

    test "namespace_config_all_default flag is included in INFO all" do
      store = MockStore.make()
      result = Server.handle("INFO", ["all"], store)
      assert result =~ "namespace_config_all_default:1"
    end
  end

  # ===========================================================================
  # CONFIG REWRITE
  # ===========================================================================

  describe "CONFIG REWRITE" do
    test "CONFIG REWRITE persists current config to disk" do
      store = MockStore.make()
      result = Server.handle("CONFIG", ["REWRITE"], store)
      assert result == :ok

      path = Ferricstore.Config.config_file_path()
      assert File.exists?(path)

      content = File.read!(path)
      # Should contain key-value pairs
      assert content =~ "hz"
      assert content =~ "maxmemory"
      assert content =~ "bind"

      # Cleanup
      File.rm(path)
    end

    test "CONFIG REWRITE reflects SET changes" do
      store = MockStore.make()
      Server.handle("CONFIG", ["SET", "hz", "50"], store)
      Server.handle("CONFIG", ["REWRITE"], store)

      path = Ferricstore.Config.config_file_path()
      content = File.read!(path)
      assert content =~ "hz 50"

      # Cleanup
      File.rm(path)
    end

    test "CONFIG REWRITE with args returns error" do
      result = Server.handle("CONFIG", ["REWRITE", "extra"], MockStore.make())
      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # CONFIG RESETSTAT
  # ===========================================================================

  describe "CONFIG RESETSTAT" do
    test "CONFIG RESETSTAT resets stats counters" do
      Ferricstore.Stats.incr_connections()
      Ferricstore.Stats.incr_commands()
      assert Ferricstore.Stats.total_connections() > 0

      result = Server.handle("CONFIG", ["RESETSTAT"], MockStore.make())
      assert result == :ok
      assert Ferricstore.Stats.total_connections() == 0
      assert Ferricstore.Stats.total_commands() == 0
    end

    test "CONFIG RESETSTAT with args returns error" do
      result = Server.handle("CONFIG", ["RESETSTAT", "extra"], MockStore.make())
      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # Full lifecycle integration
  # ===========================================================================

  describe "SET/GET/RESET lifecycle" do
    test "full lifecycle: set, get, verify, reset, verify defaults" do
      store = MockStore.make()

      # Set namespace config
      assert :ok =
               Dispatcher.dispatch(
                 "FERRICSTORE.CONFIG",
                 ["set", "rate", "window_ms", "10"],
                 store
               )

      assert {:error, msg} =
               Dispatcher.dispatch(
                 "FERRICSTORE.CONFIG",
                 ["set", "rate", "durability", "quorum"],
                 store
               )

      assert msg =~ "unknown namespace config field 'durability'"

      # Verify via GET
      result = Dispatcher.dispatch("FERRICSTORE.CONFIG", ["get", "rate"], store)
      assert "10" in result
      refute "durability" in result

      # Verify via convenience accessors
      assert NamespaceConfig.window_for("rate") == 10

      # Reset
      assert :ok = Dispatcher.dispatch("FERRICSTORE.CONFIG", ["reset", "rate"], store)

      # Verify defaults restored
      assert NamespaceConfig.window_for("rate") == 1
    end

    test "multiple prefixes can be configured independently" do
      NamespaceConfig.set("rate", "window_ms", "10")
      NamespaceConfig.set("session", "window_ms", "1")
      NamespaceConfig.set("ts", "window_ms", "25")

      assert NamespaceConfig.window_for("rate") == 10
      assert NamespaceConfig.window_for("session") == 1
      assert NamespaceConfig.window_for("ts") == 25

      entries = NamespaceConfig.get_all()
      assert length(entries) == 3
    end

    test "reset_all clears all then GET returns empty" do
      NamespaceConfig.set("rate", "window_ms", "10")
      NamespaceConfig.set("session", "window_ms", "5")
      NamespaceConfig.reset_all()

      result = Namespace.handle("FERRICSTORE.CONFIG", ["GET"], MockStore.make())
      assert result == []
    end
  end

  # ===========================================================================
  # Command Catalog
  # ===========================================================================

  describe "Command Catalog" do
    test "FERRICSTORE.CONFIG is registered in the catalog" do
      assert {:ok, cmd} = Ferricstore.Commands.Catalog.lookup("ferricstore.config")
      assert cmd.name == "ferricstore.config"
      assert cmd.arity == -2
      assert "admin" in cmd.flags
    end
  end
end
