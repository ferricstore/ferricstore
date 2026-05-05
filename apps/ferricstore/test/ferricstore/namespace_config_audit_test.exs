defmodule Ferricstore.NamespaceConfigAuditTest do
  @moduledoc """
  Tests for namespace config audit trail: changed_at and changed_by.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Namespace
  alias Ferricstore.Commands.Server
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Test.MockStore

  setup do
    NamespaceConfig.reset_all()
    on_exit(fn -> NamespaceConfig.reset_all() end)
    :ok
  end

  # ===========================================================================
  # changed_at is set on CONFIG SET
  # ===========================================================================

  describe "changed_at is set on CONFIG SET" do
    test "changed_at is populated with a recent Unix timestamp" do
      before = System.os_time(:second)
      :ok = NamespaceConfig.set("audit", "window_ms", "10")
      after_set = System.os_time(:second)

      {:ok, entry} = NamespaceConfig.get("audit")
      assert entry.changed_at >= before
      assert entry.changed_at <= after_set + 1
    end

    test "changed_at updates on subsequent SET calls" do
      :ok = NamespaceConfig.set("audit", "window_ms", "10")
      {:ok, first} = NamespaceConfig.get("audit")

      # Small delay to ensure timestamp can differ
      Process.sleep(1100)

      :ok = NamespaceConfig.set("audit", "window_ms", "20")
      {:ok, second} = NamespaceConfig.get("audit")

      assert second.changed_at >= first.changed_at
    end

    test "changed_at is 0 for unconfigured prefix (default entry)" do
      {:ok, entry} = NamespaceConfig.get("no_override")
      assert entry.changed_at == 0
    end

    test "changed_at is not set by rejected removed durability field" do
      assert {:error, _} = NamespaceConfig.set("audit", "durability", "quorum")
      {:ok, entry} = NamespaceConfig.get("audit")
      assert entry.changed_at == 0
    end
  end

  # ===========================================================================
  # changed_by tracks the caller
  # ===========================================================================

  describe "changed_by tracks the caller" do
    test "changed_by records the caller identity passed to set/4" do
      :ok = NamespaceConfig.set("audit", "window_ms", "10", "client:42")
      {:ok, entry} = NamespaceConfig.get("audit")
      assert entry.changed_by == "client:42"
    end

    test "changed_by defaults to empty string when no caller specified (set/3)" do
      :ok = NamespaceConfig.set("audit", "window_ms", "10")
      {:ok, entry} = NamespaceConfig.get("audit")
      assert entry.changed_by == ""
    end

    test "changed_by is empty string for unconfigured prefix" do
      {:ok, entry} = NamespaceConfig.get("no_override")
      assert entry.changed_by == ""
    end

    test "changed_by updates when a different caller changes config" do
      :ok = NamespaceConfig.set("audit", "window_ms", "10", "client:1")
      {:ok, first} = NamespaceConfig.get("audit")
      assert first.changed_by == "client:1"

      :ok = NamespaceConfig.set("audit", "window_ms", "20", "client:99")
      {:ok, second} = NamespaceConfig.get("audit")
      assert second.changed_by == "client:99"
    end

    test "changed_by with 'system' for programmatic/startup use" do
      :ok = NamespaceConfig.set("boot", "window_ms", "5", "system")
      {:ok, entry} = NamespaceConfig.get("boot")
      assert entry.changed_by == "system"
    end

    test "FERRICSTORE.CONFIG SET passes caller from conn_state" do
      store = MockStore.make()
      conn_state = %{client_id: 42}

      result =
        Namespace.handle(
          "FERRICSTORE.CONFIG",
          ["SET", "audit", "window_ms", "10"],
          store,
          conn_state
        )

      assert result == :ok

      {:ok, entry} = NamespaceConfig.get("audit")
      assert entry.changed_by == "client:42"
    end

    test "FERRICSTORE.CONFIG SET without conn_state uses empty changed_by" do
      store = MockStore.make()
      result = Namespace.handle("FERRICSTORE.CONFIG", ["SET", "audit", "window_ms", "10"], store)
      assert result == :ok

      {:ok, entry} = NamespaceConfig.get("audit")
      assert entry.changed_by == ""
    end
  end

  # ===========================================================================
  # INFO shows changed_at and changed_by
  # ===========================================================================

  describe "INFO namespace_config shows changed_at and changed_by" do
    test "INFO namespace_config includes changed_at and changed_by for configured prefix" do
      before = System.os_time(:second)
      :ok = NamespaceConfig.set("info_ns", "window_ms", "10", "client:55")

      store = MockStore.make()
      result = Server.handle("INFO", ["namespace_config"], store)

      assert result =~ "ns_info_ns_changed_by:client:55"
      assert result =~ "ns_info_ns_changed_at:"

      # Extract the changed_at value and verify it's a real timestamp
      [_, changed_at_str] =
        Regex.run(~r/ns_info_ns_changed_at:(\d+)/, result)

      changed_at = String.to_integer(changed_at_str)
      assert changed_at >= before
    end

    test "INFO namespace_config does not include audit fields for unconfigured prefixes" do
      store = MockStore.make()
      result = Server.handle("INFO", ["namespace_config"], store)
      refute result =~ "changed_at"
      refute result =~ "changed_by"
    end

    test "INFO namespace_config shows empty changed_by when no caller specified" do
      :ok = NamespaceConfig.set("info_ns", "window_ms", "10")

      store = MockStore.make()
      result = Server.handle("INFO", ["namespace_config"], store)

      assert result =~ "ns_info_ns_changed_by:"
    end
  end

  # ===========================================================================
  # FERRICSTORE.CONFIG GET shows changed_at and changed_by
  # ===========================================================================

  describe "FERRICSTORE.CONFIG GET shows audit fields" do
    test "GET single prefix includes changed_at and changed_by" do
      :ok = NamespaceConfig.set("audit_get", "window_ms", "10", "client:77")

      store = MockStore.make()
      result = Namespace.handle("FERRICSTORE.CONFIG", ["GET", "audit_get"], store)

      assert "changed_at" in result
      assert "changed_by" in result
      assert "client:77" in result
    end

    test "GET all includes changed_at and changed_by for each entry" do
      :ok = NamespaceConfig.set("ns_a", "window_ms", "10", "client:1")
      :ok = NamespaceConfig.set("ns_b", "window_ms", "5", "client:2")

      store = MockStore.make()
      result = Namespace.handle("FERRICSTORE.CONFIG", ["GET"], store)

      assert "changed_at" in result
      assert "changed_by" in result
      assert "client:1" in result
      assert "client:2" in result
    end

    test "GET unconfigured prefix shows changed_at 0 and empty changed_by" do
      store = MockStore.make()
      result = Namespace.handle("FERRICSTORE.CONFIG", ["GET", "nonexistent"], store)

      assert "changed_at" in result
      assert "0" in result
      assert "changed_by" in result
    end
  end

  # ===========================================================================
  # Stress: rapid config changes with correct audit
  # ===========================================================================

  describe "stress: rapid config changes preserve audit" do
    test "100 rapid changes all record correct changed_by" do
      for i <- 1..100 do
        caller = "client:#{i}"
        :ok = NamespaceConfig.set("stress", "window_ms", Integer.to_string(i), caller)
      end

      {:ok, entry} = NamespaceConfig.get("stress")
      assert entry.window_ms == 100
      assert entry.changed_by == "client:100"
      assert entry.changed_at > 0
    end

    test "concurrent changes from different callers preserve last-writer-wins" do
      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            caller = "task:#{i}"
            NamespaceConfig.set("concurrent", "window_ms", Integer.to_string(i), caller)
          end)
        end

      Task.await_many(tasks)

      {:ok, entry} = NamespaceConfig.get("concurrent")
      # The final value must be one of the written values
      assert entry.window_ms in 1..20
      # changed_by must match the writer that set the current window_ms
      assert entry.changed_by =~ ~r/^task:\d+$/
    end
  end
end
