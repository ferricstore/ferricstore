defmodule FerricstoreServer.Spec.ConfigValuesTest do
  @moduledoc """
  Spec section 18: Configuration Value Tests.

  Verifies every config key with valid values, invalid values, boundary values,
  and runtime CONFIG SET behaviour. Tests are organized into:

    1. Read-only parameters — CONFIG GET returns correct values
    2. Read-write parameters — CONFIG SET works with valid/invalid/boundary values
    3. CONFIG SET invalid param → ERR Unsupported CONFIG parameter
    4. CONFIG GET * → returns all params as flat list
    5. CONFIG GET nonexistent → empty list
    6. CONFIG REWRITE → writes to disk, file exists after
    7. CONFIG RESETSTAT → resets counters, INFO stats show zero
    8. CONFIG SET LOCAL log_level → Logger level changes
    9. INFO reflects config — INFO server section shows port, data_dir, etc.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Server
  alias Ferricstore.Config
  alias Ferricstore.Config.Local, as: ConfigLocal
  alias Ferricstore.Stats
  alias Ferricstore.Test.MockStore

  # Reset config to defaults after each test to avoid cross-test contamination.
  setup do
    orig_eviction = Application.get_env(:ferricstore, :eviction_policy)
    orig_slowlog_us = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
    orig_slowlog_max = Application.get_env(:ferricstore, :slowlog_max_len)
    orig_log_level = Logger.level()

    ConfigLocal.reset_all()

    on_exit(fn ->
      # Restore Config GenServer state for read-write params
      defaults = Config.defaults()

      Enum.each(defaults, fn {k, v} ->
        try do
          Config.set(k, v)
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end
      end)

      # Restore Application env
      if orig_eviction, do: Application.put_env(:ferricstore, :eviction_policy, orig_eviction)

      if orig_slowlog_us, do: Ferricstore.SlowLog.set_threshold(orig_slowlog_us)
      if orig_slowlog_max, do: Ferricstore.SlowLog.set_max_len(orig_slowlog_max)

      ConfigLocal.reset_all()
      Logger.configure(level: orig_log_level)

      # Clean up any config file written by REWRITE tests
      path = Config.config_file_path()
      if File.exists?(path), do: File.rm(path)
    end)

    %{store: MockStore.make()}
  end

  # ===========================================================================
  # 1. Read-only parameters — CONFIG GET returns correct values
  # ===========================================================================

  describe "read-only param: maxmemory" do
    test "CONFIG GET maxmemory returns integer string matching Application env", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "maxmemory"], store)
      assert ["maxmemory", value] = result
      expected = Application.get_env(:ferricstore, :max_memory_bytes, 0) |> to_string()
      assert value == expected
    end

    test "CONFIG GET maxmemory value is a parseable non-negative integer", %{store: store} do
      ["maxmemory", value] = Server.handle("CONFIG", ["GET", "maxmemory"], store)
      {n, ""} = Integer.parse(value)
      assert n >= 0
    end

    test "CONFIG SET maxmemory is rejected as read-only", %{store: store} do
      assert {:error, msg} = Server.handle("CONFIG", ["SET", "maxmemory", "999"], store)
      assert msg =~ "read-only"
    end
  end

  describe "read-only param: maxclients" do
    test "CONFIG GET maxclients returns positive integer string", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "maxclients"], store)
      assert ["maxclients", value] = result
      {n, ""} = Integer.parse(value)
      assert n > 0
    end

    test "CONFIG GET maxclients matches Application env", %{store: store} do
      ["maxclients", value] = Server.handle("CONFIG", ["GET", "maxclients"], store)
      expected = Application.get_env(:ferricstore, :maxclients, 10_000) |> to_string()
      assert value == expected
    end

    test "CONFIG SET maxclients is rejected as read-only", %{store: store} do
      assert {:error, msg} = Server.handle("CONFIG", ["SET", "maxclients", "5000"], store)
      assert msg =~ "read-only"
    end
  end

  describe "read-only param: tcp-port" do
    test "CONFIG GET tcp-port returns integer string matching Application env", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "tcp-port"], store)
      assert ["tcp-port", value] = result
      expected = Application.get_env(:ferricstore, :port, 6379) |> to_string()
      assert value == expected
    end

    test "CONFIG GET tcp-port value is a parseable integer", %{store: store} do
      ["tcp-port", value] = Server.handle("CONFIG", ["GET", "tcp-port"], store)
      assert {_n, ""} = Integer.parse(value)
    end

    test "CONFIG SET tcp-port is rejected as read-only", %{store: store} do
      assert {:error, msg} = Server.handle("CONFIG", ["SET", "tcp-port", "9999"], store)
      assert msg =~ "read-only"
    end
  end

  describe "read-only param: data-dir" do
    test "CONFIG GET data-dir returns non-empty string matching Application env", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "data-dir"], store)
      assert ["data-dir", value] = result
      expected = Application.get_env(:ferricstore, :data_dir, "data")
      assert value == expected
      assert String.length(value) > 0
    end

    test "CONFIG SET data-dir is rejected as read-only", %{store: store} do
      assert {:error, msg} = Server.handle("CONFIG", ["SET", "data-dir", "/tmp/new"], store)
      assert msg =~ "read-only"
    end
  end

  describe "removed param: raft-enabled" do
    test "CONFIG GET raft-enabled returns empty (Raft always on)", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "raft-enabled"], store)
      assert result == []
    end
  end

  describe "read-only param: tls-port" do
    test "CONFIG GET tls-port returns integer string", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "tls-port"], store)
      assert ["tls-port", value] = result
      {n, ""} = Integer.parse(value)
      # Default is 0 (TLS not configured)
      assert n >= 0
    end

    test "CONFIG GET tls-port matches Application env", %{store: store} do
      ["tls-port", value] = Server.handle("CONFIG", ["GET", "tls-port"], store)
      expected = Application.get_env(:ferricstore, :tls_port, 0) |> to_string()
      assert value == expected
    end

    test "CONFIG SET tls-port is rejected as read-only", %{store: store} do
      assert {:error, msg} = Server.handle("CONFIG", ["SET", "tls-port", "6380"], store)
      assert msg =~ "read-only"
    end
  end

  describe "read-only param: tls-cert-file" do
    test "CONFIG GET tls-cert-file returns string", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "tls-cert-file"], store)
      assert ["tls-cert-file", value] = result
      assert is_binary(value)
    end

    test "CONFIG GET tls-cert-file matches Application env", %{store: store} do
      ["tls-cert-file", value] = Server.handle("CONFIG", ["GET", "tls-cert-file"], store)
      expected = Application.get_env(:ferricstore, :tls_cert_file, "")
      assert value == expected
    end

    test "CONFIG SET tls-cert-file is rejected as read-only", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "tls-cert-file", "/etc/cert.pem"], store)

      assert msg =~ "read-only"
    end
  end

  describe "read-only param: tls-key-file" do
    test "CONFIG GET tls-key-file returns string", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "tls-key-file"], store)
      assert ["tls-key-file", value] = result
      assert is_binary(value)
    end

    test "CONFIG GET tls-key-file matches Application env", %{store: store} do
      ["tls-key-file", value] = Server.handle("CONFIG", ["GET", "tls-key-file"], store)
      expected = Application.get_env(:ferricstore, :tls_key_file, "")
      assert value == expected
    end

    test "CONFIG SET tls-key-file is rejected as read-only", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "tls-key-file", "/etc/key.pem"], store)

      assert msg =~ "read-only"
    end
  end

  describe "read-only param: require-tls" do
    test "CONFIG GET require-tls returns 'true' or 'false'", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "require-tls"], store)
      assert ["require-tls", value] = result
      assert value in ["true", "false"]
    end

    test "CONFIG GET require-tls matches Application env", %{store: store} do
      ["require-tls", value] = Server.handle("CONFIG", ["GET", "require-tls"], store)

      expected =
        case Application.get_env(:ferricstore, :require_tls, false) do
          true -> "true"
          false -> "false"
        end

      assert value == expected
    end

    test "CONFIG SET require-tls is rejected as read-only", %{store: store} do
      assert {:error, msg} = Server.handle("CONFIG", ["SET", "require-tls", "true"], store)
      assert msg =~ "read-only"
    end
  end

  # ===========================================================================
  # 2. Read-write parameters — CONFIG SET works with valid/invalid/boundary
  # ===========================================================================

  describe "read-write param: maxmemory-policy — valid values" do
    test "CONFIG SET maxmemory-policy volatile-lru succeeds", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "maxmemory-policy", "volatile-lru"], store)

      assert ["maxmemory-policy", "volatile-lru"] =
               Server.handle("CONFIG", ["GET", "maxmemory-policy"], store)
    end

    test "CONFIG SET maxmemory-policy allkeys-lru succeeds and updates Application env", %{
      store: store
    } do
      assert :ok = Server.handle("CONFIG", ["SET", "maxmemory-policy", "allkeys-lru"], store)

      assert ["maxmemory-policy", "allkeys-lru"] =
               Server.handle("CONFIG", ["GET", "maxmemory-policy"], store)

      assert :allkeys_lru == Application.get_env(:ferricstore, :eviction_policy)
    end

    test "CONFIG SET maxmemory-policy volatile-ttl succeeds and updates Application env", %{
      store: store
    } do
      assert :ok = Server.handle("CONFIG", ["SET", "maxmemory-policy", "volatile-ttl"], store)

      assert ["maxmemory-policy", "volatile-ttl"] =
               Server.handle("CONFIG", ["GET", "maxmemory-policy"], store)

      assert :volatile_ttl == Application.get_env(:ferricstore, :eviction_policy)
    end

    test "CONFIG SET maxmemory-policy noeviction succeeds and updates Application env", %{
      store: store
    } do
      assert :ok = Server.handle("CONFIG", ["SET", "maxmemory-policy", "noeviction"], store)

      assert ["maxmemory-policy", "noeviction"] =
               Server.handle("CONFIG", ["GET", "maxmemory-policy"], store)

      assert :noeviction == Application.get_env(:ferricstore, :eviction_policy)
    end
  end

  describe "read-write param: maxmemory-policy — invalid values rejected" do
    test "CONFIG SET maxmemory-policy with invalid string returns error", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "maxmemory-policy", "invalid-policy"], store)

      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET maxmemory-policy with empty string returns error", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "maxmemory-policy", ""], store)

      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET maxmemory-policy with numeric string returns error", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "maxmemory-policy", "42"], store)

      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET maxmemory-policy with close misspelling returns error", %{store: store} do
      assert {:error, _} =
               Server.handle("CONFIG", ["SET", "maxmemory-policy", "volatile_lru"], store)
    end
  end

  describe "read-write param: slowlog-log-slower-than — valid values" do
    test "CONFIG SET slowlog-log-slower-than with valid positive integer", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "slowlog-log-slower-than", "5000"], store)

      assert ["slowlog-log-slower-than", "5000"] =
               Server.handle("CONFIG", ["GET", "slowlog-log-slower-than"], store)

      assert 5000 == Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
    end

    test "CONFIG SET slowlog-log-slower-than with 0 logs every command", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "slowlog-log-slower-than", "0"], store)

      assert ["slowlog-log-slower-than", "0"] =
               Server.handle("CONFIG", ["GET", "slowlog-log-slower-than"], store)

      assert 0 == Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
    end

    test "CONFIG SET slowlog-log-slower-than with -1 disables slowlog", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "slowlog-log-slower-than", "-1"], store)

      assert ["slowlog-log-slower-than", "-1"] =
               Server.handle("CONFIG", ["GET", "slowlog-log-slower-than"], store)

      assert -1 == Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
    end

    test "CONFIG SET slowlog-log-slower-than with large value", %{store: store} do
      assert :ok =
               Server.handle("CONFIG", ["SET", "slowlog-log-slower-than", "999999999"], store)

      assert ["slowlog-log-slower-than", "999999999"] =
               Server.handle("CONFIG", ["GET", "slowlog-log-slower-than"], store)
    end
  end

  describe "read-write param: slowlog-log-slower-than — invalid values rejected" do
    test "CONFIG SET slowlog-log-slower-than with non-integer string returns error", %{
      store: store
    } do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "slowlog-log-slower-than", "abc"], store)

      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET slowlog-log-slower-than with float string returns error", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "slowlog-log-slower-than", "3.14"], store)

      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET slowlog-log-slower-than with -2 returns error", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "slowlog-log-slower-than", "-2"], store)

      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET slowlog-log-slower-than with empty string returns error", %{store: store} do
      assert {:error, _} =
               Server.handle("CONFIG", ["SET", "slowlog-log-slower-than", ""], store)
    end
  end

  describe "read-write param: slowlog-max-len — valid values" do
    test "CONFIG SET slowlog-max-len with valid positive integer", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "slowlog-max-len", "256"], store)

      assert ["slowlog-max-len", "256"] =
               Server.handle("CONFIG", ["GET", "slowlog-max-len"], store)

      assert 256 == Application.get_env(:ferricstore, :slowlog_max_len)
    end

    test "CONFIG SET slowlog-max-len with 0 is accepted (boundary)", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "slowlog-max-len", "0"], store)

      assert ["slowlog-max-len", "0"] =
               Server.handle("CONFIG", ["GET", "slowlog-max-len"], store)
    end

    test "CONFIG SET slowlog-max-len with 1 (minimum useful value)", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "slowlog-max-len", "1"], store)

      assert ["slowlog-max-len", "1"] =
               Server.handle("CONFIG", ["GET", "slowlog-max-len"], store)
    end

    test "CONFIG SET slowlog-max-len with large value", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "slowlog-max-len", "100000"], store)

      assert ["slowlog-max-len", "100000"] =
               Server.handle("CONFIG", ["GET", "slowlog-max-len"], store)
    end
  end

  describe "read-write param: slowlog-max-len — invalid values rejected" do
    test "CONFIG SET slowlog-max-len with negative returns error", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "slowlog-max-len", "-1"], store)

      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET slowlog-max-len with string returns error", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "slowlog-max-len", "abc"], store)

      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET slowlog-max-len with float returns error", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "slowlog-max-len", "1.5"], store)

      assert msg =~ "Invalid argument"
    end
  end

  describe "read-write param: hz" do
    test "CONFIG GET hz returns integer string", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "hz"], store)
      assert ["hz", value] = result
      assert {n, ""} = Integer.parse(value)
      assert n >= 1
    end

    test "CONFIG SET hz with valid value succeeds", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "hz", "100"], store)
      assert ["hz", "100"] = Server.handle("CONFIG", ["GET", "hz"], store)
    end

    test "CONFIG SET hz with minimum value 1", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "hz", "1"], store)
      assert ["hz", "1"] = Server.handle("CONFIG", ["GET", "hz"], store)
    end

    test "CONFIG SET hz with maximum value 500", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "hz", "500"], store)
      assert ["hz", "500"] = Server.handle("CONFIG", ["GET", "hz"], store)
    end

    test "CONFIG SET hz with 0 returns error (below minimum)", %{store: store} do
      assert {:error, msg} = Server.handle("CONFIG", ["SET", "hz", "0"], store)
      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET hz with 501 returns error (above maximum)", %{store: store} do
      assert {:error, msg} = Server.handle("CONFIG", ["SET", "hz", "501"], store)
      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET hz with negative returns error", %{store: store} do
      assert {:error, _} = Server.handle("CONFIG", ["SET", "hz", "-1"], store)
    end

    test "CONFIG SET hz with non-integer returns error", %{store: store} do
      assert {:error, _} = Server.handle("CONFIG", ["SET", "hz", "abc"], store)
    end
  end

  describe "read-write param: notify-keyspace-events" do
    test "CONFIG SET notify-keyspace-events accepts flag strings", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "notify-keyspace-events", "KEA"], store)

      assert ["notify-keyspace-events", "KEA"] =
               Server.handle("CONFIG", ["GET", "notify-keyspace-events"], store)
    end

    test "CONFIG SET notify-keyspace-events accepts empty string to disable", %{store: store} do
      Server.handle("CONFIG", ["SET", "notify-keyspace-events", "KEA"], store)
      assert :ok = Server.handle("CONFIG", ["SET", "notify-keyspace-events", ""], store)

      assert ["notify-keyspace-events", ""] =
               Server.handle("CONFIG", ["GET", "notify-keyspace-events"], store)
    end

    test "CONFIG SET notify-keyspace-events accepts arbitrary flag strings", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "notify-keyspace-events", "Kx$g"], store)

      assert ["notify-keyspace-events", "Kx$g"] =
               Server.handle("CONFIG", ["GET", "notify-keyspace-events"], store)
    end

    test "CONFIG SET notify-keyspace-events accepts single character", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "notify-keyspace-events", "A"], store)

      assert ["notify-keyspace-events", "A"] =
               Server.handle("CONFIG", ["GET", "notify-keyspace-events"], store)
    end

    test "CONFIG GET notify-keyspace-events default is empty string", %{store: store} do
      # First ensure it's at default
      Server.handle("CONFIG", ["SET", "notify-keyspace-events", ""], store)
      result = Server.handle("CONFIG", ["GET", "notify-keyspace-events"], store)
      assert ["notify-keyspace-events", ""] = result
    end
  end

  # ===========================================================================
  # 3. CONFIG SET invalid param → ERR Unsupported CONFIG parameter
  # ===========================================================================

  describe "CONFIG SET unknown/invalid parameter" do
    test "CONFIG SET with completely unknown param returns ERR Unsupported", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "totally-unknown-param", "value"], store)

      assert msg =~ "Unsupported CONFIG parameter"
    end

    test "CONFIG SET with another unknown param returns error", %{store: store} do
      assert {:error, msg} =
               Server.handle("CONFIG", ["SET", "foo-bar-baz", "123"], store)

      assert msg =~ "Unsupported CONFIG parameter"
    end

    test "CONFIG SET with empty key returns error", %{store: store} do
      result = Server.handle("CONFIG", ["SET", "", "value"], store)
      assert {:error, _} = result
    end

    test "CONFIG SET with no value (missing arg) returns error", %{store: store} do
      result = Server.handle("CONFIG", ["SET", "hz"], store)
      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end

    test "CONFIG SET with no args returns error", %{store: store} do
      result = Server.handle("CONFIG", ["SET"], store)
      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # 4. CONFIG GET * → returns all params as flat list
  # ===========================================================================

  describe "CONFIG GET * returns all params" do
    test "CONFIG GET * returns flat key-value list with even number of elements", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "*"], store)
      assert is_list(result)
      assert rem(length(result), 2) == 0
    end

    test "CONFIG GET * includes all spec-required read-only parameters", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "*"], store)
      keys = every_other(result, 0)

      assert "maxmemory" in keys
      assert "maxclients" in keys
      assert "tcp-port" in keys
      assert "data-dir" in keys
      # raft-enabled was removed — Raft is always on
      assert "tls-port" in keys
      assert "tls-cert-file" in keys
      assert "tls-key-file" in keys
      assert "require-tls" in keys
    end

    test "CONFIG GET * includes all spec-required read-write parameters", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "*"], store)
      keys = every_other(result, 0)

      assert "maxmemory-policy" in keys
      assert "slowlog-log-slower-than" in keys
      assert "slowlog-max-len" in keys
      assert "hz" in keys
      assert "notify-keyspace-events" in keys
    end

    test "CONFIG GET * includes legacy parameters", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "*"], store)
      keys = every_other(result, 0)

      assert "requirepass" in keys
      assert "bind" in keys
      assert "timeout" in keys
      assert "loglevel" in keys
    end

    test "CONFIG GET * values are all strings", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "*"], store)
      values = every_other(result, 1)

      Enum.each(values, fn val ->
        assert is_binary(val), "Expected string value, got: #{inspect(val)}"
      end)
    end

    test "CONFIG GET * reflects CONFIG SET changes", %{store: store} do
      Server.handle("CONFIG", ["SET", "hz", "42"], store)
      result = Server.handle("CONFIG", ["GET", "*"], store)

      pairs = pair_up(result)
      assert Map.get(pairs, "hz") == "42"
    end

    test "CONFIG GET redacts requirepass while keeping auth value internal", %{store: store} do
      on_exit(fn -> Ferricstore.Config.set("requirepass", "") end)

      assert :ok = Server.handle("CONFIG", ["SET", "requirepass", "super-secret"], store)
      assert ["requirepass", ""] = Server.handle("CONFIG", ["GET", "requirepass"], store)
      assert Ferricstore.Config.get_value("requirepass") == "super-secret"
    end
  end

  # ===========================================================================
  # 5. CONFIG GET nonexistent → empty list
  # ===========================================================================

  describe "CONFIG GET nonexistent parameter" do
    test "CONFIG GET with non-matching exact name returns empty list", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "nonexistent"], store)
      assert result == []
    end

    test "CONFIG GET with non-matching pattern returns empty list", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "zzz-no-match-*"], store)
      assert result == []
    end

    test "CONFIG GET with specific non-existing key returns empty list", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "totally_bogus_key"], store)
      assert result == []
    end
  end

  # ===========================================================================
  # 6. CONFIG REWRITE → writes to disk, file exists after
  # ===========================================================================

  describe "CONFIG REWRITE persists to disk" do
    test "CONFIG REWRITE returns OK", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["REWRITE"], store)
    end

    test "CONFIG REWRITE creates file on disk", %{store: store} do
      Server.handle("CONFIG", ["REWRITE"], store)
      path = Config.config_file_path()
      assert File.exists?(path)
    end

    test "CONFIG REWRITE file contains all parameter keys", %{store: store} do
      Server.handle("CONFIG", ["REWRITE"], store)
      path = Config.config_file_path()
      content = File.read!(path)

      # Check that key config params appear in the file
      assert content =~ "hz"
      assert content =~ "maxmemory"
      assert content =~ "maxclients"
      assert content =~ "bind"
      assert content =~ "tcp-port"
      assert content =~ "data-dir"
    end

    test "CONFIG REWRITE reflects CONFIG SET changes", %{store: store} do
      Server.handle("CONFIG", ["SET", "hz", "77"], store)
      Server.handle("CONFIG", ["REWRITE"], store)
      path = Config.config_file_path()
      content = File.read!(path)

      assert content =~ "hz 77"
    end

    test "CONFIG REWRITE with extra args returns error", %{store: store} do
      assert {:error, _} = Server.handle("CONFIG", ["REWRITE", "extra"], store)
    end

    test "CONFIG REWRITE does not include local-only settings", %{store: store} do
      ConfigLocal.set("log_level", "debug")
      Server.handle("CONFIG", ["REWRITE"], store)
      path = Config.config_file_path()

      if File.exists?(path) do
        content = File.read!(path)
        refute content =~ "log_level"
      end
    end
  end

  # ===========================================================================
  # 7. CONFIG RESETSTAT → resets counters, INFO stats show zero
  # ===========================================================================

  describe "CONFIG RESETSTAT resets counters" do
    test "CONFIG RESETSTAT returns OK", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["RESETSTAT"], store)
    end

    test "CONFIG RESETSTAT resets total_connections counter", %{store: store} do
      Stats.incr_connections()
      Stats.incr_connections()
      assert Stats.total_connections() > 0

      Server.handle("CONFIG", ["RESETSTAT"], store)
      assert Stats.total_connections() == 0
    end

    test "CONFIG RESETSTAT resets total_commands counter", %{store: store} do
      Stats.incr_commands()
      Stats.incr_commands()
      Stats.incr_commands()
      assert Stats.total_commands() > 0

      Server.handle("CONFIG", ["RESETSTAT"], store)
      assert Stats.total_commands() == 0
    end

    test "CONFIG RESETSTAT resets slowlog entries", %{store: store} do
      # Add a slow log entry and wait for it to be processed
      Ferricstore.SlowLog.maybe_log(["SET", "key", "val"], 999_999_999)
      GenServer.call(Ferricstore.SlowLog, :ping)

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          Ferricstore.SlowLog.len() > 0
        end,
        "slowlog entry should be recorded",
        40,
        50
      )

      Server.handle("CONFIG", ["RESETSTAT"], store)
      assert Ferricstore.SlowLog.len() == 0
    end

    test "INFO stats shows zero counters after RESETSTAT", %{store: store} do
      Stats.incr_connections()
      Stats.incr_commands()

      Server.handle("CONFIG", ["RESETSTAT"], store)

      info = Server.handle("INFO", ["stats"], store)
      assert info =~ "total_connections_received:0"
      assert info =~ "total_commands_processed:0"
    end

    test "CONFIG RESETSTAT with extra args returns error", %{store: store} do
      assert {:error, _} = Server.handle("CONFIG", ["RESETSTAT", "extra"], store)
    end
  end

  # ===========================================================================
  # 8. CONFIG SET LOCAL log_level → Logger level changes
  # ===========================================================================

  describe "CONFIG SET LOCAL log_level changes Logger" do
    test "CONFIG SET LOCAL log_level debug sets Logger to debug", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "debug"], store)
      assert Logger.level() == :debug
    end

    test "CONFIG SET LOCAL log_level warning sets Logger to warning", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "warning"], store)
      assert Logger.level() == :warning
    end

    test "CONFIG SET LOCAL log_level error sets Logger to error", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "error"], store)
      assert Logger.level() == :error
    end

    test "CONFIG SET LOCAL log_level info sets Logger to info", %{store: store} do
      assert :ok = Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "info"], store)
      assert Logger.level() == :info
    end

    test "CONFIG GET LOCAL log_level returns set value", %{store: store} do
      Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "debug"], store)
      result = Server.handle("CONFIG", ["GET", "LOCAL", "log_level"], store)
      assert result == ["log_level", "debug"]
    end

    test "CONFIG SET LOCAL log_level with invalid value returns error", %{store: store} do
      result = Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "invalid_level"], store)
      assert {:error, msg} = result
      assert msg =~ "Invalid"
    end

    test "CONFIG SET LOCAL with unknown param returns error", %{store: store} do
      result = Server.handle("CONFIG", ["SET", "LOCAL", "unknown_param", "value"], store)
      assert {:error, _} = result
    end

    test "CONFIG GET LOCAL with unknown param returns error", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "LOCAL", "unknown_param"], store)
      assert {:error, _} = result
    end

    test "local settings do not appear in CONFIG GET *", %{store: store} do
      Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "debug"], store)
      result = Server.handle("CONFIG", ["GET", "*"], store)
      keys = every_other(result, 0)
      refute "log_level" in keys
    end
  end

  # ===========================================================================
  # 9. INFO reflects config — INFO server section shows port, data_dir, etc.
  # ===========================================================================

  describe "INFO server section reflects config" do
    test "INFO server section contains tcp_port matching configured port", %{store: store} do
      info = Server.handle("INFO", ["server"], store)
      port = Application.get_env(:ferricstore, :port, 6379)
      assert info =~ "tcp_port:#{port}"
    end

    test "INFO server section contains hz", %{store: store} do
      info = Server.handle("INFO", ["server"], store)
      assert info =~ "hz:"
    end

    test "INFO server section contains redis_version", %{store: store} do
      info = Server.handle("INFO", ["server"], store)
      assert info =~ "redis_version:"
    end

    test "INFO server section contains ferricstore_version", %{store: store} do
      info = Server.handle("INFO", ["server"], store)
      assert info =~ "ferricstore_version:"
    end

    test "INFO server section contains uptime", %{store: store} do
      info = Server.handle("INFO", ["server"], store)
      assert info =~ "uptime_in_seconds:"
      assert info =~ "uptime_in_days:"
    end

    test "INFO server section contains run_id", %{store: store} do
      info = Server.handle("INFO", ["server"], store)
      assert info =~ "run_id:"
    end

    test "INFO stats section contains connection and command counters", %{store: store} do
      info = Server.handle("INFO", ["stats"], store)
      assert info =~ "total_connections_received:"
      assert info =~ "total_commands_processed:"
    end

    test "INFO memory section contains used_memory", %{store: store} do
      info = Server.handle("INFO", ["memory"], store)
      assert info =~ "used_memory:"
      assert info =~ "used_memory_human:"
    end

    test "INFO clients section contains maxclients", %{store: store} do
      info = Server.handle("INFO", ["clients"], store)
      assert info =~ "maxclients:"
    end

    test "INFO all includes server section", %{store: store} do
      info = Server.handle("INFO", ["all"], store)
      assert info =~ "# Server"
      assert info =~ "tcp_port:"
    end

    test "INFO with no args includes all sections", %{store: store} do
      info = Server.handle("INFO", [], store)
      assert info =~ "# Server"
      assert info =~ "# Clients"
      assert info =~ "# Memory"
      assert info =~ "# Stats"
      assert info =~ "# Keyspace"
    end

    test "INFO with unknown section returns empty string", %{store: store} do
      info = Server.handle("INFO", ["nonexistent_section"], store)
      assert info == ""
    end
  end

  # ===========================================================================
  # Additional: CONFIG SET read-write → CONFIG GET round-trip
  # ===========================================================================

  describe "CONFIG SET then GET round-trip consistency" do
    test "all valid maxmemory-policy values round-trip correctly", %{store: store} do
      policies = ["volatile-lru", "allkeys-lru", "volatile-ttl", "noeviction"]

      Enum.each(policies, fn policy ->
        assert :ok = Server.handle("CONFIG", ["SET", "maxmemory-policy", policy], store)

        assert ["maxmemory-policy", ^policy] =
                 Server.handle("CONFIG", ["GET", "maxmemory-policy"], store)
      end)
    end

    test "slowlog-log-slower-than round-trips through SET/GET", %{store: store} do
      values = ["0", "1", "100", "10000", "999999999", "-1"]

      Enum.each(values, fn val ->
        assert :ok = Server.handle("CONFIG", ["SET", "slowlog-log-slower-than", val], store)

        assert ["slowlog-log-slower-than", ^val] =
                 Server.handle("CONFIG", ["GET", "slowlog-log-slower-than"], store)
      end)
    end

    test "slowlog-max-len round-trips through SET/GET", %{store: store} do
      values = ["0", "1", "128", "256", "100000"]

      Enum.each(values, fn val ->
        assert :ok = Server.handle("CONFIG", ["SET", "slowlog-max-len", val], store)

        assert ["slowlog-max-len", ^val] =
                 Server.handle("CONFIG", ["GET", "slowlog-max-len"], store)
      end)
    end

    test "hz round-trips through SET/GET at boundaries", %{store: store} do
      values = ["1", "10", "100", "500"]

      Enum.each(values, fn val ->
        assert :ok = Server.handle("CONFIG", ["SET", "hz", val], store)
        assert ["hz", ^val] = Server.handle("CONFIG", ["GET", "hz"], store)
      end)
    end
  end

  # ===========================================================================
  # Additional: CONFIG SET telemetry emission
  # ===========================================================================

  describe "CONFIG SET emits telemetry" do
    test "CONFIG SET emits [:ferricstore, :config, :changed] event", %{store: store} do
      ref = make_ref()
      test_pid = self()
      handler_id = "config-values-test-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :config, :changed],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:config_changed, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      Server.handle("CONFIG", ["SET", "hz", "42"], store)

      assert_receive {:config_changed, metadata}, 1_000
      assert metadata.param == "hz"
      assert metadata.value == "42"
      assert is_binary(metadata.old_value)
    end

    test "CONFIG SET telemetry redacts sensitive values", %{store: store} do
      on_exit(fn -> Ferricstore.Config.set("requirepass", "") end)

      ref = make_ref()
      test_pid = self()
      handler_id = "config-values-sensitive-test-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :config, :changed],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:config_changed, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      Server.handle("CONFIG", ["SET", "requirepass", "top-secret"], store)

      assert_receive {:config_changed, metadata}, 1_000
      assert metadata.param == "requirepass"
      assert metadata.value == "[redacted]"
      refute inspect(metadata) =~ "top-secret"
    end
  end

  # ===========================================================================
  # Additional: CONFIG pattern matching with glob
  # ===========================================================================

  describe "CONFIG GET pattern matching" do
    test "CONFIG GET max* matches maxmemory, maxmemory-policy, maxclients", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "max*"], store)
      keys = every_other(result, 0)

      assert "maxmemory" in keys
      assert "maxmemory-policy" in keys
      assert "maxclients" in keys
      refute "hz" in keys
    end

    test "CONFIG GET slowlog-* matches both slowlog parameters", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "slowlog-*"], store)
      keys = every_other(result, 0)

      assert "slowlog-log-slower-than" in keys
      assert "slowlog-max-len" in keys
    end

    test "CONFIG GET tls-* matches TLS parameters", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "tls-*"], store)
      keys = every_other(result, 0)

      assert "tls-port" in keys
      assert "tls-cert-file" in keys
      assert "tls-key-file" in keys
    end

    test "CONFIG GET h? matches hz", %{store: store} do
      result = Server.handle("CONFIG", ["GET", "h?"], store)
      keys = every_other(result, 0)
      assert "hz" in keys
    end
  end

  # ===========================================================================
  # Additional: CONFIG unknown subcommand
  # ===========================================================================

  describe "CONFIG unknown subcommand" do
    test "unknown CONFIG subcommand returns error", %{store: store} do
      result = Server.handle("CONFIG", ["BADSUBCMD"], store)
      assert {:error, msg} = result
      assert msg =~ "unknown subcommand"
    end

    test "CONFIG with no args returns error", %{store: store} do
      result = Server.handle("CONFIG", [], store)
      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  # Extract every other element from a flat list starting at the given offset.
  defp every_other(list, offset) do
    list
    |> Enum.drop(offset)
    |> Enum.take_every(2)
  end

  # Convert a flat [key, val, key, val, ...] list into a map.
  defp pair_up(list) do
    list
    |> Enum.chunk_every(2)
    |> Enum.reduce(%{}, fn
      [k, v], acc -> Map.put(acc, k, v)
      _, acc -> acc
    end)
  end
end
