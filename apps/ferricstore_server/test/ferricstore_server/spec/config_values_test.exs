Code.require_file("config_values_test/sections/read_only_param_maxmemory.exs", __DIR__)
Code.require_file("config_values_test/sections/config_set_emits_telemetry.exs", __DIR__)

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
  @moduletag :global_state

  alias Ferricstore.Commands.Server
  alias Ferricstore.Config
  alias Ferricstore.Config.Local, as: ConfigLocal
  alias Ferricstore.Stats
  alias Ferricstore.Test.MockStore

  # Reset config to defaults after each test to avoid cross-test contamination.
  setup do
    orig_eviction = Application.get_env(:ferricstore, :eviction_policy)
    orig_slowlog_us = app_env_snapshot(:slowlog_log_slower_than_us)
    orig_slowlog_us_pt = persistent_term_snapshot(:ferricstore_slowlog_threshold)
    orig_slowlog_max = app_env_snapshot(:slowlog_max_len)
    orig_slowlog_max_pt = persistent_term_snapshot(:ferricstore_slowlog_max_len)
    orig_log_level = Logger.level()

    ConfigLocal.reset_all()
    Ferricstore.SlowLog.reset()
    Ferricstore.SlowLog.set_threshold(10_000)
    Ferricstore.SlowLog.set_max_len(128)

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

      restore_app_env(:slowlog_log_slower_than_us, orig_slowlog_us)
      restore_persistent_term(:ferricstore_slowlog_threshold, orig_slowlog_us_pt)
      restore_app_env(:slowlog_max_len, orig_slowlog_max)
      restore_persistent_term(:ferricstore_slowlog_max_len, orig_slowlog_max_pt)
      Ferricstore.SlowLog.reset()

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

  # ===========================================================================
  # 2. Read-write parameters — CONFIG SET works with valid/invalid/boundary
  # ===========================================================================

  # ===========================================================================
  # 3. CONFIG SET invalid param → ERR Unsupported CONFIG parameter
  # ===========================================================================

  # ===========================================================================
  # 4. CONFIG GET * → returns all params as flat list
  # ===========================================================================

  # ===========================================================================
  # 5. CONFIG GET nonexistent → empty list
  # ===========================================================================

  # ===========================================================================
  # 6. CONFIG REWRITE → writes to disk, file exists after
  # ===========================================================================

  # ===========================================================================
  # 7. CONFIG RESETSTAT → resets counters, INFO stats show zero
  # ===========================================================================

  # ===========================================================================
  # 8. CONFIG SET LOCAL log_level → Logger level changes
  # ===========================================================================

  # ===========================================================================
  # 9. INFO reflects config — INFO server section shows port, data_dir, etc.
  # ===========================================================================

  # ===========================================================================
  # Additional: CONFIG SET read-write → CONFIG GET round-trip
  # ===========================================================================

  # ===========================================================================
  # Additional: CONFIG SET telemetry emission
  # ===========================================================================

  # ===========================================================================
  # Additional: CONFIG pattern matching with glob
  # ===========================================================================

  # ===========================================================================
  # Additional: CONFIG unknown subcommand
  # ===========================================================================

  # ===========================================================================
  # Helpers
  # ===========================================================================

  defp app_env_snapshot(key) do
    case Application.fetch_env(:ferricstore, key) do
      {:ok, value} -> {:set, value}
      :error -> :unset
    end
  end

  defp restore_app_env(key, {:set, value}), do: Application.put_env(:ferricstore, key, value)
  defp restore_app_env(key, :unset), do: Application.delete_env(:ferricstore, key)

  defp persistent_term_snapshot(key) do
    {:set, :persistent_term.get(key)}
  rescue
    ArgumentError -> :unset
  end

  defp restore_persistent_term(key, {:set, value}), do: :persistent_term.put(key, value)
  defp restore_persistent_term(key, :unset), do: :persistent_term.erase(key)

  # Extract every other element from a flat list starting at the given offset.

  use FerricstoreServer.Spec.ConfigValuesTest.Sections.ReadOnlyParamMaxmemory

  use FerricstoreServer.Spec.ConfigValuesTest.Sections.ConfigSetEmitsTelemetry

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
