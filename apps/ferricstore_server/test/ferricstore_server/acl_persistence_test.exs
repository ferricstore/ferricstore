Code.require_file("acl_persistence_test/sections/part_01.exs", __DIR__)
Code.require_file("acl_persistence_test/sections/part_02.exs", __DIR__)
defmodule FerricstoreServer.AclPersistenceTest do
  @moduledoc """
  Tests for ACL file persistence: SAVE, LOAD, auto-load on startup,
  auto-save on mutations, and comprehensive edge cases.

  These tests use temporary directories to avoid interfering with the
  real data directory. Each test gets a fresh directory and a clean ACL
  state via Acl.reset!/0.
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl

  setup do
    Acl.reset!()
    tmp_dir = Path.join(System.tmp_dir!(), "acl_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp_dir)

    on_exit(fn ->
      Acl.reset!()
      File.rm_rf!(tmp_dir)
    end)

    %{tmp_dir: tmp_dir}
  end

  use FerricstoreServer.AclPersistenceTest.Sections.Part01

  use FerricstoreServer.AclPersistenceTest.Sections.Part02

  defp eventually(fun, msg, attempts \\ 100) do
    if fun.() do
      :ok
    else
      if attempts > 0 do
        Process.sleep(50)
        eventually(fun, msg, attempts - 1)
      else
        flunk("Timed out: #{msg}")
      end
    end
  end

  # Helper to extract key glob strings from compiled patterns
  defp key_globs(patterns) when is_list(patterns) do
    Enum.map(patterns, fn {glob, _mode, _regex} -> glob end)
  end

  defp key_globs(:all), do: :all

  # ---------------------------------------------------------------------------
  # ACL SAVE -- basic functionality
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # ACL SAVE -- overwrite
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # ACL LOAD -- basic functionality
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # Round-trip: SAVE -> reset -> LOAD -> verify
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # ACL LOAD -- validation and error handling
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # Edge cases -- file format
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # Edge cases -- security
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # Permission enforcement after LOAD
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # Auto-save (debounced)
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # Concurrent SAVE/LOAD
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # File corruption recovery
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # Large scale
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # acl_file_path/1
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # Read-only filesystem handling
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # File format: command serialization
  # ---------------------------------------------------------------------------

end
