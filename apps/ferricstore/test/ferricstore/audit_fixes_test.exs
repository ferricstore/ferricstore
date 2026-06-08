defmodule Ferricstore.AuditFixesTest do
  @moduledoc """
  Tests for all fixes from elixir-guide-audit.md and elixir-memory-audit.md.

  Each test group maps to a specific audit issue ID (C1-C4, H1-H6, M1-M6, L1-L4
  from the guide audit; C1, H3, M5-M6, L4-L5, L7 from the memory audit).
  """

  use ExUnit.Case, async: true
  # All tests have been split into:
  #   audit_fixes_critical_test.exs
  #   audit_fixes_high_test.exs
  #   audit_fixes_medium_test.exs
  #   audit_fixes_low_test.exs
  #   audit_fixes_memory_test.exs
end
