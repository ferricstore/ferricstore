defmodule Ferricstore.GlobMatcherRedisTest do
  @moduledoc """
  Character classes match as Redis's `stringmatchlen` does (checked against
  Redis 8 with KEYS): a range matches whichever end is larger, only `^`
  negates, a `-` before the closing `]` makes a range ending at `]`, and a
  class with no closing `]` runs to the end of the pattern.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.GlobMatcher

  test "a range written high to low matches the same bytes as low to high" do
    for subject <- ["a", "b", "y", "z"], do: assert(GlobMatcher.match?(subject, "[z-a]"))
    assert GlobMatcher.match?("b", "[c-a]")
    refute GlobMatcher.match?("-", "[z-a]")
  end

  test "only a caret negates a class" do
    assert GlobMatcher.match?("!", "[!a]")
    assert GlobMatcher.match?("a", "[!a]")
    refute GlobMatcher.match?("b", "[!a]")
    assert GlobMatcher.match?("b", "[^a]")
    refute GlobMatcher.match?("a", "[^a]")
  end

  test "a dash before the closing bracket ranges to the bracket" do
    assert GlobMatcher.match?("a", "[a-]")
    assert GlobMatcher.match?("_", "[a-]")
    refute GlobMatcher.match?("-", "[a-]")
    assert GlobMatcher.match?("-", "[-a]")
  end

  test "a class with no closing bracket runs to the end of the pattern" do
    assert GlobMatcher.match?("b", "[b")
    assert GlobMatcher.match?("c", "[bc")
    refute GlobMatcher.match?("[bc", "[bc")
    assert GlobMatcher.match?("c", "[^a")
    refute GlobMatcher.match?("a", "[^a")
    assert GlobMatcher.match?("xb", "x[b")
  end
end
