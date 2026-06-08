defmodule Ferricstore.Bugs.ZrandmemberScoreFormatTest do
  @moduledoc """
  Bug #3: ZRANDMEMBER WITHSCORES Returns Unformatted Scores

  ZRANDMEMBER WITHSCORES returns raw score strings instead of using
  `format_score_str()` like all other WITHSCORES commands (ZRANGE,
  ZPOPMIN, ZSCAN, etc.). This causes score format inconsistency.

  File: commands/sorted_set.ex:881,895
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag timeout: 30_000

  alias Ferricstore.Commands.SortedSet
  alias Ferricstore.Test.MockStore

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  describe "ZRANDMEMBER WITHSCORES score format consistency" do
    test "ZRANDMEMBER WITHSCORES should format scores like ZRANGE WITHSCORES" do
      store = MockStore.make()
      key = "zrm_fmt_#{:rand.uniform(1_000_000)}"

      # ZADD with score 1.1 — not exactly representable in binary float,
      # so Float.to_string and format_score_str produce different strings
      SortedSet.handle("ZADD", [key, "1.1", "alice"], store)

      # Get the formatted score from ZRANGE WITHSCORES
      zrange_result = SortedSet.handle("ZRANGE", [key, "0", "-1", "WITHSCORES"], store)

      # ZRANGE WITHSCORES returns ["alice", formatted_score]
      [_member, zrange_score] = zrange_result

      # Get the score from ZRANDMEMBER WITHSCORES (only 1 member, so it must be alice)
      zrand_result = SortedSet.handle("ZRANDMEMBER", [key, "1", "WITHSCORES"], store)

      # ZRANDMEMBER WITHSCORES returns ["alice", raw_or_formatted_score]
      [_member2, zrand_score] = zrand_result

      # Both should use the same format — format_score_str() output
      # BUG: ZRANDMEMBER returns the raw stored score string, not the formatted one.
      # ZRANGE uses format_score_str() but ZRANDMEMBER doesn't.
      assert zrange_score == zrand_score,
             "Score format mismatch: ZRANGE WITHSCORES returned #{inspect(zrange_score)}, " <>
               "but ZRANDMEMBER WITHSCORES returned #{inspect(zrand_score)}"
    end

    test "ZRANDMEMBER WITHSCORES should format decimal scores consistently" do
      store = MockStore.make()
      key = "zrm_dec_#{:rand.uniform(1_000_000)}"

      # ZADD with score 0.7 — not exactly representable in binary float,
      # Float.to_string(0.7) = "0.7" but format_score_str returns "0.69999999999999992"
      SortedSet.handle("ZADD", [key, "0.7", "bob"], store)

      zrange_result = SortedSet.handle("ZRANGE", [key, "0", "-1", "WITHSCORES"], store)
      [_m1, zrange_score] = zrange_result

      zrand_result = SortedSet.handle("ZRANDMEMBER", [key, "1", "WITHSCORES"], store)
      [_m2, zrand_score] = zrand_result

      assert zrange_score == zrand_score,
             "Decimal score format mismatch: ZRANGE returned #{inspect(zrange_score)}, " <>
               "ZRANDMEMBER returned #{inspect(zrand_score)}"
    end

    test "ZRANDMEMBER WITHSCORES with negative count should also format scores" do
      store = MockStore.make()
      key = "zrm_neg_#{:rand.uniform(1_000_000)}"

      SortedSet.handle("ZADD", [key, "3.14", "pi"], store)

      zrange_result = SortedSet.handle("ZRANGE", [key, "0", "-1", "WITHSCORES"], store)
      [_m1, zrange_score] = zrange_result

      # Negative count means allow repeats — with 1 member, result is ["pi", score]
      zrand_result = SortedSet.handle("ZRANDMEMBER", [key, "-1", "WITHSCORES"], store)
      [_m2, zrand_score] = zrand_result

      assert zrange_score == zrand_score,
             "Score format mismatch with negative count: ZRANGE returned #{inspect(zrange_score)}, " <>
               "ZRANDMEMBER returned #{inspect(zrand_score)}"
    end
  end
end
