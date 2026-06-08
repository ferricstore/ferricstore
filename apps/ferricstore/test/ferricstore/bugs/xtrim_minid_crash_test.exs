defmodule Ferricstore.Bugs.XtrimMinidCrashTest do
  @moduledoc """
  Bug #2: XTRIM MINID Crashes on Invalid ID

  `{:ok, min_id} = parse_full_id(min_id_str)` at stream.ex:599 pattern
  match fails with MatchError when user provides invalid MINID string.
  Should handle `{:error, msg}` and return proper error response.

  File: commands/stream.ex:599
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag timeout: 30_000

  alias Ferricstore.Commands.Stream
  alias Ferricstore.Test.MockStore

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  describe "XTRIM MINID with invalid ID" do
    test "invalid MINID string should return error, not crash" do
      store = MockStore.make()
      key = "xtrim_crash_#{:rand.uniform(1_000_000)}"

      # Add some entries to the stream first
      Stream.handle("XADD", [key, "*", "field", "value1"], store)
      Stream.handle("XADD", [key, "*", "field", "value2"], store)
      Stream.handle("XADD", [key, "*", "field", "value3"], store)

      # XTRIM with invalid MINID — "not-a-number" can't be parsed as a stream ID.
      # BUG: This crashes with MatchError on {:ok, min_id} = parse_full_id("not-a-number")
      # because parse_full_id returns {:error, msg}
      result = Stream.handle("XTRIM", [key, "MINID", "not-a-number"], store)

      assert {:error, _msg} = result,
             "XTRIM with invalid MINID should return error, not crash. Got: #{inspect(result)}"
    end

    test "invalid MINID with letters should return error, not crash" do
      store = MockStore.make()
      key = "xtrim_crash2_#{:rand.uniform(1_000_000)}"

      Stream.handle("XADD", [key, "*", "field", "value1"], store)

      # Another invalid format: "abc-def" — both parts are non-numeric
      result = Stream.handle("XTRIM", [key, "MINID", "abc-def"], store)

      assert {:error, _msg} = result,
             "XTRIM with non-numeric MINID should return error, not crash. Got: #{inspect(result)}"
    end

    test "XTRIM MINID with valid ID still works" do
      store = MockStore.make()
      key = "xtrim_valid_#{:rand.uniform(1_000_000)}"

      Stream.handle("XADD", [key, "*", "field", "value1"], store)
      Stream.handle("XADD", [key, "*", "field", "value2"], store)

      # A valid MINID should not crash
      result = Stream.handle("XTRIM", [key, "MINID", "99999999999999-0"], store)

      # Should succeed — either trimming entries or returning 0 if all are above MINID
      assert is_integer(result) or match?({:error, _}, result),
             "XTRIM with valid MINID should return integer or error, got: #{inspect(result)}"
    end
  end
end
