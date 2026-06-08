defmodule Ferricstore.Flow.ScoreBoundTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.ScoreBound

  test "parse supports infinities" do
    assert ScoreBound.parse("-inf") == :neg_inf
    assert ScoreBound.parse("+inf") == :pos_inf
  end

  test "parse supports inclusive and exclusive float bounds" do
    assert ScoreBound.parse("42") == {:inclusive, 42.0}
    assert ScoreBound.parse("-1.5") == {:inclusive, -1.5}
    assert ScoreBound.parse("(3.25") == {:exclusive, 3.25}
  end

  test "parse rejects non-float bounds" do
    assert ScoreBound.parse("bad") == {:error, "ERR min or max is not a float"}
    assert ScoreBound.parse("(bad") == {:error, "ERR min or max is not a float"}
  end
end
