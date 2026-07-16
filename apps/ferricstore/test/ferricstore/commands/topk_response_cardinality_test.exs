defmodule Ferricstore.Commands.TopKResponseCardinalityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.TopK

  test "LIST WITHCOUNT rejects mismatched or malformed count responses" do
    assert {:ok, ["first", 3, "second", 2]} ==
             TopK.combine_items_counts(["first", "second"], [3, 2])

    for counts <- [[3], [3, 2, 1], :invalid] do
      assert {:error, "ERR TOPK: invalid count response"} ==
               TopK.combine_items_counts(["first", "second"], counts)
    end
  end
end
