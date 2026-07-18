defmodule Ferricstore.Commands.TopKResponseCardinalityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.TopK

  test "LIST WITHCOUNT rejects malformed native pair responses" do
    assert {:ok, ["first", 3, "second", 2]} ==
             TopK.normalize_list_with_count_response(["first", 3, "second", 2])

    for response <- [["first", 3, "second"], ["first", -1], [3, "first"], :invalid] do
      assert {:error, "ERR TOPK: invalid count response"} ==
               TopK.normalize_list_with_count_response(response)
    end
  end
end
